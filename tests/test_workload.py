"""Workload: expressions, the in-memory oracle, the Iceberg read path, canonical forms."""

from __future__ import annotations

import datetime as dt
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from lakehouse import spec
from lakehouse import workload as w
from lakehouse.catalog import ensure_namespace, get_catalog

Q = spec.QUERY_BY_NAME
DAY = dt.timedelta(days=1)
US = dt.timedelta(microseconds=1)
D10 = spec.Q_DAY

# --- helpers ----------------------------------------------------------------------------------

ICEBERG_V1 = Schema(
    NestedField(1, "event_id", LongType(), required=True),
    NestedField(2, "user_id", LongType(), required=True),
    NestedField(3, "event_ts", TimestamptzType(), required=True),
    NestedField(4, "event_name", StringType(), required=True),
    NestedField(5, "channel", StringType(), required=True),
    NestedField(6, "platform", StringType(), required=True),
    NestedField(7, "amount", DoubleType(), required=False),
)
BY_DAY = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="event_ts_day")
)


def make_table(tmp_path, partitioned: bool, name: str = "events"):
    catalog = get_catalog(tmp_path / f"wh_{name}_{partitioned}")
    ensure_namespace(catalog, "lake")
    return catalog.create_table(
        f"lake.{name}",
        schema=ICEBERG_V1,
        partition_spec=BY_DAY if partitioned else PartitionSpec(),
    )


def arrow_events(rows: list[tuple]) -> pa.Table:
    """rows: (event_id, user_id, event_ts, event_name, amount|None)."""
    cols = list(zip(*rows, strict=True))
    return pa.table(
        {
            "event_id": list(cols[0]),
            "user_id": list(cols[1]),
            "event_ts": list(cols[2]),
            "event_name": list(cols[3]),
            "channel": ["organic"] * len(rows),
            "platform": ["web"] * len(rows),
            "amount": list(cols[4]),
        },
        schema=spec.ARROW_SCHEMA_V1,
    )


def random_events(n: int, first_id: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)
    base = int(spec.START.timestamp() * 1_000_000)
    ts = np.sort(rng.integers(0, spec.DAYS * 86_400 * 1_000_000, n)) + base
    names = rng.choice(spec.EVENT_NAMES, n)
    amounts = np.round(rng.random(n) * 200, 2)
    purchase = names == "purchase"
    return pa.table(
        {
            "event_id": np.arange(first_id, first_id + n, dtype=np.int64),
            "user_id": rng.integers(100, 180, n),  # includes POINT_USER = 137
            "event_ts": pa.array(ts, type=pa.timestamp("us", tz="UTC")),
            "event_name": names,
            "channel": rng.choice(spec.CHANNELS, n),
            "platform": rng.choice(spec.PLATFORMS, n),
            "amount": pa.array(amounts, mask=~purchase, type=pa.float64()),
        },
        schema=spec.ARROW_SCHEMA_V1,
    )


# Hand-built table. Ids and the answers below are worked out by hand from this list.
HAND = [
    (0, 137, D10 - US, "page_view", None),  # one microsecond before day 10
    (1, 137, D10, "purchase", 19.99),  # exactly the lower bound of Q2/Q3/Q5
    (2, 5, D10 + DAY / 2, "purchase", 0.29),
    (3, 137, D10 + DAY - US, "search", None),  # last microsecond of day 10
    (4, 137, D10 + DAY, "add_to_cart", None),  # exactly the Q2 upper bound (excluded)
    (5, 137, D10 + 7 * DAY - US, "purchase", 1.1),  # last microsecond inside Q3/Q5
    (6, 137, D10 + 7 * DAY, "purchase", 0.57),  # exactly the Q3/Q5 upper bound (excluded)
    (7, 9, spec.START, "signup", None),
    (8, 137, spec.START + DAY, "checkout", None),
]
HAND_ANSWERS = {
    "Q1_point_user": [0, 1, 3, 4, 5, 6, 8],
    "Q2_one_day": [1, 2, 3],
    "Q3_seven_days": [1, 2, 3, 4, 5],
    "Q4_purchases": [1, 2, 5, 6],
    "Q5_user_week": [1, 3, 4, 5],
}
# name -> (events, summed cents); 19.99 + 0.29 + 1.1 + 0.57 = 21.95 dollars
HAND_Q6 = [
    ("add_to_cart", 1, 0),
    ("checkout", 1, 0),
    ("page_view", 1, 0),
    ("purchase", 4, 1999 + 29 + 110 + 57),
    ("search", 1, 0),
    ("signup", 1, 0),
]


@pytest.fixture(scope="module")
def hand() -> pa.Table:
    return arrow_events(HAND)


# --- expressions ------------------------------------------------------------------------------


def test_to_iceberg_expr_shapes():
    assert w.to_iceberg_expr(()) == AlwaysTrue()
    assert w.to_iceberg_expr(Q["Q1_point_user"].preds) == EqualTo("user_id", 137)
    q5 = w.to_iceberg_expr(Q["Q5_user_week"].preds)
    assert q5 == And(
        And(EqualTo("user_id", 137), GreaterThanOrEqual("event_ts", D10)),
        LessThan("event_ts", D10 + 7 * DAY),
    )


def test_naive_datetime_is_rejected():
    naive = spec.Pred("event_ts", ">=", dt.datetime(2026, 1, 11))
    with pytest.raises(ValueError, match="naive"):
        w.to_iceberg_expr((naive,))
    with pytest.raises(ValueError, match="naive"):
        w.to_arrow_expr((naive,))


def test_arrow_expr_matches_oracle(hand):
    assert w.to_arrow_expr(()).equals(pc.scalar(True))
    assert hand.filter(w.to_arrow_expr(())).num_rows == hand.num_rows
    for q in spec.QUERIES:
        if q.kind != "rows":
            continue
        via_expr = w.canonicalize(hand.filter(w.to_arrow_expr(q.preds)), q)
        assert via_expr.equals(w.apply_query(hand, q)), q.name


# --- the oracle by hand -----------------------------------------------------------------------


@pytest.mark.parametrize("name", list(HAND_ANSWERS))
def test_oracle_rows_match_hand_answers(hand, name):
    out = w.apply_query(hand, Q[name])
    assert out.column("event_id").to_pylist() == HAND_ANSWERS[name]
    assert out.schema.equals(spec.CANONICAL_SCHEMA, check_metadata=True)


def test_oracle_q6_exact_integer_cents(hand):
    out = w.apply_query(hand, Q["Q6_full_agg"])
    assert out.schema.equals(spec.AGG_SCHEMA, check_metadata=True)
    assert [tuple(r.values()) for r in out.to_pylist()] == HAND_Q6
    # 0.57 * 100 = 56.99999999999999 and 1.1 * 100 = 110.00000000000001: rounding per row
    # is what makes the total exactly 2195.
    assert out.column("amount_cents").to_pylist()[3] == 2195


def test_q6_group_without_amounts_is_zero_not_null():
    t = arrow_events([(0, 1, D10, "search", None), (1, 2, D10, "search", None)])
    out = w.apply_query(t, Q["Q6_full_agg"])
    assert out.to_pylist() == [{"event_name": "search", "events": 2, "amount_cents": 0}]


def test_q6_on_empty_input_is_empty_with_agg_schema(hand):
    out = w.apply_query(hand.slice(0, 0), Q["Q6_full_agg"])
    assert out.num_rows == 0
    assert out.schema.equals(spec.AGG_SCHEMA, check_metadata=True)


def test_q6_rounds_half_to_even_per_row():
    t = arrow_events([(0, 1, D10, "purchase", 0.125), (1, 1, D10, "purchase", 0.375)])
    # 12.5 -> 12 and 37.5 -> 38 under round-half-to-even
    out = w.apply_query(t, Q["Q6_full_agg"])
    assert out.column("amount_cents").to_pylist() == [50]


def test_oracle_empty_result_keeps_schema(hand):
    q = spec.Query("none", "", (spec.Pred("user_id", "==", -1),))
    out = w.apply_query(hand, q)
    assert out.num_rows == 0
    assert out.schema.equals(spec.CANONICAL_SCHEMA, check_metadata=True)


# --- through Iceberg --------------------------------------------------------------------------


@pytest.fixture(params=[False, True], ids=["unpartitioned", "by_day"])
def partitioned(request):
    return request.param


def test_timestamp_boundaries_through_iceberg(tmp_path, hand, partitioned):
    table = make_table(tmp_path, partitioned)
    table.append(hand)
    for name, ids in HAND_ANSWERS.items():
        out = w.run_iceberg(table, Q[name])
        assert out.column("event_id").to_pylist() == ids, name
    # Lower bound included, upper bound excluded, 1 microsecond either side.
    q2 = w.run_iceberg(table, Q["Q2_one_day"])
    tss = q2.column("event_ts").to_pylist()
    assert D10 in tss
    assert D10 + DAY - US in tss
    assert D10 + DAY not in tss
    assert D10 - US not in tss


def test_iceberg_equals_oracle_on_random_table(tmp_path, partitioned):
    table = make_table(tmp_path, partitioned)
    parts = [random_events(1000, i * 1000, seed=i) for i in range(3)]
    for p in parts:
        table.append(p)
    whole = pa.concat_tables(parts)
    for q in spec.QUERIES:
        got, want = w.run_iceberg(table, q), w.apply_query(whole, q)
        assert got.equals(want), q.name
        assert w.fingerprint(got) == w.fingerprint(want), q.name
    assert w.apply_query(whole, Q["Q1_point_user"]).num_rows > 0
    assert w.apply_query(whole, Q["Q5_user_week"]).num_rows > 0


def test_run_iceberg_at_older_snapshot(tmp_path):
    table = make_table(tmp_path, True)
    first, second = random_events(500, 0, seed=1), random_events(500, 500, seed=2)
    table.append(first)
    snap1 = table.current_snapshot().snapshot_id
    table.append(second)
    for q in spec.QUERIES:
        assert w.run_iceberg(table, q, snapshot_id=snap1).equals(w.apply_query(first, q))
        assert w.run_iceberg(table, q).equals(w.apply_query(pa.concat_tables([first, second]), q))


def test_extra_country_column_still_compares_equal_on_v1(tmp_path, partitioned):
    table = make_table(tmp_path, partitioned)
    old, new = random_events(800, 0, seed=3), random_events(800, 800, seed=4)
    table.append(old)
    with table.update_schema() as update:
        update.add_column("country", StringType())
    countries = pa.array(["IN", "US"] * 400, type=pa.string())
    table.append(new.append_column(spec.ARROW_SCHEMA_V2.field("country"), countries))
    whole = pa.concat_tables([old, new])
    for q in spec.QUERIES:
        assert w.run_iceberg(table, q).equals(w.apply_query(whole, q)), q.name
    # The raw scan really does carry the extra column; canonical form drops it.
    assert "country" in table.scan().to_arrow().column_names
    canon = w.canonicalize(table.scan().to_arrow(), Q["Q1_point_user"])
    assert canon.column_names == [f.name for f in spec.CANONICAL_SCHEMA]


# --- canonical form and fingerprint -----------------------------------------------------------


def test_canonicalize_strips_iceberg_field_metadata(tmp_path):
    table = make_table(tmp_path, False)
    table.append(random_events(300, 0, seed=5))
    raw = table.scan().to_arrow()
    assert not all(f.nullable for f in raw.schema), "premise: Iceberg marks required fields"
    # Parquet readers attach field ids as field metadata; simulate that on top of the real scan.
    tagged = pa.schema(
        [f.with_metadata({"PARQUET:field_id": str(i + 1)}) for i, f in enumerate(raw.schema)]
    )
    canon = w.canonicalize(raw.cast(tagged), Q["Q1_point_user"])
    assert all(f.metadata for f in tagged)
    assert all(not f.metadata for f in canon.schema)
    assert all(f.nullable for f in canon.schema)
    assert canon.schema.equals(spec.CANONICAL_SCHEMA, check_metadata=True)
    assert canon.column("event_id").to_pylist() == sorted(canon.column("event_id").to_pylist())
    assert canon.column("event_id").num_chunks == 1


def test_canonicalize_casts_large_strings_and_other_timestamp_units(hand):
    alt = hand.cast(
        pa.schema(
            [
                hand.schema.field("event_id"),
                hand.schema.field("user_id"),
                pa.field("event_ts", pa.timestamp("ns", tz="UTC"), nullable=False),
                pa.field("event_name", pa.large_string(), nullable=False),
                pa.field("channel", pa.large_string(), nullable=False),
                pa.field("platform", pa.large_string(), nullable=False),
                hand.schema.field("amount"),
            ]
        )
    )
    shuffled = alt.take(pa.array([4, 0, 8, 2, 1, 7, 3, 6, 5]))
    q = Q["Q1_point_user"]
    assert w.canonicalize(shuffled, q).equals(w.canonicalize(hand, q))


def test_fingerprint_is_chunking_invariant(hand):
    q = Q["Q4_purchases"]
    whole = w.apply_query(random_events(5000, 0, seed=6), spec.Query("all", "", ()))
    one = w.fingerprint(whole)
    for size in (1, 7, 999, 100_000):
        chunked = pa.Table.from_batches(whole.to_batches(max_chunksize=size))
        assert w.fingerprint(chunked) == one, size
    sliced = pa.concat_tables([whole.slice(0, 1234), whole.slice(1234)])
    assert w.fingerprint(sliced) == one
    # Sliced arrays have a non-zero offset: same values, same hash.
    tail = w.canonicalize(whole.slice(100), q)
    again = w.canonicalize(pa.concat_tables([whole.slice(100)]), q)
    assert w.fingerprint(tail) == w.fingerprint(again)
    assert len(one) == 64


def test_fingerprint_equal_tables_equal_hash_across_paths(tmp_path):
    table = make_table(tmp_path, True)
    data = random_events(2000, 0, seed=7)
    table.append(data.slice(0, 700))
    table.append(data.slice(700))
    q = spec.Query("all", "", ())
    assert w.fingerprint(w.run_iceberg(table, q)) == w.fingerprint(w.apply_query(data, q))


def test_fingerprint_changes_when_one_value_changes(hand):
    base = w.apply_query(hand, spec.Query("all", "", ()))
    ref = w.fingerprint(base)
    for column, new_value in [
        ("event_id", 99),
        ("user_id", 138),
        ("event_ts", D10 + US),
        ("event_name", "searcH"),
        ("amount", 19.98),
    ]:
        idx = base.column_names.index(column)
        values = base.column(column).to_pylist()
        values[1] = new_value
        changed = base.set_column(
            idx, base.schema.field(column), pa.array(values, base.schema.types[idx])
        )
        assert w.fingerprint(changed) != ref, column
    # null vs a value, and null vs empty string
    amount = base.column("amount").to_pylist()
    amount[0] = 0.0
    zeroed = base.set_column(6, base.schema.field("amount"), pa.array(amount, pa.float64()))
    assert w.fingerprint(zeroed) != ref
    s1 = pa.table({"s": pa.array(["", None])})
    s2 = pa.table({"s": pa.array([None, ""])})
    assert w.fingerprint(s1) != w.fingerprint(s2)
    # row count and column names are part of the hash
    assert w.fingerprint(base.slice(0, 8)) != ref
    assert w.fingerprint(base.rename_columns(["a", *base.column_names[1:]])) != ref


def test_fingerprint_agg_and_empty_tables(hand):
    agg = w.apply_query(hand, Q["Q6_full_agg"])
    assert w.fingerprint(agg) == w.fingerprint(
        pa.Table.from_batches(agg.to_batches(max_chunksize=2))
    )
    empty = w.apply_query(hand.slice(0, 0), Q["Q1_point_user"])
    assert len(w.fingerprint(empty)) == 64
    assert w.fingerprint(empty) != w.fingerprint(agg)


def test_fingerprint_speed_300k_rows():
    n = 300_000
    big = w.canonicalize(random_events(n, 0, seed=8), spec.Query("all", "", ()))
    start = time.perf_counter()
    w.fingerprint(big)
    elapsed = time.perf_counter() - start
    print(f"fingerprint of {n} rows: {elapsed:.3f}s")
    assert elapsed < 10
