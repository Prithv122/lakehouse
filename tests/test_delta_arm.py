"""Delta arm: writes, time travel, queries against an oracle, planning, maintenance."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse import delta_arm as da
from lakehouse import spec

LAYOUTS = ("L0", "L1", "L2", "L2s")
US = dt.timedelta(microseconds=1)
BATCH_DAYS = 5  # each batch covers five consecutive days (ordered arrival)
BATCH_ROWS = 400


# --- builders and oracle ----------------------------------------------------------------------


def make_events(
    n: int,
    first_id: int,
    day0: float,
    days: float,
    seed: int,
    users: int = 300,
    user_lo: int = 1,
) -> pa.Table:
    """`n` rows sorted by event_ts within [day0, day0+days) after spec.START, ids first_id.."""
    rng = np.random.default_rng(seed)
    t0 = int((spec.START + dt.timedelta(days=day0)).timestamp() * 1_000_000)
    ts = np.sort(rng.integers(0, int(days * 86_400 * 1_000_000), n)) + t0
    names = rng.choice(np.array(spec.EVENT_NAMES), n)
    amount = np.round(rng.random(n) * 100, 2)
    no_amount = names != "purchase"
    return pa.table(
        {
            "event_id": pa.array(np.arange(first_id, first_id + n), pa.int64()),
            "user_id": pa.array(rng.integers(user_lo, user_lo + users, n), pa.int64()),
            "event_ts": pa.array(ts.astype("int64"), pa.timestamp("us", tz="UTC")),
            "event_name": pa.array(names, pa.string()),
            "channel": pa.array(rng.choice(np.array(spec.CHANNELS), n), pa.string()),
            "platform": pa.array(rng.choice(np.array(spec.PLATFORMS), n), pa.string()),
            "amount": pa.array(amount, pa.float64(), mask=no_amount),
        },
        schema=spec.ARROW_SCHEMA_V1,
    )


def ordered_batches(count: int = 4) -> list[pa.Table]:
    """Time-sliced appends starting at day 8, so the workload's days 10-16 fall in the first two."""
    return [
        make_events(BATCH_ROWS, i * BATCH_ROWS, 8 + i * BATCH_DAYS, BATCH_DAYS, seed=100 + i)
        for i in range(count)
    ]


def boundary_batch() -> pa.Table:
    """Rows of user 137 exactly around each workload bound, plus fillers."""
    d10, d11, d17 = (spec.Q_DAY + dt.timedelta(days=k) for k in (0, 1, 7))
    stamps = [d10 - US, d10, d10 + US, d11 - US, d11, d11 + US, d17 - US, d17, d17 + US]
    t = make_events(len(stamps), 0, 10, 8, seed=7)
    ts = pa.array(stamps, pa.timestamp("us", tz="UTC"))
    t = t.set_column(
        t.schema.get_field_index("event_ts"), spec.ARROW_SCHEMA_V1.field("event_ts"), ts
    )
    return t.set_column(1, spec.ARROW_SCHEMA_V1.field("user_id"), pa.array([137] * len(stamps)))


def canonical(t: pa.Table) -> pa.Table:
    return t.select(spec.CANONICAL_SCHEMA.names).cast(spec.CANONICAL_SCHEMA).sort_by("event_id")


def oracle(batches: list[pa.Table], query: spec.Query) -> pa.Table:
    """Hand-filtered pyarrow expected result (independent of delta_arm's expression builder)."""
    t = pa.concat_tables(batches)
    mask = pa.array([True] * t.num_rows)
    for p in query.preds:
        col = t[p.column]
        if isinstance(p.value, dt.datetime):
            val = pa.scalar(p.value, type=col.type)
        else:
            val = pa.scalar(p.value)
        step = {"==": pc.equal, ">=": pc.greater_equal, "<": pc.less}[p.op](col, val)
        mask = pc.and_(mask, step)
    t = t.filter(mask)
    if query.kind == "rows":
        return canonical(t)
    sums: dict[str, int] = {}
    counts: dict[str, int] = {}
    for name, amt in zip(t["event_name"].to_pylist(), t["amount"].to_pylist(), strict=True):
        counts[name] = counts.get(name, 0) + 1
        sums[name] = sums.get(name, 0) + (0 if amt is None else round(amt * 100))
    names = sorted(counts)
    return pa.table(
        {
            "event_name": names,
            "events": [counts[k] for k in names],
            "amount_cents": [sums[k] for k in names],
        },
        schema=spec.AGG_SCHEMA,
    )


def same(a: pa.Table, b: pa.Table) -> bool:
    return a.schema.equals(b.schema) and a.equals(b)


# --- fixtures ---------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> dict[str, tuple[Path, list[int]]]:
    batches = ordered_batches()
    out = {}
    for layout in LAYOUTS:
        path = tmp_path_factory.mktemp(f"delta_{layout}") / "events"
        out[layout] = (path, da.write_delta(path, batches, layout))
    return out


@pytest.fixture(scope="module")
def batches() -> list[pa.Table]:
    return ordered_batches()


# --- writes, versions, time travel ------------------------------------------------------------


@pytest.mark.parametrize("layout", LAYOUTS)
def test_write_returns_versions_and_layout(built, layout: str) -> None:
    path, versions = built[layout]
    assert versions == [0, 1, 2, 3]
    assert da.delta_version(path) == 3
    files = da.delta_files(path)
    partitioned = spec.LAYOUTS[layout].partition_by_day
    assert ("partition.event_date" in files.column_names) is partitioned
    if partitioned:
        assert files.num_rows > len(versions)  # several day partitions per batch
    else:
        assert files.num_rows == len(versions)  # one file per append
    assert pc.sum(files["num_records"]).as_py() == BATCH_ROWS * 4


@pytest.mark.parametrize("layout", ["L0", "L2"])
def test_time_travel_returns_the_first_k_batches(built, batches, layout: str) -> None:
    path, versions = built[layout]
    for k, version in enumerate(versions, start=1):
        got = da.read_delta(path, version=version)
        assert same(got, canonical(pa.concat_tables(batches[:k])))
    assert same(da.read_delta(path), canonical(pa.concat_tables(batches)))


def test_read_delta_drops_helper_columns(built) -> None:
    path, _ = built["L1"]
    got = da.read_delta(path)
    assert got.schema.equals(spec.CANONICAL_SCHEMA)
    assert "event_date" not in got.column_names


def test_event_date_is_the_utc_date_of_event_ts(built) -> None:
    path, _ = built["L1"]
    t = DeltaTable(str(path)).to_pyarrow_dataset().to_table()
    expected = pc.cast(pc.floor_temporal(t["event_ts"], unit="day"), pa.date32())
    assert t["event_date"].equals(expected)


def test_l2_batches_are_sorted_by_the_key_before_writing(built) -> None:
    path, _ = built["L2"]
    for fragment in DeltaTable(str(path)).to_pyarrow_dataset().get_fragments():
        t = fragment.to_table(columns=["user_id", "event_ts", "event_id"])
        keys = list(
            zip(*(t[c].to_pylist() for c in ("user_id", "event_ts", "event_id")), strict=True)
        )
        assert keys == sorted(keys)


def test_l2s_target_size_splits_a_partition_into_several_files(tmp_path: Path) -> None:
    # One day of 120k rows: a single file at the default target, several under the 1 MiB target.
    big = make_events(120_000, 0, 0, 1, seed=3, users=2000)
    default = tmp_path / "l2"
    small = tmp_path / "l2s"
    da.write_delta(default, [big], "L2")
    da.write_delta(small, [big], "L2s")
    assert da.delta_files(default).num_rows == 1
    files = da.delta_files(small)
    assert files.num_rows > 1
    # Sorted by user within the batch, so the files' user ranges barely overlap.
    q = spec.QUERY_BY_NAME["Q1_point_user"]
    assert da.delta_plan_metrics(small, q).files_planned < files.num_rows
    assert da.delta_plan_metrics(default, q).files_planned == 1


# --- query results ----------------------------------------------------------------------------


@pytest.mark.parametrize("layout", LAYOUTS)
def test_run_delta_matches_oracle_for_every_query(built, batches, layout: str) -> None:
    path, _ = built[layout]
    for query in spec.QUERIES:
        got = da.run_delta(path, query)
        want = oracle(batches, query)
        assert same(got, want), query.name
    assert da.run_delta(path, spec.QUERY_BY_NAME["Q6_full_agg"]).num_rows == len(spec.EVENT_NAMES)


@pytest.mark.parametrize("layout", ["L0", "L2"])
def test_run_delta_at_an_old_version(built, batches, layout: str) -> None:
    path, versions = built[layout]
    for query in spec.QUERIES:
        got = da.run_delta(path, query, version=versions[1])
        assert same(got, oracle(batches[:2], query)), query.name


@pytest.mark.parametrize("layout", LAYOUTS)
def test_timestamp_bounds_are_inclusive_below_and_exclusive_above(
    tmp_path: Path, layout: str
) -> None:
    batch = boundary_batch()
    path = tmp_path / "events"
    da.write_delta(path, [batch], layout)
    d10, d11, d17 = (spec.Q_DAY + dt.timedelta(days=k) for k in (0, 1, 7))
    for name, lo, hi in (
        ("Q2_one_day", d10, d11),
        ("Q3_seven_days", d10, d17),
        ("Q5_user_week", d10, d17),
    ):
        query = spec.QUERY_BY_NAME[name]
        got = da.run_delta(path, query)["event_ts"].to_pylist()
        assert same(da.run_delta(path, query), oracle([batch], query))
        assert lo in got  # exactly at the lower bound: included
        assert hi not in got  # exactly at the upper bound: excluded
        assert hi - US in got  # one microsecond below the upper bound: included
        assert lo - US not in got


# --- planning ---------------------------------------------------------------------------------


@pytest.mark.parametrize("layout", LAYOUTS)
def test_engine_and_own_evaluator_agree_on_the_workload(built, layout: str) -> None:
    path, _ = built[layout]
    for query in spec.QUERIES:
        engine = da.delta_plan_metrics(path, query).files_planned
        assert engine == da.delta_stats_planned(path, query), query.name


def test_engine_file_pruning_predicate_agrees_too(built) -> None:
    path, _ = built["L0"]
    dtab = DeltaTable(str(path))
    d10, d11 = spec.Q_DAY, spec.Q_DAY + dt.timedelta(days=1)
    sql = f"event_ts >= '{d10:%Y-%m-%dT%H:%M:%SZ}' AND event_ts < '{d11:%Y-%m-%dT%H:%M:%SZ}'"
    via_uris = len(dtab.file_uris(file_pruning_predicate=sql))
    assert via_uris == da.delta_plan_metrics(path, spec.QUERY_BY_NAME["Q2_one_day"]).files_planned
    assert (
        len(dtab.file_uris(file_pruning_predicate="user_id = 137"))
        == da.delta_plan_metrics(path, spec.QUERY_BY_NAME["Q1_point_user"]).files_planned
    )


@pytest.mark.parametrize("layout", LAYOUTS)
def test_plan_metrics_are_consistent(built, batches, layout: str) -> None:
    path, _ = built[layout]
    files = da.delta_files(path)
    total = pc.sum(files["size_bytes"]).as_py()
    for query in spec.QUERIES:
        pm = da.delta_plan_metrics(path, query)
        assert pm.files_total == files.num_rows
        assert pm.bytes_total == total
        assert pm.records_total == BATCH_ROWS * 4
        assert pm.manifests_total is None
        assert 0 < pm.files_planned <= pm.files_total or query.name == "Q5_user_week"
        assert pm.bytes_planned <= pm.bytes_total
        # The planner may keep too much, never too little.
        assert pm.records_planned >= oracle(batches, query).num_rows or query.kind == "agg"
        if query.kind == "agg":
            assert pm.files_planned == pm.files_total
            assert pm.records_planned == pm.records_total


def test_time_sliced_unpartitioned_appends_prune_a_date_query(built) -> None:
    path, _ = built["L0"]
    pm = da.delta_plan_metrics(path, spec.QUERY_BY_NAME["Q2_one_day"])
    assert pm.files_total == 4
    assert pm.files_planned == 1  # day 10 sits in the first batch only
    assert pm.bytes_planned < pm.bytes_total
    seven = da.delta_plan_metrics(path, spec.QUERY_BY_NAME["Q3_seven_days"])
    assert seven.files_planned == 2


def test_partitioned_layout_plans_only_the_days_asked_for(built) -> None:
    path, _ = built["L1"]
    one = da.delta_plan_metrics(path, spec.QUERY_BY_NAME["Q2_one_day"])
    seven = da.delta_plan_metrics(path, spec.QUERY_BY_NAME["Q3_seven_days"])
    assert one.files_planned == 1
    assert seven.files_planned == 7


def test_sorted_tiny_files_let_a_user_query_prune(tmp_path: Path) -> None:
    # Six appends of one day each, each batch holding its own disjoint block of 100 user ids.
    batches = [
        make_events(150, i * 150, 10, 1, seed=50 + i, users=100, user_lo=1 + 100 * i)
        for i in range(6)
    ]
    path = tmp_path / "events"
    da.write_delta(path, batches, "L2")
    q = spec.QUERY_BY_NAME["Q1_point_user"]  # user 137 lives in the second block only
    pm = da.delta_plan_metrics(path, q)
    assert pm.files_total == 6
    assert pm.files_planned == 1
    assert pm.records_planned >= oracle(batches, q).num_rows
    assert da.delta_stats_planned(path, q) == 1


def test_millisecond_truncated_statistics_never_drop_a_matching_file(tmp_path: Path) -> None:
    # The stored max is rounded down to the millisecond, so a file whose true maximum is
    # base + 999 us has a stored maximum of base. A bound of base + 500 us must keep the file.
    base = spec.START + dt.timedelta(days=3, milliseconds=7)
    stamps = [base - dt.timedelta(milliseconds=5), base + dt.timedelta(microseconds=999)]
    t = make_events(2, 0, 0, 1, seed=1)
    t = t.set_column(
        2, spec.ARROW_SCHEMA_V1.field("event_ts"), pa.array(stamps, t["event_ts"].type)
    )
    path = tmp_path / "events"
    da.write_delta(path, [t], "L0")
    stored_max = da.delta_files(path)["max.event_ts"][0].as_py()
    assert stored_max == base  # truncated, below the true maximum
    for op, value in ((">=", base + dt.timedelta(microseconds=500)), ("==", stamps[1])):
        q = spec.Query("probe", "boundary", (spec.Pred("event_ts", op, value),))
        assert da.delta_stats_planned(path, q) == 1
        assert da.delta_plan_metrics(path, q).files_planned == 1
        assert da.run_delta(path, q).num_rows >= 1
    # A bound clearly past the true maximum does drop it, by both paths.
    q = spec.Query(
        "beyond", "x", (spec.Pred("event_ts", ">=", base + dt.timedelta(milliseconds=3)),)
    )
    assert da.delta_stats_planned(path, q) == 0
    assert da.delta_plan_metrics(path, q).files_planned == 0


def _stats_table() -> pa.Table:
    ts = pa.timestamp("us", tz="UTC")
    d = spec.START
    return pa.table(
        {
            "min.user_id": pa.array([1, 50, None], pa.int64()),
            "max.user_id": pa.array([49, 99, None], pa.int64()),
            "min.event_ts": pa.array([d, d + dt.timedelta(days=1), None], ts),
            "max.event_ts": pa.array(
                [d + dt.timedelta(days=1), d + dt.timedelta(days=2), None], ts
            ),
            "min.event_name": pa.array(["a", "m", None], pa.string()),
            "max.event_name": pa.array(["l", "z", None], pa.string()),
        }
    )


def test_stats_could_match_semantics() -> None:
    stats = _stats_table()
    p = spec.Pred
    d = spec.START
    assert da.stats_could_match(stats, (p("user_id", "==", 49),)) == [True, False, True]
    assert da.stats_could_match(stats, (p("user_id", "==", 50),)) == [False, True, True]
    assert da.stats_could_match(stats, (p("user_id", ">=", 50),)) == [False, True, True]
    assert da.stats_could_match(stats, (p("user_id", "<", 50),)) == [True, False, True]
    assert da.stats_could_match(stats, (p("user_id", "<", 1),)) == [False, False, True]
    assert da.stats_could_match(stats, (p("event_name", "==", "purchase"),)) == [False, True, True]
    both = (
        p("event_ts", ">=", d + dt.timedelta(days=1)),
        p("event_ts", "<", d + dt.timedelta(days=2)),
    )
    assert da.stats_could_match(stats, both) == [True, True, True]
    assert da.stats_could_match(stats, ()) == [True, True, True]
    # Timestamp maxima are widened by 1 ms: a bound 0.5 ms above the stored max still matches.
    near = (p("event_ts", ">=", d + dt.timedelta(days=1, microseconds=500)),)
    assert da.stats_could_match(stats, near) == [True, True, True]
    far = (p("event_ts", ">=", d + dt.timedelta(days=1, milliseconds=2)),)
    assert da.stats_could_match(stats, far) == [False, True, True]


# --- maintenance ------------------------------------------------------------------------------


def tiny_batches(count: int = 24) -> list[pa.Table]:
    return [make_events(30, i * 30, 10, 3, seed=200 + i, users=200) for i in range(count)]


@pytest.mark.parametrize("layout", ["L0", "L1", "L2"])
def test_compaction_lowers_file_count_and_keeps_rows(tmp_path: Path, layout: str) -> None:
    tiny = tiny_batches()
    path = tmp_path / "events"
    versions = da.write_delta(path, tiny, layout)
    before_rows = da.read_delta(path)
    result = da.compact_delta(path, layout)
    assert result["files_after"] < result["files_before"]
    assert result["version_before"] == versions[-1]
    assert result["version_after"] == versions[-1] + 1
    assert isinstance(result["metrics"], dict)
    assert da.delta_version(path) == result["version_after"]
    assert same(da.read_delta(path), before_rows)
    assert same(da.read_delta(path), canonical(pa.concat_tables(tiny)))
    # The pre-compaction version is still readable and unchanged.
    assert same(da.read_delta(path, result["version_before"]), before_rows)
    for query in spec.QUERIES:
        assert same(da.run_delta(path, query), oracle(tiny, query)), query.name


def test_compaction_does_not_resort_an_l2_table(tmp_path: Path) -> None:
    tiny = tiny_batches()
    path = tmp_path / "events"
    da.write_delta(path, tiny, "L2")
    da.compact_delta(path, "L2")
    unsorted_files = 0
    for fragment in DeltaTable(str(path)).to_pyarrow_dataset().get_fragments():
        users = fragment.to_table(columns=["user_id"])["user_id"].to_pylist()
        unsorted_files += users != sorted(users)
    assert unsorted_files > 0  # sorted runs concatenated, not one sorted run


def test_zorder_rewrites_files_without_changing_rows(tmp_path: Path) -> None:
    tiny = tiny_batches(8)
    path = tmp_path / "events"
    da.write_delta(path, tiny, "L1")
    before = da.read_delta(path)
    metrics = da.zorder_delta(path, ("user_id",))
    assert isinstance(metrics, dict)
    assert same(da.read_delta(path), before)


def test_add_country_rewrites_nothing_and_old_rows_read_null(tmp_path: Path) -> None:
    batches = ordered_batches(2)
    path = tmp_path / "events"
    versions = da.write_delta(path, batches, "L1")
    result = da.evolve_delta_add_country(path)
    assert result["files_before"] == result["files_after"]
    assert result["version_before"] == versions[-1]
    assert result["version_after"] == versions[-1] + 1

    dtab = DeltaTable(str(path))
    assert "country" in dtab.to_pyarrow_dataset().schema.names
    full = dtab.to_pyarrow_dataset().to_table()
    assert pc.count(full["country"], mode="only_valid").as_py() == 0
    assert full.num_rows == BATCH_ROWS * 2

    # Results are unchanged and still V1-shaped.
    assert same(da.read_delta(path), canonical(pa.concat_tables(batches)))
    for query in spec.QUERIES:
        assert same(da.run_delta(path, query), oracle(batches, query)), query.name

    # A v2 append (with country) works, and the earlier version is still readable.
    extra = make_events(50, 10_000, 10, 2, seed=9)
    v2 = da._prepare(extra, "L1").append_column(
        "country", pa.array(["IN"] * extra.num_rows, pa.string())
    )
    write_deltalake(str(path), v2, mode="append")
    after = DeltaTable(str(path)).to_pyarrow_dataset().to_table()
    assert after.num_rows == BATCH_ROWS * 2 + 50
    assert pc.count(after["country"], mode="only_valid").as_py() == 50
    assert da.read_delta(path).num_rows == BATCH_ROWS * 2 + 50
    assert da.read_delta(path, version=result["version_before"]).num_rows == BATCH_ROWS * 2
