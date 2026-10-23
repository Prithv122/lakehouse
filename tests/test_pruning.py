"""Pruning: planned files/bytes/records from metadata, and row-group statistics matching."""

from __future__ import annotations

import datetime as dt
import math

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.conversions import from_bytes
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

from lakehouse import pruning, spec
from lakehouse import workload as w
from lakehouse.catalog import ensure_namespace, get_catalog

Q = spec.QUERY_BY_NAME
DAY = dt.timedelta(days=1)
D10 = spec.Q_DAY
BASE_US = int(spec.START.timestamp() * 1_000_000)
DAY_US = 86_400 * 1_000_000

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


def make_table(tmp_path, partitioned: bool, properties: dict[str, str] | None = None, name="e"):
    catalog = get_catalog(tmp_path / f"wh_{name}_{partitioned}")
    ensure_namespace(catalog, "lake")
    return catalog.create_table(
        f"lake.{name}",
        schema=ICEBERG_V1,
        partition_spec=BY_DAY if partitioned else PartitionSpec(),
        properties=properties or {},
    )


def events(
    n: int,
    first_id: int = 0,
    seed: int = 0,
    day_lo: float = 0.0,
    day_hi: float = float(spec.DAYS),
    users: tuple[int, int] = (100, 180),
    sort_by_user: bool = False,
) -> pa.Table:
    """n events with timestamps uniform in [day_lo, day_hi) days, sorted by time."""
    rng = np.random.default_rng(seed)
    lo, hi = int(day_lo * DAY_US), int(day_hi * DAY_US)
    ts = np.sort(rng.integers(lo, hi, n)) + BASE_US
    names = rng.choice(spec.EVENT_NAMES, n)
    t = pa.table(
        {
            "event_id": np.arange(first_id, first_id + n, dtype=np.int64),
            "user_id": rng.integers(users[0], users[1], n),
            "event_ts": pa.array(ts, type=pa.timestamp("us", tz="UTC")),
            "event_name": names,
            "channel": rng.choice(spec.CHANNELS, n),
            "platform": rng.choice(spec.PLATFORMS, n),
            "amount": pa.array(
                np.round(rng.random(n) * 100, 2), mask=names != "purchase", type=pa.float64()
            ),
        },
        schema=spec.ARROW_SCHEMA_V1,
    )
    return t.sort_by("user_id") if sort_by_user else t


def decode_ts_bounds(task) -> tuple[dt.datetime, dt.datetime]:
    f = task.file
    lo = from_bytes(TimestamptzType(), f.lower_bounds[3])
    hi = from_bytes(TimestamptzType(), f.upper_bounds[3])
    return (
        dt.datetime.fromtimestamp(lo / 1e6, dt.UTC),
        dt.datetime.fromtimestamp(hi / 1e6, dt.UTC),
    )


# --- stats_could_match ------------------------------------------------------------------------

EQ5 = (spec.Pred("c", "==", 5),)
GE5 = (spec.Pred("c", ">=", 5),)
LT5 = (spec.Pred("c", "<", 5),)


@pytest.mark.parametrize(
    ("lo", "hi", "expected"),
    [(5, 5, True), (1, 5, True), (5, 9, True), (1, 9, True), (6, 9, False), (1, 4, False)],
)
def test_equality_is_inclusive_on_both_ends(lo, hi, expected):
    assert pruning.stats_could_match(EQ5, {"c": lo}, {"c": hi}) is expected


@pytest.mark.parametrize(
    ("lo", "hi", "expected"), [(0, 5, True), (0, 4, False), (0, 9, True), (7, 9, True)]
)
def test_greater_equal_needs_max_at_least_v(lo, hi, expected):
    assert pruning.stats_could_match(GE5, {"c": lo}, {"c": hi}) is expected


@pytest.mark.parametrize(
    ("lo", "hi", "expected"), [(4, 9, True), (5, 9, False), (6, 9, False), (0, 2, True)]
)
def test_less_than_needs_min_strictly_below_v(lo, hi, expected):
    assert pruning.stats_could_match(LT5, {"c": lo}, {"c": hi}) is expected


def test_missing_statistics_never_rule_a_group_out():
    for preds in (EQ5, GE5, LT5):
        assert pruning.stats_could_match(preds, {}, {})
    # Only one side present: equality cannot decide, the one-sided ops still can.
    assert pruning.stats_could_match(EQ5, {"c": 9}, {})
    assert pruning.stats_could_match(EQ5, {}, {"c": 1})
    assert not pruning.stats_could_match(GE5, {}, {"c": 4})
    assert not pruning.stats_could_match(LT5, {"c": 5}, {})
    assert pruning.stats_could_match(GE5, {"c": 99}, {})
    # statistics of an unrelated column do not matter
    assert pruning.stats_could_match(EQ5, {"other": 100}, {"other": 200})


def test_empty_predicates_always_match():
    assert pruning.stats_could_match((), {}, {})
    assert pruning.stats_could_match((), {"c": 1}, {"c": 2})


def test_all_predicates_must_hold():
    preds = (spec.Pred("u", "==", 137), *Q["Q2_one_day"].preds)
    in_day = {"event_ts": D10 + dt.timedelta(hours=1)}
    inside = pruning.stats_could_match(preds, {"u": 100, **in_day}, {"u": 200, **in_day})
    assert inside
    wrong_user = pruning.stats_could_match(preds, {"u": 138, **in_day}, {"u": 200, **in_day})
    assert not wrong_user
    wrong_day = {"event_ts": D10 + 3 * DAY}
    assert not pruning.stats_could_match(preds, {"u": 100, **wrong_day}, {"u": 200, **wrong_day})


def test_strings_str_and_bytes():
    p = (spec.Pred("event_name", "==", "purchase"),)
    assert pruning.stats_could_match(p, {"event_name": "add_to_cart"}, {"event_name": "search"})
    assert not pruning.stats_could_match(p, {"event_name": "search"}, {"event_name": "signup"})
    assert pruning.stats_could_match(p, {"event_name": b"add_to_cart"}, {"event_name": b"search"})
    assert not pruning.stats_could_match(p, {"event_name": b"search"}, {"event_name": b"signup"})


def utc_micros(ts: dt.datetime) -> int:
    return (ts - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)) // dt.timedelta(microseconds=1)


@pytest.mark.parametrize("kind", ["aware", "naive", "micros"])
def test_timestamp_statistics_in_every_representation(kind):
    def rep(ts: dt.datetime):
        return {"aware": ts, "naive": ts.replace(tzinfo=None), "micros": utc_micros(ts)}[kind]

    q2 = Q["Q2_one_day"].preds
    us = dt.timedelta(microseconds=1)
    end = D10 + DAY

    def match(lo: dt.datetime, hi: dt.datetime) -> bool:
        return pruning.stats_could_match(q2, {"event_ts": rep(lo)}, {"event_ts": rep(hi)})

    assert match(D10, D10)  # lower bound is inclusive
    assert match(end - us, end - us)  # last microsecond of the day
    assert not match(end, end)  # upper bound is exclusive
    assert not match(D10 - DAY, D10 - us)  # ends one microsecond before the day starts
    assert match(D10 - DAY, D10)  # touches the first instant
    assert match(D10 - DAY, end + DAY)  # spans the whole day
    assert not match(end, end + DAY)


def test_naive_statistic_is_taken_as_utc():
    q = (spec.Pred("event_ts", ">=", D10),)
    naive_before = (D10 - dt.timedelta(seconds=1)).replace(tzinfo=None)
    assert not pruning.stats_could_match(q, {}, {"event_ts": naive_before})
    assert pruning.stats_could_match(q, {}, {"event_ts": D10.replace(tzinfo=None)})


def test_unsupported_operator_and_statistic_type():
    with pytest.raises(ValueError, match="unsupported"):
        pruning.stats_could_match((spec.Pred("c", "!=", 1),), {"c": 1}, {"c": 2})  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        pruning.stats_could_match(Q["Q2_one_day"].preds, {"event_ts": "x"}, {"event_ts": "y"})


@pytest.mark.parametrize(
    ("arrow_type", "expect_aware"),
    [
        (pa.timestamp("us", tz="UTC"), True),
        (pa.timestamp("ms", tz="UTC"), True),
        (pa.timestamp("us"), False),
    ],
    ids=["us_utc", "ms_utc", "us_naive"],
)
def test_real_parquet_timestamp_statistics(tmp_path, arrow_type, expect_aware):
    """Footer statistics of a real file, fed through stats_could_match row group by row group."""
    n = 1000
    ts = np.arange(n, dtype=np.int64) * (DAY_US // 100) + BASE_US  # 100 rows per day
    arr = pa.array(ts, type=pa.timestamp("us", tz="UTC")).cast(arrow_type)
    path = tmp_path / "ts.parquet"
    pq.write_table(pa.table({"event_ts": arr}), path, row_group_size=100)
    md = pq.ParquetFile(path).metadata
    assert md.num_row_groups == 10
    stat = md.row_group(0).column(0).statistics
    assert isinstance(stat.min, dt.datetime)
    assert (stat.min.tzinfo is not None) is expect_aware
    q = (
        spec.Pred("event_ts", ">=", spec.START + 3 * DAY),
        spec.Pred("event_ts", "<", spec.START + 4 * DAY),
    )
    hits = []
    for i in range(md.num_row_groups):
        s = md.row_group(i).column(0).statistics
        if pruning.stats_could_match(q, {"event_ts": s.min}, {"event_ts": s.max}):
            hits.append(i)
    assert hits == [3]  # rows 300..399 are exactly day 3 (statistics are exact, not truncated)


def test_real_parquet_string_and_int_statistics(tmp_path):
    t = pa.table({"user_id": list(range(100)), "event_name": ["a"] * 50 + ["z"] * 50})
    path = tmp_path / "x.parquet"
    pq.write_table(t, path, row_group_size=50)
    md = pq.ParquetFile(path).metadata
    got = []
    for i in range(2):
        mins = {md.schema.column(j).path: md.row_group(i).column(j).statistics.min for j in (0, 1)}
        maxs = {md.schema.column(j).path: md.row_group(i).column(j).statistics.max for j in (0, 1)}
        got.append(
            (
                pruning.stats_could_match((spec.Pred("user_id", "==", 70),), mins, maxs),
                pruning.stats_could_match((spec.Pred("event_name", "==", "a"),), mins, maxs),
            )
        )
    assert got == [(False, True), (True, False)]


# --- file-level planning ----------------------------------------------------------------------


def test_unpartitioned_disjoint_slices_prune_by_min_max(tmp_path):
    table = make_table(tmp_path, False)
    bounds = [(0, 7), (7, 14), (14, 21), (21, 30)]
    for i, (lo, hi) in enumerate(bounds):
        table.append(events(400, first_id=i * 400, seed=i, day_lo=lo, day_hi=hi))
    totals = pruning.table_totals(table)
    assert totals["files_total"] == 4
    assert totals["records_total"] == 1600
    m2 = pruning.plan_metrics(table, Q["Q2_one_day"])
    assert m2.files_planned == 1  # day 10 sits only in the 7-14 slice
    assert m2.files_total == 4
    assert 0 < m2.bytes_planned < m2.bytes_total
    m3 = pruning.plan_metrics(table, Q["Q3_seven_days"])
    assert m3.files_planned == 2  # days 10-16 span the 7-14 and 14-21 slices
    m6 = pruning.plan_metrics(table, Q["Q6_full_agg"])
    assert (m6.files_planned, m6.bytes_planned, m6.records_planned) == (
        m6.files_total,
        m6.bytes_total,
        m6.records_total,
    )
    # no predicate overlaps a slice entirely outside the data
    outside = spec.Query("far", "", (spec.Pred("event_ts", ">=", spec.START + 100 * DAY),))
    assert pruning.plan_metrics(table, outside).files_planned == 0


def test_day_partitioned_q2_plans_exactly_that_days_files(tmp_path):
    table = make_table(tmp_path, True)
    appends = [events(1500, first_id=i * 1500, seed=10 + i) for i in range(3)]
    for a in appends:
        table.append(a)
    m2 = pruning.plan_metrics(table, Q["Q2_one_day"])
    # one file per (append, day) with data on that day
    assert m2.files_planned == 3
    assert m2.files_total == 3 * spec.DAYS
    tasks = table.scan(row_filter=w.to_iceberg_expr(Q["Q2_one_day"].preds)).plan_files()
    for task in tasks:
        lo, hi = decode_ts_bounds(task)
        assert D10 <= lo <= hi < D10 + DAY
    m3 = pruning.plan_metrics(table, Q["Q3_seven_days"])
    assert m3.files_planned == 3 * 7
    rows_day10 = sum(w.apply_query(a, Q["Q2_one_day"]).num_rows for a in appends)
    assert m2.records_planned == rows_day10  # partition-aligned: no extra rows


def test_sorted_batches_with_tiny_target_files_prune_user_lookup(tmp_path):
    props = {"write.target-file-size-bytes": "20000"}
    sorted_t = make_table(tmp_path, False, props, name="sorted")
    unsorted_t = make_table(tmp_path, False, props, name="unsorted")
    data = events(20_000, seed=20, users=(1, 400))
    sorted_t.append(data.sort_by("user_id"))
    unsorted_t.append(data)
    ms = pruning.plan_metrics(sorted_t, Q["Q1_point_user"])
    mu = pruning.plan_metrics(unsorted_t, Q["Q1_point_user"])
    assert ms.files_total > 4, "the tiny target size must split the append into several files"
    assert 1 <= ms.files_planned < ms.files_total
    assert ms.bytes_planned < ms.bytes_total
    # same data, arrival order: every file spans (nearly) every user id
    assert mu.files_planned > ms.files_planned
    result = w.apply_query(data, Q["Q1_point_user"]).num_rows
    assert result > 0
    assert ms.records_planned >= result
    assert mu.records_planned >= result


def test_records_planned_never_below_result_rows(tmp_path):
    for partitioned in (False, True):
        table = make_table(tmp_path, partitioned, {"write.target-file-size-bytes": "30000"})
        parts = [events(3000, i * 3000, seed=30 + i, users=(100, 180)) for i in range(3)]
        for p in parts:
            table.append(p)
        whole = pa.concat_tables(parts)
        for q in spec.QUERIES:
            m = pruning.plan_metrics(table, q)
            result_rows = w.run_iceberg(table, q).num_rows
            assert m.records_planned >= result_rows, (partitioned, q.name)
            assert result_rows == w.apply_query(whole, q).num_rows
            assert m.files_planned <= m.files_total
            assert m.bytes_planned <= m.bytes_total
            assert m.records_planned <= m.records_total


def test_plan_files_only_matches_plan_metrics(tmp_path):
    table = make_table(tmp_path, True)
    for i in range(3):
        table.append(events(1500, first_id=i * 1500, seed=40 + i))
    first = table.snapshots()[0].snapshot_id
    for q in spec.QUERIES:
        assert pruning.plan_files_only(table, q) == pruning.plan_metrics(table, q).files_planned
    q2 = Q["Q2_one_day"]
    assert pruning.plan_files_only(table, q2, first) == 1
    assert pruning.plan_files_only(table, q2) == 3


def test_plan_files_only_on_an_empty_table_is_zero(tmp_path):
    table = make_table(tmp_path, False)
    assert pruning.plan_files_only(table, Q["Q1_point_user"]) == 0


def test_plan_metrics_refuses_tasks_with_delete_files(tmp_path, monkeypatch):
    table = make_table(tmp_path, False)
    table.append(events(500, first_id=0, seed=50))
    real = pruning._planned_tasks

    class WithDeletes:
        """A planned task that claims a delete file applies to it."""

        def __init__(self, task):
            self.file = task.file
            self.delete_files = {object()}

    monkeypatch.setattr(
        pruning, "_planned_tasks", lambda *a, **k: [WithDeletes(t) for t in real(*a, **k)]
    )
    with pytest.raises(RuntimeError, match="delete files"):
        pruning.plan_metrics(table, Q["Q6_full_agg"])
    # the file-count helper does not look at bytes, so it is unaffected
    assert pruning.plan_files_only(table, Q["Q6_full_agg"]) == 1


def test_plan_metrics_accepts_real_tasks_without_deletes(tmp_path):
    table = make_table(tmp_path, False)
    table.append(events(500, first_id=0, seed=51))
    tasks = pruning._planned_tasks(table, Q["Q6_full_agg"], None)
    assert tasks and all(not t.delete_files for t in tasks)
    assert pruning.plan_metrics(table, Q["Q6_full_agg"]).files_planned == len(tasks)


def test_totals_match_inspect_and_manifest_count(tmp_path):
    table = make_table(tmp_path, True)
    snaps = []
    for i in range(5):
        table.append(events(300, first_id=i * 300, seed=40 + i, day_lo=i * 6, day_hi=i * 6 + 6))
        snaps.append(table.current_snapshot().snapshot_id)
    for k, snap_id in enumerate(snaps, start=1):
        totals = pruning.table_totals(table, snap_id)
        files = table.inspect.files(snap_id).to_pylist()
        assert totals["files_total"] == len(files)
        assert totals["bytes_total"] == sum(f["file_size_in_bytes"] for f in files)
        assert totals["records_total"] == sum(f["record_count"] for f in files) == 300 * k
        # Append-only table, one manifest per append (verified here, not assumed).
        assert totals["manifests_total"] == k
    assert pruning.table_totals(table)["manifests_total"] == len(table.inspect.manifests()) == 5
    assert pruning.table_totals(table) == pruning.table_totals(table, snaps[-1])


def test_snapshot_id_argument_returns_older_numbers(tmp_path):
    table = make_table(tmp_path, False)
    table.append(events(500, 0, seed=50, day_lo=8, day_hi=12))
    snap1 = table.current_snapshot().snapshot_id
    table.append(events(500, 500, seed=51, day_lo=9, day_hi=11))
    old = pruning.plan_metrics(table, Q["Q2_one_day"], snapshot_id=snap1)
    new = pruning.plan_metrics(table, Q["Q2_one_day"])
    assert (old.files_total, old.records_total, old.manifests_total) == (1, 500, 1)
    assert (new.files_total, new.records_total, new.manifests_total) == (2, 1000, 2)
    assert (old.files_planned, new.files_planned) == (1, 2)
    assert old.bytes_planned < new.bytes_planned
    assert new == pruning.plan_metrics(
        table, Q["Q2_one_day"], snapshot_id=table.current_snapshot().snapshot_id
    )
    with pytest.raises(ValueError, match="not found"):
        pruning.plan_metrics(table, Q["Q2_one_day"], snapshot_id=12345)


def test_empty_table_has_zero_totals(tmp_path):
    table = make_table(tmp_path, True)
    m = pruning.plan_metrics(table, Q["Q2_one_day"], rowgroups=True)
    assert (m.files_total, m.files_planned, m.bytes_total, m.manifests_total) == (0, 0, 0, 0)
    assert (m.rowgroups_planned_files, m.rowgroups_matching) == (0, 0)


# --- row groups -------------------------------------------------------------------------------


def test_rowgroups_none_unless_requested(tmp_path):
    table = make_table(tmp_path, False)
    table.append(events(200, seed=60))
    m = pruning.plan_metrics(table, Q["Q4_purchases"])
    assert m.rowgroups_planned_files is None
    assert m.rowgroups_matching is None


def test_rowgroup_counting_with_small_row_groups(tmp_path):
    table = make_table(tmp_path, False, {"write.parquet.row-group-limit": "100"})
    data = events(2000, seed=61, day_lo=8, day_hi=13)  # time-sorted: row groups are time slices
    table.append(data)
    m2 = pruning.plan_metrics(table, Q["Q2_one_day"], rowgroups=True)
    assert m2.files_planned == 1
    assert m2.rowgroups_planned_files == 20
    assert 0 < m2.rowgroups_matching < m2.rowgroups_planned_files
    day_rows = w.apply_query(data, Q["Q2_one_day"]).num_rows
    assert m2.rowgroups_matching >= math.ceil(day_rows / 100)
    assert m2.rowgroups_matching <= math.ceil(day_rows / 100) + 1  # at most one straddler
    # Q6 has no predicate: every row group matches.
    m6 = pruning.plan_metrics(table, Q["Q6_full_agg"], rowgroups=True)
    assert m6.rowgroups_matching == m6.rowgroups_planned_files == 20
    # a predicate no row group can satisfy
    nobody = spec.Query("nobody", "", (spec.Pred("user_id", "==", 1),))
    m0 = pruning.plan_metrics(table, nobody, rowgroups=True)
    assert m0.files_planned == 0
    assert (m0.rowgroups_planned_files, m0.rowgroups_matching) == (0, 0)


def test_rowgroups_in_day_partitioned_table_sum_over_planned_files(tmp_path):
    table = make_table(tmp_path, True, {"write.parquet.row-group-limit": "50"})
    data = events(3000, seed=62, users=(1, 300), sort_by_user=True)
    table.append(data)
    m = pruning.plan_metrics(table, Q["Q1_point_user"], rowgroups=True)
    assert m.files_planned > 0
    assert m.rowgroups_planned_files >= m.files_planned
    assert 0 <= m.rowgroups_matching < m.rowgroups_planned_files  # user-sorted: groups prune
    # the rows matched by row-group statistics cover every result row
    result = w.apply_query(data, Q["Q1_point_user"]).num_rows
    assert m.rowgroups_matching * 50 >= result


def test_plan_metrics_reads_no_data_pages(tmp_path, monkeypatch):
    table = make_table(tmp_path, True, {"write.parquet.row-group-limit": "100"})
    table.append(events(1000, seed=63))

    def boom(*args, **kwargs):
        raise AssertionError("data page read")

    for name in ("read", "read_row_group", "read_row_groups", "iter_batches"):
        monkeypatch.setattr(pq.ParquetFile, name, boom)
    monkeypatch.setattr(pq, "read_table", boom)
    for q in spec.QUERIES:
        m = pruning.plan_metrics(table, q, rowgroups=True)
        assert m.rowgroups_planned_files >= m.rowgroups_matching
