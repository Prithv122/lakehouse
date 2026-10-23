import datetime as dt

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from pyiceberg.table.sorting import SortDirection
from pyiceberg.transforms import DayTransform

from lakehouse import spec, tables
from lakehouse.gen import generate_events, generate_v2_events

ROWS = 3_000
LAYOUT_NAMES = list(spec.LAYOUTS)
SORT_COLUMNS = [column for column, _ in spec.SORT_KEY]


@pytest.fixture(scope="module")
def events() -> pa.Table:
    return generate_events(ROWS)


def canonical(t: pa.Table) -> pa.Table:
    return t.cast(spec.CANONICAL_SCHEMA).sort_by("event_id")


def build(catalog, events, layout, batches=5, arrival="ordered", name="events"):
    table = tables.create_events_table(catalog, f"lake.{name}", layout)
    parts = tables.split_batches(events, batches, arrival, seed=1)
    ids = tables.append_batches(table, parts, layout)
    return table, parts, ids


def assert_file_sorted(path: str, columns: list[str]) -> None:
    f = pq.read_table(path, columns=columns)
    assert f.equals(f.sort_by([(c, "ascending") for c in columns]))


# --- split_batches ----------------------------------------------------------------------------


@pytest.mark.parametrize("n", [1, 7, 10, ROWS])
def test_split_sizes_differ_by_at_most_one(events, n):
    for arrival in ("ordered", "shuffled"):
        batches = tables.split_batches(events, n, arrival, seed=3)
        sizes = [b.num_rows for b in batches]
        assert len(batches) == n
        assert max(sizes) - min(sizes) <= 1
        assert sum(sizes) == ROWS


def test_split_ordered_is_consecutive_slices(events):
    batches = tables.split_batches(events, 4, "ordered", seed=3)
    assert pa.concat_tables(batches).equals(events)
    firsts = [b["event_id"][0].as_py() for b in batches]
    assert firsts == sorted(firsts)


def test_split_shuffled_is_seeded_permutation(events):
    a = tables.split_batches(events, 4, "shuffled", seed=3)
    b = tables.split_batches(events, 4, "shuffled", seed=3)
    c = tables.split_batches(events, 4, "shuffled", seed=4)
    assert pa.concat_tables(a).equals(pa.concat_tables(b))
    assert not pa.concat_tables(a).equals(pa.concat_tables(c))
    joined = pa.concat_tables(a)
    assert not joined.equals(events)
    assert joined.sort_by("event_id").equals(events)


def test_split_rejects_bad_arguments(events):
    with pytest.raises(ValueError, match="n="):
        tables.split_batches(events, 0, "ordered", 1)
    with pytest.raises(ValueError, match="n="):
        tables.split_batches(events, ROWS + 1, "ordered", 1)
    with pytest.raises(ValueError, match="arrival"):
        tables.split_batches(events, 2, "sideways", 1)


# --- create_events_table ----------------------------------------------------------------------


@pytest.mark.parametrize("layout", LAYOUT_NAMES)
def test_create_table_matches_layout(catalog, layout):
    ls = spec.LAYOUTS[layout]
    table = tables.create_events_table(catalog, "lake.events", layout)
    fields = table.spec().fields
    if ls.partition_by_day:
        assert len(fields) == 1
        assert isinstance(fields[0].transform, DayTransform)
        assert fields[0].source_id == table.schema().find_field("event_ts").field_id
    else:
        assert not fields
    order = table.sort_order().fields
    if ls.sort_by_user:
        names = [table.schema().find_column_name(f.source_id) for f in order]
        assert names == SORT_COLUMNS
        assert all(f.direction == SortDirection.ASC for f in order)
    else:
        assert not order
    prop = table.properties.get(tables.TARGET_FILE_SIZE_PROPERTY)
    expected = ls.target_file_size_bytes
    assert prop == (None if expected is None else str(expected))


def test_iceberg_schema_matches_arrow_schema():
    for version, arrow in ((1, spec.ARROW_SCHEMA_V1), (2, spec.ARROW_SCHEMA_V2)):
        schema = tables.iceberg_schema(version)
        assert [f.name for f in schema.fields] == arrow.names
        assert [f.field_id for f in schema.fields] == list(range(1, len(arrow) + 1))
        assert [f.required for f in schema.fields] == [not f.nullable for f in arrow]


def test_create_replace_drops_old_table(catalog, events):
    table, _, _ = build(catalog, events, "L1", batches=2)
    assert tables.data_file_paths(table)
    fresh = tables.create_events_table(catalog, "lake.events", "L1", replace=True)
    assert fresh.current_snapshot() is None
    assert tables.data_file_paths(fresh) == frozenset()
    with pytest.raises(Exception, match="events"):
        tables.create_events_table(catalog, "lake.events", "L1", replace=False)


# --- append, read back ------------------------------------------------------------------------


@pytest.mark.parametrize("arrival", ["ordered", "shuffled"])
@pytest.mark.parametrize("layout", LAYOUT_NAMES)
def test_full_scan_equals_generated_table(catalog, events, layout, arrival):
    table, _, ids = build(catalog, events, layout, batches=5, arrival=arrival)
    assert len(ids) == 5
    assert len(set(ids)) == 5
    assert canonical(table.scan().to_arrow()).equals(canonical(events))


@pytest.mark.parametrize("layout", ["L2", "L2s"])
def test_sorted_layouts_write_sorted_files(catalog, events, layout):
    table, _, _ = build(catalog, events, layout, batches=3, arrival="shuffled")
    paths = tables.data_file_paths(table)
    assert paths
    for path in paths:
        assert_file_sorted(path, SORT_COLUMNS)


def test_unsorted_layouts_do_not_sort_by_user(catalog, events):
    table, _, _ = build(catalog, events, "L0", batches=1)
    (path,) = tables.data_file_paths(table)
    users = pq.read_table(path, columns=["user_id"])["user_id"].to_pylist()
    assert users != sorted(users)


def test_l2s_makes_more_files_than_l2_at_a_small_target(catalog, events):
    l2, _, _ = build(catalog, events, "L2", batches=1, name="l2")
    l2s = tables.create_events_table(catalog, "lake.l2s", "L2s")
    assert l2s.properties[tables.TARGET_FILE_SIZE_PROPERTY] == str(1024 * 1024)
    # the real 1 MiB target needs far more rows than a unit test should write
    with l2s.transaction() as tx:
        tx.set_properties({tables.TARGET_FILE_SIZE_PROPERTY: "2000"})
    tables.append_batches(l2s, [events], "L2s")
    assert len(tables.data_file_paths(l2s)) > len(tables.data_file_paths(l2))
    assert canonical(l2s.scan().to_arrow()).equals(canonical(events))


# --- snapshots and time travel ----------------------------------------------------------------


@pytest.mark.parametrize("arrival", ["ordered", "shuffled"])
def test_time_travel_returns_batches_one_to_k(catalog, events, arrival):
    table, parts, ids = build(catalog, events, "L1", batches=4, arrival=arrival)
    for k in range(1, 5):
        expected = pa.concat_tables(parts[:k])
        assert canonical(tables.time_travel(table, ids[k - 1])).equals(canonical(expected))


def test_snapshots_oldest_first_with_record_counts(catalog, events):
    table, parts, ids = build(catalog, events, "L1", batches=4)
    snaps = tables.snapshots(table)
    assert [s[0] for s in snaps] == ids
    assert [s[2] for s in snaps] == [sum(p.num_rows for p in parts[:k]) for k in range(1, 5)]
    stamps = [s[1] for s in snaps]
    assert stamps == sorted(stamps)
    assert all(s.tzinfo is not None and s.utcoffset() == dt.timedelta(0) for s in stamps)


def test_data_file_paths_per_snapshot_grow(catalog, events):
    table, _, ids = build(catalog, events, "L0", batches=3)
    counts = [len(tables.data_file_paths(table, i)) for i in ids]
    assert counts == [1, 2, 3]
    assert tables.data_file_paths(table) == tables.data_file_paths(table, ids[-1])
    assert tables.data_file_paths(table, ids[0]) < tables.data_file_paths(table, ids[1])


# --- schema evolution -------------------------------------------------------------------------


@pytest.mark.parametrize("layout", ["L0", "L1", "L2"])
def test_add_country_rewrites_nothing(catalog, events, layout):
    table, _, ids = build(catalog, events, layout, batches=3)
    before = tables.data_file_paths(table)
    tables.evolve_add_country(table)
    assert tables.data_file_paths(table) == before
    assert table.schema().fields[-1].name == "country"
    assert table.current_snapshot().snapshot_id == ids[-1]
    old = table.scan().to_arrow()
    assert old.num_rows == ROWS
    assert old["country"].null_count == ROWS

    v2 = generate_v2_events(400, spec.SEED, first_event_id=ROWS)
    new_ids = tables.append_batches(table, [v2], layout)
    assert len(new_ids) == 1
    after = table.scan().to_arrow()
    assert after.num_rows == ROWS + 400
    assert after["country"].null_count == ROWS
    new_rows = after.filter(pc.greater_equal(after["event_id"], ROWS))
    assert set(new_rows["country"].to_pylist()) <= set(spec.COUNTRIES)
    assert new_rows["country"].null_count == 0
    assert tables.data_file_paths(table, ids[-1]) == before


# --- compaction -------------------------------------------------------------------------------


@pytest.mark.parametrize("layout", LAYOUT_NAMES)
def test_compact_reduces_files_keeps_rows_one_commit(catalog, events, layout):
    table, parts, ids = build(catalog, events, layout, batches=8, arrival="shuffled")
    before_rows = canonical(table.scan().to_arrow())
    commits_before = len(table.metadata.metadata_log)
    snapshots_before = tables.snapshots(table)

    result = tables.compact(table, layout)

    assert result.files_before == len(tables.data_file_paths(table, result.snapshot_before))
    assert result.files_after == len(tables.data_file_paths(table))
    assert result.files_after < result.files_before
    assert result.rows_before == result.rows_after == ROWS
    assert result.snapshot_before == ids[-1]
    assert result.snapshot_after == table.current_snapshot().snapshot_id
    assert result.snapshot_after != result.snapshot_before
    assert canonical(table.scan().to_arrow()).equals(before_rows)

    # one metadata commit, even though PyIceberg records a DELETE and an APPEND snapshot in it
    assert len(table.metadata.metadata_log) == commits_before + 1
    snapshots_after = tables.snapshots(table)
    assert len(snapshots_after) == len(snapshots_before) + 2
    assert snapshots_after[-2][2] == 0
    assert snapshots_after[-1][2] == ROWS

    # the pre-compaction snapshots still read exactly their rows, from the old files
    assert canonical(tables.time_travel(table, ids[-1])).equals(before_rows)
    assert canonical(tables.time_travel(table, ids[2])).equals(
        canonical(pa.concat_tables(parts[:3]))
    )


def test_compact_orders_rows_by_the_layout_rule(catalog, events):
    l1, _, _ = build(catalog, events, "L1", batches=6, arrival="shuffled", name="l1")
    tables.compact(l1, "L1")
    for path in tables.data_file_paths(l1):
        assert_file_sorted(path, ["event_id"])

    l2, _, _ = build(catalog, events, "L2", batches=6, arrival="shuffled", name="l2")
    tables.compact(l2, "L2")
    for path in tables.data_file_paths(l2):
        assert_file_sorted(path, SORT_COLUMNS)
