import datetime as dt

import pytest

from lakehouse import contract, spec
from lakehouse.gen import generate_events

ROWS = 2_000


@pytest.fixture
def canonical_warehouse(warehouse):
    contract.build_canonical_table(warehouse, rows=ROWS)
    return warehouse


def test_build_canonical_table_is_l1_b1_ordered(warehouse):
    table = contract.build_canonical_table(warehouse, rows=ROWS)
    assert len(table.spec().fields) == 1
    assert not table.sort_order().fields
    snaps = contract.snapshots(table)
    assert len(snaps) == spec.LARGE_BATCHES
    assert snaps[-1][2] == ROWS
    # ordered arrival: the first append holds the first slice of event_ids
    first = contract.to_arrow(table, snaps[0][0])
    assert sorted(first["event_id"].to_pylist()) == list(range(ROWS // spec.LARGE_BATCHES))


def test_open_table_with_warehouse_argument(canonical_warehouse):
    table = contract.open_table(warehouse=canonical_warehouse)
    assert contract.to_arrow(table).num_rows == ROWS


def test_open_table_from_env_var(canonical_warehouse, monkeypatch):
    monkeypatch.setenv(contract.WAREHOUSE_ENV, str(canonical_warehouse))
    table = contract.open_table()
    assert contract.to_arrow(table).num_rows == ROWS


def test_open_table_defaults_to_local_warehouse_dir(canonical_warehouse, monkeypatch):
    monkeypatch.delenv(contract.WAREHOUSE_ENV, raising=False)
    monkeypatch.chdir(canonical_warehouse.parent)
    # ./warehouse is the fixture's own directory name, so the default resolves to it
    assert contract.open_table().schema().fields[0].name == "event_id"


def test_snapshots_oldest_first_with_counts(canonical_warehouse):
    table = contract.open_table(warehouse=canonical_warehouse)
    snaps = contract.snapshots(table)
    counts = [s[2] for s in snaps]
    assert counts == sorted(counts)
    assert counts == [ROWS * k // spec.LARGE_BATCHES for k in range(1, spec.LARGE_BATCHES + 1)]
    stamps = [s[1] for s in snaps]
    assert stamps == sorted(stamps)
    assert all(isinstance(s, dt.datetime) and s.utcoffset() == dt.timedelta(0) for s in stamps)


def test_to_arrow_with_and_without_snapshot_id(canonical_warehouse):
    table = contract.open_table(warehouse=canonical_warehouse)
    snaps = contract.snapshots(table)
    expected = generate_events(ROWS)
    full = contract.to_arrow(table)
    assert full.sort_by("event_id")["event_id"].to_pylist() == expected["event_id"].to_pylist()
    mid = contract.to_arrow(table, snaps[3][0])
    assert mid.num_rows == snaps[3][2]
    assert contract.to_arrow(table, snaps[-1][0]).num_rows == ROWS


def test_open_table_missing_table_raises(canonical_warehouse):
    with pytest.raises(Exception, match="nope"):
        contract.open_table("nope", warehouse=canonical_warehouse)
