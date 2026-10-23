"""The whole grid on a tiny table: every gate passes and every cell is present."""

from __future__ import annotations

import pytest

from lakehouse.experiment import GridConfig, run_grid
from lakehouse.spec import LAYOUTS, QUERIES

ROWS = 1_200


@pytest.fixture(scope="module")
def grid(tmp_path_factory):
    wh = tmp_path_factory.mktemp("grid")
    cfg = GridConfig(warehouse=wh, rows=ROWS, large_batches=3, tiny_batches=10)
    return run_grid(cfg, log=lambda _m: None)


def test_every_gate_passes(grid):
    assert [g.name for g in grid.gates] == ["G1", "G2", "G3", "G4", "D1"]
    failed = {g.name: g.detail for g in grid.gates if not g.passed}
    assert failed == {}


def test_every_cell_is_measured(grid):
    cells = {(r.engine, r.layout, r.regime, r.arrival, r.query) for r in grid.rows}
    expected = {
        (e, layout, regime, arrival, q.name)
        for e in ("iceberg", "delta")
        for layout in LAYOUTS
        for regime in ("B1", "B2", "B2c")
        for arrival in ("ordered", "shuffled")
        for q in QUERIES
    }
    assert cells == expected
    assert len(grid.rows) == len(expected)


def test_results_agree_across_every_cell(grid):
    by_query: dict[str, set[str]] = {}
    for r in grid.rows:
        by_query.setdefault(r.query, set()).add(r.result_sha256)
    assert all(len(shas) == 1 for shas in by_query.values())


def test_compaction_reduces_files(grid):
    totals = {
        (r.engine, r.layout, r.arrival, r.regime): r.files_total
        for r in grid.rows
        if r.query == QUERIES[0].name
    }
    for (engine, layout, arrival, regime), files in totals.items():
        if regime == "B2c":
            assert files < totals[(engine, layout, arrival, "B2")], (engine, layout, arrival)


def test_planner_never_drops_matching_rows(grid):
    for r in grid.rows:
        if r.query != "Q6_full_agg":
            assert r.records_planned >= r.result_rows
        assert r.files_planned <= r.files_total
        assert r.bytes_planned <= r.bytes_total


def test_metadata_is_deterministic_serial_and_parallel(tmp_path):
    cfg = dict(
        rows=600,
        large_batches=2,
        tiny_batches=6,
        delta=False,
        arrivals=("shuffled",),
        layouts=("L0", "L2s"),
    )
    a = run_grid(GridConfig(warehouse=tmp_path / "a", **cfg), log=lambda _m: None)
    # The second run uses worker processes: scheduling must not change any metric.
    b = run_grid(GridConfig(warehouse=tmp_path / "b", **cfg), log=lambda _m: None, jobs=2)

    def key(rows):
        return [
            (r.layout, r.regime, r.query, r.files_total, r.files_planned, r.records_planned,
             r.bytes_total, r.bytes_planned, r.manifests_total, r.result_sha256)
            for r in rows
        ]  # fmt: skip

    assert key(a.rows) == key(b.rows)


def test_rejects_more_batches_than_rows(tmp_path):
    with pytest.raises(ValueError):
        run_grid(GridConfig(warehouse=tmp_path, rows=10, tiny_batches=20), log=lambda _m: None)


def test_delta_cross_check_counts_are_recorded(grid):
    for r in grid.rows:
        if r.engine == "delta":
            assert r.extra["native_files"] == r.extra["own_eval_files"] == str(r.files_planned)


def test_timing_refuses_parallel_cells(tmp_path):
    with pytest.raises(ValueError, match="timing"):
        cfg = GridConfig(warehouse=tmp_path, rows=100, tiny_batches=10, timing=True)
        run_grid(cfg, log=lambda _m: None, jobs=2)
