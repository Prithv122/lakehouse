"""Run the pre-registered grid: build every table, run every query, check every gate."""

from __future__ import annotations

import datetime as dt
import importlib.metadata as md
import platform
import shutil
import statistics
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from lakehouse import delta_arm, tables
from lakehouse.catalog import ensure_namespace, get_catalog, is_s3
from lakehouse.gen import generate_events, generate_v2_events
from lakehouse.pruning import plan_metrics
from lakehouse.report import Gate
from lakehouse.spec import (
    LARGE_BATCHES,
    LAYOUTS,
    QUERIES,
    SEED,
    TINY_BATCHES,
    MetricRow,
    PlanMetrics,
    Query,
)
from lakehouse.workload import apply_query, canonicalize, fingerprint, run_iceberg

ROWS_QUERY = QUERIES[0]  # any rows-kind query: canonicalize() only needs the kind
TIMING_REPEATS = 5


@dataclass(frozen=True)
class GridConfig:
    warehouse: Path | str
    rows: int = 50_000
    seed: int = SEED
    layouts: tuple[str, ...] = tuple(LAYOUTS)
    arrivals: tuple[str, ...] = ("ordered", "shuffled")
    large_batches: int = LARGE_BATCHES
    tiny_batches: int = TINY_BATCHES
    delta: bool = True
    rowgroups: bool = False
    timing: bool = False
    namespace: str = "bench"


@dataclass
class GridResult:
    rows: list[MetricRow]
    gates: list[Gate]
    meta: dict
    problems: dict[str, list[str]] = field(default_factory=dict)


def same_rows(a: pa.Table, b: pa.Table) -> bool:
    return canonicalize(a, ROWS_QUERY).equals(canonicalize(b, ROWS_QUERY))


class _Checks:
    """Collects gate failures; a gate passes when it has run at least once with no problem."""

    def __init__(self) -> None:
        self.problems: dict[str, list[str]] = {g: [] for g in ("G1", "G2", "G3", "G4")}
        self.ran: dict[str, int] = dict.fromkeys(self.problems, 0)

    def check(self, gate: str, ok: bool, what: str) -> None:
        self.ran[gate] += 1
        if not ok:
            self.problems[gate].append(what)

    def gates(self) -> list[Gate]:
        out = []
        for name, probs in self.problems.items():
            if self.ran[name] == 0:
                out.append(Gate(name, False, "not run"))
            elif probs:
                shown = "; ".join(probs[:5]) + (
                    f" (+{len(probs) - 5} more)" if len(probs) > 5 else ""
                )
                out.append(
                    Gate(name, False, f"{len(probs)} of {self.ran[name]} checks failed: {shown}")
                )
            else:
                out.append(Gate(name, True, f"{self.ran[name]} checks passed"))
        return out


def _median_ms(fn: Callable[[], object]) -> float:
    times = []
    for _ in range(TIMING_REPEATS):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return round(statistics.median(times), 3)


def _row(
    engine: str,
    cell: tuple[str, str, str],
    cfg: GridConfig,
    query: Query,
    snapshot: int,
    pm: PlanMetrics,
    result: pa.Table,
    extra: dict[str, str] | None = None,
) -> MetricRow:
    layout, regime, arrival = cell
    return MetricRow(
        engine=engine,
        layout=layout,
        regime=regime,
        arrival=arrival,
        rows=cfg.rows,
        seed=cfg.seed,
        query=query.name,
        snapshot=snapshot,
        files_total=pm.files_total,
        bytes_total=pm.bytes_total,
        records_total=pm.records_total,
        files_planned=pm.files_planned,
        bytes_planned=pm.bytes_planned,
        records_planned=pm.records_planned,
        result_rows=result.num_rows,
        result_sha256=fingerprint(result),
        manifests_total=pm.manifests_total,
        rowgroups_planned_files=pm.rowgroups_planned_files,
        rowgroups_matching=pm.rowgroups_matching,
        extra=extra or {},
    )


def _check_result(
    checks: _Checks, oracle: dict[str, tuple[str, int]], row: MetricRow, query: Query
) -> None:
    where = f"{row.engine}/{row.layout}/{row.regime}/{row.arrival}/{query.name}"
    want_sha, want_rows = oracle[query.name]
    checks.check("G1", row.result_sha256 == want_sha, f"{where}: result differs from oracle")
    if query.kind == "rows":
        # The planner may keep extra records, never fewer than the matching ones.
        checks.check(
            "G1",
            row.records_planned >= want_rows,
            f"{where}: records_planned {row.records_planned} < {want_rows} matching rows",
        )


def _measure_iceberg(table, cell, cfg, checks, oracle) -> list[MetricRow]:
    snapshot = table.current_snapshot().snapshot_id
    out = []
    for q in QUERIES:
        pm = plan_metrics(table, q, rowgroups=cfg.rowgroups)
        result = run_iceberg(table, q)
        extra = {}
        if cfg.timing:
            extra["plan_ms"] = str(_median_ms(lambda q=q: plan_metrics(table, q)))
            extra["scan_ms"] = str(_median_ms(lambda q=q: run_iceberg(table, q)))
        row = _row("iceberg", cell, cfg, q, snapshot, pm, result, extra)
        _check_result(checks, oracle, row, q)
        out.append(row)
    return out


def _measure_delta(path: str, cell, cfg, checks, oracle) -> list[MetricRow]:
    version = delta_arm.delta_version(path)
    out = []
    for q in QUERIES:
        pm = delta_arm.delta_plan_metrics(path, q)
        result = delta_arm.run_delta(path, q)
        own = delta_arm.delta_stats_planned(path, q)
        row = _row("delta", cell, cfg, q, version, pm, result, {"own_eval_files": str(own)})
        _check_result(checks, oracle, row, q)
        out.append(row)
    return out


def _check_time_travel(checks, where: str, read: Callable[[int], pa.Table], ids, batches) -> None:
    acc: list[pa.Table] = []
    for k, (sid, batch) in enumerate(zip(ids, batches, strict=True), start=1):
        acc.append(batch)
        checks.check("G2", same_rows(read(sid), pa.concat_tables(acc)), f"{where}: snapshot {k}")


def _iceberg_cells(catalog, data, arrival, layout, b1, b2, cfg, checks, oracle, log):
    rows: list[MetricRow] = []
    ident = f"{cfg.namespace}.ev_{layout}_{arrival}".lower()

    log(f"iceberg {layout}/{arrival}: B1 ({len(b1)} appends)")
    t = tables.create_events_table(catalog, f"{ident}_b1", layout)
    ids = tables.append_batches(t, b1, layout)
    rows += _measure_iceberg(t, (layout, "B1", arrival), cfg, checks, oracle)
    _check_time_travel(
        checks, f"iceberg/{layout}/{arrival}", lambda s: tables.time_travel(t, s), ids, b1
    )

    log(f"iceberg {layout}/{arrival}: B2 ({len(b2)} appends)")
    t = tables.create_events_table(catalog, f"{ident}_b2", layout)
    tables.append_batches(t, b2, layout)
    rows += _measure_iceberg(t, (layout, "B2", arrival), cfg, checks, oracle)

    log(f"iceberg {layout}/{arrival}: compaction")
    res = tables.compact(t, layout)
    where = f"iceberg/{layout}/{arrival}"
    checks.check("G4", res.rows_before == res.rows_after == cfg.rows, f"{where}: row count changed")
    checks.check("G4", same_rows(t.scan().to_arrow(), data), f"{where}: rows changed by compaction")
    checks.check(
        "G2",
        same_rows(tables.time_travel(t, res.snapshot_before), data),
        f"{where}: pre-compaction snapshot no longer exact",
    )
    rows += _measure_iceberg(t, (layout, "B2c", arrival), cfg, checks, oracle)
    return rows


def _delta_cells(root: Path, data, arrival, layout, b1, b2, cfg, checks, oracle, log):
    rows: list[MetricRow] = []
    base = root / f"{layout}_{arrival}".lower()

    log(f"delta {layout}/{arrival}: B1")
    p1 = str(base / "b1")
    versions = delta_arm.write_delta(p1, b1, layout)
    rows += _measure_delta(p1, (layout, "B1", arrival), cfg, checks, oracle)
    _check_time_travel(
        checks,
        f"delta/{layout}/{arrival}",
        lambda v: delta_arm.read_delta(p1, version=v),
        versions,
        b1,
    )

    log(f"delta {layout}/{arrival}: B2")
    p2 = str(base / "b2")
    delta_arm.write_delta(p2, b2, layout)
    rows += _measure_delta(p2, (layout, "B2", arrival), cfg, checks, oracle)
    res = delta_arm.compact_delta(p2, layout)
    where = f"delta/{layout}/{arrival}"
    checks.check("G4", same_rows(delta_arm.read_delta(p2), data), f"{where}: rows changed")
    checks.check(
        "G2",
        same_rows(delta_arm.read_delta(p2, version=res["version_before"]), data),
        f"{where}: pre-compaction version no longer exact",
    )
    rows += _measure_delta(p2, (layout, "B2c", arrival), cfg, checks, oracle)
    return rows


def _schema_evolution(catalog, data, b1, cfg, checks, log) -> None:
    log("schema evolution (G3)")
    ensure_namespace(catalog, cfg.namespace)
    t = tables.create_events_table(catalog, f"{cfg.namespace}.events_evolve", "L1")
    tables.append_batches(t, b1, "L1")
    before = tables.data_file_paths(t)
    tables.evolve_add_country(t)
    after = tables.data_file_paths(t)
    checks.check("G3", before == after, f"data files rewritten: {len(before ^ after)} changed")
    old = t.scan().to_arrow()
    checks.check("G3", "country" in old.column_names, "country column missing after evolution")
    if "country" in old.column_names:
        checks.check("G3", old["country"].null_count == old.num_rows, "old rows not null")
    v2 = generate_v2_events(max(1, cfg.rows // 100), cfg.seed, first_event_id=cfg.rows)
    t.append(v2.cast(t.schema().as_arrow()))
    new = t.scan().to_arrow()
    checks.check("G3", new.num_rows == data.num_rows + v2.num_rows, "v2 append row count")
    added = new.filter(pc.greater_equal(new["event_id"], cfg.rows))
    checks.check(
        "G3",
        added.num_rows == v2.num_rows and added["country"].null_count == v2["country"].null_count,
        "v2 rows lost their country values",
    )


def _git_state() -> str:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True, check=True
        ).stdout.strip()
        return sha + (" (dirty tree)" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _meta(cfg: GridConfig, started: dt.datetime) -> dict:
    return {
        "rows": cfg.rows,
        "seed": cfg.seed,
        "layouts": ",".join(cfg.layouts),
        "arrivals": ",".join(cfg.arrivals),
        "large_batches": cfg.large_batches,
        "tiny_batches": cfg.tiny_batches,
        "delta": cfg.delta,
        "warehouse": "s3" if is_s3(cfg.warehouse) else "local filesystem",
        "git": _git_state(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "pyiceberg": md.version("pyiceberg"),
        "deltalake": md.version("deltalake"),
        "pyarrow": md.version("pyarrow"),
        "started_utc": started.isoformat(timespec="seconds"),
    }


def run_grid(cfg: GridConfig, log: Callable[[str], None] = print) -> GridResult:
    started = dt.datetime.now(dt.UTC)
    if cfg.tiny_batches > cfg.rows or cfg.large_batches > cfg.rows:
        raise ValueError("more batches than rows")
    if cfg.delta and is_s3(cfg.warehouse):
        raise ValueError("the Delta arm runs on a local warehouse only")

    log(f"generating {cfg.rows:,} rows (seed {cfg.seed})")
    data = generate_events(cfg.rows, cfg.seed)
    oracle = {}
    for q in QUERIES:
        truth = apply_query(data, q)
        oracle[q.name] = (fingerprint(truth), truth.num_rows)

    catalog = get_catalog(cfg.warehouse)
    ensure_namespace(catalog, cfg.namespace)
    delta_root = None
    if cfg.delta:
        delta_root = Path(cfg.warehouse) / "delta"
        shutil.rmtree(delta_root, ignore_errors=True)

    checks = _Checks()
    rows: list[MetricRow] = []
    for arrival in cfg.arrivals:
        b1 = tables.split_batches(data, cfg.large_batches, arrival, cfg.seed)
        b2 = tables.split_batches(data, cfg.tiny_batches, arrival, cfg.seed)
        for layout in cfg.layouts:
            rows += _iceberg_cells(catalog, data, arrival, layout, b1, b2, cfg, checks, oracle, log)
            if delta_root is not None:
                rows += _delta_cells(
                    delta_root, data, arrival, layout, b1, b2, cfg, checks, oracle, log
                )

    b1_ordered = tables.split_batches(data, cfg.large_batches, "ordered", cfg.seed)
    _schema_evolution(catalog, data, b1_ordered, cfg, checks, log)

    meta = _meta(cfg, started)
    meta["finished_utc"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    return GridResult(rows=rows, gates=checks.gates(), meta=meta, problems=checks.problems)
