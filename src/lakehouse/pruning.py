"""What a scan must touch, from metadata only.

`plan_metrics` asks PyIceberg's own planner which data files survive partition and column
min/max pruning, and sums their sizes. Nothing here reads a data page: only the manifests and,
when `rowgroups=True`, the Parquet footers of the planned files.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

import pyarrow.parquet as pq
from pyiceberg.expressions import AlwaysTrue

from lakehouse import spec
from lakehouse.workload import to_iceberg_expr

if TYPE_CHECKING:
    from pyiceberg.table import Table

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


def _snapshot(table: Table, snapshot_id: int | None):
    snap = table.snapshot_by_id(snapshot_id) if snapshot_id is not None else None
    if snapshot_id is not None and snap is None:
        raise ValueError(f"snapshot {snapshot_id} not found")
    return snap if snap is not None else table.current_snapshot()


def table_totals(table: Table, snapshot_id: int | None = None) -> dict[str, int]:
    """Files, bytes, records and manifests of one snapshot (default: the current one).

    Files are those a full scan plans (live data files); manifests are the entries of the
    snapshot's manifest list, read with `snapshot.manifests(io)`. (`table.inspect.manifests()`
    takes no snapshot id: it always describes the current snapshot.)
    """
    snap = _snapshot(table, snapshot_id)
    if snap is None:  # table with no snapshot yet
        return {"files_total": 0, "bytes_total": 0, "records_total": 0, "manifests_total": 0}
    tasks = table.scan(row_filter=AlwaysTrue(), snapshot_id=snap.snapshot_id).plan_files()
    return {
        "files_total": len(tasks),
        "bytes_total": sum(t.file.file_size_in_bytes for t in tasks),
        "records_total": sum(t.file.record_count for t in tasks),
        "manifests_total": len(snap.manifests(table.io)),
    }


# --- row-group statistics ---------------------------------------------------------------------


def _to_comparable(stat: Any, value: Any) -> Any:
    """Bring a Parquet statistic to the type of the predicate value so the two compare.

    Timestamp statistics arrive as `datetime` (naive or aware, depending on the file's logical
    type) or as a raw int (microseconds since the epoch); strings as `str` or `bytes`.
    """
    if isinstance(value, dt.datetime):
        if isinstance(stat, dt.datetime):
            return stat if stat.tzinfo is not None else stat.replace(tzinfo=dt.UTC)
        if isinstance(stat, int):
            return _EPOCH + dt.timedelta(microseconds=stat)
        raise TypeError(f"cannot compare statistic {stat!r} with a timestamp")
    if isinstance(value, str) and isinstance(stat, bytes):
        return stat.decode()
    return stat


def stats_could_match(
    preds: tuple[spec.Pred, ...], mins: dict[str, Any], maxs: dict[str, Any]
) -> bool:
    """Could a row group with these column statistics hold a row satisfying every predicate?

    Inclusive-metrics semantics, the same as Iceberg's: `==` needs min <= v <= max, `>=` needs
    max >= v, `<` needs min < v. A column with no min or no max (statistics missing) cannot rule
    the group out, so that predicate counts as satisfiable.
    """
    for p in preds:
        lo, hi = mins.get(p.column), maxs.get(p.column)
        if p.op == "==":
            if lo is None or hi is None:
                continue
            if not (_to_comparable(lo, p.value) <= p.value <= _to_comparable(hi, p.value)):
                return False
        elif p.op == ">=":
            if hi is not None and not _to_comparable(hi, p.value) >= p.value:
                return False
        elif p.op == "<":
            if lo is not None and not _to_comparable(lo, p.value) < p.value:
                return False
        else:
            raise ValueError(f"unsupported operator {p.op!r}")
    return True


def _rowgroup_counts(table: Table, tasks: list, preds: tuple[spec.Pred, ...]) -> tuple[int, int]:
    """(row groups in the planned files, those whose footer statistics could match)."""
    columns = {p.column for p in preds}
    total = matching = 0
    for task in tasks:
        with table.io.new_input(task.file.file_path).open() as f:
            meta = pq.ParquetFile(f).metadata  # reads the footer only
        names = [meta.schema.column(j).path for j in range(meta.num_columns)]
        for i in range(meta.num_row_groups):
            rg = meta.row_group(i)
            mins: dict[str, Any] = {}
            maxs: dict[str, Any] = {}
            for j, name in enumerate(names):
                if name not in columns:
                    continue
                stats = rg.column(j).statistics
                if stats is not None and stats.has_min_max:
                    mins[name], maxs[name] = stats.min, stats.max
            total += 1
            matching += stats_could_match(preds, mins, maxs)
    return total, matching


def _planned_tasks(table: Table, query: spec.Query, snapshot_id: int | None) -> list:
    snap = _snapshot(table, snapshot_id)
    if snap is None:
        return []
    scan = table.scan(row_filter=to_iceberg_expr(query.preds), snapshot_id=snap.snapshot_id)
    return list(scan.plan_files())


def plan_files_only(table: Table, query: spec.Query, snapshot_id: int | None = None) -> int:
    """Number of data files the planner keeps for `query`: the filtered plan and nothing else.

    No totals, no byte sums, no row-group footers, so it is the cheap call to time.
    """
    return len(_planned_tasks(table, query, snapshot_id))


def plan_metrics(
    table: Table,
    query: spec.Query,
    snapshot_id: int | None = None,
    rowgroups: bool = False,
) -> spec.PlanMetrics:
    """Files, bytes and records the planner keeps for `query`, against the snapshot's totals.

    The planned set is `scan(row_filter).plan_files()`: partition pruning, then each file's
    column min/max from the manifest. With `rowgroups=True` it also opens the footer of each
    planned file and counts row groups, and those whose own statistics could match.

    Raises RuntimeError if any planned task carries delete files: a scan would also read them, so
    the bytes and records here would be undercounted. This harness never writes deletes.
    """
    totals = table_totals(table, snapshot_id)
    tasks = _planned_tasks(table, query, snapshot_id)
    with_deletes = [t.file.file_path for t in tasks if t.delete_files]
    if with_deletes:
        raise RuntimeError(
            f"{len(with_deletes)} planned file(s) have delete files (e.g. {with_deletes[0]}); "
            "planned bytes would be undercounted"
        )
    rg_total = rg_match = None
    if rowgroups:
        rg_total, rg_match = _rowgroup_counts(table, tasks, query.preds)
    return spec.PlanMetrics(
        files_total=totals["files_total"],
        bytes_total=totals["bytes_total"],
        records_total=totals["records_total"],
        files_planned=len(tasks),
        bytes_planned=sum(t.file.file_size_in_bytes for t in tasks),
        records_planned=sum(t.file.record_count for t in tasks),
        manifests_total=totals["manifests_total"],
        rowgroups_planned_files=rg_total,
        rowgroups_matching=rg_match,
    )
