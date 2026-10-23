"""Delta Lake arm (delta-rs, no JVM): the same events and workload as the Iceberg arm.

Descriptive only, no hypothesis. Differences from the Iceberg arm that matter for reading the
numbers:

* **No hidden partitioning.** Iceberg derives ``day(event_ts)`` for pruning; Delta can only
  partition by a stored column. L1/L2/L2s therefore carry a derived ``event_date`` (``date32``,
  the UTC date of ``event_ts``) as the partition column. It is not part of the result schema:
  every read drops it, and results are compared on the V1 columns only. The workload filters on
  ``event_ts`` and never on ``event_date``, so the partition values cannot prune anything by
  themselves. Whether day partitioning helps Delta here comes down to the per-file ``event_ts``
  min/max statistics, a real difference from Iceberg that is measured, not assumed.
* **Statistics are truncated.** Delta stores timestamp statistics at millisecond precision, so a
  stored maximum can sit up to 999 microseconds below the true maximum. delta-rs widens the
  maximum by 1 ms when it builds pruning guarantees, and the evaluator here does the same.
* **File size.** ``write_deltalake(target_file_size=...)`` is a byte target on the writer's
  buffered data. L2s passes 1 MiB (``spec.LAYOUTS['L2s']``). The other layouts use delta-rs's own
  default (100 MiB), not Iceberg's 512 MiB; this is a difference between the engines, not tuned.
* **Sorting.** The writer does not sort, so L2/L2s sort each batch by ``spec.SORT_KEY`` before
  writing (same as the Iceberg arm). ``optimize.compact()`` does not re-sort: it concatenates
  the rows of the files it merges in the order it reads them, so within a compacted file the data
  is a series of sorted runs, not one sorted run (see ``compact_delta``).
* **Planning.** "Planned" files are the fragments the pyarrow dataset built by delta-rs keeps for
  the predicate. Each fragment carries the file's partition values and min/max statistics as a
  guarantee expression, so ``Dataset.get_fragments(filter=...)`` drops files without reading
  data. ``delta_stats_planned`` recomputes the same count from the add-action statistics with
  an independent evaluator, as a cross-check.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from deltalake import DeltaTable, write_deltalake
from deltalake.schema import Field as DeltaField
from deltalake.schema import PrimitiveType

from lakehouse import spec

PARTITION_COLUMN = "event_date"
_V1_COLUMNS = [f.name for f in spec.ARROW_SCHEMA_V1]

# Delta stores timestamp statistics at millisecond precision (rounded down); the true maximum
# can be up to this much larger than the stored one.
_TS_STAT_SLACK = dt.timedelta(milliseconds=1)
# Stored string statistics are truncated at this many characters.
_STRING_STAT_LIMIT = 32


# --- helpers ----------------------------------------------------------------------------------


def _uri(path: str | Path) -> str:
    return path.as_posix() if isinstance(path, Path) else str(path)


def _open(path: str | Path, version: int | None = None) -> DeltaTable:
    return DeltaTable(_uri(path), version=version)


def _sorted_by_key(t: pa.Table) -> pa.Table:
    return t.take(pc.sort_indices(t, sort_keys=list(spec.SORT_KEY)))


def _with_event_date(t: pa.Table) -> pa.Table:
    day = pc.cast(pc.floor_temporal(t["event_ts"], unit="day"), pa.date32())
    return t.append_column(PARTITION_COLUMN, day)


def _prepare(batch: pa.Table, layout: str) -> pa.Table:
    lay = spec.LAYOUTS[layout]
    t = batch.select(_V1_COLUMNS).cast(spec.ARROW_SCHEMA_V1)
    if lay.sort_by_user:
        t = _sorted_by_key(t)
    if lay.partition_by_day:
        t = _with_event_date(t)
    return t


def _scalar(pred: spec.Pred, typ: pa.DataType) -> pa.Scalar:
    return pa.scalar(pred.value, type=typ)


def _arrow_expr(preds: tuple[spec.Pred, ...], schema: pa.Schema) -> ds.Expression | None:
    """pyarrow dataset expression for a conjunction of predicates (None when there are none)."""
    expr: ds.Expression | None = None
    for p in preds:
        field = ds.field(p.column)
        value = _scalar(p, schema.field(p.column).type)
        term = {"==": field == value, ">=": field >= value, "<": field < value}[p.op]
        expr = term if expr is None else expr & term
    return expr


def _add_actions(dtab: DeltaTable) -> pa.Table:
    return pa.table(dtab.get_add_actions(flatten=True))


# --- write / inspect --------------------------------------------------------------------------


def write_delta(path: str | Path, batches: list[pa.Table], layout: str) -> list[int]:
    """Write `batches` to a Delta table at `path`, one commit per batch.

    The first batch creates the table (or overwrites an existing one, which then does not start
    at version 0; callers should use a fresh path). Returns the table version after each batch.
    """
    lay = spec.LAYOUTS[layout]
    uri = _uri(path)
    partition_by = [PARTITION_COLUMN] if lay.partition_by_day else None
    versions: list[int] = []
    for i, batch in enumerate(batches):
        kwargs: dict[str, Any] = {}
        if i == 0:
            kwargs.update(mode="overwrite", schema_mode="overwrite")
        else:
            kwargs.update(mode="append")
        if lay.target_file_size_bytes is not None:
            kwargs["target_file_size"] = lay.target_file_size_bytes
        write_deltalake(uri, _prepare(batch, layout), partition_by=partition_by, **kwargs)
        versions.append(DeltaTable(uri).version())
    return versions


def delta_version(path: str | Path) -> int:
    """Current (latest) version of the table."""
    return _open(path).version()


def delta_files(path: str | Path, version: int | None = None) -> pa.Table:
    """Flattened add actions (path, size_bytes, num_records, min./max./null_count. stats,
    partition. values) of the files live at `version` (default: latest)."""
    return _add_actions(_open(path, version))


def read_delta(path: str | Path, version: int | None = None) -> pa.Table:
    """All rows at `version`, V1 columns only, canonical schema, sorted by event_id."""
    dtab = _open(path, version)
    t = dtab.to_pyarrow_dataset().to_table(columns=_V1_COLUMNS)
    return t.cast(spec.CANONICAL_SCHEMA).sort_by("event_id")


# --- planning ---------------------------------------------------------------------------------


def _stat_truncated(typ: pa.DataType) -> bool:
    return pa.types.is_timestamp(typ) or pa.types.is_string(typ)


def stats_could_match(actions: pa.Table, preds: tuple[spec.Pred, ...]) -> list[bool]:
    """Per file: could any row satisfy every predicate, judged from the add-action statistics?

    `==` needs min <= v <= max, `>=` needs max >= v, `<` needs min < v. A missing statistic
    means the file could match. Conservative about truncated statistics: timestamp maxima are
    widened by 1 ms and string comparisons against values at or beyond the truncation length
    are not pruned, so a file holding a matching row is never dropped.
    """
    n = actions.num_rows
    keep = pa.array([True] * n)
    for p in preds:
        lo_name, hi_name = f"min.{p.column}", f"max.{p.column}"
        if lo_name not in actions.column_names or hi_name not in actions.column_names:
            continue
        lo, hi = actions[lo_name], actions[hi_name]
        typ = lo.type
        if pa.types.is_string(typ) and len(str(p.value)) >= _STRING_STAT_LIMIT:
            continue
        v = _scalar(p, typ)
        if pa.types.is_timestamp(typ):
            hi = pc.add(
                hi, pa.scalar(_TS_STAT_SLACK, type=pa.duration("ms")).cast(pa.duration(typ.unit))
            )
        match p.op:
            case "==":
                ok = pc.and_(pc.less_equal(lo, v), pc.greater_equal(hi, v))
            case ">=":
                ok = pc.greater_equal(hi, v)
            case "<":
                ok = pc.less(lo, v)
            case _:
                raise ValueError(f"unsupported operator {p.op!r}")
        # Null statistic -> unknown -> keep.
        keep = pc.and_(keep, pc.fill_null(ok, True))
    return keep.to_pylist()


def delta_stats_planned(path: str | Path, query: spec.Query, version: int | None = None) -> int:
    """Number of files our own evaluator keeps for `query` (cross-check of the engine path)."""
    actions = delta_files(path, version)
    return int(sum(stats_could_match(actions, query.preds)))


def _sql_literal(value: int | str | dt.datetime) -> str:
    """A predicate value as a SQL literal for delta-rs's `file_pruning_predicate`.

    Timestamps are a quoted ISO-8601 string with six fractional digits and a `Z` suffix, which
    the engine casts to the column's UTC timestamp type at microsecond precision. A seconds-only
    string loses sub-second bounds, and `TIMESTAMP '...'` is rejected for a UTC column.
    """
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            raise ValueError(f"naive datetime in predicate: {value!r}")
        return f"'{value.astimezone(dt.UTC):%Y-%m-%dT%H:%M:%S.%fZ}'"
    if isinstance(value, bool):
        raise TypeError("boolean predicate values are not part of the spec")
    if isinstance(value, int):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


def native_predicate(preds: tuple[spec.Pred, ...]) -> str | None:
    """SQL predicate string for delta-rs's own file pruning (None when there are no predicates)."""
    if not preds:
        return None
    sql_op = {"==": "=", ">=": ">=", "<": "<"}
    return " AND ".join(f"{p.column} {sql_op[p.op]} {_sql_literal(p.value)}" for p in preds)


def delta_native_planned(path: str | Path, query: spec.Query, version: int | None = None) -> int:
    """Number of files delta-rs's own `DeltaTable.file_uris(file_pruning_predicate=...)` keeps.

    A third route to the planned-file count, next to the dataset fragments (the reported number)
    and our own statistics evaluator. Meant to agree with both; where a file's timestamp maximum
    sits within 1 ms below a bound, delta-rs's native pruning can be tighter than the fragment
    path's 1 ms widening, which is a conservative superset.
    """
    dtab = _open(path, version)
    predicate = native_predicate(query.preds)
    if predicate is None:
        return len(dtab.file_uris())
    return len(dtab.file_uris(file_pruning_predicate=predicate))


def _planned_paths(dtab: DeltaTable, query: spec.Query) -> set[str]:
    """Table-relative paths of the files the dataset's fragments keep for the predicate."""
    dataset = dtab.to_pyarrow_dataset()
    expr = _arrow_expr(query.preds, dataset.schema)
    fragments = dataset.get_fragments() if expr is None else dataset.get_fragments(filter=expr)
    return {f.path.removeprefix("/") for f in fragments}


def delta_plan_metrics(
    path: str | Path, query: spec.Query, version: int | None = None
) -> spec.PlanMetrics:
    """Files, bytes and records a scan of `query` must touch, from table metadata only.

    Planned = the files delta-rs's pyarrow dataset keeps for the predicate (partition values and
    min/max statistics, no data read).
    """
    dtab = _open(path, version)
    actions = _add_actions(dtab)
    planned = _planned_paths(dtab, query)
    paths = [_norm(p) for p in actions["path"].to_pylist()]
    mask = pa.array([p in planned for p in paths])
    sel = actions.filter(mask)
    if sel.num_rows != len(planned):
        raise RuntimeError(
            f"planned fragments ({len(planned)}) do not match add actions ({sel.num_rows})"
        )
    return spec.PlanMetrics(
        files_total=actions.num_rows,
        bytes_total=int(pc.sum(actions["size_bytes"]).as_py() or 0),
        records_total=int(pc.sum(actions["num_records"]).as_py() or 0),
        files_planned=sel.num_rows,
        bytes_planned=int(pc.sum(sel["size_bytes"]).as_py() or 0),
        records_planned=int(pc.sum(sel["num_records"]).as_py() or 0),
        manifests_total=None,
    )


def _norm(p: str) -> str:
    """Add-action paths are URL-encoded; fragment paths are the decoded file names."""
    from urllib.parse import unquote

    return unquote(p)


# --- reading ----------------------------------------------------------------------------------


def run_delta(path: str | Path, query: spec.Query, version: int | None = None) -> pa.Table:
    """Execute `query` on the Delta table at `version` (default latest).

    Rows queries return the matching rows (V1 columns, canonical schema, sorted by event_id).
    The aggregate query returns ``spec.AGG_SCHEMA`` sorted by event_name.
    """
    dtab = _open(path, version)
    dataset = dtab.to_pyarrow_dataset()
    expr = _arrow_expr(query.preds, dataset.schema)
    if query.kind == "agg":
        t = dataset.to_table(columns=["event_name", "event_id", "amount"], filter=expr)
        cents = pc.cast(pc.round(pc.multiply(t["amount"], 100.0)), pa.int64())
        t = t.append_column("cents", cents)
        g = t.group_by("event_name").aggregate([("event_id", "count"), ("cents", "sum")])
        out = pa.table(
            {
                "event_name": g["event_name"],
                "events": g["event_id_count"],
                "amount_cents": pc.fill_null(g["cents_sum"], 0),
            }
        )
        return out.cast(spec.AGG_SCHEMA).sort_by("event_name")
    t = dataset.to_table(columns=_V1_COLUMNS, filter=expr)
    return t.cast(spec.CANONICAL_SCHEMA).sort_by("event_id")


# --- maintenance ------------------------------------------------------------------------------


def compact_delta(path: str | Path, layout: str) -> dict[str, Any]:
    """Compact small files with Delta's native ``optimize.compact()``.

    Returns ``files_before``, ``files_after``, ``version_before`` (the version just before the
    compaction commit, for time travel), ``version_after`` and delta-rs's own ``metrics``.
    The layout's target file size (L2s: 1 MiB) is passed as the compaction target; other layouts
    use delta-rs's default.

    Compaction does not re-sort. It merges files per partition and writes their rows in the
    order it reads them, so a compacted L2 file is a concatenation of the old sorted runs, not
    one sorted run: the user_id min/max of its files gets wider, unlike Iceberg's rewrite in
    this project, which re-applies the sort key.
    """
    lay = spec.LAYOUTS[layout]
    dtab = _open(path)
    version_before = dtab.version()
    files_before = len(dtab.file_uris())
    metrics = dtab.optimize.compact(target_size=lay.target_file_size_bytes)
    after = _open(path)
    return {
        "files_before": files_before,
        "files_after": len(after.file_uris()),
        "version_before": version_before,
        "version_after": after.version(),
        "metrics": metrics,
    }


def zorder_delta(path: str | Path, columns: tuple[str, ...] = ("user_id",)) -> dict[str, Any]:
    """Exploratory: Z-order the table by `columns` (rewrites files; returns delta-rs metrics)."""
    return _open(path).optimize.z_order(list(columns))


def evolve_delta_add_country(path: str | Path) -> dict[str, Any]:
    """Add a nullable string column ``country`` as a metadata-only commit.

    Returns the sets of data-file paths before and after (``files_before``, ``files_after``) so
    a caller can assert that nothing was rewritten, plus the versions around the change.
    Old files do not contain the column; reading them yields null.
    """
    dtab = _open(path)
    version_before = dtab.version()
    files_before = set(_add_actions(dtab)["path"].to_pylist())
    dtab.alter.add_columns([DeltaField("country", PrimitiveType("string"), nullable=True)])
    after = _open(path)
    return {
        "files_before": files_before,
        "files_after": set(_add_actions(after)["path"].to_pylist()),
        "version_before": version_before,
        "version_after": after.version(),
    }
