"""The fixed workload as executable code: predicates -> expressions, the in-memory oracle,
the Iceberg read path, and canonical forms + fingerprints for comparing results.

Everything is compared on the V1 columns only. A table that went through schema evolution has
an extra `country` column; `canonicalize` drops it, so old and new tables compare equal.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from functools import reduce
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThanOrEqual,
    LessThan,
)

from lakehouse import spec

if TYPE_CHECKING:
    from pyiceberg.table import Table

_TS_TYPE = pa.timestamp("us", tz="UTC")
_V1_COLUMNS = tuple(f.name for f in spec.ARROW_SCHEMA_V1)
_AGG_QUERY = spec.Query("_agg", "internal", (), kind="agg")


# --- predicates -> expressions ----------------------------------------------------------------


def _as_utc(value: dt.datetime) -> dt.datetime:
    """Timestamps in the spec are tz-aware UTC; a naive one is a bug, not something to guess."""
    if value.tzinfo is None:
        raise ValueError(f"naive datetime in predicate: {value!r} (spec timestamps are UTC-aware)")
    return value.astimezone(dt.UTC)


def to_iceberg_expr(preds: tuple[spec.Pred, ...]) -> BooleanExpression:
    """AND of the predicates as a PyIceberg expression; no predicates -> AlwaysTrue().

    A tz-aware datetime becomes a TimestampLiteral in epoch microseconds, which binds to a
    `timestamptz` column without conversion. `>=` is inclusive and `<` exclusive, exactly as in
    the spec.
    """
    exprs: list[BooleanExpression] = []
    for p in preds:
        value = _as_utc(p.value) if isinstance(p.value, dt.datetime) else p.value
        match p.op:
            case "==":
                exprs.append(EqualTo(p.column, value))
            case ">=":
                exprs.append(GreaterThanOrEqual(p.column, value))
            case "<":
                exprs.append(LessThan(p.column, value))
            case _:
                raise ValueError(f"unsupported operator {p.op!r}")
    if not exprs:
        return AlwaysTrue()
    return reduce(And, exprs)


def _scalar(value: int | str | dt.datetime) -> pa.Scalar:
    if isinstance(value, dt.datetime):
        return pa.scalar(_as_utc(value), type=_TS_TYPE)
    if isinstance(value, bool):
        raise TypeError("boolean predicate values are not part of the spec")
    if isinstance(value, int):
        return pa.scalar(value, type=pa.int64())
    return pa.scalar(value, type=pa.string())


def to_arrow_expr(preds: tuple[spec.Pred, ...]) -> pc.Expression:
    """AND of the predicates as a pyarrow compute Expression (for datasets and `Table.filter`).

    No predicates -> `pc.scalar(True)`, a valid always-true Expression, so callers can pass the
    result straight to `filter` without a None check.
    """
    exprs: list[pc.Expression] = []
    for p in preds:
        field, value = pc.field(p.column), _scalar(p.value)
        match p.op:
            case "==":
                exprs.append(field == value)
            case ">=":
                exprs.append(field >= value)
            case "<":
                exprs.append(field < value)
            case _:
                raise ValueError(f"unsupported operator {p.op!r}")
    if not exprs:
        return pc.scalar(True)
    return reduce(lambda a, b: a & b, exprs)


# --- aggregate (Q6) ---------------------------------------------------------------------------


def aggregate(t: pa.Table) -> pa.Table:
    """Q6: per event_name, `events` = row count, `amount_cents` = sum(round(amount * 100)).

    Rounding is per row (half to even), cast to int64 before summing, so the sum is exact and does
    not depend on the order an engine adds rows in. Null amounts are skipped. A group with no
    non-null amount gets 0 (not null): the output has no nulls, so engines cannot disagree on
    null-vs-zero. Output is AGG_SCHEMA, sorted by event_name. Needs only `event_name` and
    `amount`; works on any input chunking.
    """
    cents = pc.cast(pc.round(pc.multiply(t["amount"], 100.0), ndigits=0), pa.int64())
    work = pa.table({"event_name": t["event_name"], "amount_cents": cents})
    grouped = work.group_by("event_name").aggregate([([], "count_all"), ("amount_cents", "sum")])
    out = pa.table(
        {
            "event_name": grouped["event_name"],
            "events": grouped["count_all"],
            "amount_cents": pc.fill_null(grouped["amount_cents_sum"], 0),
        }
    )
    return canonicalize(out, _AGG_QUERY)


# --- canonical form ---------------------------------------------------------------------------


def canonicalize(t: pa.Table, query: spec.Query) -> pa.Table:
    """Bring any engine's result for `query` to one comparable form.

    Rows queries: V1 columns only, cast to CANONICAL_SCHEMA (all nullable, no field metadata such
    as Iceberg field ids, timestamps in microseconds UTC, plain `string`), sorted by event_id,
    one chunk. Aggregate queries: cast to AGG_SCHEMA, sorted by event_name, one chunk.
    """
    if query.kind == "agg":
        out = t.select([f.name for f in spec.AGG_SCHEMA]).cast(spec.AGG_SCHEMA)
        return out.sort_by("event_name").combine_chunks()
    out = t.select(list(_V1_COLUMNS)).cast(spec.CANONICAL_SCHEMA)
    return out.sort_by("event_id").combine_chunks()


def _hash_column(h, col: pa.ChunkedArray) -> None:
    """Feed one column into `h` as validity bytes + value bytes, whatever its chunking."""
    arr = col.combine_chunks()
    if isinstance(arr, pa.ChunkedArray):  # zero chunks
        arr = pa.array([], type=col.type)
    n = len(arr)
    valid = pc.is_valid(arr).to_numpy(zero_copy_only=False).astype(np.uint8)
    h.update(valid.tobytes())
    if pa.types.is_string(arr.type):
        arr = pc.fill_null(arr, "")
        lengths = pc.binary_length(arr).to_numpy(zero_copy_only=False).astype(np.int64)
        h.update(lengths.tobytes())
        offsets = np.frombuffer(arr.buffers()[1], dtype=np.int32)[arr.offset : arr.offset + n + 1]
        data = arr.buffers()[2]
        if data is not None and n:
            h.update(memoryview(data)[int(offsets[0]) : int(offsets[-1])])
    elif pa.types.is_floating(arr.type):
        # + 0.0 folds -0.0 into 0.0; nulls become 0.0 (the validity bytes tell them apart).
        values = pc.fill_null(arr, 0.0).to_numpy(zero_copy_only=False) + 0.0
        h.update(values.astype("<f8").tobytes())
    else:  # int64, timestamp[us, UTC]
        values = pc.fill_null(arr, 0).cast(pa.int64()).to_numpy(zero_copy_only=False)
        h.update(values.astype("<i8").tobytes())


def fingerprint(t: pa.Table) -> str:
    """SHA-256 (hex) of a canonical table, independent of chunking.

    The hash covers column names and types, the row count, and per column a validity byte per row
    followed by the values (string lengths then bytes; floats and ints as little-endian 8-byte
    words; timestamps as int64 microseconds). Call it on the output of `canonicalize`: two tables
    that are equal under `Table.equals` give the same value however they are chunked. Supported
    column types are the ones in CANONICAL_SCHEMA and AGG_SCHEMA.
    """
    h = hashlib.sha256()
    columns = "|".join(f"{f.name}:{f.type}" for f in t.schema)
    h.update(f"{columns}|rows={t.num_rows};".encode())
    for name in t.column_names:
        _hash_column(h, t[name])
    return h.hexdigest()


# --- the oracle and the Iceberg path ------------------------------------------------------------


def _oracle_mask(t: pa.Table, preds: tuple[spec.Pred, ...]):
    """Row mask built column by column with plain compute kernels (no expression machinery)."""
    mask = None
    for p in preds:
        col, value = t[p.column], _scalar(p.value)
        if p.op == "==":
            m = pc.equal(col, value)
        elif p.op == ">=":
            m = pc.greater_equal(col, value)
        elif p.op == "<":
            m = pc.less(col, value)
        else:
            raise ValueError(f"unsupported operator {p.op!r}")
        m = pc.fill_null(m, False)
        mask = m if mask is None else pc.and_(mask, m)
    return mask


def apply_query(t: pa.Table, query: spec.Query) -> pa.Table:
    """The in-memory oracle: run `query` on a plain Arrow table, return its canonical form."""
    mask = _oracle_mask(t, query.preds)
    rows = t if mask is None else t.filter(mask)
    if query.kind == "agg":
        return aggregate(rows)
    return canonicalize(rows, query)


def run_iceberg(table: Table, query: spec.Query, snapshot_id: int | None = None) -> pa.Table:
    """Run `query` through an Iceberg scan (optionally at a snapshot); return its canonical form.

    Only V1 columns are read, so a table that gained `country` returns the same result as before.
    Aggregates are computed here from the scanned rows with the same helper the oracle uses.
    """
    fields = ("event_name", "amount") if query.kind == "agg" else _V1_COLUMNS
    scan = table.scan(
        row_filter=to_iceberg_expr(query.preds),
        selected_fields=fields,
        snapshot_id=snapshot_id,
    )
    rows = scan.to_arrow()
    if query.kind == "agg":
        return aggregate(rows)
    return canonicalize(rows, query)
