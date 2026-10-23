"""Create, fill, inspect and compact the Iceberg events table under each layout."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from lakehouse import spec

TARGET_FILE_SIZE_PROPERTY = "write.target-file-size-bytes"
PARTITION_FIELD_ID = 1000


def iceberg_schema(version: int = 1) -> Schema:
    """Iceberg schema matching the Arrow schema of that version (field ids 1..7, or 1..8)."""
    fields = [
        NestedField(1, "event_id", LongType(), required=True),
        NestedField(2, "user_id", LongType(), required=True),
        NestedField(3, "event_ts", TimestamptzType(), required=True),
        NestedField(4, "event_name", StringType(), required=True),
        NestedField(5, "channel", StringType(), required=True),
        NestedField(6, "platform", StringType(), required=True),
        NestedField(7, "amount", DoubleType(), required=False),
    ]
    if version == 2:
        fields.append(NestedField(8, "country", StringType(), required=False))
    elif version != 1:
        raise ValueError(f"unknown schema version {version}")
    return Schema(*fields)


def _partition_spec(schema: Schema, by_day: bool) -> PartitionSpec:
    if not by_day:
        return PartitionSpec()
    return PartitionSpec(
        PartitionField(
            source_id=schema.find_field("event_ts").field_id,
            field_id=PARTITION_FIELD_ID,
            transform=DayTransform(),
            name="event_ts_day",
        )
    )


def _sort_order(schema: Schema, by_user: bool) -> SortOrder:
    if not by_user:
        return SortOrder()
    return SortOrder(
        *(
            SortField(
                source_id=schema.find_field(column).field_id,
                transform=IdentityTransform(),
                direction=SortDirection.ASC,
                null_order=NullOrder.NULLS_FIRST,
            )
            for column, _ in spec.SORT_KEY
        )
    )


def create_events_table(
    catalog: Catalog, identifier: str, layout: str, replace: bool = True
) -> Table:
    """Create the v1 events table with the partition spec, sort order and file size of `layout`.

    With replace=True an existing table of that name is dropped first, and its files purged.
    """
    ls = spec.LAYOUTS[layout]
    if replace and catalog.table_exists(identifier):
        catalog.purge_table(identifier)
    schema = iceberg_schema(1)
    properties = {}
    if ls.target_file_size_bytes is not None:
        properties[TARGET_FILE_SIZE_PROPERTY] = str(ls.target_file_size_bytes)
    return catalog.create_table(
        identifier,
        schema=schema,
        partition_spec=_partition_spec(schema, ls.partition_by_day),
        sort_order=_sort_order(schema, ls.sort_by_user),
        properties=properties,
    )


def split_batches(t: pa.Table, n: int, arrival: str, seed: int) -> list[pa.Table]:
    """Cut `t` into `n` batches whose sizes differ by at most one.

    "ordered": consecutive slices of `t` as given. "shuffled": a seeded permutation of the rows
    first. Concatenating the batches gives back the rows of `t` (reordered if shuffled).
    """
    rows = t.num_rows
    if not 1 <= n <= rows:
        raise ValueError(f"need 1 <= n <= rows, got n={n}, rows={rows}")
    if arrival == "shuffled":
        t = t.take(pa.array(np.random.default_rng(seed).permutation(rows)))
    elif arrival != "ordered":
        raise ValueError(f"unknown arrival {arrival!r}")
    base, extra = divmod(rows, n)
    batches, start = [], 0
    for i in range(n):
        size = base + (1 if i < extra else 0)
        batches.append(t.slice(start, size))
        start += size
    return batches


def prepare_batch(batch: pa.Table, layout: str, table: Table) -> pa.Table:
    """Apply the layout's write rule (sort for L2/L2s) and cast to the table's Arrow schema.

    PyIceberg does not sort on write even when the table records a sort order, so this is where
    the sorted layouts get their sort.
    """
    if spec.LAYOUTS[layout].sort_by_user:
        batch = batch.sort_by(list(spec.SORT_KEY))
    return batch.cast(table.schema().as_arrow())


def append_batches(table: Table, batches: list[pa.Table], layout: str) -> list[int]:
    """One append (one commit) per batch; returns the snapshot id after each."""
    snapshot_ids = []
    for batch in batches:
        table.append(prepare_batch(batch, layout, table))
        snapshot_ids.append(table.current_snapshot().snapshot_id)
    return snapshot_ids


def snapshots(table: Table) -> list[tuple[int, dt.datetime, int]]:
    """(snapshot_id, committed_at in UTC, total records), oldest first."""
    out = []
    for snap in sorted(table.snapshots(), key=lambda s: (s.sequence_number or 0, s.timestamp_ms)):
        committed = dt.datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=dt.UTC)
        props = snap.summary.additional_properties if snap.summary else {}
        out.append((snap.snapshot_id, committed, int(props.get("total-records", 0))))
    return out


def time_travel(table: Table, snapshot_id: int) -> pa.Table:
    """The table's rows as of `snapshot_id`."""
    return table.scan(snapshot_id=snapshot_id).to_arrow()


def data_file_paths(table: Table, snapshot_id: int | None = None) -> frozenset[str]:
    """Paths of the data files in a snapshot (default: current); empty for an empty table."""
    if snapshot_id is None and table.current_snapshot() is None:
        return frozenset()
    tasks = table.scan(snapshot_id=snapshot_id).plan_files()
    return frozenset(task.file.file_path for task in tasks)


def evolve_add_country(table: Table) -> None:
    """Add the optional string column `country` (metadata only: no data file is rewritten)."""
    with table.update_schema() as update:
        update.add_column("country", StringType())


@dataclass(frozen=True)
class CompactionResult:
    files_before: int
    files_after: int
    snapshot_before: int
    snapshot_after: int
    rows_before: int
    rows_after: int


def compact(table: Table, layout: str) -> CompactionResult:
    """Rewrite the whole current table with the layout's write rule, in one commit.

    Rows are ordered by event_id (L0/L1) or by SORT_KEY (L2/L2s), then written with a single
    `table.overwrite`. Files are written with the table's target file size.

    Measured with PyIceberg 0.12: `overwrite` is one metadata commit but records TWO snapshots,
    a DELETE snapshot (total-records 0) followed by an APPEND snapshot with the rewritten rows.
    Readers of the table's current state never see the empty intermediate, but a time-travel
    read of the DELETE snapshot is empty, and `snapshots()` lists both. Old snapshots keep
    pointing at the old files, so reads at those snapshots are unchanged.
    """
    snapshot_before = table.current_snapshot().snapshot_id
    files_before = len(data_file_paths(table))
    rows = table.scan().to_arrow()
    keys = spec.SORT_KEY if spec.LAYOUTS[layout].sort_by_user else (("event_id", "ascending"),)
    ordered = rows.sort_by(list(keys)).cast(table.schema().as_arrow())
    table.overwrite(ordered)
    return CompactionResult(
        files_before=files_before,
        files_after=len(data_file_paths(table)),
        snapshot_before=snapshot_before,
        snapshot_after=table.current_snapshot().snapshot_id,
        rows_before=rows.num_rows,
        rows_after=ordered.num_rows,
    )
