"""The interface other projects use to read `lake.events` (consumed by 36-data-quality).

`lake.events` is layout L1 (partitioned by day), regime B1 (ten equal appends), ordered
arrival, schema v1.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
from pyiceberg.table import Table

from lakehouse import spec, tables
from lakehouse.catalog import ensure_namespace, get_catalog
from lakehouse.gen import generate_events

WAREHOUSE_ENV = "LAKEHOUSE_WAREHOUSE"
DEFAULT_WAREHOUSE = "./warehouse"
NAMESPACES = ("lake", "bench")

snapshots = tables.snapshots


def _warehouse(warehouse: str | Path | None) -> str | Path:
    return warehouse or os.environ.get(WAREHOUSE_ENV) or DEFAULT_WAREHOUSE


def open_table(
    name: str = "events", namespace: str = "lake", warehouse: str | Path | None = None
) -> Table:
    """Load `namespace.name`; warehouse = argument, else $LAKEHOUSE_WAREHOUSE, else ./warehouse."""
    catalog = get_catalog(_warehouse(warehouse))
    return catalog.load_table(f"{namespace}.{name}")


def to_arrow(table: Table, snapshot_id: int | None = None) -> pa.Table:
    """All rows of the table, as of `snapshot_id` or (default) the current snapshot."""
    if snapshot_id is None:
        return table.scan().to_arrow()
    return tables.time_travel(table, snapshot_id)


def build_canonical_table(
    warehouse: str | Path, rows: int = 50_000, seed: int = spec.SEED
) -> Table:
    """Create `lake.events` as layout L1, regime B1, ordered arrival, schema v1."""
    catalog = get_catalog(warehouse)
    for namespace in NAMESPACES:
        ensure_namespace(catalog, namespace)
    table = tables.create_events_table(catalog, "lake.events", "L1")
    batches = tables.split_batches(generate_events(rows, seed), spec.LARGE_BATCHES, "ordered", seed)
    tables.append_batches(table, batches, "L1")
    return table
