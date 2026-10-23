# lakehouse — E5

**Tier:** 3 · **Category:** Data engineering · **Wave:** 4

The project's own engineering guide. Keep it short and current.

## What this is

A measurement of Apache Iceberg table layouts. A fixed event table and a fixed six-query workload
are written under three layouts (unpartitioned, partitioned by day, partitioned by day and sorted
by user) and two ingest regimes (10 large appends, 500 tiny appends, then compaction). For every
query the harness records how many data files and bytes a scan must read, taken from table
metadata, so the numbers are deterministic. Every query result is checked against an in-memory
oracle. A thin Delta Lake arm (delta-rs, no JVM) runs the same workload for comparison.

The experiment is fixed in `PREREGISTRATION.md`. `src/lakehouse/spec.py` holds everything it
depends on (schema, layouts, regimes, queries, metric rows); changing that file needs a dated
amendment first.

## Stack

Python 3.13 · uv · PyIceberg 0.12 (SQL catalog on SQLite, pyarrow FileIO) · pyarrow · delta-rs
(`deltalake`) · numpy · MinIO via Docker Compose for the optional S3 run. No JVM, no Spark.

## Module contracts

| Module | Public API | Notes |
|---|---|---|
| `spec.py` | constants, `LAYOUTS`, `QUERIES`, `Pred`, `Query`, `PlanMetrics`, `MetricRow`, Arrow schemas | Frozen by the pre-registration |
| `catalog.py` | `get_catalog(warehouse, name="lake", catalog_dir=None) -> SqlCatalog`, `ensure_namespace(catalog, ns)`, `warehouse_location(w)` | Local warehouses as POSIX paths, never `file://` (breaks on Windows) |
| `gen.py` | `generate_events(rows, seed=SEED) -> pa.Table` (exactly `ARROW_SCHEMA_V1`), `generate_v2_events(rows, seed, first_event_id) -> pa.Table` (`ARROW_SCHEMA_V2`) | Deterministic for a seed; rows sorted by `event_ts`, `event_id` = 0..n-1 in that order |
| `tables.py` | `create_events_table(catalog, identifier, layout, replace=True)`, `split_batches(t, n, arrival, seed)`, `prepare_batch(batch, layout, table)`, `append_batches(table, batches, layout) -> list[int]`, `snapshots(table)`, `time_travel(table, snapshot_id)`, `data_file_paths(table, snapshot_id=None)`, `evolve_add_country(table)`, `compact(table, layout) -> CompactionResult`, `layout_of(table)` | L2/L2s sort each batch by `SORT_KEY` before append: PyIceberg does not sort on write |
| `workload.py` | `to_iceberg_expr(preds)`, `to_arrow_expr(preds)`, `apply_query(t, query)` (in-memory oracle: numpy filter and plain-Python aggregate, sharing no code with the engine path), `run_iceberg(table, query, snapshot_id=None)`, `canonicalize(t, query)`, `fingerprint(t) -> str` | Results compared as canonical tables (`CANONICAL_SCHEMA` / `AGG_SCHEMA`, sorted) |
| `pruning.py` | `plan_metrics(table, query, snapshot_id=None, rowgroups=False) -> PlanMetrics`, `table_totals(table, snapshot_id=None)`, `plan_files_only(...)` | Planned files = PyIceberg `scan(row_filter).plan_files()`; no data read; refuses plans with delete files |
| `delta_arm.py` | `write_delta(path, batches, layout) -> list[int]`, `delta_plan_metrics(path, query, version=None)`, `delta_stats_planned(...)`, `delta_native_planned(...)`, `run_delta(path, query, version=None)`, `read_delta(path, version=None)`, `delta_version(path)`, `compact_delta(path, layout)`, `evolve_delta_add_country(path)`, `delta_files(path, version=None)` | Descriptive arm, no hypothesis. Planned files come from delta-rs's own pruning (pyarrow dataset fragments carrying its stats guarantees), cross-checked by a stats evaluator |
| `contract.py` | `open_table(name="events", namespace="lake", warehouse=None)`, `snapshots(table)`, `to_arrow(table, snapshot_id=None)` | The interface `36-data-quality` consumes |
| `experiment.py` | `run_grid(config, log, jobs=1, verbose=False) -> GridResult` | Builds every cell (each with its own warehouse and catalog, in worker processes when `jobs` > 1), runs every query, checks G1–G4 and D1 |
| `report.py` | `evaluate(rows, gates) -> list[Verdict]`, `render_markdown(...)`, `write_csv(rows, path)` | Verdicts are mechanical, from the rules in the pre-registration |
| `cli.py` | `lakehouse init / ingest / evolve-schema / time-travel / compact / workload-report / render` | `compact` infers the layout's write rule from table metadata |

`contract.py` (for #36): `lake.events` is layout L1, regime B1, ordered arrival, schema v1.
`snapshots(table)` returns `[(snapshot_id, committed_at: datetime UTC, total_records)]` oldest
first; `to_arrow(table, snapshot_id=None)` returns that snapshot's rows.

## Acceptance criteria

- [ ] Every correctness gate (G1–G4 in the pre-registration) passes at 50k rows in CI and at the
      main local scale.
- [ ] `lakehouse workload-report` writes `results/pruning.csv` and `results/REPORT.md` with the
      verdicts computed by code, not by hand.
- [ ] Delta arm runs the same workload; differences from Iceberg are documented, not hidden.
- [ ] Runs from a clean clone using only the documented steps.
- [ ] Tests, lint and CI green; README results come from real runs only.

## Conventions

- `uv` for dependencies, pinned exactly; `ruff check` + `ruff format`; `pytest`.
- Tests write warehouses under pytest's `tmp_path`, never into the repo. Warehouses, Parquet and
  catalog databases are never committed.
- Local pytest on this machine: `--basetemp=<scratch dir>` (the sandbox blocks `%TEMP%`). Never put
  that in `pyproject.toml`.
- Tests marked `minio` need `docker compose up -d` and are deselected by default
  (`uv run pytest -m minio` runs them).
- No secrets. MinIO credentials are the documented local-development defaults only.

## Project-specific notes

- PyIceberg 0.12 has no data-file rewrite (its `maintenance` has only `expire_snapshots`), so
  compaction is a rewrite in one overwrite transaction. Old snapshots keep their files until
  snapshots are expired, so time travel across a compaction still works.
- That overwrite is one metadata commit but two snapshots: a DELETE snapshot with 0 records, then
  the APPEND with the rewritten rows. Anything that walks `snapshots()` (including #36) sees the
  empty one after a compaction and must not treat it as a real version of the data.
- Delta has no hidden partitioning, so its day layouts store a derived `event_date` partition
  column. Queries filter on `event_ts`, so Delta's date pruning comes from min/max stats, not
  partition values. delta-rs's default target file size is 100 MiB (Iceberg's is 512 MiB);
  `optimize.compact()` does not re-sort, so a compacted L2 Delta file is a run of sorted pieces.
- Delta timestamp statistics are truncated to milliseconds; delta-rs widens the max by 1 ms when
  pruning, and the cross-check evaluator in `delta_arm.py` does the same.
- PyIceberg ignores a table's sort order when writing. The harness sorts L2/L2s batches itself and
  records the sort order in table metadata. With the default 512 MiB target file size each day
  partition of each append is one file spanning every user id, so file-level pruning by user is
  expected to need the smaller target of L2s.
- Ordered arrival (each append covers a time slice) gives even an unpartitioned table tight
  `event_ts` min/max per file. That is why the pre-registration also runs shuffled arrival.
