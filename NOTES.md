# Build Notes — lakehouse

Working notes: what broke, what was tried, why X over Y.

---

## Log

### 2026-10-23 — probing the libraries before writing contracts

- **Tried:** PyIceberg's SQL catalog with a `file:///C:/...` warehouse on Windows.
- **Broke:** every metadata write failed with `WinError 123`: the pyarrow FileIO turns
  `file:///C:/x` into `/C:/x`.
- **Fixed by:** passing local warehouses as absolute POSIX paths (`C:/x`). The fsspec FileIO also
  works with URIs, but one rule in `catalog.py` is simpler than a second IO implementation.
- **Learned:** check a library on the target OS before designing around it.

- **Tried:** day partitioning without `pyiceberg-core`.
- **Broke:** `NotInstalledError` on the first partitioned append: the partition transforms on
  Arrow data live in the Rust extension.
- **Fixed by:** the `pyiceberg-core` extra, pinned.

- **Tried:** a table sort order, expecting PyIceberg to sort on write.
- **Broke:** nothing failed, which was the problem: a 200k-row append with a `user_id` sort
  order produced 27 files that each spanned user ids 1 to 9,999.
- **Fixed by:** L2 sorts each batch itself before append and records the sort order in metadata
  only. This shaped H2: at the default 512 MiB target a day partition of one append is a single
  file, so sorting cannot prune files by user. The L2s arm (1 MiB target) was added to the
  pre-registration to test whether smaller files change that.

- **Tried:** looking for compaction (`rewrite_data_files`) in PyIceberg 0.12.
- **Found:** `Table.maintenance` has only `expire_snapshots`. Compaction is therefore a full
  rewrite through `table.overwrite`, the brief's fallback. It is one metadata commit but two
  snapshots (DELETE with 0 records, then APPEND); the empty one is visible to anything that walks
  the snapshot list.

### 2026-10-23 — pre-registration choices

- Ordered arrival gives an unpartitioned table tight `event_ts` min/max per file, so L0 already
  prunes date queries. Measuring only ordered arrival would have made "partitioning does nothing"
  look like a finding about partitioning when it is a finding about ingest order. Shuffled arrival
  is the contrast.
- Q6 sums amounts as integer cents, so the sum is exact whatever order an engine adds rows in;
  a float sum would differ in the last bits between layouts and break result equality for no
  real reason.

### 2026-10-23 — parallel build

- Four modules were built in parallel against `spec.py` and the contracts in `GUIDELINES.md`;
  the grid runner and CLI were written against the same contracts and integrated afterwards.
- The first fingerprint used `pyarrow.csv` with full quoting: 22 s for 300k rows. Replaced by a
  per-column hash of validity bitmaps and values: about 1 s, and independent of chunking.
- Each Iceberg commit costs about 0.3 s on this machine (SQLite catalog plus manifest and
  manifest-list writes; 265 ms ordered, 374 ms shuffled with 30 files per commit). The first
  integration grid (3,000 rows, 24 tiny appends) took 5 minutes, so the CI grids were shrunk;
  the reported numbers come from the local run, not from CI.
- The CLI's `render` re-rendered a different header order than the run: `meta.json` was written
  with sorted keys. Written in insertion order now, and a test checks the re-render is identical.

### 2026-10-23 — review, Amendment 1, 50k gate run

- An independent review (fresh context) found the registered H1/H2 rules count files *written*,
  not files pruned: day partitioning splits every append into one file per day, so L1 can never
  plan fewer files than L0. Its 5M-row B1 dry run showed L1 reading 28x fewer bytes and still
  "not supported". The rules were kept; a bytes-only reading was added as Amendment 1, labelled
  as defined after the dry run.
- Other review fixes: the Q6 oracle shared `aggregate()` with the engine path (now plain Python);
  G3 checked v2 rows only by null count (now full equality); G4's row count was the in-memory
  count (now read back); the Delta cross-check was stored but never checked (now D1, three ways:
  fragments, delta-rs native `file_uris`, own evaluator).
- delta-rs's fragment path widens truncated millisecond timestamp stats by 1 ms, so at an exact
  millisecond boundary it can keep one file more than the native pruner. Conservative, never
  drops a match; D1 would show it.
- Grid cells now run in worker processes (`--jobs`), each with its own warehouse and SQLite
  catalog. A test runs the same grid serially and with 2 workers and compares every metric.
- **50,000-row gate run, all 16 cells, 500 tiny appends** (`--jobs 6`, other sessions active,
  4,521 s): G1 528/528, G2 176, G3 5, G4 24, D1 144/144 passed. Shuffled partitioned B2 tables
  had 14,474 files. Each cell took 8–35 minutes: the cost is commits, not data.
- Caution from that run (CI-scale data, not the reported scale): the bytes-only rule called H2b
  "supported" on a 1.01x difference, which is compression noise from re-sorted files. The rule
  stays as written; the report prints every ratio, and the write-up must not lean on a
  "supported" that rests on 1%.

---

## Rejected approaches

| Approach | Why rejected |
|---|---|
| Spark or Flink for Iceberg | A JVM for a single-machine measurement; PyIceberg's planner is enough to count files and bytes from metadata |
| Wall-clock time as the main metric | Depends on the machine and what else runs; planned files and bytes are deterministic and testable in CI |
| Our own min/max evaluator as Delta's "planned files" | It would measure our code, not delta-rs. delta-rs's pyarrow dataset fragments carry its stats as guarantees, so `get_fragments(filter=...)` is the engine's own pruning; the evaluator stays as a cross-check |
| Float `sum(amount)` in Q6 | Order-dependent rounding would fail result equality between layouts |
| `pyarrow.csv` for fingerprints | 20x slower than hashing column buffers |

## Open questions

- [ ] Does real Iceberg (Spark `rewrite_data_files` with a sort strategy) change H2's answer at
      default file sizes? Out of scope here; worth one sentence in the README.
