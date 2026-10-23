# Pre-registration — Iceberg layouts under a fixed workload

Written 2026-10-23, before any table, workload or measurement code exists. The definitions it
refers to live in `src/lakehouse/spec.py`, committed with this file. Any later change is a dated
amendment at the bottom, made before the run it governs.

## Question

For a fixed event table and a fixed six-query workload, how many data files and bytes must a scan
read under each layout (unpartitioned / by day / by day + sorted by user), and how much does
small-file accumulation from many tiny appends inflate that before versus after compaction?

## Data

Seeded synthetic events (`gen.py`, seed 35): `event_id, user_id, event_ts, event_name, channel,
platform, amount`, 30 days from 2026-01-01 UTC, diurnal hour weights, user ids drawn with
P(k) ∝ k^-1.1 over `max(200, rows // 20)` users, `amount` only on purchases. `country` is added in
schema v2. Rows are sorted by `event_ts` and `event_id` numbers them in that order.

Scales: **50,000 rows** in CI (gates only, no reported result); **5,000,000 rows** for the
reported run, on this machine.

## Design

| Factor | Levels |
|---|---|
| Layout (primary) | **L0** unpartitioned · **L1** partitioned by `day(event_ts)` · **L2** = L1 + every batch sorted by `(user_id, event_ts, event_id)` before append |
| Layout (sensitivity) | **L2s** = L2 with `write.target-file-size-bytes` = 1 MiB |
| Ingest regime | **B1** 10 appends of equal size · **B2** 500 appends · **B2c** B2 after compaction |
| Arrival order | **ordered** (each batch is a consecutive slice of `event_id`, i.e. of time) · **shuffled** (seeded permutation, then sliced) |
| Engine | **Iceberg** (primary) · **Delta** via delta-rs (descriptive only, no hypothesis) |

Compaction rewrites the whole current table in one overwrite transaction, with the layout's own
write rule: L0/L1 rows ordered by `event_id`, L2/L2s by the sort key. Default target file size
(512 MiB) unless the layout sets one.

### Workload (fixed)

| Query | Predicate |
|---|---|
| Q1_point_user | `user_id = 137` |
| Q2_one_day | `event_ts` in [day 10, day 11) |
| Q3_seven_days | `event_ts` in [day 10, day 17) |
| Q4_purchases | `event_name = 'purchase'` |
| Q5_user_week | `user_id = 137` and `event_ts` in [day 10, day 17) |
| Q6_full_agg | none; per `event_name`: row count and `sum(round(amount * 100))` as an integer |

### Metrics

Per (engine, layout, regime, arrival, query), from table metadata, no data read:
`files_planned`, `bytes_planned`, `records_planned` (what the scan planner keeps after partition
and min/max pruning), against `files_total`, `bytes_total`, `records_total` and, for Iceberg,
`manifests_total`. For Iceberg the planner is PyIceberg's own `plan_files()`.

Exploratory, no hypothesis: Parquet row groups in planned files whose footer statistics could
match; wall-clock planning and scan times (local machine, median of 5, recorded with what else was
running).

## Correctness gates — all must pass or the run is invalid

- **G1 results.** Every query result, in every cell, equals the in-memory oracle (`apply_query` on
  the generated table): same canonical rows, compared by SHA-256 of a canonical form.
- **G2 time travel.** In B1, reading snapshot *k* returns exactly the rows of batches 1..*k*, for
  every layout and arrival; the pre-compaction snapshot still returns exactly the B2 rows after
  compaction.
- **G3 schema evolution.** Adding `country` (optional string) to `lake.events` leaves the set of
  data files unchanged (zero rewritten), old rows read `country` as null, and a v2 append works.
- **G4 compaction.** Compaction changes no query result (G1 on B2c) and does not lose or duplicate
  rows.

An invalid run reports no verdict. The bug is fixed, the whole grid re-run, and the failure logged
in `NOTES.md`; the hypotheses below do not change.

## Hypotheses and decision rules (Iceberg, reported run)

**H1 — day partitioning prunes date queries.** Within each arrival order, regime B1:
supported iff L1 has strictly fewer `files_planned` **and** strictly fewer `bytes_planned` than L0
for both Q2 and Q3. Reported separately for ordered and shuffled.
*Expectation:* supported under shuffled. Under ordered, L0's files already have narrow `event_ts`
ranges, so the gap may be small or absent; the ratio L0/L1 of `bytes_planned` is reported either
way.

**H2 — sorting by user prunes user queries.** Within each arrival order, regime B1: supported iff
L2 has strictly fewer `files_planned` than L1 for both Q1 and Q5.
*Expectation:* not supported at the default target file size (each day partition of each append is
one file spanning every user id). The L2s arm tests whether smaller files change that; its result
is reported as sensitivity and does not decide H2.

**H3 — compaction undoes small-file accumulation.** For each primary layout and arrival order:
supported iff `files_total(B2) / files_total(B2c) >= 10` and G4 holds.
*Also reported:* `bytes_planned` per query before and after compaction, because compacting L0 into
one file removes the min/max pruning that ordered arrival gave it.

**Null results are results.** Partitioning by day doing nothing for user lookups is expected and
reported; sorting giving no file-level pruning because file min/max ranges overlap is a legitimate
finding, not a failure.

## What would make me distrust the numbers

- A planned-file count that differs between two runs of the same seed (metadata must be
  deterministic apart from file names and snapshot ids).
- A Delta number computed by our own evaluator that disagrees with delta-rs's own pruning on the
  same predicate.
- `records_planned` lower than `result_rows` for any query (the planner would have dropped
  matching data).

## Limits stated up front

Synthetic data with a chosen skew and diurnal shape; one machine; local filesystem for the
reported run (MinIO run is a smoke check, not a benchmark). Planned files and bytes measure I/O
the planner allows, not time. PyIceberg is not Spark: real Iceberg deployments compact with
`rewrite_data_files`, sort on write and may read row-group statistics differently.
