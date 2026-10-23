# Lakehouse layout experiment — report

Verdicts below are computed by code from the decision rules in `PREREGISTRATION.md`, applied to the Iceberg rows only; nothing in this file is edited by hand.

## Run metadata

| key | value |
|---|---|
| rows | 5000000 |
| seed | 35 |
| layouts | L0,L1,L2,L2s |
| arrivals | ordered,shuffled |
| large_batches | 10 |
| tiny_batches | 500 |
| delta | True |
| warehouse | local filesystem |
| git | dbc2dda |
| python | 3.13.13 |
| platform | Windows-11-10.0.26200-SP0 |
| pyiceberg | 0.12.0 |
| deltalake | 1.6.6 |
| pyarrow | 25.0.1 |
| started_utc | 2026-10-23T13:24:33+00:00 |
| note | reported run; other interactive sessions and browsers active (metadata metrics only, no timing) |
| jobs | 5 |
| finished_utc | 2026-10-23T14:20:17+00:00 |

## Correctness gates

| gate | result | detail |
|---|---|---|
| G1 | pass | 528 checks passed |
| G2 | pass | 176 checks passed |
| G3 | pass | 5 checks passed |
| G4 | pass | 24 checks passed |
| D1 | pass | 144 checks passed |

## Verdicts

| hypothesis | scope | status | evidence |
|---|---|---|---|
| H1 | ordered | not supported | L1 vs L0; Q2_one_day: files 1 vs 1, bytes 1.8 MiB vs 4.6 MiB (bytes ratio 2.53x) -> fail; Q3_seven_days: files 9 vs 3, bytes 12.8 MiB vs 13.7 MiB (bytes ratio 1.07x) -> fail |
| H1 | shuffled | not supported | L1 vs L0; Q2_one_day: files 10 vs 10, bytes 2.6 MiB vs 73.9 MiB (bytes ratio 28.22x) -> fail; Q3_seven_days: files 70 vs 10, bytes 18.3 MiB vs 73.9 MiB (bytes ratio 4.04x) -> fail |
| H2 | ordered | not supported | L2 vs L1; Q1_point_user: files 39 vs 39 -> fail (bytes 66.8 MiB vs 55.8 MiB, ratio 0.84x); Q5_user_week: files 9 vs 9 -> fail (bytes 15.5 MiB vs 12.8 MiB, ratio 0.83x) |
| H2 | shuffled | not supported | L2 vs L1; Q1_point_user: files 300 vs 300 -> fail (bytes 70.9 MiB vs 78.6 MiB, ratio 1.11x); Q5_user_week: files 70 vs 70 -> fail (bytes 16.5 MiB vs 18.3 MiB, ratio 1.11x) |
| H3 | L0/ordered | supported | files_total B2 500 vs B2c 1 = 500.00x (needs >= 10x); G4 passed |
| H3 | L0/shuffled | supported | files_total B2 500 vs B2c 1 = 500.00x (needs >= 10x); G4 passed |
| H3 | L1/ordered | supported | files_total B2 529 vs B2c 30 = 17.63x (needs >= 10x); G4 passed |
| H3 | L1/shuffled | supported | files_total B2 15000 vs B2c 30 = 500.00x (needs >= 10x); G4 passed |
| H3 | L2/ordered | supported | files_total B2 529 vs B2c 30 = 17.63x (needs >= 10x); G4 passed |
| H3 | L2/shuffled | supported | files_total B2 15000 vs B2c 30 = 500.00x (needs >= 10x); G4 passed |

## Secondary reading (Amendment 1, defined after a dry run)

H1b and H2b decide on planned bytes alone (strictly fewer bytes for both queries; files ignored). They were defined after seeing a B1 dry run and do not replace H1 and H2, which stand as the primary verdicts.

| hypothesis | scope | status | evidence |
|---|---|---|---|
| H1b | ordered | supported | L1 vs L0; Q2_one_day: bytes 1.8 MiB vs 4.6 MiB (bytes ratio 2.53x) -> pass (files 1 vs 1); Q3_seven_days: bytes 12.8 MiB vs 13.7 MiB (bytes ratio 1.07x) -> pass (files 9 vs 3) |
| H1b | shuffled | supported | L1 vs L0; Q2_one_day: bytes 2.6 MiB vs 73.9 MiB (bytes ratio 28.22x) -> pass (files 10 vs 10); Q3_seven_days: bytes 18.3 MiB vs 73.9 MiB (bytes ratio 4.04x) -> pass (files 70 vs 10) |
| H2b | ordered | not supported | L2 vs L1; Q1_point_user: bytes 66.8 MiB vs 55.8 MiB (bytes ratio 0.84x) -> fail (files 39 vs 39); Q5_user_week: bytes 15.5 MiB vs 12.8 MiB (bytes ratio 0.83x) -> fail (files 9 vs 9) |
| H2b | shuffled | supported | L2 vs L1; Q1_point_user: bytes 70.9 MiB vs 78.6 MiB (bytes ratio 1.11x) -> pass (files 300 vs 300); Q5_user_week: bytes 16.5 MiB vs 18.3 MiB (bytes ratio 1.11x) -> pass (files 70 vs 70) |

## Sensitivity (H2-L2s)

Same rules as H2 and H2b with the 1 MiB target-file-size arm (L2s) in place of L2: H2-L2s decides on files, H2b-L2s (secondary, Amendment 1) on bytes. Reported for information; they do not decide H2.

| hypothesis | scope | status | evidence |
|---|---|---|---|
| H2-L2s | ordered | not supported | L2s vs L1; Q1_point_user: files 39 vs 39 -> fail (bytes 5.3 MiB vs 55.8 MiB, ratio 10.48x); Q5_user_week: files 9 vs 9 -> fail (bytes 1.2 MiB vs 12.8 MiB, ratio 10.37x) |
| H2-L2s | shuffled | not supported | L2s vs L1; Q1_point_user: files 300 vs 300 -> fail (bytes 54.9 MiB vs 78.6 MiB, ratio 1.43x); Q5_user_week: files 70 vs 70 -> fail (bytes 12.8 MiB vs 18.3 MiB, ratio 1.43x) |
| H2b-L2s | ordered | supported | L2s vs L1; Q1_point_user: bytes 5.3 MiB vs 55.8 MiB (bytes ratio 10.48x) -> pass (files 39 vs 39); Q5_user_week: bytes 1.2 MiB vs 12.8 MiB (bytes ratio 10.37x) -> pass (files 9 vs 9) |
| H2b-L2s | shuffled | supported | L2s vs L1; Q1_point_user: bytes 54.9 MiB vs 78.6 MiB (bytes ratio 1.43x) -> pass (files 300 vs 300); Q5_user_week: bytes 12.8 MiB vs 18.3 MiB (bytes ratio 1.43x) -> pass (files 70 vs 70) |

## Iceberg pruning

Each cell is `files_planned/files_total · bytes_planned (share of bytes_total)`.

### ordered

#### B1

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 10/10 · 45.8 MiB (100.0%) | 39/39 · 55.8 MiB (100.0%) | 39/39 · 66.8 MiB (100.0%) | 39/399 · 5.3 MiB (8.4%) |
| Q2_one_day | 1/10 · 4.6 MiB (10.1%) | 1/39 · 1.8 MiB (3.3%) | 1/39 · 2.2 MiB (3.3%) | 13/399 · 2.2 MiB (3.4%) |
| Q3_seven_days | 3/10 · 13.7 MiB (30.0%) | 9/39 · 12.8 MiB (23.0%) | 9/39 · 15.5 MiB (23.2%) | 93/399 · 14.9 MiB (23.4%) |
| Q4_purchases | 10/10 · 45.8 MiB (100.0%) | 39/39 · 55.8 MiB (100.0%) | 39/39 · 66.8 MiB (100.0%) | 399/399 · 63.6 MiB (100.0%) |
| Q5_user_week | 3/10 · 13.7 MiB (30.0%) | 9/39 · 12.8 MiB (23.0%) | 9/39 · 15.5 MiB (23.2%) | 9/399 · 1.2 MiB (1.9%) |
| Q6_full_agg | 10/10 · 45.8 MiB (100.0%) | 39/39 · 55.8 MiB (100.0%) | 39/39 · 66.8 MiB (100.0%) | 399/399 · 63.6 MiB (100.0%) |

Planned-bytes ratios (first layout / second):

- Q1_point_user: L1/L2 bytes ratio 0.84x; L1/L2s bytes ratio 10.48x
- Q2_one_day: L0/L1 bytes ratio 2.53x
- Q3_seven_days: L0/L1 bytes ratio 1.07x
- Q5_user_week: L1/L2 bytes ratio 0.83x; L1/L2s bytes ratio 10.37x

#### B2

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 500/500 · 53.6 MiB (100.0%) | 529/529 · 53.7 MiB (100.0%) | 529/529 · 60.7 MiB (100.0%) | 529/529 · 60.7 MiB (100.0%) |
| Q2_one_day | 18/500 · 1.9 MiB (3.6%) | 18/529 · 1.8 MiB (3.3%) | 18/529 · 2.1 MiB (3.4%) | 18/529 · 2.1 MiB (3.4%) |
| Q3_seven_days | 118/500 · 12.7 MiB (23.6%) | 124/529 · 12.5 MiB (23.3%) | 124/529 · 14.2 MiB (23.4%) | 124/529 · 14.2 MiB (23.4%) |
| Q4_purchases | 500/500 · 53.6 MiB (100.0%) | 529/529 · 53.7 MiB (100.0%) | 529/529 · 60.7 MiB (100.0%) | 529/529 · 60.7 MiB (100.0%) |
| Q5_user_week | 118/500 · 12.7 MiB (23.6%) | 124/529 · 12.5 MiB (23.3%) | 124/529 · 14.2 MiB (23.4%) | 124/529 · 14.2 MiB (23.4%) |
| Q6_full_agg | 500/500 · 53.6 MiB (100.0%) | 529/529 · 53.7 MiB (100.0%) | 529/529 · 60.7 MiB (100.0%) | 529/529 · 60.7 MiB (100.0%) |

Planned-bytes ratios (first layout / second):

- Q1_point_user: L1/L2 bytes ratio 0.89x; L1/L2s bytes ratio 0.89x
- Q2_one_day: L0/L1 bytes ratio 1.08x
- Q3_seven_days: L0/L1 bytes ratio 1.01x
- Q5_user_week: L1/L2 bytes ratio 0.88x; L1/L2s bytes ratio 0.88x

#### B2c

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 1/1 · 40.1 MiB (100.0%) | 30/30 · 55.9 MiB (100.0%) | 30/30 · 66.6 MiB (100.0%) | 30/390 · 5.2 MiB (8.3%) |
| Q2_one_day | 1/1 · 40.1 MiB (100.0%) | 1/30 · 1.8 MiB (3.3%) | 1/30 · 2.2 MiB (3.3%) | 13/390 · 2.2 MiB (3.4%) |
| Q3_seven_days | 1/1 · 40.1 MiB (100.0%) | 7/30 · 13.0 MiB (23.2%) | 7/30 · 15.4 MiB (23.1%) | 91/390 · 14.8 MiB (23.2%) |
| Q4_purchases | 1/1 · 40.1 MiB (100.0%) | 30/30 · 55.9 MiB (100.0%) | 30/30 · 66.6 MiB (100.0%) | 390/390 · 63.5 MiB (100.0%) |
| Q5_user_week | 1/1 · 40.1 MiB (100.0%) | 7/30 · 13.0 MiB (23.2%) | 7/30 · 15.4 MiB (23.1%) | 7/390 · 1.2 MiB (1.9%) |
| Q6_full_agg | 1/1 · 40.1 MiB (100.0%) | 30/30 · 55.9 MiB (100.0%) | 30/30 · 66.6 MiB (100.0%) | 390/390 · 63.5 MiB (100.0%) |

Planned-bytes ratios (first layout / second):

- Q1_point_user: L1/L2 bytes ratio 0.84x; L1/L2s bytes ratio 10.66x
- Q2_one_day: L0/L1 bytes ratio 22.04x
- Q3_seven_days: L0/L1 bytes ratio 3.08x
- Q5_user_week: L1/L2 bytes ratio 0.84x; L1/L2s bytes ratio 10.67x

### shuffled

#### B1

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 10/10 · 73.9 MiB (100.0%) | 300/300 · 78.6 MiB (100.0%) | 300/300 · 70.9 MiB (100.0%) | 300/600 · 54.9 MiB (78.5%) |
| Q2_one_day | 10/10 · 73.9 MiB (100.0%) | 10/300 · 2.6 MiB (3.3%) | 10/300 · 2.4 MiB (3.3%) | 20/600 · 2.3 MiB (3.3%) |
| Q3_seven_days | 10/10 · 73.9 MiB (100.0%) | 70/300 · 18.3 MiB (23.3%) | 70/300 · 16.5 MiB (23.3%) | 140/600 · 16.3 MiB (23.3%) |
| Q4_purchases | 10/10 · 73.9 MiB (100.0%) | 300/300 · 78.6 MiB (100.0%) | 300/300 · 70.9 MiB (100.0%) | 600/600 · 69.9 MiB (100.0%) |
| Q5_user_week | 10/10 · 73.9 MiB (100.0%) | 70/300 · 18.3 MiB (23.3%) | 70/300 · 16.5 MiB (23.3%) | 70/600 · 12.8 MiB (18.3%) |
| Q6_full_agg | 10/10 · 73.9 MiB (100.0%) | 300/300 · 78.6 MiB (100.0%) | 300/300 · 70.9 MiB (100.0%) | 600/600 · 69.9 MiB (100.0%) |

Planned-bytes ratios (first layout / second):

- Q1_point_user: L1/L2 bytes ratio 1.11x; L1/L2s bytes ratio 1.43x
- Q2_one_day: L0/L1 bytes ratio 28.22x
- Q3_seven_days: L0/L1 bytes ratio 4.04x
- Q5_user_week: L1/L2 bytes ratio 1.11x; L1/L2s bytes ratio 1.43x

#### B2

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 500/500 · 82.4 MiB (100.0%) | 15000/15000 · 118.4 MiB (100.0%) | 15000/15000 · 114.9 MiB (100.0%) | 15000/15000 · 114.9 MiB (100.0%) |
| Q2_one_day | 500/500 · 82.4 MiB (100.0%) | 500/15000 · 3.9 MiB (3.3%) | 500/15000 · 3.8 MiB (3.3%) | 500/15000 · 3.8 MiB (3.3%) |
| Q3_seven_days | 500/500 · 82.4 MiB (100.0%) | 3500/15000 · 27.6 MiB (23.3%) | 3500/15000 · 26.8 MiB (23.3%) | 3500/15000 · 26.8 MiB (23.3%) |
| Q4_purchases | 500/500 · 82.4 MiB (100.0%) | 15000/15000 · 118.4 MiB (100.0%) | 15000/15000 · 114.9 MiB (100.0%) | 15000/15000 · 114.9 MiB (100.0%) |
| Q5_user_week | 500/500 · 82.4 MiB (100.0%) | 3500/15000 · 27.6 MiB (23.3%) | 3500/15000 · 26.8 MiB (23.3%) | 3500/15000 · 26.8 MiB (23.3%) |
| Q6_full_agg | 500/500 · 82.4 MiB (100.0%) | 15000/15000 · 118.4 MiB (100.0%) | 15000/15000 · 114.9 MiB (100.0%) | 15000/15000 · 114.9 MiB (100.0%) |

Planned-bytes ratios (first layout / second):

- Q1_point_user: L1/L2 bytes ratio 1.03x; L1/L2s bytes ratio 1.03x
- Q2_one_day: L0/L1 bytes ratio 20.87x
- Q3_seven_days: L0/L1 bytes ratio 2.98x
- Q5_user_week: L1/L2 bytes ratio 1.03x; L1/L2s bytes ratio 1.03x

#### B2c

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 1/1 · 40.1 MiB (100.0%) | 30/30 · 55.9 MiB (100.0%) | 30/30 · 66.6 MiB (100.0%) | 30/390 · 5.2 MiB (8.3%) |
| Q2_one_day | 1/1 · 40.1 MiB (100.0%) | 1/30 · 1.8 MiB (3.3%) | 1/30 · 2.2 MiB (3.3%) | 13/390 · 2.2 MiB (3.4%) |
| Q3_seven_days | 1/1 · 40.1 MiB (100.0%) | 7/30 · 13.0 MiB (23.2%) | 7/30 · 15.4 MiB (23.1%) | 91/390 · 14.8 MiB (23.2%) |
| Q4_purchases | 1/1 · 40.1 MiB (100.0%) | 30/30 · 55.9 MiB (100.0%) | 30/30 · 66.6 MiB (100.0%) | 390/390 · 63.5 MiB (100.0%) |
| Q5_user_week | 1/1 · 40.1 MiB (100.0%) | 7/30 · 13.0 MiB (23.2%) | 7/30 · 15.4 MiB (23.1%) | 7/390 · 1.2 MiB (1.9%) |
| Q6_full_agg | 1/1 · 40.1 MiB (100.0%) | 30/30 · 55.9 MiB (100.0%) | 30/30 · 66.6 MiB (100.0%) | 390/390 · 63.5 MiB (100.0%) |

Planned-bytes ratios (first layout / second):

- Q1_point_user: L1/L2 bytes ratio 0.84x; L1/L2s bytes ratio 10.66x
- Q2_one_day: L0/L1 bytes ratio 22.04x
- Q3_seven_days: L0/L1 bytes ratio 3.08x
- Q5_user_week: L1/L2 bytes ratio 0.84x; L1/L2s bytes ratio 10.67x

## Compaction (B2 vs B2c)

### ordered

| layout | files_total B2 | files_total B2c | ratio | manifests B2 | manifests B2c | Q1_point_user bytes_planned | Q2_one_day bytes_planned | Q3_seven_days bytes_planned | Q4_purchases bytes_planned | Q5_user_week bytes_planned | Q6_full_agg bytes_planned |
|---|---|---|---|---|---|---|---|---|---|---|---|
| L0 | 500 | 1 | 500.00x | 500 | 1 | 53.6 MiB → 40.1 MiB | 1.9 MiB → 40.1 MiB | 12.7 MiB → 40.1 MiB | 53.6 MiB → 40.1 MiB | 12.7 MiB → 40.1 MiB | 53.6 MiB → 40.1 MiB |
| L1 | 529 | 30 | 17.63x | 500 | 1 | 53.7 MiB → 55.9 MiB | 1.8 MiB → 1.8 MiB | 12.5 MiB → 13.0 MiB | 53.7 MiB → 55.9 MiB | 12.5 MiB → 13.0 MiB | 53.7 MiB → 55.9 MiB |
| L2 | 529 | 30 | 17.63x | 500 | 1 | 60.7 MiB → 66.6 MiB | 2.1 MiB → 2.2 MiB | 14.2 MiB → 15.4 MiB | 60.7 MiB → 66.6 MiB | 14.2 MiB → 15.4 MiB | 60.7 MiB → 66.6 MiB |
| L2s | 529 | 390 | 1.36x | 500 | 1 | 60.7 MiB → 5.2 MiB | 2.1 MiB → 2.2 MiB | 14.2 MiB → 14.8 MiB | 60.7 MiB → 63.5 MiB | 14.2 MiB → 1.2 MiB | 60.7 MiB → 63.5 MiB |

### shuffled

| layout | files_total B2 | files_total B2c | ratio | manifests B2 | manifests B2c | Q1_point_user bytes_planned | Q2_one_day bytes_planned | Q3_seven_days bytes_planned | Q4_purchases bytes_planned | Q5_user_week bytes_planned | Q6_full_agg bytes_planned |
|---|---|---|---|---|---|---|---|---|---|---|---|
| L0 | 500 | 1 | 500.00x | 500 | 1 | 82.4 MiB → 40.1 MiB | 82.4 MiB → 40.1 MiB | 82.4 MiB → 40.1 MiB | 82.4 MiB → 40.1 MiB | 82.4 MiB → 40.1 MiB | 82.4 MiB → 40.1 MiB |
| L1 | 15000 | 30 | 500.00x | 500 | 1 | 118.4 MiB → 55.9 MiB | 3.9 MiB → 1.8 MiB | 27.6 MiB → 13.0 MiB | 118.4 MiB → 55.9 MiB | 27.6 MiB → 13.0 MiB | 118.4 MiB → 55.9 MiB |
| L2 | 15000 | 30 | 500.00x | 500 | 1 | 114.9 MiB → 66.6 MiB | 3.8 MiB → 2.2 MiB | 26.8 MiB → 15.4 MiB | 114.9 MiB → 66.6 MiB | 26.8 MiB → 15.4 MiB | 114.9 MiB → 66.6 MiB |
| L2s | 15000 | 390 | 38.46x | 500 | 1 | 114.9 MiB → 5.2 MiB | 3.8 MiB → 2.2 MiB | 26.8 MiB → 14.8 MiB | 114.9 MiB → 63.5 MiB | 26.8 MiB → 1.2 MiB | 114.9 MiB → 63.5 MiB |

## Delta (descriptive)

Delta has no hypothesis and takes no part in any verdict.

### ordered

#### B1

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 10/10 · 71.2 MiB (100.0%) | 39/39 · 82.3 MiB (100.0%) | 39/39 · 81.7 MiB (100.0%) | 39/129 · 23.9 MiB (28.1%) |
| Q2_one_day | 1/10 · 7.1 MiB (10.0%) | 1/39 · 2.7 MiB (3.3%) | 1/39 · 2.7 MiB (3.3%) | 4/129 · 2.8 MiB (3.3%) |
| Q3_seven_days | 3/10 · 21.3 MiB (30.0%) | 9/39 · 19.2 MiB (23.3%) | 9/39 · 19.0 MiB (23.3%) | 30/129 · 19.8 MiB (23.3%) |
| Q4_purchases | 10/10 · 71.2 MiB (100.0%) | 39/39 · 82.3 MiB (100.0%) | 39/39 · 81.7 MiB (100.0%) | 129/129 · 85.0 MiB (100.0%) |
| Q5_user_week | 3/10 · 21.3 MiB (30.0%) | 9/39 · 19.2 MiB (23.3%) | 9/39 · 19.0 MiB (23.3%) | 9/129 · 5.6 MiB (6.6%) |
| Q6_full_agg | 10/10 · 71.2 MiB (100.0%) | 39/39 · 82.3 MiB (100.0%) | 39/39 · 81.7 MiB (100.0%) | 129/129 · 85.0 MiB (100.0%) |

Cross-check, files planned as `fragments/native/own` (reported number / delta-rs native pruning / stats evaluator); ✗ marks a disagreement:

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 10/10/10 | 39/39/39 | 39/39/39 | 39/39/39 |
| Q2_one_day | 1/1/1 | 1/1/1 | 1/1/1 | 4/4/4 |
| Q3_seven_days | 3/3/3 | 9/9/9 | 9/9/9 | 30/30/30 |
| Q4_purchases | 10/10/10 | 39/39/39 | 39/39/39 | 129/129/129 |
| Q5_user_week | 3/3/3 | 9/9/9 | 9/9/9 | 9/9/9 |
| Q6_full_agg | 10/10/10 | 39/39/39 | 39/39/39 | 129/129/129 |

#### B2

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 500/500 · 86.3 MiB (100.0%) | 529/529 · 86.4 MiB (100.0%) | 529/529 · 84.3 MiB (100.0%) | 529/529 · 84.3 MiB (100.0%) |
| Q2_one_day | 18/500 · 3.1 MiB (3.6%) | 18/529 · 2.9 MiB (3.3%) | 18/529 · 2.8 MiB (3.3%) | 18/529 · 2.8 MiB (3.3%) |
| Q3_seven_days | 118/500 · 20.4 MiB (23.6%) | 124/529 · 20.1 MiB (23.3%) | 124/529 · 19.7 MiB (23.3%) | 124/529 · 19.7 MiB (23.3%) |
| Q4_purchases | 500/500 · 86.3 MiB (100.0%) | 529/529 · 86.4 MiB (100.0%) | 529/529 · 84.3 MiB (100.0%) | 529/529 · 84.3 MiB (100.0%) |
| Q5_user_week | 118/500 · 20.4 MiB (23.6%) | 124/529 · 20.1 MiB (23.3%) | 124/529 · 19.7 MiB (23.3%) | 124/529 · 19.7 MiB (23.3%) |
| Q6_full_agg | 500/500 · 86.3 MiB (100.0%) | 529/529 · 86.4 MiB (100.0%) | 529/529 · 84.3 MiB (100.0%) | 529/529 · 84.3 MiB (100.0%) |

Cross-check, files planned as `fragments/native/own` (reported number / delta-rs native pruning / stats evaluator); ✗ marks a disagreement:

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 500/500/500 | 529/529/529 | 529/529/529 | 529/529/529 |
| Q2_one_day | 18/18/18 | 18/18/18 | 18/18/18 | 18/18/18 |
| Q3_seven_days | 118/118/118 | 124/124/124 | 124/124/124 | 124/124/124 |
| Q4_purchases | 500/500/500 | 529/529/529 | 529/529/529 | 529/529/529 |
| Q5_user_week | 118/118/118 | 124/124/124 | 124/124/124 | 124/124/124 |
| Q6_full_agg | 500/500/500 | 529/529/529 | 529/529/529 | 529/529/529 |

#### B2c

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 1/1 · 36.7 MiB (100.0%) | 30/30 · 49.9 MiB (100.0%) | 30/30 · 55.4 MiB (100.0%) | 116/116 · 58.9 MiB (100.0%) |
| Q2_one_day | 1/1 · 36.7 MiB (100.0%) | 1/30 · 1.7 MiB (3.3%) | 1/30 · 1.8 MiB (3.3%) | 4/116 · 2.0 MiB (3.3%) |
| Q3_seven_days | 1/1 · 36.7 MiB (100.0%) | 7/30 · 11.6 MiB (23.3%) | 7/30 · 12.9 MiB (23.3%) | 27/116 · 13.7 MiB (23.3%) |
| Q4_purchases | 1/1 · 36.7 MiB (100.0%) | 30/30 · 49.9 MiB (100.0%) | 30/30 · 55.4 MiB (100.0%) | 116/116 · 58.9 MiB (100.0%) |
| Q5_user_week | 1/1 · 36.7 MiB (100.0%) | 7/30 · 11.6 MiB (23.3%) | 7/30 · 12.9 MiB (23.3%) | 27/116 · 13.7 MiB (23.3%) |
| Q6_full_agg | 1/1 · 36.7 MiB (100.0%) | 30/30 · 49.9 MiB (100.0%) | 30/30 · 55.4 MiB (100.0%) | 116/116 · 58.9 MiB (100.0%) |

Cross-check, files planned as `fragments/native/own` (reported number / delta-rs native pruning / stats evaluator); ✗ marks a disagreement:

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 1/1/1 | 30/30/30 | 30/30/30 | 116/116/116 |
| Q2_one_day | 1/1/1 | 1/1/1 | 1/1/1 | 4/4/4 |
| Q3_seven_days | 1/1/1 | 7/7/7 | 7/7/7 | 27/27/27 |
| Q4_purchases | 1/1/1 | 30/30/30 | 30/30/30 | 116/116/116 |
| Q5_user_week | 1/1/1 | 7/7/7 | 7/7/7 | 27/27/27 |
| Q6_full_agg | 1/1/1 | 30/30/30 | 30/30/30 | 116/116/116 |

### shuffled

#### B1

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 10/10 · 84.7 MiB (100.0%) | 300/300 · 95.0 MiB (100.0%) | 300/300 · 89.8 MiB (100.0%) | 300/300 · 89.8 MiB (100.0%) |
| Q2_one_day | 10/10 · 84.7 MiB (100.0%) | 10/300 · 3.2 MiB (3.3%) | 10/300 · 3.0 MiB (3.3%) | 10/300 · 3.0 MiB (3.3%) |
| Q3_seven_days | 10/10 · 84.7 MiB (100.0%) | 70/300 · 22.1 MiB (23.3%) | 70/300 · 20.9 MiB (23.3%) | 70/300 · 20.9 MiB (23.3%) |
| Q4_purchases | 10/10 · 84.7 MiB (100.0%) | 300/300 · 95.0 MiB (100.0%) | 300/300 · 89.8 MiB (100.0%) | 300/300 · 89.8 MiB (100.0%) |
| Q5_user_week | 10/10 · 84.7 MiB (100.0%) | 70/300 · 22.1 MiB (23.3%) | 70/300 · 20.9 MiB (23.3%) | 70/300 · 20.9 MiB (23.3%) |
| Q6_full_agg | 10/10 · 84.7 MiB (100.0%) | 300/300 · 95.0 MiB (100.0%) | 300/300 · 89.8 MiB (100.0%) | 300/300 · 89.8 MiB (100.0%) |

Cross-check, files planned as `fragments/native/own` (reported number / delta-rs native pruning / stats evaluator); ✗ marks a disagreement:

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 10/10/10 | 300/300/300 | 300/300/300 | 300/300/300 |
| Q2_one_day | 10/10/10 | 10/10/10 | 10/10/10 | 10/10/10 |
| Q3_seven_days | 10/10/10 | 70/70/70 | 70/70/70 | 70/70/70 |
| Q4_purchases | 10/10/10 | 300/300/300 | 300/300/300 | 300/300/300 |
| Q5_user_week | 10/10/10 | 70/70/70 | 70/70/70 | 70/70/70 |
| Q6_full_agg | 10/10/10 | 300/300/300 | 300/300/300 | 300/300/300 |

#### B2

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 500/500 · 100.0 MiB (100.0%) | 15000/15000 · 126.7 MiB (100.0%) | 15000/15000 · 124.8 MiB (100.0%) | 15000/15000 · 124.8 MiB (100.0%) |
| Q2_one_day | 500/500 · 100.0 MiB (100.0%) | 500/15000 · 4.2 MiB (3.3%) | 500/15000 · 4.2 MiB (3.3%) | 500/15000 · 4.2 MiB (3.3%) |
| Q3_seven_days | 500/500 · 100.0 MiB (100.0%) | 3500/15000 · 29.5 MiB (23.3%) | 3500/15000 · 29.1 MiB (23.3%) | 3500/15000 · 29.1 MiB (23.3%) |
| Q4_purchases | 500/500 · 100.0 MiB (100.0%) | 15000/15000 · 126.7 MiB (100.0%) | 15000/15000 · 124.8 MiB (100.0%) | 15000/15000 · 124.8 MiB (100.0%) |
| Q5_user_week | 500/500 · 100.0 MiB (100.0%) | 3500/15000 · 29.5 MiB (23.3%) | 3500/15000 · 29.1 MiB (23.3%) | 3500/15000 · 29.1 MiB (23.3%) |
| Q6_full_agg | 500/500 · 100.0 MiB (100.0%) | 15000/15000 · 126.7 MiB (100.0%) | 15000/15000 · 124.8 MiB (100.0%) | 15000/15000 · 124.8 MiB (100.0%) |

Cross-check, files planned as `fragments/native/own` (reported number / delta-rs native pruning / stats evaluator); ✗ marks a disagreement:

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 500/500/500 | 15000/15000/15000 | 15000/15000/15000 | 15000/15000/15000 |
| Q2_one_day | 500/500/500 | 500/500/500 | 500/500/500 | 500/500/500 |
| Q3_seven_days | 500/500/500 | 3500/3500/3500 | 3500/3500/3500 | 3500/3500/3500 |
| Q4_purchases | 500/500/500 | 15000/15000/15000 | 15000/15000/15000 | 15000/15000/15000 |
| Q5_user_week | 500/500/500 | 3500/3500/3500 | 3500/3500/3500 | 3500/3500/3500 |
| Q6_full_agg | 500/500/500 | 15000/15000/15000 | 15000/15000/15000 | 15000/15000/15000 |

#### B2c

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 1/1 · 65.1 MiB (100.0%) | 30/30 · 69.8 MiB (100.0%) | 30/30 · 67.0 MiB (100.0%) | 150/150 · 71.2 MiB (100.0%) |
| Q2_one_day | 1/1 · 65.1 MiB (100.0%) | 1/30 · 2.3 MiB (3.3%) | 1/30 · 2.2 MiB (3.3%) | 5/150 · 2.4 MiB (3.3%) |
| Q3_seven_days | 1/1 · 65.1 MiB (100.0%) | 7/30 · 16.3 MiB (23.3%) | 7/30 · 15.6 MiB (23.3%) | 35/150 · 16.6 MiB (23.3%) |
| Q4_purchases | 1/1 · 65.1 MiB (100.0%) | 30/30 · 69.8 MiB (100.0%) | 30/30 · 67.0 MiB (100.0%) | 150/150 · 71.2 MiB (100.0%) |
| Q5_user_week | 1/1 · 65.1 MiB (100.0%) | 7/30 · 16.3 MiB (23.3%) | 7/30 · 15.6 MiB (23.3%) | 35/150 · 16.6 MiB (23.3%) |
| Q6_full_agg | 1/1 · 65.1 MiB (100.0%) | 30/30 · 69.8 MiB (100.0%) | 30/30 · 67.0 MiB (100.0%) | 150/150 · 71.2 MiB (100.0%) |

Cross-check, files planned as `fragments/native/own` (reported number / delta-rs native pruning / stats evaluator); ✗ marks a disagreement:

| query | L0 | L1 | L2 | L2s |
|---|---|---|---|---|
| Q1_point_user | 1/1/1 | 30/30/30 | 30/30/30 | 150/150/150 |
| Q2_one_day | 1/1/1 | 1/1/1 | 1/1/1 | 5/5/5 |
| Q3_seven_days | 1/1/1 | 7/7/7 | 7/7/7 | 35/35/35 |
| Q4_purchases | 1/1/1 | 30/30/30 | 30/30/30 | 150/150/150 |
| Q5_user_week | 1/1/1 | 7/7/7 | 7/7/7 | 35/35/35 |
| Q6_full_agg | 1/1/1 | 30/30/30 | 30/30/30 | 150/150/150 |

## Exploratory: row groups

Not collected in this run.
