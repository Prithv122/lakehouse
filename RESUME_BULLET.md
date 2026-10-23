# Resume Bullets — lakehouse

Form: **action → technical specifics → measured outcome.** Numbers or it doesn't go on the resume.
All numbers are from `results/REPORT.md` (5M synthetic rows, seed 35, all correctness gates passed).

---

## Bullets

- Measured how Apache Iceberg table layout changes planned query I/O (from metadata, not timed) on
  a 5M-row synthetic event table: counted
  the files and bytes PyIceberg's scan planner keeps for six fixed queries across 3 layouts
  plus a small-file variant, 2 ingest regimes and 2 arrival orders, with every result checked against an independent
  oracle; day partitioning cut a one-day query from 73.9 MiB to 2.6 MiB (28x) under shuffled
  arrival but only 2.5x when appends were already time-ordered.
- Showed that compaction is not free: rewriting 15,000 small files into 30 halved a user lookup's
  planned bytes (118.4 → 55.9 MiB, partitioned table, shuffled arrival), but compacting a time-ordered unpartitioned table into one file made a one-day query
  plan 21x more bytes (1.9 → 40.1 MiB); with time-ordered arrival, sorted 1 MiB files cut a user
  lookup 10.5x (55.8 → 5.3 MiB).
- Pre-registered the hypotheses and decision rules, kept a failed rule's verdict after an
  independent review exposed its flaw, and published a labelled amendment; cross-checked Delta
  Lake (delta-rs) pruning three ways (144/144 agreed).

## Which roles this supports

- [ ] Data Scientist / ML
- [ ] AI Engineer (LLM/NLP/CV)
- [x] Data Engineer
- [x] Data Analyst / Python Developer

## Keywords this project earns

Apache Iceberg, PyIceberg, Delta Lake, delta-rs, Parquet, partitioning, file pruning, min/max
statistics, compaction, small-file problem, time travel, schema evolution, MinIO / S3, pyarrow,
pre-registration, reproducible benchmarking.
