# Interview Prep — lakehouse

**Five questions, five answers.** An unanswered question means this project is not shipped.

---

### Q1. Walk me through the architecture in 90 seconds.

_A:_ A seeded generator makes one event table: 5 million rows over 30 days, heavy-tailed users,
a daily traffic curve. The same rows are appended to Iceberg tables under three layouts
(unpartitioned, partitioned by day, partitioned by day and sorted by user) and two ingest
regimes (10 large appends, or 500 tiny ones that are then compacted), once in time order and once
shuffled. For each of six fixed queries I ask PyIceberg's own scan planner which data files it
would read, and count files, bytes and records from the manifests, so nothing is timed and every
number is reproducible. Every query is also executed and its result compared, by a SHA-256 of a
canonical form, to an in-memory oracle that shares no code with the engine path. The same grid
runs on Delta Lake through delta-rs. A report module applies the decision rules from the
pre-registration and writes the verdicts; I never type a verdict by hand.

### Q2. Why count files and bytes from metadata instead of timing the queries?

_A:_ Because time measures my laptop. On object storage the cost of a query is mostly the files
and bytes it fetches, and that is decided by the planner before any data is read: partition
pruning, then min/max statistics per file. Those counts are deterministic, so CI can test them
and a test runs the whole grid twice, once in parallel, and checks every number matches. The
limit is that planned bytes are an upper bound on what a reader fetches; a Parquet reader can
still skip row groups inside a planned file. Counting matching row groups was planned as an
exploratory extra but was not collected in the reported run.

### Q3. What surprised you?

_A:_ Three things. First, PyIceberg ignores a table's sort order when writing; a "sorted" table
was not sorted until my writer sorted each batch. Second, appending in time order already gives
an unpartitioned table narrow timestamp ranges per file, so it prunes date queries with no
partitioning at all; that is why I added shuffled arrival as a contrast. Third, my own
pre-registered rule was wrong: it compared file counts, and partitioning *splits* every append
into one file per day, so the partitioned table could never "win" on files even when it read 28
times fewer bytes. A reviewer found that in a dry run. I kept the original verdicts and added a
bytes-only reading as a labelled amendment, rather than quietly changing the rule.

### Q4. What did the measurement show?

_A:_ On 5 million rows, with all correctness gates passing: day partitioning cut a one-day
query from 73.9 MiB to 2.6 MiB planned when events arrived shuffled, but only from 4.6 to
1.8 MiB when they arrived in time order, because time-ordered appends already cluster the data.
Sorting by user did nothing for user queries at the default 512 MiB file size (and made the
time-ordered table 20% bigger); with sorted 1 MiB files a user lookup dropped from 55.8 to
5.3 MiB in time-ordered arrival (1.4x when shuffled), at the price of ten times as many files.
500 tiny shuffled appends made 15,000 files, and the user lookup planned roughly twice the bytes
of the compacted table (118.4 vs 55.9 MiB); compaction took it to 30 files. The surprise was the other direction: compacting the
unpartitioned, time-ordered table into one file made the one-day query plan 21 times more bytes, 1.9 to 40.1 MiB,
because compaction threw away the clustering the small appends had. So my answer to "should we
compact?" is "yes, but per partition, and with a sort order plus a small target file size if you want
user-key pruning, or you can make reads worse". On the
registered hypotheses: H1 and H2 were not supported as written, H3 was; the bytes-only reading
supported H1 in both arrival orders.

### Q5. How do you know the numbers are right?

_A:_ Four gates must pass or the run reports no verdict. G1: every query result in every cell
equals the oracle. G2: reading snapshot *k* returns exactly the rows of the first *k* appends,
and the pre-compaction snapshot still reads correctly after compaction. G3: adding a column
rewrites zero data files, old rows read null, and new rows read back exactly. G4: compaction
loses or duplicates no row. For Delta a fifth check, D1, compares three independent counts of
planned files: pyarrow fragments with delta-rs's statistics, delta-rs's native pruner, and my own
evaluator. They agreed on all 144 checks at both 50,000 and 5,000,000 rows. The determinism test also guards
against flaky metadata.
