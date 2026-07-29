# SeaStar benchmarks

**This is the baseline.** It was taken deliberately *before* the lock hierarchy rework
(`b_plan_locks_and_concurrency.txt` B2) and before the clustering-order sort
(`d_plan_query_engine.txt` D1), so the "after" numbers are comparable. Re-run the same tasks after
those land and diff against this file.

Both have since landed. Their measurements are the last two sections:
[After D1](#after-d1-what-read-time-ordering-costs) and
[After the lock rework and the partition index](#after-the-lock-rework-and-the-partition-index).
**Compare within a section, not across them** - this laptop throttles, and the same code measures 2-3x
apart between sittings, so each section pairs its own before and after taken back to back.

[What the wire costs](#what-the-wire-costs) is later still, and is the one section that was taken on
**different hardware and a different JDK**. It carries its own environment table and re-measures every
figure it compares, including ones that already had numbers above.

Measured at commit `1145dae`. No library code changed for this run: everything under
`seastar/src/main` is identical to `3708bfa`, and the only additions are the benchmark source sets,
which are not on the default build and not in the published jar.

## Environment

| | |
| --- | --- |
| CPU | AMD Ryzen 7 4700U, 8 cores / 8 threads, 1.4-2.0 GHz |
| RAM | 14 GiB |
| OS | Debian GNU/Linux 13 (trixie), kernel 6.12.96+deb13-amd64 |
| JDK | Oracle JDK 17.0.12+8-LTS-286 |
| Gradle | 9.4.1 |
| Docker | Engine 29.6.2 |
| cassandra-all | 5.0.8 |
| java-driver-core | 4.19.3 |
| Container image | `cassandra:5.0.8` |
| JMH | 1.36 |

This is a 15 W laptop part. Absolute numbers will be lower on a desktop; the ratios are the
interesting part. Sustained runs throttle it noticeably, which is why the benchmark tasks are
serialized against each other and why the comparative measurements are interleaved rather than run
back to back.

## Headline

| | SeaStar | TestContainers Cassandra |
| --- | --- | --- |
| Ready for the first query (warm image / warm page cache) | **743 ms** from JVM start | **13 397 ms** from `container.start()` |
| Ready for the first query (cold, image not present) | 743 ms | 18 575 ms (5 569 ms pull + 13 006 ms) |
| Ready with a 75-statement fixture schema loaded | **825 ms** from JVM start | not measured (see below) |
| Second and subsequent sessions in the same JVM | **8 ms** | a second container, so the same ~13 s again |

SeaStar is roughly **18x faster than a warm container** and **25x faster than a cold one** to first
query, and roughly **1 700x faster** for every session after the first in the same JVM. The
container column is the honest comparison for a fixture that starts a node per test class; a suite
that shares one container across the whole build pays the 13 s once.

Both comparisons are conservative. SeaStar's figure is measured from JVM start and includes its own
class loading; the container's is measured from `container.start()` and excludes the JVM start the
test would pay anyway. The container number also does not include seeding a schema, while SeaStar's
825 ms figure does - so the like-for-like gap for a real fixture is wider than the first row
suggests.

## Startup, SeaStar

20 cold JVMs per variant, one fresh JVM per sample. Milliseconds.

| metric | min | median | mean | p90 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `build.cold` - first `SeaStarCqlSession.builder().build()` | 406.5 | **435.5** | 456.1 | 544.6 | 634.7 |
| `query.first` - first `execute` after that build | 230.4 | **264.1** | 270.9 | 281.8 | 364.6 |
| `jvm.to.first.query` - JVM start to first query returning | 710 | **743** | 778 | 890 | 1049 |
| `build.warm` - subsequent builds in the same JVM | 7.6 | **8.0** | 8.3 | 8.4 | 11.9 |
| `query.warm` - subsequent first-queries in the same JVM | 0.47 | **0.52** | 0.53 | 0.54 | 0.71 |

The warm build of 8.0 ms is well under the ~27 ms anchor the review recorded.

Seeded with a realistic fixture schema through `withSchema` - 5 keyspaces x 10 tables, 10 UDTs and
10 secondary indexes, 75 statements in total:

| metric | min | median | mean | p90 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `build.schema.cold` - build including the schema replay | 752.6 | **768.2** | 789.3 | 838.0 | 1015.4 |
| `jvm.to.schema.ready` - JVM start to a usable seeded session | 810 | **825** | 848 | 895 | 1075 |
| `query.first` - first query after that (parser already loaded) | 7.6 | **8.3** | 8.6 | 9.0 | 13.6 |
| `build.schema.warm` - subsequent seeded builds in the same JVM | 26.5 | **29.2** | 31.6 | 34.1 | 64.7 |

Seeding 75 statements costs about 333 ms cold (768.2 - 435.5) and about 21 ms warm
(29.2 - 8.0), i.e. roughly **0.28 ms per schema statement** once the JVM is warm. A test class that
builds a seeded session per test method pays 29 ms each time, not 768 ms.

## Startup, TestContainers Cassandra

Milliseconds. Warm is 3 samples, cold is 1 - a cold pull is bandwidth bound and not usefully
repeatable, so treat it as an order of magnitude, not a measurement.

| metric | warm (median of 3) | cold (1 sample) |
| --- | ---: | ---: |
| `image.pull` | n/a (image present) | 5 568.9 |
| `container.start` | 10 891.8 | 10 552.2 |
| `session.build` - driver connecting to the node | 1 309.0 | 1 258.8 |
| `query.first` - `CREATE KEYSPACE` | 1 193.0 | 1 195.0 |
| `start.to.first.query` | **13 397.1** | **13 005.9** |

`container.start()` already blocks on TestContainers' readiness check, and the driver still needs
another ~2.5 s on top of it to connect and run one statement.

## Per statement, warm

JMH, `AverageTime`, 1 fork, 5 x 1 s warmup, 5 x 1 s measurement. Microseconds per operation.

Every statement with a WHERE clause runs against a table holding **1 000 rows**. That is not
incidental: SeaStar has no index on the partition key, so a point lookup scans the whole table and
the row count is part of every number below.

| benchmark | us/op | error |
| --- | ---: | ---: |
| `prepareCached` - `prepare` of an already prepared query | **0.377** | ± 0.004 |
| `prepareUncached` - `prepare` of a new query string | **1.774** | ± 0.919 |
| `prepareUncachedResolved` - new query, bind metadata resolved | **6.487** | ± 0.555 |
| `createTable` | **20.106** | ± 0.790 |
| `insertPrepared` - bound statement | **41.403** | ± 0.912 |
| `insertLiteral` - literal values in the CQL string | **48.178** | ± 23.677 |
| `selectPoint` - `SELECT name ... WHERE id = ?` | **62.178** | ± 1.304 |
| `update` - `UPDATE ... SET name = ? WHERE id = ?` | **69.188** | ± 0.706 |
| `deleteByPrimaryKey` | **78.276** | ± 16.913 |
| `selectScan` - `SELECT *`, all 1 000 rows | **325.664** | ± 43.493 |
| `batch100` - driver `BatchStatement` of 100 INSERTs | **1 012.962** | ± 45.548 |

Caveats, so these are read for what they are:

- **`prepare` is lazy.** `SeaStarPreparedStatement` defers `QueryProcessor.parseStatement` until
  something asks for the bind variable metadata, so `prepareCached` and `prepareUncached` measure a
  cache lookup and an object allocation, not a parse. `prepareUncachedResolved` forces the parse a
  caller pays on the first `bind`, and 6.5 us is the real "cold prepare" figure. Note that a warm
  CQL parse is ~6 us while the *first* parse in a JVM is ~264 ms - a five-order-of-magnitude
  difference that is entirely class loading and JIT.
- **`deleteByPrimaryKey`** removes the row on its first invocation; steady state therefore measures
  parse, resolution and the full-table scan that finds nothing. The scan dominates either way.
- **`batch100`** works out at ~10 us per child statement against a 100-row table, so it is close to
  linear in the batch size with no per-statement parse (the children are pre-built).
- `insertLiteral` and `deleteByPrimaryKey` have wide error bars; they are the noisiest of the set on
  this machine.

## Scaling: the O(rows) full scan

| rows | `selectPoint` us/op | `selectAll` us/op |
| ---: | ---: | ---: |
| 10 | 5.823 ± 0.181 | 5.925 ± 0.144 |
| 1 000 | 63.125 ± 1.137 | 330.984 ± 5.924 |
| 100 000 | 12 748.872 ± 143.603 | 45 114.081 ± 5 529.285 |

This is the number the query-engine and storage plans need.

- A **point lookup by full primary key costs 12.7 ms at 100k rows**. There is no index on the
  partition key at all, so `WHERE id = ?` is a linear scan; it is 200x slower at 100k rows than at
  1k - superlinear for 100x the rows, so cache and allocation pressure are compounding the scan -
  and 2 200x slower than at 10. Any fixture with a large table makes every single-row read
  expensive.
- `selectAll` is only ~3.5x the cost of `selectPoint` at 100k despite returning 100 000 rows rather
  than one, which says the scan itself - not the projection or the result set - is the bulk of the
  cost. The extra 32 ms is the deserialize/re-serialize round trip and the per-row `snapshot()` the
  README already flags.
- At 10 rows the two are indistinguishable (5.8 vs 5.9 us): below ~100 rows the fixed per-statement
  cost, dominated by the CQL parse, is everything.

## Parser cost: where the first 264 ms goes

The review recorded ~258 ms for the first `QueryProcessor.parseStatement` in a JVM and asked whether
it is ANTLR class loading, the prepared statement cache initializer, or a static initializer chain.
It is **all three, and the split is measurable.**

`QueryProcessor.parseStatement` is a one-line delegate:

```java
public static CQLStatement.Raw parseStatement(String queryStr) throws SyntaxException {
    try {
        return CQLFragmentParser.parseAnyUnhandled(CqlParser::query, queryStr);
    }
    ...
}
```

Calling it triggers `QueryProcessor.<clinit>`, which is not free. It reads
`DatabaseDescriptor.getPreparedStatementsCacheSizeMiB()`, builds a Caffeine cache, initializes
`CassandraVersion`, and **schedules a recurring task on `ScheduledExecutors.scheduledTasks`** - the
source of the familiar `Initialized prepared statement caches with 0 MiB` log line. SeaStar never
prepares a statement through Cassandra and never evicts one from that cache, so none of it is used.

### Standalone: a bare JVM that does nothing but parse

20 cold JVMs per variant, interleaved. Milliseconds.

| variant | first parse (median) | classes loaded | live threads |
| --- | ---: | ---: | ---: |
| `CQLFragmentParser.parseAny(CqlParser::query, q)` - no `QueryProcessor` | **292.6** | 1 196 | 6 |
| `QueryProcessor.parseStatement(q)` | **538.5** | 2 316 | 7 |
| `QueryProcessor` static initializer alone, no parse | 423.3 | 1 923 | 7 |

Going through `QueryProcessor` costs **1 120 extra classes**, roughly **246 ms**, and **one extra
live thread that never goes away**. The second parse costs ~3 ms either way, so this is entirely a
one-time cost.

### In situ: the same split inside a real SeaStar startup

The standalone figure overstates the saving, because `builder().build()` has already loaded some of
the classes the two initializers share. Measured inside a real cold start, 20 cold JVMs per
variant, interleaved, milliseconds (median):

| | `QueryProcessor` init | ANTLR parse | handler dispatch | total |
| --- | ---: | ---: | ---: | ---: |
| baseline `plain` (all three together) | | | | 264.1 |
| charged initializer first | 167.2 | 95.4 | 12.7 | 275.3 |
| charged parse first | 75.2 | 179.9 | 13.7 | 268.8 |

The two initializers share several hundred classes, so whichever runs first is charged for them.
That brackets the answer:

- **Upper bound: 167 ms** could be attributed to `QueryProcessor`.
- **Marginal cost: 75 ms.** This is the number that matters. The ANTLR parse is not optional, so it
  will load the shared classes regardless; 75 ms is what is actually recoverable.

### Finding, and what came of it

**SeaStar can call `CQLFragmentParser` instead of `QueryProcessor.parseStatement(query)`.** Both are
public. `parseStatement` *is* that call plus exception translation, so this is not a
reimplementation.

Verified equivalent: `./gradlew :seastar:parserEquivalenceCheck` parses 14 statements covering every
type SeaStar handles - CREATE KEYSPACE/TYPE/TABLE/INDEX, ALTER TYPE, USE, INSERT, UPDATE ... IF
EXISTS, DELETE, SELECT DISTINCT ... ALLOW FILTERING, TRUNCATE, DROP TABLE/KEYSPACE and BEGIN BATCH -
both ways and compares the parse tree types. **0 mismatches out of 14.**

**This has since landed**, as `handlers.CqlParsers`, at the two call sites in
`SeaStarCqlRequestHandler` and `SeaStarPreparedStatement`. Measured the same way, on the same
machine, back to back:

| `plain` metric (median, ms) | via `QueryProcessor` | via `CQLFragmentParser` | delta |
| --- | ---: | ---: | ---: |
| `build.cold` | 436.8 | 438.3 | +1.5 |
| `query.first` | 272.5 | **197.4** | **-75.1 (-28%)** |
| `jvm.to.first.query` | 759 | **678** | **-81 (-11%)** |
| `build.warm` | 8.38 | 8.35 | -0.03 |
| `query.warm` | 0.56 | 0.58 | +0.02 |

That is the 75 ms marginal cost, recovered in full and only on the cold path, as predicted. A JVM
that builds a session, runs a statement, prepares one and closes the session now ends with **9 live
threads instead of 10**: `ScheduledTasks:1` is gone, along with the prepared-statement-eviction
warning it existed to run for a cache SeaStar never filled.

What it cost: `parseAnyUnhandled` throws `RecognitionException` and raw `RuntimeException` where
`parseStatement` translates both into Cassandra's server-side `SyntaxException`. `CqlParsers` does
the translation itself, and takes the opportunity to report a malformed query the way a live cluster
reports it to a client - as the driver's `SyntaxError` rather than a Cassandra exception no driver
consumer would expect. The container suite asserts that, so the swap is covered rather than assumed.

**Explicitly out of scope:** none of this involves `DatabaseDescriptor.clientInitialization()`,
which is disqualified for the reasons recorded in `c_plan_cassandra_reflection.txt` C1.

## What was not benchmarked, and why

Everything `i_plan_benchmarks.txt` I1 asks for was measured. These were left out deliberately,
and each was verified to fail today rather than assumed to:

| statement | result today |
| --- | --- |
| `INSERT ... VALUES (['a','b'])` | `UnsupportedOperationException: Unsupported term ['a', 'b']` |
| `INSERT ... VALUES ({1,2})` | `UnsupportedOperationException: Unsupported term {1, 2}` |
| `INSERT ... VALUES ({'a':'b'})` | `UnsupportedOperationException: Unsupported term {'a': 'b'}` |
| `SELECT ... WHERE ck > 1` | `UnsupportedOperationException: Unsupported operator > in WHERE` |
| `SELECT ... WHERE ck >= 1 AND ck <= 2` | `UnsupportedOperationException: Unsupported operator >= in WHERE` |
| `SELECT COUNT(*)` | `UnsupportedOperationException: Unsupported select item ... Selectable$WithFunction$Raw` |
| `SELECT MAX(ck)` | `UnsupportedOperationException: Unsupported select item ... Selectable$WithFunction$Raw` |
| `UPDATE ... SET tags = tags + ['c']` | `UnsupportedOperationException: Unsupported UPDATE assignment ... Operation$Addition` |
| `ALTER TABLE ... ADD` | `IllegalArgumentException: No request processor found for ... AlterTableStatement$Raw` |
| `CREATE MATERIALIZED VIEW` | `IllegalArgumentException: No request processor found for ... CreateViewStatement$Raw` |

These are the known gaps in `d_plan_query_engine.txt` and `e_plan_missing_statements.txt`. A later
wave should add benchmarks for them as they land - collection literals and range operators in
particular, because a range scan and a collection round trip are exactly the operations where the
current full-scan-plus-reserialize storage model will hurt most.

Two other things had to be worked around rather than skipped, and both are findings in their own
right:

- **Seeding 100 000 rows cannot go through `INSERT`.** `InsertHandler` scans the table for an
  existing primary key on every insert, so bulk loading by CQL is O(n^2) - at 100k rows that is
  ~5x10^9 row comparisons and takes far longer than the benchmark it sets up. `BenchmarkFixture`
  seeds through `SeaStarTable.addRow` instead, which is the same call the INSERT path makes after
  its scan. **Fixed by the partition index**; measured in the last section, 100 000 rows in 1.7 s.
- **`SeaStarRow.validate` builds its exception message with mismatched format arguments.** A
  type-mismatch on seeding reported `Value %d (1) is not compatible with column type name-0 [INT]`,
  which names neither the offending value nor the right column. Worth a line in
  `a_plan_correctness_bugs.txt`.

## After D1: what read-time ordering costs

`d_plan_query_engine.txt` D1 sorts every SELECT at read time - partitions by Murmur3 token, rows
within a partition by the clustering columns - so this is the measurement the baseline above was
taken for.

Measured on the same machine in one sitting, "before" being `e9c5c72` (the commit D1 was written on
top of) rather than the `1145dae` baseline, so the numbers isolate the sort rather than everything
that has landed since. Same JMH settings: `AverageTime`, 1 fork, 3x1 s warmup, 5x1 s measurement.
Microseconds per operation.

| benchmark | before | after | change |
| --- | ---: | ---: | ---: |
| `StatementBenchmark.selectPoint` - 1 row out of 1 000 | 68.498 ± 10.788 | 64.841 ± 2.270 | none |
| `StatementBenchmark.selectScan` - all 1 000 rows | 352.760 ± 4.147 | 631.089 ± 64.805 | **+79 %** |
| `SelectScalingBenchmark.selectPoint`, 10 rows | 6.580 ± 0.103 | 7.477 ± 0.040 | +14 % |
| `SelectScalingBenchmark.selectPoint`, 1 000 rows | 63.631 ± 0.583 | 66.321 ± 2.902 | +4 % |
| `SelectScalingBenchmark.selectPoint`, 100 000 rows | 12 199 ± 751 | 15 107 ± 2 969 | +24 % |
| `SelectScalingBenchmark.selectAll`, 10 rows | 5.897 ± 0.117 | 8.823 ± 0.108 | +50 % |
| `SelectScalingBenchmark.selectAll`, 1 000 rows | 333.822 ± 8.391 | 623.726 ± 19.925 | **+87 %** |
| `SelectScalingBenchmark.selectAll`, 100 000 rows | 49 900 ± 6 386 | 111 471 ± 5 881 | **+123 %** |

Reading the numbers:

- **A point lookup is unaffected.** Ordering happens after filtering, so a query that matches one
  row sorts one row. The 100 000-row figure moved, but its error bar is ±3 ms on this laptop and it
  is measuring the scan that finds the row, not the sort.
- **A full scan roughly doubles**, and the cost is per returned row: it is the partition key
  encoding, not the comparison. Every row's key goes through `getBytesUnsafe`, which takes the row
  lock and the table lock and runs the codec. Trimming the allocations around it - a fast path for
  a single-column key, arrays instead of lists - measured as noise and was reverted, which is the
  evidence for where the time actually goes.
- **The fix is the partition key index the handover already lists as debt.** With one, a scan would
  iterate partitions in token order instead of hashing every row, and a point lookup would stop
  scanning. Both plans point at the same missing structure.

**The index has since landed**, and it took the point lookup with it - see the next section. The full
scan did not improve: the index removes the *search*, not the per-row key encoding the sort needs, so
a query that returns every row still encodes every row's key. Iterating partitions in token order,
which would remove it, is a further change and was not made.

This is a fidelity-for-speed trade the design goal decides: SeaStar existed to return rows in an
order Cassandra never returns them in, and a test that asserted on the order passed here and failed
against a cluster.

## After the lock rework and the partition index

`b_plan_locks_and_concurrency.txt` collapsed the lock hierarchy onto one lock per keyspace, and the
same wave gave a table an index from partition key to its rows. This is the measurement
`i_plan_benchmarks.txt` I3 asked for: baseline before, same benchmarks after.

**Read these against each other and not against the tables above.** Before is `87dbbce`, after is the
merge of this wave, and the two were run back to back in one sitting on the machine described at the
top. The older tables were taken on other days; this laptop throttles enough that its absolute
numbers drift by 2-3x between sittings, which is exactly why D1 was measured the same way. Same JMH
settings as the rest of the file: `AverageTime`, 1 fork, 5 x 1 s warmup, 5 x 1 s measurement.
Microseconds per operation.

| benchmark | before | after | change |
| --- | ---: | ---: | ---: |
| `SelectScalingBenchmark.selectPoint`, 10 rows | 12.589 ± 0.305 | 12.765 ± 0.544 | none |
| `SelectScalingBenchmark.selectPoint`, 1 000 rows | 176.433 ± 19.776 | **12.670 ± 0.102** | **-93 %** |
| `SelectScalingBenchmark.selectPoint`, 100 000 rows | 25 838 ± 802 | **13.414 ± 0.259** | **-99.9 %** |
| `StatementBenchmark.selectPoint` - 1 row out of 1 000 | 171.416 ± 4.011 | **12.451 ± 0.164** | **-93 %** |
| `StatementBenchmark.update` | 188.374 ± 29.036 | **16.337 ± 0.426** | **-91 %** |
| `StatementBenchmark.deleteByPrimaryKey` | 182.370 ± 1.854 | **13.493 ± 0.064** | **-93 %** |
| `StatementBenchmark.insertPrepared` | 65.823 ± 0.790 | **12.214 ± 0.385** | **-81 %** |
| `StatementBenchmark.insertLiteral` | 68.110 ± 1.537 | **13.278 ± 0.384** | **-81 %** |
| `StatementBenchmark.batch100` - 100 INSERTs | 1 619.098 ± 19.382 | **1 200.849 ± 11.586** | **-26 %** |
| `SelectScalingBenchmark.selectAll`, 100 000 rows | 139 100 ± 5 495 | 120 760 ± 847 | -13 % |
| `StatementBenchmark.createTable` | 6.279 ± 1.052 | 5.991 ± 1.170 | none |
| `StatementBenchmark.prepareCached` | 0.383 ± 0.005 | 0.386 ± 0.013 | none |
| `StatementBenchmark.prepareUncached` | 6.387 ± 0.392 | 6.456 ± 0.458 | none |
| `StatementBenchmark.prepareUncachedResolved` | 6.461 ± 0.293 | 6.524 ± 0.277 | none |
| `StatementBenchmark.selectScan` - all 1 000 rows | 744.960 ± 60.670 | 758.788 ± 34.224 | +2 % |
| `SelectScalingBenchmark.selectAll`, 1 000 rows | 743.833 ± 23.872 | 774.527 ± 36.219 | **+4 %** |
| `SelectScalingBenchmark.selectAll`, 10 rows | 9.242 ± 0.057 | 10.238 ± 0.049 | **+11 %** |

Reading the numbers:

- **The point lookup stopped scaling with the table**, which was the whole point. 12.7 us at 1 000
  rows and 13.4 us at 100 000: a 1 900x improvement at 100k, and what is left is the per-statement
  cost - the parse - rather than the store. The three plans that pointed at "the missing partition
  key index" were all pointing at the same number.
- **Every write improved by the same mechanism.** INSERT no longer scans for a matching primary key,
  UPDATE and DELETE no longer scan for the rows they address, and the static-row sweep and the
  partition-wide delete walk a partition instead of the table.
- **The full scan is the honest regression: +11 % at 10 rows, +4 % at 1 000.** `rows()` now copies
  the row list under the read lock rather than handing out a lazy stream over live storage - a lazy
  one escapes the lock and gives its consumer a `ConcurrentModificationException`, which is
  b_plan B5 - and walking a map of partitions has worse locality than walking one array. It is a
  correctness cost, paid on the operation that is O(rows) by definition, and it is bought back at
  100 000 rows where the same change measures 13 % *faster*.
- **Nothing else moved.** The parse path, `CREATE TABLE` and the prepared statement cache are
  untouched by both changes, and they measure untouched. That is the useful control in the table:
  four benchmarks that should not have changed did not.

### Bulk loading through INSERT

benchmarks.md recorded that seeding 100 000 rows through `INSERT` was not possible, and that
`BenchmarkFixture` had to write through `SeaStarTable.addRow` instead. Measured the same way, one
prepared statement executed per row, milliseconds:

| rows | before | after |
| ---: | ---: | ---: |
| 1 000 | 649 | 484 |
| 10 000 | 9 608 | 605 |
| 25 000 | 70 005 | not measured |
| 100 000 | not measured | **1 693** |

The two columns stop at different row counts on purpose. Before, 25 000 rows already took 70
seconds, and the shape is unmistakably quadratic - 2.5x the rows for 7.3x the time - so 100 000
extrapolates to something like 19 minutes and was not worth waiting for. After, 25 000 was not worth
measuring because 100 000 finished in under two seconds.

At 1 000 rows both columns are mostly JIT warmup, which is why neither looks linear there. Past that
the after column settles at roughly **17 us per row**, and **100 000 rows seed in 1.7 seconds**.

`BenchmarkFixture` could go through CQL now rather than writing to `SeaStarTable.addRow`. It has been
left alone deliberately, so the scaling benchmarks keep measuring what they measured before.

### Startup

`:seastar:startupBenchmark`, 20 cold JVMs per variant, before and after in the same sitting.
Milliseconds, median.

| metric | before | after |
| --- | ---: | ---: |
| `build.cold` | 436.02 | 441.35 |
| `query.first` | 203.19 | 200.70 |
| `jvm.to.first.query` | 694 | 685 |
| `build.warm` | 7.98 | 8.24 |
| `query.warm` | 0.56 | 0.56 |

Unchanged, as expected: goal 2 is class loading, and neither locking nor row storage is on that path.
The check was worth running because b_plan B2's cost note asked for it - the four-level hierarchy it
proposed would have added two or three uncontended acquisitions per row mutation. One lock per
keyspace adds none.

## What the wire costs

`seastar-server` puts a socket, Netty and a real driver between the caller and the same in-memory
session. This is the third point on the startup axis, and it comes with the per-statement number that
goes with it.

**Nothing in this section may be compared with anything above it, and this time it is not only the
throttling.** Every table above was taken on the 15 W laptop described at the top of this file, on
Oracle JDK 17. This section was taken on a different machine and a different JDK, inside a container:

| | |
| --- | --- |
| CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads |
| RAM | 62 GiB |
| OS | Debian GNU/Linux 13 (trixie) in a container, kernel 6.12.96+deb13-amd64 |
| JDK | Temurin 25.0.3+9-LTS |
| Gradle | 9.4.1 |
| Container image | `cassandra:5.0.8` |

So all three points below were re-measured in **one sitting on one machine**, including the two that
already had numbers. That is the whole discipline this file has: the comparison is only meaningful
within a section.

### The three-way comparison

Median, milliseconds. SeaStar's two columns are 20 cold JVMs each; the container is 3.

| | in process | over a socket | TestContainers Cassandra |
| --- | ---: | ---: | ---: |
| Ready for the first query | **690** | **1 266** | **9 729** |

Putting the whole native protocol in the middle costs **576 ms and roughly doubles the number** —
and still leaves it **7.7x faster than a warm container**, against 14x for the in-process session.
The comparison is conservative in the same direction it always was: both SeaStar columns are measured
from JVM start and include their own class loading, while the container column is measured from
`container.start()` and excludes the JVM start a test pays anyway.

### Where the 1 266 ms goes

The probe measures its own phases, so the split is not an estimate. Median, milliseconds, from
`./gradlew :seastar-server:wireStartupBenchmark`:

| phase | ms |
| --- | ---: |
| JVM start to a built `SeaStarCqlSession` (`jvm.to.listening` minus the bind) | 435 |
| `SeaStarProtocolServer...build().start()` — Netty, the bootstrap, the bind | 110 |
| an unconfigured `CqlSession` connecting — handshake, `system.local`, schema metadata | 535 |
| the first statement over the socket (a `CREATE KEYSPACE`, so a DDL round trip) | 184 |
| **JVM start to the first query returning** | **1 266** |

The listener itself is the cheap part. **The driver is the expensive part**: 535 ms to connect is
more than the whole of SeaStar's own startup, and it is the driver's class loading, its Netty event
loops, its four-attempt version negotiation from `DSE_V2` down to v5, and the eleven schema queries
it runs before `build()` returns. A second `CqlSession` to the same listener in the same JVM costs
**77 ms**, which is what that figure looks like once nothing is being loaded.

Note that `seastar.build` inside the wire probe (387 ms) is not the same as `build.cold` in the
in-process one (465 ms). The two probes have different classpaths — the wire one has no JMH and no
`me.champeau.jmh` shading, the in-process one has no Netty or driver — and class loading is most of
what a cold build costs, so only the within-probe split above and the `jvm.to.first.query` row are
worth reading across the two.

### Per statement, warm: the round trip is the cost

Both figures below come from the **same JVM in the same sample**, against the same session — one
through the socket, one through the in-process handle the listener is serving. That pairing is the
point. SeaStar does identical work either way, so the difference is the loopback round trip and the
driver's request pipeline, and nothing else.

| `SELECT name FROM probe.t WHERE id = 1` | ms |
| --- | ---: |
| in process | **0.32** |
| over the socket, as a query string | **1.09** |
| over the socket, prepared and bound | **0.79** |

- **The wire costs about 0.77 ms per statement**, which is more than twice what answering the
  statement costs. A harness should size its expectations off the round trip, not off SeaStar.
- **Preparing is worth 0.30 ms of that**, because the server does not re-parse a prepared statement's
  CQL. It is the one optimisation available on this path and it is the same one that helps against a
  real node.
- For scale: a warm container answers the same query in about a millisecond too, over the same kind
  of loopback socket. Once both are warm the protocol dominates and SeaStar's advantage is startup,
  which is exactly what goal 2 claims and nothing more.

### What was not measured

- **A DDL-heavy seeding run.** The driver's schema debouncer, not SeaStar, decides that number — one
  second per statement by default, which the probe sets to 1 ms exactly as the wire fidelity suite
  does. Measuring the default would be measuring `advanced.metadata.schema.debouncer.window`.
- **Concurrent connections.** Every request is answered on one funnel thread by design, so
  throughput under concurrency is a deliberate non-goal and a benchmark of it would only restate the
  design.
- **A cold container over the wire.** The cold container column above the fold is still the right
  order of magnitude for that; nothing about SeaStar changes it.

## Reproducing

Requires JDK 17 (Gradle toolchain downloads it) and, for the container comparison only, Docker.
Benchmarks are not on the default build and their classes are not in the published jar.

```bash
# Per-statement and scaling benchmarks (JMH). ~3 minutes.
./gradlew :seastar:jmh

# A single benchmark class or method.
./gradlew :seastar:jmh -PjmhIncludes='com.tagadvance.seastar.bench.StatementBenchmark.selectPoint'

# Cold and warm startup, plus the in-situ parser split. 20 forked JVMs per variant. ~1 minute.
./gradlew :seastar:startupBenchmark

# Startup with a 75-statement fixture schema. ~30 seconds.
./gradlew :seastar:startupSchemaBenchmark

# Standalone parser cost attribution. ~1 minute.
./gradlew :seastar:parserCostBenchmark

# Proof the two parser entry points return the same parse tree types. ~5 seconds.
./gradlew :seastar:parserEquivalenceCheck

# What the wire costs: a stock driver over seastar-server, and a statement through the socket
# against the same statement in process. 20 forked JVMs. ~2 minutes.
# Run it together with :seastar:startupBenchmark - the two are compared against each other and
# must come from one sitting.
./gradlew :seastar:startupBenchmark :seastar-server:wireStartupBenchmark

# TestContainers comparison. Needs Docker. ~1 and ~2 minutes respectively.
./gradlew :seastar:containerWarmBenchmark
./gradlew :seastar:containerColdBenchmark
```

`containerColdBenchmark` removes every local tag of `cassandra:5.0.8` so that the pull is real, then
restores the tags it removed. Run it only when you want that.

The tasks are serialized against each other by a Gradle shared service, so listing several in one
invocation is safe even with `org.gradle.parallel=true`. They will still be affected by anything
else running on the machine; close everything else first.
