# SeaStar benchmarks

**This is the baseline.** It was taken deliberately *before* the lock hierarchy rework
(`b_plan_locks_and_concurrency.txt` B2) and before the clustering-order sort
(`d_plan_query_engine.txt` D1), so the "after" numbers are comparable. Re-run the same tasks after
those land and diff against this file.

Measured at commit `1145dae`. No library code changed for this run: everything under
`lib/src/main` is identical to `3708bfa`, and the only additions are the benchmark source sets,
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

### Finding

**SeaStar can call `CQLFragmentParser.parseAny(CqlParser::query, query, "query")` instead of
`QueryProcessor.parseStatement(query)`.** Both are public. `parseStatement` *is* that call plus
exception translation, so this is not a reimplementation.

Verified equivalent: `./gradlew :lib:parserEquivalenceCheck` parses 14 statements covering every
type SeaStar handles - CREATE KEYSPACE/TYPE/TABLE/INDEX, ALTER TYPE, USE, INSERT, UPDATE ... IF
EXISTS, DELETE, SELECT DISTINCT ... ALLOW FILTERING, TRUNCATE, DROP TABLE/KEYSPACE and BEGIN BATCH -
both ways and compares the parse tree types. **0 mismatches out of 14.**

What it buys, at the two call sites in `SeaStarCqlRequestHandler:86` and
`SeaStarPreparedStatement:75`:

- **~75 ms off time-to-first-query** - 28% of the first query, 10% of the 743 ms total.
- **One fewer live thread** in every JVM that uses SeaStar. `ScheduledExecutors.scheduledTasks`
  currently starts and stays started to run a prepared-statement-eviction warning for a cache that
  will never hold anything.
- 1 120 fewer classes loaded in a JVM that only parses.

What it costs: `parseAnyUnhandled` throws `RecognitionException` and raw `RuntimeException` where
`parseStatement` translates both into `SyntaxException`. `CQLFragmentParser.parseAny` does that
translation already and is what the equivalence check used, so the handler's error path is
unchanged in shape - but SeaStar's syntax-error behaviour must be re-tested against the container
suite before the swap is trusted.

This is a finding, not a change. Nothing in `lib/src/main` was modified for this run.

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
  its scan.
- **`SeaStarRow.validate` builds its exception message with mismatched format arguments.** A
  type-mismatch on seeding reported `Value %d (1) is not compatible with column type name-0 [INT]`,
  which names neither the offending value nor the right column. Worth a line in
  `a_plan_correctness_bugs.txt`.

## Reproducing

Requires JDK 17 (Gradle toolchain downloads it) and, for the container comparison only, Docker.
Benchmarks are not on the default build and their classes are not in the published jar.

```bash
# Per-statement and scaling benchmarks (JMH). ~3 minutes.
./gradlew :lib:jmh

# A single benchmark class or method.
./gradlew :lib:jmh -PjmhIncludes='com.tagadvance.seastar.bench.StatementBenchmark.selectPoint'

# Cold and warm startup, plus the in-situ parser split. 20 forked JVMs per variant. ~1 minute.
./gradlew :lib:startupBenchmark

# Startup with a 75-statement fixture schema. ~30 seconds.
./gradlew :lib:startupSchemaBenchmark

# Standalone parser cost attribution. ~1 minute.
./gradlew :lib:parserCostBenchmark

# Proof the two parser entry points return the same parse tree types. ~5 seconds.
./gradlew :lib:parserEquivalenceCheck

# TestContainers comparison. Needs Docker. ~1 and ~2 minutes respectively.
./gradlew :lib:containerWarmBenchmark
./gradlew :lib:containerColdBenchmark
```

`containerColdBenchmark` removes every local tag of `cassandra:5.0.8` so that the pull is real, then
restores the tags it removed. Run it only when you want that.

The tasks are serialized against each other by a Gradle shared service, so listing several in one
invocation is safe even with `org.gradle.parallel=true`. They will still be affected by anything
else running on the machine; close everything else first.
