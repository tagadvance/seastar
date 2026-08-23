# SeaStar benchmarks

Every number in this file was taken in **one sitting on one machine**, at commit `1600072`,
starting 2026-08-23T20:33:47Z, on an otherwise idle box. Total wall clock about 17 minutes.

A routine `apt` upgrade ran in the few minutes before this sitting - confirmed finished, with no
`apt`/`dpkg` process running, before the first benchmark counted here started. The very first
`:seastar:jmh` run overlapped the tail end of it; that run was discarded and redone clean, and the
redone numbers are what appear below.

**Earlier revisions of this file are superseded, not supplemented.** The previous sitting
(commit `bd16934`, also 2026-08-23, several hours earlier) is superseded by this one for one
reason: `batch100`. Profiling with async-profiler found `java.util.stream` pipeline setup
dominating SeaStar's INSERT translation path, and it was fixed (`1600072` - see
[Query turnaround](#query-turnaround-apples-to-apples)); every other number below moved only
within normal run-to-run noise. That is the only code change between the two sittings - `815d715`,
the one commit in between, only reformatted numbers in this file and two comments.

## Environment

| | |
| --- | --- |
| CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads, 550-4,980 MHz |
| RAM | 62 GiB |
| OS | Debian GNU/Linux 13 (trixie) in a container, kernel 6.12.101+deb13-amd64 |
| JDK, cold-JVM harnesses | Temurin 25.0.3+9-LTS |
| JDK, JMH | Temurin 17.0.20+8 |
| JDK, cassandra-unit | Temurin 8 (1.8.0_502-b07) |
| Gradle | 9.4.1 |
| Docker | Engine 28.5.2 |
| cassandra-all | 5.0.8 |
| java-driver-core | 4.19.3 |
| Container image | `cassandra:5.0.8` |
| cassandra-unit | 4.3.1.0 (cassandra-all 3.11.5, driver 4.3.1) |
| JMH | 1.36 |

Same machine as the previous two sittings. Three JDKs, and it is not an oversight - see
[Reproducing](#reproducing).

## The first-instruction clock

Every backend's headline number is measured from the first statement of its harness's `main()` -
before any reference to a SeaStar, cassandra-all, or Cassandra class that could trigger class
loading - rather than from JVM start. `main.to.first.query` is that convention for every backend
below.

## Headline: five ways to get a usable session

Time from the first instruction of the harness to a query that has returned. Median, milliseconds,
`main.to.first.query`.

| | ms | versus a warm container |
| --- | ---: | ---: |
| SeaStar, in process | **605** | **13.5x faster** |
| SeaStar, over the wire (`seastar-server` and a stock driver) | **1,152** | **7.1x faster** |
| A second SeaStar session in the same JVM | **6.2** | ~1,320x faster |
| cassandra-unit (embedded Cassandra 3.11.5) | 4,020 | 2.0x faster |
| TestContainers Cassandra, image already pulled | 8,169 | — |
| TestContainers Cassandra, image not present | 15,026 (3,592 pull + ~11,434) | — |

("A second SeaStar session" is `build.warm` from the startup table below, 6.20 ms - building the
session only, same convention as previous sittings; add `query.warm`'s 0.44 ms for
build-and-query. The JMH steady-state numbers in
[Query turnaround](#query-turnaround-apples-to-apples) are smaller still but measure a single
statement, not a whole session.)

Read this as a fixture cost, not a claim about query speed - see
[Query turnaround](#query-turnaround-apples-to-apples) for that. cassandra-unit's node boots in
about 2.1 s (embedded, no Docker, no network), but its 75-statement schema replay and the driver's
own connect cost put it well behind SeaStar and only 2x ahead of a real container.

## Memory

Two numbers per backend, both after the same sequence: build/boot, seed the 75-statement fixture
schema, then load *N* rows into one extra table, *N* in {0, 1,000, 100,000}. Heap is the test JVM's
own `MemoryMXBean` reading after three `System.gc()` passes, minus the same reading taken before
anything happened; RSS is `/proc/self/status` `VmRSS` (Linux only).

| backend | rows | heap used (MB) | RSS delta (MB) |
| --- | ---: | ---: | ---: |
| SeaStar, in process | 0 | 7.5 | 78.0 |
| SeaStar, in process | 1,000 | 8.1 | 85.9 |
| SeaStar, in process | 100,000 | 63.1 | 254.9 |
| Container, driver side | 0 | 17.9 | 234.3 |
| Container, driver side | 1,000 | 18.2 | 277.4 |
| Container, driver side | 100,000 | 18.0 | 328.6 |
| cassandra-unit, embedded (schema only, no row sweep) | — | 55.3 | — |

SeaStar's heap grows **~0.6 MB per 1,000 rows** at low counts and **~55.5 MB for 100,000** - call
it **~580 bytes/row**, which matches the "rows are stored deserialized" note in
[Per statement, warm](#per-statement-warm): a row is a handful of boxed `Integer`/`String`/`Double`
objects, not a byte-packed record. The container's driver-side heap barely moves with row count (it
is metadata and connection-pool state, not application data); its RSS delta grows a bit with rows,
most plausibly the 200-wide async-insert window's own buffering rather than anything durable.

**Process footprint.** The SeaStar/cassandra-unit numbers above are the whole story for an
in-process backend - there is no second process. For the container:

| | RSS | notes |
| --- | ---: | --- |
| Container process | 2.13-2.38 GB | grows with row count; the same 0/1,000/100,000-row samples above, not a separate `docker stats` snapshot |
| Container heap reservation (`nodetool info`, committed) | **1,024 MB** | see below |
| cassandra-unit JVM | ~653-698 MB | one process holds both the embedded node and the test |

`cassandra-env.sh`'s own formula - `max(min(RAM/2, 1 GB), min(RAM/4, 8 GB))` - would pick **8 GB**
on a box with 62 GiB visible to it. TestContainers does not let it: `CassandraContainer` pins
`MAX_HEAP_SIZE=1024M` and `HEAP_NEWSIZE=128M` explicitly, overriding the auto-sizing formula
entirely. That pin turned out to be load-bearing on this machine, not just tidy: a bare
`docker run cassandra:5.0.8` with no heap override gets **OOM-killed within ~20 seconds** here - the
node's auto-sized 8 GB heap overshoots something in this environment's nested Docker setup (a
sidecar-hosted daemon; see the container's own `CLAUDE.md`) even though `free -h` and
`/sys/fs/cgroup/memory.max` both claim 62 GiB and "unlimited" are available. TestContainers'
pin is why every container benchmark in this file ran at all. A reader on a differently-sized or
differently-nested box should not expect 8 GB either way - the pin, not the formula, is what
actually governs.

## Query turnaround, apples to apples

`StatementBenchmark`'s core set, run three ways against the same fixture (1,000 rows,
`AverageTime`, 1 fork, 5 x 1 s warmup, 5 x 1 s measurement). Microseconds per operation.

| benchmark | in process | wire | container |
| --- | ---: | ---: | ---: |
| `selectPoint` | **12.5** | 91.3 | 166.5 |
| `insertPrepared` | **9.7** | 79.9 | 173.5 |
| `update` | **14.8** | 89.1 | 168.6 |
| `deleteByPrimaryKey` | **11.9** | 83.7 | 161.0 |
| `selectScan` (1,000 rows) | **662.3** | 1,208.5 | 2,957.6 |
| `batch100` | **930.0** | 1,057.0 | 1,254.6 |

The wire path is 6-8x SeaStar's in-process cost for a single statement - the socket round trip and
the driver's request pipeline - and the container adds another 1.5-2x on top of that: a real
replica write, a commit log, a memtable, none of which the wire path pays.

**`batch100` no longer breaks the pattern.** Last sitting's anomaly - the container answering a
100-statement logged batch *faster* than either SeaStar path - is gone. Profiling `batch100` with
async-profiler (`-prof async`, cpu event, off the built `jmh` jar directly - the Gradle plugin has
no wired-up way to pass `-prof`) found `java.util.stream` pipeline setup - a fresh
`Spliterator`/`Sink` chain and megamorphic interface dispatch, rebuilt on every call - as the
single largest cost in the INSERT translation path, ahead of even the ANTLR re-parse each
un-prepared batch child requires: `Target`'s primary/partition-key-name sets and several
`InsertHandler`/`Modifications` helpers were rebuilding a `Stream` from a 1-3 element collection on
every INSERT, two or three times over in places. Converting those to loops (`1600072`) took
`batch100` in process from **1,337.1** (`bd16934`) to **930.0** us/op here - a 30% drop - while the
container number moved only within noise (**1,239.5** -> **1,254.6**); the wire path improved too
(**1,559.5** -> **1,057.0**). The table above now reads the same way every other row does: in
process fastest, wire in the middle, container slowest.

**Batch atomicity's cost, for context:** between `f7d21b5` and `bd16934`, `insertPrepared` moved
13.2 -> 14.2 us/op and `batch100` moved 1,257.7 -> 1,337.1 us/op (both +6-8%) - the
write-lock-for-the-whole-batch change `7bbe242` shipped is exactly that shape of cost, and it is
small next to the stream-vs-loop difference above.

### TRUNCATE's reset cost

The README matrix says TRUNCATE is slow on a container because `auto_snapshot` snapshots the table
to disk first. SeaStar's own TRUNCATE is **1.35 us/op** (JMH, steady state) - there is no disk, so
there is nothing to snapshot - against an **18.4-ms fresh seeded session** as the realistic
alternative (`build.schema.warm` below). On the container, one sample each:

| `auto_snapshot` | TRUNCATE (ms) |
| --- | ---: |
| `true` (default) | 227.5 |
| `false` | 225.1 |

The two are within about 1% of each other - one sample per setting is not enough to say whether
`auto_snapshot`'s snapshot-before-truncate cost is real or noise at this table's size. Last sitting
`false` measured slower; this sitting `true` did; both margins are smaller than ordinary
run-to-run variance. What both sittings agree on: **~225 ms for one TRUNCATE**, against SeaStar's
1.35 us, is the actual gap behind the README's "TRUNCATE is slow" line, snapshot or not.

## cassandra-unit

Facts verified before the first sitting, restated here rather than re-researched: cassandra-unit's
latest release is 4.3.1.0 (2020-01-09), bundling cassandra-all 3.11.5 and (as an `<optional>`
Maven dependency that has to be added by hand) driver 4.3.1; last commit 2022-09-03; repository not
archived; Cassandra 3.11 runs on JDK 8 only.

Harness: `CassandraUnitProbe`, a standalone class in its own `cassandraUnitBench` source set with
no dependency on anything else in this build - cassandra-all 3.11.5 and driver 4.3.1 would shadow
the pinned 5.0.8/4.19.3 versions the same way TestContainers' driver does (HANDOVER trap 4), so
isolation is structural, not just convention. Compiled with the build's normal (17) compiler using
`-source 8 -target 8` (an actual JDK 8 `javac` rejects Gradle's `--release` flag), run under a JDK 8
launcher `ColdJvmBenchmark` forks by hand. 5 cold JVMs; the probe calls `System.exit` at the end
because cassandra-unit leaves non-daemon threads running after `stopEmbeddedCassandra()`.

| metric | median | min-max |
| --- | ---: | :--- |
| `boot` — `startEmbeddedCassandra` | **2,085** | 2,041-2,134 |
| `connect` — driver 4.3.1 to 127.0.0.1:9142 | **624** | 606-657 |
| `schema` — 75 statements, replayed one at a time | **1,218** | 1,184-1,236 |
| `main.to.first.query` | **4,020** | 3,963-4,129 |
| `query.warm` | **0.35** | 0.33-0.49 |
| `memory.heap.used.mb` | **55.3** | 54.1-56.0 |
| `memory.rss.kb` | **695,912** (≈680 MB) | 668,660-715,184 |

Like the container and wire benchmarks, the schema replay is measured with the driver's schema
debouncer shortened to 1 ms - left alone, 75 DDL statements cost **~76 seconds** here, almost
entirely debounce window; shortened, it is 1.2 seconds. Say so because the unshortened number was
the first thing this harness produced, and it would have been a wildly misleading headline.

**Compatibility.** All 75 fixture-schema statements succeed on 3.11.5 - `schema.rejected` is 0
every sample. `BenchmarkSchema` uses only basic types, UDTs, and secondary indexes, all supported
since well before 3.11, so this is not a surprising result, but it is a measured one rather than an
assumed one.

**JDK.** The probe does not fail loudly on JDK 17 - it **hangs silently**, past `boot`, with no
exception: logging stops mid-initialization (`AbstractCommitLogService`'s "Will update the
commitlog markers..." line is the last thing printed) and the process never returns. Ancient Netty
and JNA are the likely cause; not investigated further, since the version numbers already make the
point.

**Fidelity.** cassandra-unit is a real Cassandra 3.11.5 node - the honest statement, and also the
problem: 3.11 is not what a reader's production cluster runs, and the gap between what 3.11 accepts
and what 4.0+/5.0 accepts (or vice versa) is real even though this fixture schema doesn't happen to
cross it. Whether the fidelity suite's exception expectations would hold against 3.11 was judged out
of scope - the version numbers already carry the point.

## Startup, SeaStar

`:seastar:startupBenchmark`, 20 cold JVMs, one fresh JVM per sample. Milliseconds.

| metric | median | min-max |
| --- | ---: | :--- |
| `jvm.to.main` — JVM start to the first statement of `main` | **60** | 59-64 |
| `build.cold` — first `SeaStarCqlSession.builder().build()` | **415.0** | 383.0-442.3 |
| `main.to.build.cold` | **434.0** | 403.0-461.2 |
| `jvm.to.build.cold` | **478** | 450-505 |
| `query.first` — first `execute` after that build | **170.7** | 167.4-175.7 |
| `main.to.first.query` | **605.4** | 574.0-629.4 |
| `jvm.to.first.query` | **649** | 621-673 |
| `build.warm` — subsequent builds in the same JVM | **6.20** | 5.90-6.65 |
| `query.warm` — subsequent first-queries in the same JVM | **0.44** | 0.41-0.49 |

Nearly all of `query.first` is cassandra-all's CQL parser loading itself. `:seastar:parserCostBenchmark`
measures a bare JVM that does nothing but parse one statement at **310.6 ms and 1,195 classes**, and
reaching the parser through `QueryProcessor.parseStatement` rather than `CQLFragmentParser` would
cost **473.7 ms, 1,126 more classes** - which is why `handlers.CqlParsers` calls the latter.

### Seeded with a fixture schema

`:seastar:startupSchemaBenchmark`, same harness. The realistic fixture - 5 keyspaces x 10 tables, 10
UDTs and 10 secondary indexes, **75 statements** - replayed through `withSchema`.

| metric | median | min-max |
| --- | ---: | :--- |
| `build.schema.cold` — build including the schema replay | **642.7** | 603.8-679.1 |
| `main.to.schema.ready` | **679.0** | 640.0-715.6 |
| `jvm.to.schema.ready` | **723** | 685-759 |
| `query.first` — first query after that (parser already loaded) | **19.9** | 19.1-22.3 |
| `build.schema.warm` — subsequent seeded builds in the same JVM | **18.4** | 17.2-20.3 |

Seeding 75 statements costs about **227.8 ms cold** (642.7 - 415.0) and about **12.2 ms warm**
(18.4 - 6.2), i.e. roughly **0.16 ms per schema statement** once the JVM is warm. A test class that
builds a seeded session per test method pays 18.4 ms each time, not 642.7 ms.

## Over the wire

`:seastar-server:wireStartupBenchmark`, 20 cold JVMs. `seastar-server` puts a socket, Netty and a
stock driver in front of the same in-memory session. Milliseconds.

| phase | median | min-max |
| --- | ---: | :--- |
| `jvm.to.main` | 59 | 58-60 |
| `seastar.build` — the in-memory session | 309.0 | 303.5-315.0 |
| `server.start` — Netty, the bootstrap, the bind | 103.5 | 101.7-106.1 |
| `main.to.listening` | 476.2 | 466.8-480.5 |
| `jvm.to.listening` | 518 | 509-523 |
| `driver.connect.cold` — handshake, `system.local`, schema metadata | 503.3 | 492.7-518.2 |
| `query.first` — a `CREATE KEYSPACE` over the socket | 174.2 | 169.9-176.1 |
| **`main.to.first.query`** | **1,152.1** | 1,135.2-1,168.6 |
| `jvm.to.first.query` | 1,195 | 1,178-1,211 |
| `driver.connect.warm` — a second `CqlSession` in the same JVM | 78.6 | 65.0-93.7 |

The driver is still the expensive part: half a second to connect is more than the whole of SeaStar's
own startup. Per-statement figures from this same harness - `select.wire` (100-sample warm median,
1.08 ms), `select.inProcess` (0.31 ms), `select.wire.prepared` (0.74 ms) - are superseded by the JMH
steady-state numbers in
[Query turnaround](#query-turnaround-apples-to-apples), which are what to cite; they are kept here
only because the phase breakdown above is this harness's own story, not a per-statement one.

## On a socket: shortening the schema debouncer

The driver holds a DDL statement's answer until the metadata refresh it triggered has finished, and
debounces that refresh by `advanced.metadata.schema.debouncer.window` - one second by default. Every
wire, container, and cassandra-unit benchmark in this file that runs DDL sets this to 1 ms; it is
client-side driver config, not something `seastar-server` can impose on a connecting client (there
is no SeaStar-owned builder for the wire path - a connecting driver always uses the stock
`CqlSession.builder()`), so the config is duplicated per harness rather than centralized. Left alone
it is not a small effect: cassandra-unit's 75-statement schema replay goes from ~76 s to 1.2 s - the
number quoted in [cassandra-unit](#cassandra-unit) above is the shortened one.

## Per statement, warm

`:seastar:jmh`. `AverageTime`, 1 fork, 5 x 1 s measurement (`StatementBenchmark` warms 5 x 1 s,
`SelectScalingBenchmark` 3 x 1 s). Microseconds per operation. Every statement with a WHERE clause
runs against a table holding **1,000 rows**.

| benchmark | us/op |
| --- | ---: |
| `prepareCached` — `prepare` of an already prepared query | **0.354** ± 0.002 |
| `truncate` — steady state is TRUNCATE on an already-empty table | **1.349** ± 0.028 |
| `createTable` | **6.370** ± 0.409 |
| `prepareUncached` — `prepare` of a new query string | **7.465** ± 0.211 |
| `prepareUncachedResolved` — new query, bind metadata read | **7.643** ± 0.714 |
| `insertPrepared` — bound statement | **9.731** ± 0.038 |
| `deleteByPrimaryKey` | **11.944** ± 0.891 |
| `insertLiteral` — literal values in the CQL string | **11.568** ± 0.508 |
| `selectPoint` — `SELECT name ... WHERE id = ?` | **12.527** ± 0.160 |
| `update` — `UPDATE ... SET name = ? WHERE id = ?` | **14.775** ± 1.031 |
| `selectScan` — `SELECT *`, all 1,000 rows | **662.290** ± 6.772 |
| `batch100` — driver `BatchStatement` of 100 INSERTs | **929.980** ± 26.856 |

- **`prepare` is eager**, same as before: `prepareUncached`/`prepareUncachedResolved` measure the
  same full parse (~7.5 us); `prepareCached` is a cache lookup (0.35 us).
- `truncate()`'s first invocation empties a 1,000-row table; steady state is TRUNCATE against an
  already-empty one, the same caveat `deleteByPrimaryKey` has always carried.
- `deleteByPrimaryKey` removes the row on its first invocation, so steady state measures the parse
  and a lookup that finds nothing.
- `insertPrepared` is now the fastest of the single-row mutations (was between `selectPoint` and
  `deleteByPrimaryKey` last sitting) - the batch100 stream-to-loop fix (see
  [Query turnaround](#query-turnaround-apples-to-apples)) touches every INSERT, not just batched
  ones, so its control moved along with `batch100`.
- `createTable`'s error bar is ~6% of its score, down from ~16% two sittings ago.

### Scaling with table size

`SelectScalingBenchmark`. Microseconds per operation.

| rows | `selectPoint` | `selectAll` |
| ---: | ---: | ---: |
| 10 | 12.61 ± 0.18 | 9.93 ± 0.11 |
| 1,000 | 13.04 ± 0.44 | 668.43 ± 51.58 |
| 100,000 | 13.28 ± 0.04 | 106,089.52 ± 16,013.52 |

Unchanged story from previous sittings: a point lookup does not scale with the table (12.6 us at 10
rows, 13.3 us at 100,000 rows), so what is left is the per-statement parse cost, not the store.
`selectAll` stays O(rows), settling at roughly **1.06 us/row** at 100,000.

## What is not benchmarked

- **Range operators.** `WHERE ck > 1` and `WHERE ck >= 1 AND ck <= 2` are implemented but not yet
  measured: a range scan is exactly where the full-scan read path will hurt.
- **Materialized views** are unsupported by design rather than missing. `KeyspaceMetadata#getViews()`
  is always empty, which is what a cluster answers for a keyspace without views.
- **cassandra-unit's per-statement cost** - this sitting measured its startup/schema/memory shape,
  not a JMH turnaround table. Its own driver (4.3.1) and node (3.11.5) are different enough from the
  pinned versions that a steady-state comparison would need its own isolated JMH source set, the
  same way the container benchmark needed `containerBench`; not done here.

[docs/support-matrix.md](docs/support-matrix.md) is the maintained list of what works and what does
not. A second copy of it here would rot faster than anything else in the repo, so there is not one.

One historical note that still shapes the harness: bulk loading through `INSERT` used to be O(n^2),
because `InsertHandler` scanned the table for an existing primary key on every insert. The partition
index fixed that, but `BenchmarkFixture` still seeds through `SeaStarTable.addRow` - deliberately, so
the scaling benchmarks keep measuring what they have always measured. The container and cassandra-unit
benchmarks in this sitting cannot use that shortcut - there is no bypassing `INSERT` against a real
node - so their fixtures seed through bounded-concurrency async `INSERT`s instead; see
`ContainerStatementBenchmark`/`ContainerProbe`'s `seed()`/async-window code.

## Reproducing

Docker is needed for the container comparisons; a JDK 8 toolchain (downloaded via foojay on first
use) is needed for cassandra-unit. Neither is needed for `./gradlew build`. Benchmarks are not on
the default build and their classes are not in the published jar.

```bash
# Per-statement and scaling benchmarks (JMH): StatementBenchmark, SelectScalingBenchmark. ~3 minutes.
./gradlew :seastar:jmh

# A single benchmark class or method.
./gradlew :seastar:jmh -PjmhIncludes='com.tagadvance.seastar.bench.StatementBenchmark.selectPoint'

# The wire backend's per-statement JMH turnaround, WireStatementBenchmark. Lives in :seastar-server,
# not :seastar's jmh source set, so it can depend on SeaStarProtocolServer. ~1.5 minutes.
./gradlew :seastar-server:wireTurnaroundBenchmark

# Cold and warm startup, plus the in-situ parser split. 20 forked JVMs per variant. ~50 seconds.
./gradlew :seastar:startupBenchmark

# Startup with a 75-statement fixture schema. ~20 seconds.
./gradlew :seastar:startupSchemaBenchmark

# Heap/RSS after seeding the fixture schema and loading 0/1,000/100,000 rows. ~25 seconds.
./gradlew :seastar:startupMemoryBenchmark

# Standalone parser cost attribution. ~30 seconds.
./gradlew :seastar:parserCostBenchmark

# A correctness gate, not a measurement: 14 queries parsed through both entry points, and anything
# other than 0 mismatches is a bug. ~10 seconds.
./gradlew :seastar:parserEquivalenceCheck

# What the wire costs. 20 forked JVMs. ~3 minutes.
# Run it in the same sitting as :seastar:startupBenchmark - the two are compared against each other.
./gradlew :seastar:startupBenchmark :seastar-server:wireStartupBenchmark

# TestContainers comparison. Needs Docker. ~35 seconds warm, ~20 seconds cold.
./gradlew :seastar:containerWarmBenchmark
./gradlew :seastar:containerColdBenchmark

# Container and driver-side memory after seeding the schema and 0/1,000/100,000 rows. Needs Docker.
# ~2 minutes.
./gradlew :seastar:containerMemoryBenchmark

# TRUNCATE reset cost, with and without auto_snapshot. Needs Docker. ~40 seconds.
./gradlew :seastar:truncateBenchmark

# Per-statement JMH turnaround against a real container. Needs Docker. ~2-3 minutes; one container
# boot per @Benchmark method, since JMH forks per method.
./gradlew :seastar:containerTurnaroundBenchmark

# cassandra-unit: startup, 75-statement schema replay, memory, compatibility. Downloads a JDK 8
# toolchain on first run. ~1 minute for 5 samples.
./gradlew :seastar:cassandraUnitBenchmark
```

`containerColdBenchmark` removes every local tag of `cassandra:5.0.8` so that the pull is real, then
restores the tags it removed. Run it only when you want that, and run it last in a sitting - nothing
else in this file depends on the image being absent, and everything after it would otherwise pay an
unwanted pull.

**Which JDK you get.** The toolchain is pinned to 17, so `:seastar:jmh`, `containerTurnaroundBenchmark`
and `wireTurnaroundBenchmark` all fork the toolchain's launcher and their numbers are JDK 17 numbers,
even though the latter two are plain `JavaExec` tasks running JMH's `Main` rather than the
`me.champeau.jmh` plugin's own task. The cold-JVM harnesses
are `JavaExec` tasks with no `javaLauncher`, so they fork **the Gradle daemon's JVM** - 25 here,
except `cassandraUnitBenchmark`, whose forked child is pinned to a JDK 8 launcher regardless of the
daemon's JDK, because Cassandra 3.11 does not run on anything newer. Startup figures therefore depend
on the daemon's JDK for every backend except cassandra-unit; check `./gradlew --version` before
comparing against this file.

The tasks are serialized against each other by a Gradle shared service, so listing several in one
invocation is safe even with `org.gradle.parallel=true`. They are still affected by anything else
running on the machine; close everything else first.
