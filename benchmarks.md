# SeaStar benchmarks

Every number in this file was taken in **one sitting on one machine**, at commit `f0ed8e8`, starting
2026-08-23T05:20:45Z, on an otherwise idle box. Total wall clock about 18 minutes.

**Earlier revisions of this file are superseded, not supplemented.** The previous sitting
(commit `f7d21b5`, 2026-07-30) measured everything from JVM start, which folds the JVM's own boot
into every number; TestContainers and cassandra-unit were credited a head start they didn't
actually have because their harnesses started the clock later. This sitting fixes that — see
[The first-instruction clock](#the-first-instruction-clock) — and adds memory, apples-to-apples
turnaround for the wire and container backends, TRUNCATE's reset cost, and a cassandra-unit
harness. Batch atomicity (`7bbe242`) landed between the two sittings; `batch100` and the insert
benchmarks moved a few percent (noted where relevant below), nothing more.

## Environment

| | |
| --- | --- |
| CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads |
| RAM | 62 GiB |
| OS | Debian GNU/Linux 13 (trixie) in a container, kernel 6.12.101+deb13-amd64 |
| JDK, cold-JVM harnesses | Temurin 25.0.3+9-LTS |
| JDK, JMH | Temurin 17.0.20+8 |
| JDK, cassandra-unit | Temurin 8 (foojay-downloaded) |
| Gradle | 9.4.1 |
| Docker | Engine 28.5.2 |
| cassandra-all | 5.0.8 |
| java-driver-core | 4.19.3 |
| Container image | `cassandra:5.0.8` |
| cassandra-unit | 4.3.1.0 (cassandra-all 3.11.5, driver 4.3.1) |
| JMH | 1.36 |

Same machine as the previous sitting; the CPU frequency range dropped out of `lscpu` this time and
isn't worth chasing. Three JDKs, and it is not an oversight — see [Reproducing](#reproducing).

## The first-instruction clock

Every backend's headline number is now measured from the first statement of its harness's
`main()` — before any reference to a SeaStar, cassandra-all, or Cassandra class that could trigger
class loading — rather than from JVM start. The previous sitting used `RuntimeMXBean` uptime for
SeaStar and the wire probe, which includes the JVM's own boot (**~61 ms** here, `jvm.to.main` below,
paid identically by every backend whether or not it touches SeaStar), while the container and
cassandra-unit probes started their clock even later, at `container.start()` and
`EmbeddedCassandraServerHelper.startEmbeddedCassandra()` respectively — excluding not just JVM boot
but the harness's own contact-point/Docker-client setup. Both were unfair to SeaStar's two
in-process rows, which is the opposite of a flattering bug but a bug regardless.

`main.to.first.query` is that convention for every backend below. The ratios barely move from the
old `jvm.to.*` numbers — SeaStar's own class loading dwarfs 61 ms — but the convention is now the
same for all five rows, which it wasn't before.

## Headline: five ways to get a usable session

Time from the first instruction of the harness to a query that has returned. Median, milliseconds,
`main.to.first.query`.

| | ms | versus a warm container |
| --- | ---: | ---: |
| SeaStar, in process | **626** | **13.1x faster** |
| SeaStar, over the wire (`seastar-server` and a stock driver) | **1 157** | **7.1x faster** |
| A second SeaStar session in the same JVM | **6.2** | ~1 320x faster |
| cassandra-unit (embedded Cassandra 3.11.5) | 4 059 | 2.0x faster |
| TestContainers Cassandra, image already pulled | 8 189 | — |
| TestContainers Cassandra, image not present | 14 841 (3 774 pull + ~11 067) | — |

("A second SeaStar session" is `build.warm` from the startup table below, 6.21 ms — building the
session only, same convention as the previous sitting; add `query.warm`'s 0.44 ms for build-and-query.
The JMH steady-state numbers in [Query turnaround](#query-turnaround-apples-to-apples) are smaller
still but measure a single statement, not a whole session.)

Read this as a fixture cost, not a claim about query speed — see
[Query turnaround](#query-turnaround-apples-to-apples) for that. cassandra-unit's node boots in
about 2.1 s (embedded, no Docker, no network), but its 75-statement schema replay and the driver's
own connect cost put it well behind SeaStar and only 2x ahead of a real container. The container
figures are `main.to.first.query`, which — new this sitting — includes the Docker client and
contact-point setup that `container.start()` alone excluded; that adds a few tens of milliseconds,
not the difference between rows.

## Memory

Two numbers per backend, both after the same sequence: build/boot, seed the 75-statement fixture
schema, then load *N* rows into one extra table, *N* in {0, 1 000, 100 000}. Heap is the test JVM's
own `MemoryMXBean` reading after three `System.gc()` passes, minus the same reading taken before
anything happened; RSS is `/proc/self/status` `VmRSS` (Linux only).

| backend | rows | heap used (MB) | RSS delta (MB) |
| --- | ---: | ---: | ---: |
| SeaStar, in process | 0 | 7.5 | 81.6 |
| SeaStar, in process | 1 000 | 8.1 | 80.2 |
| SeaStar, in process | 100 000 | 63.1 | 265.3 |
| Container, driver side | 0 | 18.0 | 227.3 |
| Container, driver side | 1 000 | 18.0 | 256.6 |
| Container, driver side | 100 000 | 17.9 | 351.6 |
| cassandra-unit, embedded (schema only, no row sweep) | — | 54.4 | — |

SeaStar's heap grows **~0.6 MB per 1 000 rows** at low counts and **~56 MB for 100 000** — call it
**~560 bytes/row**, which matches the "rows are stored deserialized" note in
[Per statement, warm](#per-statement-warm): a row is a handful of boxed `Integer`/`String`/`Double`
objects, not a byte-packed record. The container's driver-side heap barely moves with row count (it
is metadata and connection-pool state, not application data); its RSS delta grows a bit with rows,
most plausibly the 200-wide async-insert window's own buffering rather than anything durable.

**Process footprint.** The SeaStar/cassandra-unit numbers above are the whole story for an
in-process backend — there is no second process. For the container:

| | RSS | notes |
| --- | ---: | --- |
| Container process (`docker stats`) | ~2.1–2.4 GB | grows slightly with row count; single samples, not a tight range |
| Container heap reservation (`nodetool info`, committed) | **1 024 MB** | see below |
| cassandra-unit JVM (`VmRSS`) | ~648–708 MB | one process holds both the embedded node and the test |

`cassandra-env.sh`'s own formula — `max(min(RAM/2, 1 GB), min(RAM/4, 8 GB))` — would pick **8 GB**
on a box with 62 GiB visible to it. TestContainers does not let it: `CassandraContainer` pins
`MAX_HEAP_SIZE=1024M` and `HEAP_NEWSIZE=128M` explicitly, overriding the auto-sizing formula
entirely. That pin turned out to be load-bearing on this machine, not just tidy: a bare
`docker run cassandra:5.0.8` with no heap override gets **OOM-killed within ~20 seconds** here — the
node's auto-sized 8 GB heap overshoots something in this environment's nested Docker setup (a
sidecar-hosted daemon; see the container's own `CLAUDE.md`) even though `free -h` and
`/sys/fs/cgroup/memory.max` both claim 62 GiB and "unlimited" are available. TestContainers'
pin is why every container benchmark in this file ran at all. A reader on a differently-sized or
differently-nested box should not expect 8 GB either way — the pin, not the formula, is what
actually governs.

## Query turnaround, apples to apples

The README used to say a warm container answers a point select "in about the same millisecond" as
SeaStar's wire path — that was one un-JITted shot from `WireStartupProbe`'s 100-sample warm median,
unfair to both sides. Below is the real comparison: `StatementBenchmark`'s core set, run three ways
against the same fixture (1 000 rows, `AverageTime`, 1 fork, 5 x 1 s warmup, 5 x 1 s measurement).
Microseconds per operation.

| benchmark | in process | wire | container |
| --- | ---: | ---: | ---: |
| `selectPoint` | **13.4** | 90.7 | 167.4 |
| `insertPrepared` | **14.2** | 85.9 | 177.2 |
| `update` | **17.8** | 94.9 | 170.4 |
| `deleteByPrimaryKey` | **15.3** | 88.6 | 162.9 |
| `selectScan` (1 000 rows) | **676.5** | 1 214.0 | 2 961.3 |
| `batch100` | **1 337.1** | 1 559.5 | **1 239.5** |

The wire path is 6-7x SeaStar's in-process cost for a single statement — the socket round trip and
the driver's request pipeline, same as before — and the container adds another 1.5-2x on top of
that: a real replica write, a commit log, a memtable, none of which the wire path pays. The
container was run with the same 1 ms schema-debounce shortening the wire benchmarks use (see
[On a socket](#on-a-socket-shortening-the-schema-debouncer) below), so none of this is debounce
window.

**`batch100` is the exception, and it is real:** the container answers a 100-statement logged batch
*faster* than the wire path does, not slower. A logged batch is one native-protocol message
either way, so the container's request pipeline evidently amortizes 100 children better than
`seastar-server`'s does over the same socket — worth a look, not explained further here. Printed
because a table with only one direction of surprise isn't believable.

**The wire number that moved:** `WireStartupProbe`'s own warm figures — the ones the old README
quoted (`select.wire` **1.11 ms**, `select.wire.prepared` **0.76 ms**, reproduced below) — are a
100-sample median taken right after a cold JVM boots. JMH's steady state above (**90.7 us** prepared
and bound) is **~8x faster still**. A hundred iterations is not nearly enough to JIT Netty's codecs
and the driver's async request pipeline; the cold-JVM harness was measuring an intermediate warmup
state, not the floor. Both numbers are real and both are labeled — "warm" in a 20-cold-JVM harness
and "steady state" in a 5 x 1 s JMH run are different things, and conflating them is exactly the
mistake goal 2's "about the same millisecond" line made once already.

**Batch atomicity's cost, one sitting later:** `insertPrepared` moved 13.2 -> 14.2 us/op and
`batch100` moved 1 257.7 -> 1 337.1 us/op since `f7d21b5` (both +6-8%) — the write-lock-for-the-whole-batch
change `7bbe242` shipped is exactly this shape of cost, and it is small.

### TRUNCATE's reset cost

The README matrix says TRUNCATE is slow on a container because `auto_snapshot` snapshots the table
to disk first. SeaStar's own TRUNCATE is **1.34 us/op** (JMH, steady state) — there is no disk, so
there is nothing to snapshot — against a **19-ms fresh seeded session** as the realistic
alternative (`build.schema.warm` above). On the container, one sample each:

| `auto_snapshot` | TRUNCATE (ms) |
| --- | ---: |
| `true` (default) | 215 |
| `false` | 285 |

That is the *opposite* of the expected direction, and it is one sample per setting — noise-level
for a single TRUNCATE against a small table, not a finding. What the container number does say
plainly: **215-285 ms for one TRUNCATE**, against SeaStar's 1.34 us, is the actual gap behind the
README's "TRUNCATE is slow" line, snapshot or not.

## cassandra-unit

Facts verified before this sitting, restated here rather than re-researched: cassandra-unit's
latest release is 4.3.1.0 (2020-01-09), bundling cassandra-all 3.11.5 and (as an `<optional>`
Maven dependency that has to be added by hand) driver 4.3.1; last commit 2022-09-03; repository not
archived; Cassandra 3.11 runs on JDK 8 only.

Harness: `CassandraUnitProbe`, a standalone class in its own `cassandraUnitBench` source set with
no dependency on anything else in this build — cassandra-all 3.11.5 and driver 4.3.1 would shadow
the pinned 5.0.8/4.19.3 versions the same way TestContainers' driver does (HANDOVER trap 4), so
isolation is structural, not just convention. Compiled with the build's normal (17) compiler using
`-source 8 -target 8` (an actual JDK 8 `javac` rejects Gradle's `--release` flag), run under a JDK 8
launcher `ColdJvmBenchmark` forks by hand. 5 cold JVMs; the probe calls `System.exit` at the end
because cassandra-unit leaves non-daemon threads running after `stopEmbeddedCassandra()`.

| metric | median | min-max |
| --- | ---: | :--- |
| `boot` — `startEmbeddedCassandra` | **2 118** | 2 081-2 153 |
| `connect` — driver 4.3.1 to 127.0.0.1:9142 | **630** | 608-652 |
| `schema` — 75 statements, replayed one at a time | **1 211** | 1 199-1 238 |
| `main.to.first.query` | **4 059** | 4 013-4 104 |
| `query.warm` | **0.50** | 0.35-0.52 |
| `memory.heap.used.mb` | **54.4** | 54.2-55.6 |
| `memory.rss.kb` | **686 092** (≈670 MB) | 663 852-725 480 |

Like the container and wire benchmarks, the schema replay is measured with the driver's schema
debouncer shortened to 1 ms — left alone, 75 DDL statements cost **~76 seconds** here, almost
entirely debounce window; shortened, it is 1.2 seconds. Say so because the unshortened number was
the first thing this harness produced, and it would have been a wildly misleading headline.

**Compatibility.** All 75 fixture-schema statements succeed on 3.11.5 — `schema.rejected` is 0
every sample. `BenchmarkSchema` uses only basic types, UDTs, and secondary indexes, all supported
since well before 3.11, so this is not a surprising result, but it is a measured one rather than an
assumed one.

**JDK.** The probe does not fail loudly on JDK 17 — it **hangs silently**, past `boot`, with no
exception: logging stops mid-initialization (`AbstractCommitLogService`'s "Will update the
commitlog markers..." line is the last thing printed) and the process never returns. Confirmed by
running it under this build's normal JDK 17 compiler-and-launcher default for over 90 seconds with
no further progress and no exit. Ancient Netty and JNA are the likely cause; not investigated
further, since the version numbers already make the point.

**Fidelity.** cassandra-unit is a real Cassandra 3.11.5 node — the honest statement, and also the
problem: 3.11 is not what a reader's production cluster runs, and the gap between what 3.11 accepts
and what 4.0+/5.0 accepts (or vice versa) is real even though this fixture schema doesn't happen to
cross it. Whether the fidelity suite's exception expectations would hold against 3.11 was judged out
of scope — the version numbers already carry the point.

## Startup, SeaStar

`:seastar:startupBenchmark`, 20 cold JVMs, one fresh JVM per sample. Milliseconds.

| metric | median | min-max |
| --- | ---: | :--- |
| `jvm.to.main` — JVM start to the first statement of `main` | **61** | 60-62 |
| `build.cold` — first `SeaStarCqlSession.builder().build()` | **432.0** | 406.7-460.0 |
| `main.to.build.cold` | **451.5** | — |
| `jvm.to.build.cold` | **497** | 470-525 |
| `query.first` — first `execute` after that build | **175.5** | 174.2-183.5 |
| `main.to.first.query` | **626.3** | — |
| `jvm.to.first.query` | **671** | 646-703 |
| `build.warm` — subsequent builds in the same JVM | **6.21** | 6.04-7.70 |
| `query.warm` — subsequent first-queries in the same JVM | **0.44** | 0.39-0.51 |

Nearly all of `query.first` is cassandra-all's CQL parser loading itself. `:seastar:parserCostBenchmark`
measures a bare JVM that does nothing but parse one statement at **312 ms and 1 195 classes**, and
reaching the parser through `QueryProcessor.parseStatement` rather than `CQLFragmentParser` would
cost **495 ms, 1 125 more classes** — which is why `handlers.CqlParsers` calls the latter.

### Seeded with a fixture schema

`:seastar:startupSchemaBenchmark`, same harness. The realistic fixture — 5 keyspaces x 10 tables, 10
UDTs and 10 secondary indexes, **75 statements** — replayed through `withSchema`.

| metric | median | min-max |
| --- | ---: | :--- |
| `build.schema.cold` — build including the schema replay | **646.2** | 622.3-669.0 |
| `main.to.schema.ready` | **683.0** | — |
| `jvm.to.schema.ready` | **727** | 703-756 |
| `query.first` — first query after that (parser already loaded) | **19.9** | 19.7-21.4 |
| `build.schema.warm` — subsequent seeded builds in the same JVM | **18.0** | 17.1-20.2 |

Seeding 75 statements costs about **214 ms cold** (646.2 - 432.0) and about **11.8 ms warm**
(18.0 - 6.2), i.e. roughly **0.16 ms per schema statement** once the JVM is warm. A test class that
builds a seeded session per test method pays 18 ms each time, not 646 ms.

## Over the wire

`:seastar-server:wireStartupBenchmark`, 20 cold JVMs. `seastar-server` puts a socket, Netty and a
stock driver in front of the same in-memory session. Milliseconds, median.

| phase | ms |
| --- | ---: |
| `jvm.to.main` | 58 |
| `seastar.build` — the in-memory session | 309.8 |
| `server.start` — Netty, the bootstrap, the bind | 103.1 |
| `main.to.listening` | 475.2 |
| `jvm.to.listening` | 517 |
| `driver.connect.cold` — handshake, `system.local`, schema metadata | 504.4 |
| `query.first` — a `CREATE KEYSPACE` over the socket | 175.3 |
| **`main.to.first.query`** | **1 157.3** |
| `jvm.to.first.query` | 1 199 |
| `driver.connect.warm` — a second `CqlSession` in the same JVM | 76.1 |

The driver is still the expensive part: half a second to connect is more than the whole of SeaStar's
own startup. Per-statement figures from this same harness — `select.wire` (100-sample warm median,
1.11 ms), `select.inProcess` (0.31 ms), `select.wire.prepared` (0.76 ms) — are superseded by the JMH
steady-state numbers in
[Query turnaround](#query-turnaround-apples-to-apples), which are what to cite; they are kept here
only because the phase breakdown above is this harness's own story, not a per-statement one.

## On a socket: shortening the schema debouncer

The driver holds a DDL statement's answer until the metadata refresh it triggered has finished, and
debounces that refresh by `advanced.metadata.schema.debouncer.window` — one second by default. Every
wire, container, and cassandra-unit benchmark in this file that runs DDL sets this to 1 ms; it is
client-side driver config, not something `seastar-server` can impose on a connecting client (there
is no SeaStar-owned builder for the wire path — a connecting driver always uses the stock
`CqlSession.builder()`), so the config is duplicated per harness rather than centralized. Left alone
it is not a small effect: cassandra-unit's 75-statement schema replay went from ~76 s to 1.2 s: the
number quoted in [cassandra-unit](#cassandra-unit) above is the shortened one.

## Per statement, warm

`:seastar:jmh`. `AverageTime`, 1 fork, 5 x 1 s measurement (`StatementBenchmark` warms 5 x 1 s,
`SelectScalingBenchmark` 3 x 1 s). Microseconds per operation. Every statement with a WHERE clause
runs against a table holding **1 000 rows**.

| benchmark | us/op |
| --- | ---: |
| `prepareCached` — `prepare` of an already prepared query | **0.386** ± 0.002 |
| `truncate` — steady state is TRUNCATE on an already-empty table | **1.335** ± 0.026 |
| `createTable` | **6.320** ± 1.035 |
| `prepareUncached` — `prepare` of a new query string | **7.345** ± 0.467 |
| `prepareUncachedResolved` — new query, bind metadata read | **7.573** ± 0.708 |
| `selectPoint` — `SELECT name ... WHERE id = ?` | **13.355** ± 0.538 |
| `insertPrepared` — bound statement | **14.214** ± 1.053 |
| `deleteByPrimaryKey` | **15.328** ± 0.622 |
| `insertLiteral` — literal values in the CQL string | **15.910** ± 0.621 |
| `update` — `UPDATE ... SET name = ? WHERE id = ?` | **17.773** ± 1.090 |
| `selectScan` — `SELECT *`, all 1 000 rows | **676.454** ± 16.570 |
| `batch100` — driver `BatchStatement` of 100 INSERTs | **1 337.101** ± 28.081 |

- **`prepare` is eager**, same as before: `prepareUncached`/`prepareUncachedResolved` measure the
  same full parse (~7.5 us); `prepareCached` is a cache lookup (0.39 us).
- `truncate()`'s first invocation empties a 1 000-row table; steady state is TRUNCATE against an
  already-empty one, the same caveat `deleteByPrimaryKey` has always carried.
- `deleteByPrimaryKey` removes the row on its first invocation, so steady state measures the parse
  and a lookup that finds nothing.
- `createTable`'s error bar is ~16% of its score, the noisiest of the set (was 60% last sitting;
  still noisy, less so).

### Scaling with table size

`SelectScalingBenchmark`. Microseconds per operation.

| rows | `selectPoint` | `selectAll` |
| ---: | ---: | ---: |
| 10 | 14.00 ± 1.79 | 10.22 ± 0.24 |
| 1 000 | 14.45 ± 0.37 | 680.90 ± 51.34 |
| 100 000 | 14.65 ± 0.62 | 102 381.90 ± 12 492.34 |

Unchanged story from last sitting: a point lookup does not scale with the table (14.0 us at 10 rows,
14.7 us at 100 000), so what is left is the per-statement parse cost, not the store. `selectAll`
stays O(rows), settling at roughly **1.02 us/row** at 100 000.

## What is not benchmarked

- **Range operators.** `WHERE ck > 1` and `WHERE ck >= 1 AND ck <= 2` are implemented but not yet
  measured: a range scan is exactly where the full-scan read path will hurt.
- **Materialized views** are unsupported by design rather than missing. `KeyspaceMetadata#getViews()`
  is always empty, which is what a cluster answers for a keyspace without views.
- **cassandra-unit's per-statement cost** — this sitting measured its startup/schema/memory shape,
  not a JMH turnaround table. Its own driver (4.3.1) and node (3.11.5) are different enough from the
  pinned versions that a steady-state comparison would need its own isolated JMH source set, the
  same way the container benchmark needed `containerBench`; not done here.

[docs/support-matrix.md](docs/support-matrix.md) is the maintained list of what works and what does
not. A second copy of it here would rot faster than anything else in the repo, so there is not one.

One historical note that still shapes the harness: bulk loading through `INSERT` used to be O(n^2),
because `InsertHandler` scanned the table for an existing primary key on every insert. The partition
index fixed that, but `BenchmarkFixture` still seeds through `SeaStarTable.addRow` — deliberately, so
the scaling benchmarks keep measuring what they have always measured. The container and cassandra-unit
benchmarks in this sitting cannot use that shortcut — there is no bypassing `INSERT` against a real
node — so their fixtures seed through bounded-concurrency async `INSERT`s instead; see
`ContainerStatementBenchmark`/`ContainerProbe`'s `seed()`/async-window code.

## Reproducing

Docker is needed for the container comparisons; a JDK 8 toolchain (downloaded via foojay on first
use) is needed for cassandra-unit. Neither is needed for `./gradlew build`. Benchmarks are not on
the default build and their classes are not in the published jar.

```bash
# Per-statement and scaling benchmarks (JMH): StatementBenchmark, SelectScalingBenchmark. ~4 minutes.
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

# Heap/RSS after seeding the fixture schema and loading 0/1 000/100 000 rows. ~15 seconds.
./gradlew :seastar:startupMemoryBenchmark

# Standalone parser cost attribution. ~30 seconds.
./gradlew :seastar:parserCostBenchmark

# A correctness gate, not a measurement: 14 queries parsed through both entry points, and anything
# other than 0 mismatches is a bug. ~10 seconds.
./gradlew :seastar:parserEquivalenceCheck

# What the wire costs. 20 forked JVMs. ~2 minutes.
# Run it in the same sitting as :seastar:startupBenchmark - the two are compared against each other.
./gradlew :seastar:startupBenchmark :seastar-server:wireStartupBenchmark

# TestContainers comparison. Needs Docker. ~25 seconds warm, ~20 seconds cold.
./gradlew :seastar:containerWarmBenchmark
./gradlew :seastar:containerColdBenchmark

# Container and driver-side memory after seeding the schema and 0/1 000/100 000 rows. Needs Docker.
# ~1 minute.
./gradlew :seastar:containerMemoryBenchmark

# TRUNCATE reset cost, with and without auto_snapshot. Needs Docker. ~1 minute.
./gradlew :seastar:truncateBenchmark

# Per-statement JMH turnaround against a real container. Needs Docker. ~2-3 minutes; one container
# boot per @Benchmark method, since JMH forks per method.
./gradlew :seastar:containerTurnaroundBenchmark

# cassandra-unit: startup, 75-statement schema replay, memory, compatibility. Downloads a JDK 8
# toolchain on first run. ~1 minute for 5 samples.
./gradlew :seastar:cassandraUnitBenchmark
```

`containerColdBenchmark` removes every local tag of `cassandra:5.0.8` so that the pull is real, then
restores the tags it removed. Run it only when you want that, and run it last in a sitting — nothing
else in this file depends on the image being absent, and everything after it would otherwise pay an
unwanted pull.

**Which JDK you get.** The toolchain is pinned to 17, so `:seastar:jmh`, `containerTurnaroundBenchmark`
and `wireTurnaroundBenchmark` all fork the toolchain's launcher and their numbers are JDK 17 numbers,
even though the latter two are plain `JavaExec` tasks running JMH's `Main` rather than the
`me.champeau.jmh` plugin's own task. The cold-JVM harnesses
are `JavaExec` tasks with no `javaLauncher`, so they fork **the Gradle daemon's JVM** — 25 here,
except `cassandraUnitBenchmark`, whose forked child is pinned to a JDK 8 launcher regardless of the
daemon's JDK, because Cassandra 3.11 does not run on anything newer. Startup figures therefore depend
on the daemon's JDK for every backend except cassandra-unit; check `./gradlew --version` before
comparing against this file.

The tasks are serialized against each other by a Gradle shared service, so listing several in one
invocation is safe even with `org.gradle.parallel=true`. They are still affected by anything else
running on the machine; close everything else first.
