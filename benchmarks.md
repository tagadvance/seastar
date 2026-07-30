# SeaStar benchmarks

Every number in this file was taken in **one sitting on one machine**, at commit `f7d21b5`, starting
2026-07-30T02:13:22Z, on an otherwise idle box. Total wall clock about eight minutes.

**Earlier revisions of this file are superseded, not supplemented.** They carried two environment
tables — a Ryzen 7 4700U laptop on Oracle JDK 17 for most sections, a Ryzen 9 5900XT on Temurin 25
for the wire section — so no figure could honestly be compared with any other. Both tables are gone.
Do not mix numbers out of git history with the ones below.

## Environment

| | |
| --- | --- |
| CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads, 0.55-4.98 GHz |
| RAM | 62 GiB |
| OS | Debian GNU/Linux 13 (trixie) in a container, kernel 6.12.96+deb13-amd64 |
| JDK, cold-JVM harnesses | Temurin 25.0.3+9-LTS |
| JDK, JMH | Temurin 17.0.20+8 |
| Gradle | 9.4.1 |
| Docker | Engine 28.5.2 |
| cassandra-all | 5.0.8 |
| java-driver-core | 4.19.3 |
| Container image | `cassandra:5.0.8` |
| JMH | 1.36 |

Two JDKs, and it is not an oversight — see [Reproducing](#reproducing). The bytecode target is 17
either way.

## Headline: SeaStar versus a real Cassandra container

Time from nothing to a query that has returned. Median, milliseconds.

| | ms | versus a warm container |
| --- | ---: | ---: |
| SeaStar, in process | **692** | **11.6x faster** |
| SeaStar, over the wire (`seastar-server` and a stock driver) | **1 258** | **6.4x faster** |
| A second SeaStar session in the same JVM | **6.4** | ~1 250x faster |
| TestContainers Cassandra, image already pulled | 8 010 | — |
| TestContainers Cassandra, image not present | 11 945 (3 160 pull + 8 785) | — |

Read this as a fixture cost, not a claim about query speed. The container figures include Docker
starting an image and Cassandra booting a node, which is what a suite actually pays per container;
once both are warm they answer a point select in about the same millisecond — see
[Over the wire](#over-the-wire).

Both SeaStar rows are measured from JVM start and include their own class loading, while the
container rows are measured from `container.start()` and exclude the JVM start a test pays anyway.
The gap therefore understates SeaStar's advantage rather than flattering it.

## Startup, SeaStar

`:seastar:startupBenchmark`, 20 cold JVMs, one fresh JVM per sample. Milliseconds.

| metric | median | min-max |
| --- | ---: | :--- |
| `build.cold` — first `SeaStarCqlSession.builder().build()` | **468.2** | 433.3-485.4 |
| `jvm.to.build.cold` — JVM start to that session existing | **518** | 482-535 |
| `query.first` — first `execute` after that build | **173.6** | 170.9-178.7 |
| `jvm.to.first.query` — JVM start to the first query returning | **692** | 654-713 |
| `build.warm` — subsequent builds in the same JVM | **6.44** | 6.14-6.88 |
| `query.warm` — subsequent first-queries in the same JVM | **0.45** | 0.41-0.51 |

Nearly all of `query.first` is cassandra-all's CQL parser loading itself. `:seastar:parserCostBenchmark`
measures a bare JVM that does nothing but parse one statement at **334 ms and 1 195 classes**, and
reaching the parser through `QueryProcessor.parseStatement` rather than `CQLFragmentParser` would
cost **494 ms, 1 126 more classes and a scheduled-tasks thread that never exits** — which is why
`handlers.CqlParsers` calls the latter.

### Seeded with a fixture schema

`:seastar:startupSchemaBenchmark`, same harness. A realistic fixture — 5 keyspaces x 10 tables, 10
UDTs and 10 secondary indexes, **75 statements** — replayed through `withSchema`.

| metric | median | min-max |
| --- | ---: | :--- |
| `build.schema.cold` — build including the schema replay | **672.6** | 641.7-700.4 |
| `jvm.to.schema.ready` — JVM start to a usable seeded session | **746** | 714-775 |
| `query.first` — first query after that (parser already loaded) | **20.6** | 20.2-22.6 |
| `build.schema.warm` — subsequent seeded builds in the same JVM | **19.2** | 18.6-20.1 |

Seeding 75 statements costs about **204 ms cold** (672.6 - 468.2) and about **12.8 ms warm**
(19.2 - 6.44), i.e. roughly **0.17 ms per schema statement** once the JVM is warm. A test class that
builds a seeded session per test method pays 19 ms each time, not 673 ms.

## Startup, TestContainers Cassandra

`:seastar:containerWarmBenchmark` (3 samples) and `:seastar:containerColdBenchmark` (1 sample).
Milliseconds, median. A cold pull is bandwidth bound and not usefully repeatable — treat that column
as an order of magnitude, not a measurement.

| metric | warm | cold |
| --- | ---: | ---: |
| `image.pull` | n/a (image present) | 3 159.9 |
| `container.start` | 6 167.0 | 6 985.9 |
| `session.build` — the driver connecting to the node | 748.4 | 723.6 |
| `query.first` — a `CREATE KEYSPACE`, so a DDL round trip | 1 076.1 | 1 075.2 |
| `start.to.first.query` — excludes the pull | **8 010.4** | **8 784.7** |

`container.start()` already blocks on TestContainers' readiness check, and the driver still needs
another ~1.8 s on top of it to connect and run one statement.

## Over the wire

`:seastar-server:wireStartupBenchmark`, 20 cold JVMs. `seastar-server` puts a socket, Netty and a
stock driver in front of the same in-memory session. Milliseconds, median.

| phase | ms |
| --- | ---: |
| `seastar.build` — the in-memory session | 388.8 |
| `server.start` — Netty, the bootstrap, the bind | 109.1 |
| `jvm.to.listening` | 547 |
| `driver.connect.cold` — handshake, `system.local`, schema metadata | 526.7 |
| `query.first` — a `CREATE KEYSPACE` over the socket | 184.0 |
| **`jvm.to.first.query`** | **1 258** |

The listener is the cheap part. **The driver is the expensive part:** 527 ms to connect is more than
the whole of SeaStar's own startup, and it is the driver's class loading, its Netty event loops, its
version negotiation down from `DSE_V2`, and the schema queries it runs before `build()` returns. A
second `CqlSession` to the same listener in the same JVM costs **81 ms**, which is what that figure
looks like once nothing is being loaded.

Per statement, both figures from the **same JVM in the same sample** — one call through the socket,
one through the in-process handle the listener is serving, so the difference is the loopback round
trip and the driver's request pipeline, and nothing else:

| `SELECT name FROM probe.t WHERE id = 1` | ms |
| --- | ---: |
| in process | **0.32** |
| over the socket, as a query string | **1.12** |
| over the socket, prepared and bound | **0.81** |

The wire costs about **0.8 ms per statement**, more than twice what answering it costs; preparing
buys back 0.3 ms of that, because the server does not re-parse a prepared statement's CQL. A warm
container answers the same query in about a millisecond too, over the same kind of loopback socket.
Once everything is warm the protocol dominates and SeaStar's advantage is startup — which is what
goal 2 claims and nothing more.

Note that `seastar.build` here (389 ms) is not `build.cold` in the in-process harness (468 ms): the
two probes have different classpaths, and class loading is most of what a cold build costs. Read the
phase split within a probe, and only `jvm.to.first.query` across them.

## Per statement, warm

`:seastar:jmh`. `AverageTime`, 1 fork, 5 x 1 s measurement (`StatementBenchmark` warms 5 x 1 s,
`SelectScalingBenchmark` 3 x 1 s). Microseconds per operation. Every statement with a WHERE clause
runs against a table holding **1 000 rows**.

| benchmark | us/op |
| --- | ---: |
| `prepareCached` — `prepare` of an already prepared query | **0.390** ± 0.003 |
| `prepareUncached` — `prepare` of a new query string | **7.585** ± 0.811 |
| `prepareUncachedResolved` — new query, bind metadata read | **7.478** ± 0.490 |
| `createTable` | **7.618** ± 4.661 |
| `insertPrepared` — bound statement | **13.197** ± 0.098 |
| `selectPoint` — `SELECT name ... WHERE id = ?` | **13.506** ± 1.239 |
| `deleteByPrimaryKey` | **14.663** ± 0.111 |
| `insertLiteral` — literal values in the CQL string | **15.320** ± 1.119 |
| `update` — `UPDATE ... SET name = ? WHERE id = ?` | **17.484** ± 1.444 |
| `selectScan` — `SELECT *`, all 1 000 rows | **670.624** ± 10.377 |
| `batch100` — driver `BatchStatement` of 100 INSERTs | **1 257.710** ± 40.584 |

- **`prepare` is eager.** `SeaStarCqlPrepareHandler` resolves the bind variable metadata inside
  `prepare()`, so that a statement this session rejects is rejected there rather than on first bind.
  That is why `prepareUncached` and `prepareUncachedResolved` measure the same thing — a full parse,
  ~7.5 us. Only `prepareCached` is a cache lookup, and it is 0.39 us.
- **A warm parse is ~7.5 us; the first parse in a JVM is ~174 ms.** Five orders of magnitude, all of
  it class loading and JIT.
- `deleteByPrimaryKey` removes the row on its first invocation, so steady state measures the parse
  and a lookup that finds nothing.
- `batch100` works out at ~12.6 us per child statement, close to linear in the batch size and with
  no per-statement parse — the children are pre-built.
- `createTable`'s error bar is 60 % of its score; it is the noisiest of the set.

### Scaling with table size

`SelectScalingBenchmark`. Microseconds per operation.

| rows | `selectPoint` | `selectAll` |
| ---: | ---: | ---: |
| 10 | 13.637 ± 0.362 | 10.304 ± 0.086 |
| 1 000 | 13.435 ± 0.310 | 684.693 ± 120.680 |
| 100 000 | 14.544 ± 1.320 | 107 249.660 ± 9 629.808 |

**A point lookup no longer scales with the table** — 13.6 us at 10 rows against 14.5 us at 100 000 —
so what is left is the per-statement cost, the parse, rather than the store. `selectAll` is O(rows)
by definition and stays that way, settling at roughly **1.07 us per row** at 100 000; that per-row
cost is the partition key encoding the read-time ordering needs plus the deserialize/re-serialize
round trip.

## What is not benchmarked

- **Range operators.** `WHERE ck > 1` and `WHERE ck >= 1 AND ck <= 2` are parsed and rejected. This
  is the one genuine query-engine gap, and the most interesting thing to measure when it lands: a
  range scan is exactly where the full-scan read path will hurt.
- **Materialized views** are unsupported by design rather than missing. `KeyspaceMetadata#getViews()`
  is always empty, which is what a cluster answers for a keyspace without views.

[docs/support-matrix.md](docs/support-matrix.md) is the maintained list of what works and what does
not. A second copy of it here would rot faster than anything else in the repo, so there is not one.

One historical note that still shapes the harness: bulk loading through `INSERT` used to be O(n^2),
because `InsertHandler` scanned the table for an existing primary key on every insert. The partition
index fixed that, but `BenchmarkFixture` still seeds through `SeaStarTable.addRow` — deliberately, so
the scaling benchmarks keep measuring what they have always measured.

## Reproducing

Docker is needed for the container comparison only. Benchmarks are not on the default build and their
classes are not in the published jar.

```bash
# Per-statement and scaling benchmarks (JMH). ~3 minutes.
./gradlew :seastar:jmh

# A single benchmark class or method.
./gradlew :seastar:jmh -PjmhIncludes='com.tagadvance.seastar.bench.StatementBenchmark.selectPoint'

# Cold and warm startup, plus the in-situ parser split. 20 forked JVMs per variant. ~50 seconds.
./gradlew :seastar:startupBenchmark

# Startup with a 75-statement fixture schema. ~20 seconds.
./gradlew :seastar:startupSchemaBenchmark

# Standalone parser cost attribution. ~30 seconds.
./gradlew :seastar:parserCostBenchmark

# A correctness gate, not a measurement: 14 queries parsed through both entry points, and anything
# other than 0 mismatches is a bug. ~10 seconds.
./gradlew :seastar:parserEquivalenceCheck

# What the wire costs. 20 forked JVMs. ~2 minutes.
# Run it in the same sitting as :seastar:startupBenchmark - the two are compared against each other.
./gradlew :seastar:startupBenchmark :seastar-server:wireStartupBenchmark

# TestContainers comparison. Needs Docker. ~35 and ~15 seconds.
./gradlew :seastar:containerWarmBenchmark
./gradlew :seastar:containerColdBenchmark
```

`containerColdBenchmark` removes every local tag of `cassandra:5.0.8` so that the pull is real, then
restores the tags it removed. Run it only when you want that.

**Which JDK you get.** The toolchain is pinned to 17, so `:seastar:jmh` forks the toolchain's
launcher and its numbers are JDK 17 numbers. The cold-JVM harnesses are `JavaExec` tasks with no
`javaLauncher`, so they fork **the Gradle daemon's JVM** — 25 here. Startup figures therefore depend
on the daemon's JDK; check `./gradlew --version` before comparing against this file.

The tasks are serialized against each other by a Gradle shared service, so listing several in one
invocation is safe even with `org.gradle.parallel=true`. They are still affected by anything else
running on the machine; close everything else first.
