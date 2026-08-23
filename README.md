# SeaStar

[![build](https://github.com/tagadvance/seastar/actions/workflows/build.yml/badge.svg)](https://github.com/tagadvance/seastar/actions/workflows/build.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.tagadvance/seastar)](https://central.sonatype.com/artifact/com.tagadvance/seastar)
[![release](https://img.shields.io/github/v/release/tagadvance/seastar)](https://github.com/tagadvance/seastar/releases/latest)
[![license](https://img.shields.io/github/license/tagadvance/seastar)](LICENSE)

An in-memory implementation of the DataStax Java driver's `CqlSession`, for tests that exercise
CQL. SeaStar implements the driver's own public interfaces, so it drops in wherever your code already
takes a `CqlSession` — no Docker, no embedded node, and **a fresh database per test method for about
6 ms**.

- **Fast.** ~0.63 s from the first instruction of a cold JVM's `main` to the first query — that
  excludes the JVM's own ~61 ms boot, which every backend pays whether or not it uses SeaStar — and
  every session after that is ~6 ms, or ~18 ms seeded with a 75-statement schema. A warm
  Testcontainers Cassandra is ~8.2 s on the same clock.
- **Faithful.** The same 179-test fidelity suite runs in process, over a socket, and against a real
  `cassandra:5.0.8` node. A query that fails on Cassandra fails on SeaStar with the same driver
  exception, and every deliberate divergence is written down in
  [docs/support-matrix.md](docs/support-matrix.md).
- **Drop-in.** Your code gets a `CqlSession`; if it insists on building its own from a host and port,
  `seastar-server` puts the same session behind Cassandra's native protocol on a loopback socket.

## Quickstart

```kotlin
testImplementation("com.tagadvance:seastar:1.0.0")
```

```java
class ProductRepositoryTest {

    SeaStarCqlSession session;

    @BeforeEach
    void freshDatabase() {
        // ~19 ms, so a new database per test method is affordable: no shared state, no TRUNCATE.
        session = SeaStarCqlSession.builder()
            .withSchemaResource("/schema.cql", SchemaImport.LENIENT)
            .build();
    }

    @AfterEach
    void tearDown() {
        session.close();
    }

    @Test
    void findsWhatItStored() {
        var repository = new ProductRepository(session); // takes a CqlSession

        repository.save(new Product(1, "Widget"));

        assertEquals("Widget", repository.findById(1).name());
    }
}
```

`schema.cql` can be the `DESCRIBE SCHEMA` dump off your real cluster — `LENIENT` logs and skips what
SeaStar does not implement (materialized views, functions, aggregates) instead of failing. Inline
CQL, a `Path`, a `File` and a hand-built model are in [Seeding a schema](#seeding-a-schema).

## Why SeaStar

The honest framing: **SeaStar is not Cassandra, and it is not trying to be.** It is the thing you
run a thousand times a day so that you only have to run Cassandra once. Keep a Testcontainers
Cassandra for the final integration pass; give the other 99 % of your tests a `CqlSession` that
exists before the container would have finished pulling.

| | SeaStar | [Testcontainers Cassandra](https://java.testcontainers.org/modules/databases/cassandra/) | [cassandra-unit](https://github.com/jsevellec/cassandra-unit) |
| --- | --- | --- | --- |
| What it is | In-process implementation of `CqlSession`; CQL is parsed by `cassandra-all` and executed against an in-memory model | Real Cassandra in a Docker container | A real Cassandra node embedded in the test JVM |
| Time to first query | **0.63 s** cold JVM; **6 ms** per subsequent session | **~8.2 s** with the image pulled, **~14.8 s** without | **~4.1 s** — embedded node boot (~2.1 s) plus schema replay and driver connect |
| Needs Docker | No | Yes | No |
| Cassandra semantics | 5.0.8: the real parser, plus behavior verified against a `cassandra:5.0.8` node by the fidelity suite | Whatever image you pick | 3.11.5 (bundled) |
| Driver | Implements the 4.19.3 interfaces | Any | Built against 4.3.1 |
| Java | 17+ | Any | 8, the JDK Cassandra 3.11 supports |
| Fresh database per test | A new session, ~6 ms | A new container, ~8 s; in practice one container per class and `TRUNCATE` between tests — measured at **~215-285 ms per TRUNCATE** here, against SeaStar's own **~1.3 us** (no disk, nothing to snapshot) | One node per JVM; clean-up helpers between tests |
| Code that builds its own connection | Yes, via `seastar-server` on a loopback socket | Yes | Yes |
| CQL coverage | Most of what an application uses; MVs, UDFs/UDAs, auth, paging and tombstones are the notable gaps — see the [support matrix](docs/support-matrix.md) | Everything | Everything in 3.11 |
| Maintenance | Active; 1.0.0 released 2026-08 | Active | Last release January 2020, last commit September 2022 |

**When not to use SeaStar:** when the behavior under test *is* the database — tombstones and
compaction, paging, consistency levels, multi-node topology, materialized views, UDFs. SeaStar
refuses those by name rather than approximating them, so the test fails loudly; reach for the
container.

## How it stays honest

Fidelity is a test suite, not a promise. Every behavior claim in this README and the support matrix
is backed by a fidelity test in `seastar/src/testFixtures`, and each of those tests has three
runners:

| runner | what it proves | when it runs |
| --- | --- | --- |
| `SeaStar*FidelityTest` | the in-process session | every build |
| `Wire*FidelityTest` | the same session through `seastar-server` and a stock driver | every build |
| `Container*FidelityTest` | real Cassandra 5.0.8 via Testcontainers — the authority | nightly, and on demand with `./gradlew :seastar:containerTest` |

When SeaStar and the container disagree, the container is right and SeaStar gets fixed. The tests
assert on exception type and on the offending keyspace, table or column being named; they do not pin
Cassandra's exact wording, which is the one place the two are allowed to differ.

## Goals (in order of precedence)
1. **Fidelity.** A query that fails against real Cassandra should fail in a similar fashion (same
   exception type) against SeaStar. [docs/support-matrix.md](docs/support-matrix.md) is the
   authoritative list of what is supported, what is rejected by name, and what is a deliberate
   trade-off.
2. **Startup time**, to be a viable alternative to a per-test-class container. See
   [Benchmarks](#benchmarks) below.
3. **Concurrency.** All code is thread-safe unless a class is explicitly documented otherwise —
   and thread safety alone is the floor, not the goal. Being safe to call from two threads does not
   make an operation atomically correct; that is what the locks are for. Each statement runs
   atomically under its keyspace's lock, so a concurrent reader never observes a half-applied
   statement.

## Install

The current release is `1.0.0`, on Maven Central.

There are two artifacts, and **most people need only the first**. Take `seastar-server` only if the
code under test builds its own `CqlSession` from a host and a port and cannot be handed one — because
it is another process, another language, or a framework that owns the connection.

| Artifact | What it is |
| --- | --- |
| `com.tagadvance:seastar` | the in-memory `CqlSession`. This is SeaStar. |
| `com.tagadvance:seastar-server` | an optional listener that serves one of those sessions over Cassandra's native protocol on a socket. Depends on `seastar`; versioned in lockstep with it. |

**Gradle**
```kotlin
testImplementation("com.tagadvance:seastar:1.0.0")
testImplementation("com.tagadvance:seastar-server:1.0.0") // only if you need a socket
```

**Maven**
```xml
<dependency>
    <groupId>com.tagadvance</groupId>
    <artifactId>seastar</artifactId>
    <version>1.0.0</version>
    <scope>test</scope>
</dependency>

<!-- Only if you need a socket. -->
<dependency>
    <groupId>com.tagadvance</groupId>
    <artifactId>seastar-server</artifactId>
    <version>1.0.0</version>
    <scope>test</scope>
</dependency>
```

## Seeding a schema

Inline CQL works the same way as a resource, and the session is then used exactly like a real one:

```java
try (var session = SeaStarCqlSession.builder()
        .withSchema("""
            CREATE KEYSPACE shop WITH replication =
                {'class': 'SimpleStrategy', 'replication_factor': 1};
            CREATE TABLE shop.products (id int PRIMARY KEY, name text);
            """, SchemaImport.LENIENT)
        .build()) {
    session.execute("INSERT INTO shop.products (id, name) VALUES (1, 'Widget')");

    var row = session.execute("SELECT name FROM shop.products WHERE id = 1").one();
    assertEquals("Widget", row.getString("name"));
}
```

`withSchemaFile(Path)` and `withSchemaResource(String)` do the same from a file or a classpath
resource. `SchemaImport.LENIENT` is what lets a `DESCRIBE SCHEMA` dump taken off a live cluster
seed as-is: statements SeaStar refuses (materialized views, functions, aggregates) are logged and
skipped instead of failing the build, and table options Cassandra itself has removed are stripped.
Leave the argument off for strict mode, where the first statement that fails fails the build - the
better choice for CQL you wrote by hand, since a typo is then an error rather than a warning. For a
fixture that is easier to build in Java than in CQL, populate the model directly through
`getContext()` instead:

```java
try (var session = SeaStarCqlSession.builder().build()) {
    var table = session.getContext().newSeaStarKeyspace("shop").newSeaStarTable("products");
    table.addColumn("id", DataTypes.INT);
    table.addColumn("name", DataTypes.TEXT);
    table.markPartitionKey(CqlIdentifier.fromInternal("id"));
    table.addRow(1, "Widget");

    var row = session.execute("SELECT name FROM shop.products WHERE id = 1").one();
    assertEquals("Widget", row.getString("name"));
}
```

## On a socket

When the code under test cannot be given a `CqlSession`, `seastar-server` puts the same in-memory
session behind Cassandra's native protocol. That is the whole harness:

```java
try (var session = SeaStarCqlSession.builder()
        .withSchemaResource("/schema.cql", SchemaImport.LENIENT)
        .build();
    var server = SeaStarProtocolServer.builder().session(session).build().start()) {

    var contactPoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port());
    // hand contactPoint to whatever builds its own driver
}
```

The port is ephemeral and the bind address is loopback, so nothing collides with a real local
Cassandra; `port()` after `start()` is the one that was granted, and `close()` releases the socket
without closing the session you built. `port(int)`, `bindAddress(InetAddress)`, `clusterName`,
`datacenter` and `rack` are all on the builder.

An ordinary, unconfigured driver connects to it — no pinned protocol version, no metadata switched
off. Only the datacenter has to match, and the driver demands one of everybody:

```java
try (var connected = CqlSession.builder()
        .addContactPoint(contactPoint)
        .withLocalDatacenter("datacenter1")   // SeaStar's default
        .build()) {
    connected.execute("INSERT INTO shop.products (id, name) VALUES (2, 'Sprocket')");
}
```

It negotiates protocol v5 with segment framing, reads `system.local` and builds its schema metadata
out of `system_schema` exactly as it would against a node. The same fidelity suite that runs in
process and against a real container also runs over this socket.

**If your harness seeds a large schema, shorten the driver's schema debouncer.** The driver holds a
DDL statement's answer until the metadata refresh it triggered finishes, and debounces that by
`advanced.metadata.schema.debouncer.window` — one second by default, which is the whole of the wait,
since SeaStar answers from memory. SeaStar's own wire suite went from 190 s to under 7 s by setting
it to 1 ms. Nothing on the server's side can shorten it.

[docs/support-matrix.md](docs/support-matrix.md) has a section on the protocol itself: what is
answered, what is refused by name, and what differs from a node on purpose.

## Benchmarks

Full numbers, environment and reproduction steps live in [benchmarks.md](benchmarks.md); this is
the headline. Absolute numbers depend on the machine (see that file) — the ratios are the
interesting part.

Every number below is `main.to.first.query`: measured from the first instruction of the harness, for
every backend, so the JVM's own boot — paid whether or not a test uses SeaStar — is out of the
comparison. See "The first-instruction clock" in benchmarks.md for why that changed.

| | SeaStar | TestContainers Cassandra |
| --- | --- | --- |
| Ready for the first query (warm image) | **626 ms** | 8 189 ms |
| Ready for the first query (cold, image not present) | 626 ms | 14 841 ms |
| Second and subsequent sessions in the same JVM | **6.2 ms** | a second container, so ~8 s again |
| Heap retained after 100 000 rows | **63 MB** | 1 024 MB reserved (TestContainers pins the container's heap; unconstrained, this box's own sizing formula would pick 8 GB and the node gets OOM-killed here) |

Roughly **13x faster than a warm container** and **24x faster than a cold one** to first query, and
roughly **1 320x faster** for every session after the first in the same JVM.

Serving the same session over a socket costs about half a second of startup, and both the startup and
per-statement gap are the driver and the round trip rather than SeaStar. Measured three ways in one
sitting: **626 ms** to first query in process, **1 157 ms** over `seastar-server`, **8 189 ms** for a
warm TestContainers Cassandra — so even through the whole native protocol it is still 7.1x faster to
a usable session.

Per statement, steady state (JMH, not a cold-JVM harness — see benchmarks.md for why the two differ
by an order of magnitude for the wire path): a point select costs **13.4 us** in process, **90.7 us**
over the socket, and **167.4 us** against a real container — the wire path pays the round trip and
the driver's request pipeline, and the container adds a real replica write and commit log on top of
that.

## Known limitations

Every deliberate divergence from real Cassandra is catalogued in
[docs/support-matrix.md](docs/support-matrix.md) rather than duplicated here. The short version: a
`SELECT` always returns a single page, a delete does not leave a tombstone (a write stamped older
than one is applied instead of suppressed), a conditional `BATCH` evaluates its `IF` conditions one
child at a time, and there are no materialized views, UDFs/aggregates, auth, roles, or a token map.
Each is a considered trade-off for an in-memory fake, not an oversight — the matrix says why.

On a socket, `seastar-server` adds its own short list. Compression, TLS and a paging state in a
request are each refused by name rather than ignored. Consistency, serial consistency, the request's
timestamp and tracing are accepted and have no effect, there being one replica in one process.
`TOPOLOGY_CHANGE` and `STATUS_CHANGE` never fire, there being one node. `system.local` describes that
node, `system_schema` is projected live from the model, and everything else a real node keeps in
`system` — `system_auth`, `system_traces`, `system_distributed` — answers `InvalidQueryException`.
Table options read back over the wire as Cassandra 5.0.8 defaults rather than as what you set, so
`comment` is always empty.

## Roadmap

* Table options are not stored. `TableMetadata#getOptions()` is empty in process, and over the wire
  the same table reads back with 5.0.8 defaults — the two disagree until the model holds them.
* `SchemaChangeListener` is not implemented for an in-process session. A driver connected to
  `seastar-server` does get its listeners called, because the listener publishes real
  `SCHEMA_CHANGE` events.
* Over the wire, `skip_metadata` is ignored and a prepared statement's result metadata id never
  changes. Both are the same piece of work, and honouring the first is what makes the second matter.
* Compression is refused rather than implemented. It buys nothing on loopback, which is the only
  place this listens.
* Rows are stored through the driver's own codecs (deserialize on write, re-serialize on read)
  rather than natively, which is unnecessary overhead this could shed by overriding the default
  getters on the row model.

## Built with AI assistance

AI is a contentious issue that elicits strong emotions, both positive and negative. I,
[@tagadvance](https://github.com/tagadvance), built the initial scaffolding and fidelity test suite
by hand. Every commit since [6115072](https://github.com/tagadvance/seastar/commit/6115072) was
written with help from [Claude](https://claude.com/claude-code), usually Opus 5 or Fable, and
occasionally reviewed by [Gemini](https://gemini.google.com/).

Here's the thing: if I take my truck to a mechanic and find out they're using wrenches instead of
an impact to tear down my engine, I'm going to be a little upset — especially if they're charging
by the hour. AI is a tool like any other, and this library probably never would have been released
without Claude doing most of the grunt work implementing handlers for all the various query types.

### What's with the name?
It's a bad pun. Cassandra is often abbreviated to C*, so I called this library SeaStar.
