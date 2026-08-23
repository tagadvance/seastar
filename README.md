# SeaStar

An in-memory implementation of the DataStax Java driver's `CqlSession` — a fast, in-process
alternative to [TestContainers](https://java.testcontainers.org/modules/databases/cassandra/) for
tests that exercise CQL. SeaStar implements the driver's own public interfaces, so it drops in
wherever your code already takes a `CqlSession`.

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
   statement, and a `BATCH` holds the locks of every keyspace it touches for the whole batch. The
   one deliberate carve-out — condition evaluation inside a conditional batch — is called out in
   the support matrix.

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

## Install

The current release is `1.0.0-alpha`, on Maven Central.

There are two artifacts, and **most people need only the first**. Take `seastar-server` only if the
code under test builds its own `CqlSession` from a host and a port and cannot be handed one — because
it is another process, another language, or a framework that owns the connection.

| Artifact | What it is |
| --- | --- |
| `com.tagadvance:seastar` | the in-memory `CqlSession`. This is SeaStar. |
| `com.tagadvance:seastar-server` | an optional listener that serves one of those sessions over Cassandra's native protocol on a socket. Depends on `seastar`; versioned in lockstep with it. |

**Gradle**
```kotlin
testImplementation("com.tagadvance:seastar:1.0.0-alpha")
testImplementation("com.tagadvance:seastar-server:1.0.0-alpha") // only if you need a socket
```

**Maven**
```xml
<dependency>
    <groupId>com.tagadvance</groupId>
    <artifactId>seastar</artifactId>
    <version>1.0.0-alpha</version>
    <scope>test</scope>
</dependency>

<!-- Only if you need a socket. -->
<dependency>
    <groupId>com.tagadvance</groupId>
    <artifactId>seastar-server</artifactId>
    <version>1.0.0-alpha</version>
    <scope>test</scope>
</dependency>
```

## Example

Seed a schema from CQL and use the session exactly like a real one:

```java
try (var session = SeaStarCqlSession.builder()
        .withSchema("""
            CREATE KEYSPACE shop WITH replication =
                {'class': 'SimpleStrategy', 'replication_factor': 1};
            CREATE TABLE shop.products (id int PRIMARY KEY, name text);
            """)
        .build()) {
    session.execute("INSERT INTO shop.products (id, name) VALUES (1, 'Widget')");

    var row = session.execute("SELECT name FROM shop.products WHERE id = 1").one();
    assertEquals("Widget", row.getString("name"));
}
```

`withSchemaFile(Path)` and `withSchemaResource(String)` do the same from a file or a classpath
resource. To seed from a `DESCRIBE SCHEMA` dump taken off a live cluster, pass
`SchemaImport.LENIENT` as a second argument: statements SeaStar refuses (materialized views,
functions, aggregates) are logged and skipped instead of failing the build, and table options
Cassandra itself has removed are stripped. For a fixture that is easier to build in Java than in CQL, populate the model directly
through `getContext()` instead:

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
try (var session = SeaStarCqlSession.builder().withSchemaResource("/schema.cql").build();
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

| | SeaStar | TestContainers Cassandra |
| --- | --- | --- |
| Ready for the first query (warm image) | **692 ms** from JVM start | 8 010 ms from `container.start()` |
| Ready for the first query (cold, image not present) | 692 ms | 11 945 ms |
| Second and subsequent sessions in the same JVM | **6.4 ms** | a second container, so ~8 s again |

Roughly **12x faster than a warm container** and **17x faster than a cold one** to first query, and
roughly **1 250x faster** for every session after the first in the same JVM.

Serving the same session over a socket costs about half a second of startup and roughly 0.8 ms per
statement, and both are the driver and the round trip rather than SeaStar. Measured three ways in one
sitting: **692 ms** to first query in process, **1 258 ms** over `seastar-server`, **8 010 ms** for a
warm TestContainers Cassandra — so even through the whole native protocol it is still 6.4x faster to
a usable session. A statement then costs 0.32 ms in process and 1.12 ms over the socket.

## Known limitations

Every deliberate divergence from real Cassandra is catalogued in
[docs/support-matrix.md](docs/support-matrix.md) rather than duplicated here. The short version: a
`SELECT` always returns a single page, a delete does not leave a tombstone (a write stamped older
than one is applied instead of suppressed), a conditional `BATCH` evaluates its `IF` conditions one
child at a time, and there are no materialized views, UDFs/aggregates, auth, roles, or a token map. Each is a considered trade-off for
an in-memory fake, not an oversight — the matrix says why.

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

### What's with the name?
It's a bad pun. Cassandra is often abbreviated to C*, so I called this library SeaStar.
