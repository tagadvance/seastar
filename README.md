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
3. All code is thread-safe unless a class is explicitly documented otherwise.

## Install

Not yet published — the artifact coordinates below are what `1.0.0-alpha` will resolve to once it
ships; `publishToMavenLocal` works today for local use.

**Gradle**
```kotlin
testImplementation("com.tagadvance:seastar:1.0.0-alpha")
```

**Maven**
```xml
<dependency>
    <groupId>com.tagadvance</groupId>
    <artifactId>seastar</artifactId>
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
resource. For a fixture that is easier to build in Java than in CQL, populate the model directly
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

## Benchmarks

Full numbers, environment and reproduction steps live in [benchmarks.md](benchmarks.md); this is
the headline. Laptop-class hardware (see that file) — absolute numbers will be lower on a desktop,
the ratios are the interesting part.

| | SeaStar | TestContainers Cassandra |
| --- | --- | --- |
| Ready for the first query (warm image) | **743 ms** from JVM start | 13 397 ms from `container.start()` |
| Ready for the first query (cold, image not present) | 743 ms | 18 575 ms |
| Second and subsequent sessions in the same JVM | **8 ms** | a second container, so ~13 s again |

Roughly **18x faster than a warm container** and **25x faster than a cold one** to first query, and
roughly **1 700x faster** for every session after the first in the same JVM.

## Known limitations

Every deliberate divergence from real Cassandra is catalogued in
[docs/support-matrix.md](docs/support-matrix.md) rather than duplicated here. The short version: a
`SELECT` always returns a single page, a delete does not leave a tombstone (a write stamped older
than one is applied instead of suppressed), a `BATCH` is not atomic or isolated, and there are no
materialized views, UDFs/aggregates, auth, roles, or a token map. Each is a considered trade-off for
an in-memory fake, not an oversight — the matrix says why.

## Roadmap

* `SchemaChangeListener` support is not implemented.
* Rows are stored through the driver's own codecs (deserialize on write, re-serialize on read)
  rather than natively, which is unnecessary overhead this could shed by overriding the default
  getters on the row model.

### What's with the name?
It's a bad pun. Cassandra is often abbreviated to C*, so I called this library SeaStar.
