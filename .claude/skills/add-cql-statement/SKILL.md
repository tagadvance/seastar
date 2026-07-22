---
name: add-cql-statement
description: Implement SeaStar support for a new CQL statement type (a CqlHandler). Use when asked to "add support for", "handle", or "implement" a CQL query/statement (INSERT, DELETE, DROP, ALTER, CREATE INDEX, etc.) in the SeaStar fake CqlSession. Covers inspecting the parse tree, matching real Cassandra failure behavior, writing the handler, and wiring it up.
---

# Add support for a new CQL statement type

SeaStar mirrors real Cassandra: a query that fails against Cassandra must fail with the **same driver exception type** in SeaStar. Follow this loop for each statement type. Keep it tight; the user is token-limited and wants many statement types done per session.

## 1. Inspect the parse tree

Never guess the `CQLStatement.Raw` class or its field names. Run the inspector:

```bash
./gradlew :lib:inspectRaw -Pquery="<the CQL>" --console=plain -q 2>/dev/null
```

It prints the fully qualified `Raw` class and a recursive dump of its fields with the concrete values for that query. Read it to learn:
- The FQCN to `instanceof`-check in `canProcess` (e.g. `org.apache.cassandra.cql3.statements.UpdateStatement$ParsedInsert`).
- Which fields are `public` (access directly, e.g. `raw.keyspace`, `raw.parameters`) vs package-private (read via `Reflections.getDeclaredField(raw, "fieldName", Type.class)`).
- Field names/shapes for bind markers (`AbstractMarker$Raw.bindIndex`), columns, where clauses, `ifExists`/`ifNotExists`, etc.

For `javap` on a type: the classes are in the `cassandra-all` sources/binary jar under `~/.gradle/caches/.../cassandra-all/5.0.8/`.

## 2. Determine real Cassandra failure behavior

For every way the query can fail (missing keyspace, missing table, already exists, invalid column, ...), find the driver exception type Cassandra actually throws. Add a test to `AbstractCqlSessionTest` asserting the behavior, then run it against the real server:

```bash
./gradlew :lib:test --tests 'com.tagadvance.seastar.ContainerCqlSessionTest.<method>'
```

`ContainerCqlSessionTest` needs Docker (Testcontainers). Note the exception **type** (`AlreadyExistsException`, `InvalidQueryException`, `InvalidQueryException` subtypes, ...) and roughly the message. Existing handlers show the pattern: construct with `executionInfo.getCoordinator()`.

## 3. Write the handler

Create `lib/src/main/java/com/tagadvance/seastar/handlers/<Name>Handler.java` implementing `CqlHandler<TheRawType>`:
- `canProcess(raw)` -> `raw instanceof TheRawType`.
- `processCql(context, executionInfo, raw, bindings)` -> mutate/read the `Volatile*` model, return `CompletableFuture.completedStage(newAsyncResultSet(...))` or `failedStage(new SomeException(...))`.
- Read package-private fields with `Reflections.getDeclaredField(...)`. Wrap reflected raw types with `SeaStarRawType.from(...)` when you need the driver `DataType`.
- `bindings` are the bound values for prepared statements, positional by `bindIndex`.
- Identifiers: use `CqlIdentifier.fromInternal(name)` to match the rest of the codebase (case-sensitive, no quote parsing) unless the field already carries quoting semantics.
- Mark thread-safety intent with jcip annotations, match surrounding style (see `CreateKeyspaceHandler`, `CreateTableHandler`, `UseKeyspaceHandler`, `SelectHandler`).

Reference the storage model in `CLAUDE.md`: `VolatileDriverContext` (keyspaces) -> `VolatileKeyspace` (tables/UDTs) -> `VolatileTable` (columns/rows).

## 4. Wire it up

Register the handler in the `CqlHandlerRegistry` constructor call inside
`SeaStarCqlRequestHandler` (the `new CqlHandlerRegistry(...)` list). Order matters only if two handlers' `canProcess` overlap; they normally don't.

## 5. Verify parity

Run the fast SeaStar test, which must now pass with the same assertions the container test passed:

```bash
./gradlew :lib:test --tests 'com.tagadvance.seastar.SeaStarCqlSessionTest.<method>'
```

Both `SeaStarCqlSessionTest` and `ContainerCqlSessionTest` extend `AbstractCqlSessionTest`, so one test method runs against both the fake and real Cassandra. Green on both = parity achieved.

## Notes

- Do not assume an existing handler is complete; several have `TODO`/`FIXME`/`UnsupportedOperationException` bodies (e.g. `CreateTableHandler` parses columns but does not persist them).
- If a needed `Volatile*` mutation method is missing (e.g. `addColumn`, `addRow`), add it to the interface + implementation before the handler can use it.
- Upgrading `cassandra-all` can rename the reflected fields; the inspector is the source of truth for current names.
