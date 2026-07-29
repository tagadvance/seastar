---
name: add-cql-statement
description: Implement SeaStar support for a new CQL statement type (a CqlHandler). Use when asked to "add support for", "handle", or "implement" a CQL query/statement (INSERT, DELETE, DROP, ALTER, CREATE INDEX, etc.) in the SeaStar fake CqlSession. Covers inspecting the parse tree, matching real Cassandra failure behavior, writing the handler, and wiring it up.
---

# Add support for a new CQL statement type

SeaStar mirrors real Cassandra: a query that fails against Cassandra must fail with the **same driver exception type** in SeaStar. Follow this loop for each statement type. Keep it tight; the user is token-limited and wants many statement types done per session.

## 1. Inspect the parse tree

Never guess the `CQLStatement.Raw` class or its field names. Run the inspector:

```bash
./gradlew :seastar:inspectRaw -Pquery="<the CQL>" --console=plain -q 2>/dev/null
```

It prints the fully qualified `Raw` class and a recursive dump of its fields with the concrete values for that query. Read it to learn:
- The FQCN to `instanceof`-check in `canProcess` (e.g. `org.apache.cassandra.cql3.statements.UpdateStatement$ParsedInsert`).
- Which fields are `public` (access directly, e.g. `raw.parameters`) or have a public accessor, versus package-private (needs a binding in `FieldBindings`).
- Field names/shapes for bind markers (`AbstractMarker$Raw.bindIndex`), columns, where clauses, `ifExists`/`ifNotExists`, etc.

The inspector dumps everything, public or not, so it does not tell you whether an accessor exists. Before adding a binding, confirm with `javap` that there is genuinely no public way to ask:

```bash
javap -classpath ~/.gradle/caches/modules-2/files-2.1/org.apache.cassandra/cassandra-all/5.0.8/*/cassandra-all-5.0.8.jar 'org.apache.cassandra.cql3.statements.ModificationStatement$Parsed'
```

For `javap` on a type: the classes are in the `cassandra-all` sources/binary jar under `~/.gradle/caches/.../cassandra-all/5.0.8/`.

## 2. Determine real Cassandra failure behavior

For every way the query can fail (missing keyspace, missing table, already exists, invalid column, ...), find the driver exception type Cassandra actually throws. Add a test to `AbstractCqlSessionTest` (`seastar/src/testFixtures`, so every backend runs it) asserting the behavior, then run it against the real server:

```bash
./gradlew :seastar:containerTest --tests 'com.tagadvance.seastar.ContainerCqlSessionTest'
```

Note both halves of that command. The container backend is on the `containerTest` task, not `test`, which excludes it - and the suite is ordered and stateful, so a **single-method** filter fails with *keyspace foo does not exist* instead of running your test. Filter to the class.

`ContainerCqlSessionTest` needs Docker (Testcontainers). Note the exception **type** (`AlreadyExistsException`, `InvalidQueryException`, `InvalidQueryException` subtypes, ...) and roughly the message. Existing handlers show the pattern: construct with `executionInfo.getCoordinator()`.

## 3. Write the handler

Create `seastar/src/main/java/com/tagadvance/seastar/handlers/<Name>Handler.java` implementing `CqlHandler<TheRawType>`:
- `canProcess(raw)` -> `raw instanceof TheRawType`.
- `processCql(context, executionInfo, raw, bindings)` -> mutate/read the `Volatile*` model, return `CompletableFuture.completedStage(newAsyncResultSet(...))` or `failedStage(new SomeException(...))`.
- Read package-private fields through `FieldBindings`: add a `FieldBinding` constant there, then call `FieldBindings.MY_FIELD.require(raw)` (state the statement always carries) or `.find(raw)` (genuinely optional, e.g. an unnamed index). Never default a missing required field. Wrap a parsed type with `new SeaStarRawType(...)` when you need the driver `DataType`.
- `bindings` are the bound values for prepared statements, positional by `bindIndex`.
- Identifiers: use `CqlIdentifier.fromInternal(name)` to match the rest of the codebase (case-sensitive, no quote parsing) unless the field already carries quoting semantics.
- Mark thread-safety intent with jcip annotations, match surrounding style (see `CreateKeyspaceHandler`, `CreateTableHandler`, `UseKeyspaceHandler`, `SelectHandler`).

Reference the storage model in `CLAUDE.md`: `VolatileDriverContext` (keyspaces) -> `VolatileKeyspace` (tables/UDTs) -> `VolatileTable` (columns/rows).

## 4. Wire it up

Register the handler in the `CqlHandlerRegistry` constructor call inside
`SeaStarCqlSession#buildHandlerRegistry` (the `new CqlHandlerRegistry(...)` list). Order matters only if two handlers' `canProcess` overlap; they normally don't.

## 5. Say what it does to the schema, for the wire

`:seastar-server` has to answer a statement with the right *protocol message*, and an `AsyncResultSet` cannot say which. `CqlStatementSummary.of(metadata, keyspace, query)` is what decides, and it is computed in `StatementSummaries` from the same parse tree - **before** the statement runs, because `DROP INDEX` names only the index and the table a driver must refresh can only be found while the index still exists.

So if the new statement changes the schema or selects a keyspace, add a branch to `StatementSummaries.of(...)`:

- schema change -> `SchemaChanged(CREATED|UPDATED|DROPPED, KEYSPACE|TABLE|TYPE, keyspace, object)`, which becomes a `SCHEMA_CHANGE` result to the connection that ran it, a `SCHEMA_CHANGE` event to every registered connection, and a bump of `system.local.schema_version`.
- `USE`-like -> `KeyspaceSelected`, which becomes `SET_KEYSPACE`.
- anything else -> `Result`, the default, where rows mean `ROWS` and no columns mean `VOID`.

An index statement reports the **table** it indexes, not the index. Miss this step and a DDL statement answers `VOID` over the wire, and a connected driver keeps stale metadata for it while working perfectly in process - which is why it is a step rather than a note. `WireStatementTest` is where a case gets pinned.

## 6. Verify parity

Run the fast SeaStar test, which must now pass with the same assertions the container test passed:

```bash
./gradlew :seastar:test --tests 'com.tagadvance.seastar.SeaStarCqlSessionTest'
```

`SeaStarCqlSessionTest`, `ContainerCqlSessionTest` and `:seastar-server`'s `WireCqlSessionTest` all extend `AbstractCqlSessionTest`, so one test method runs in process, against real Cassandra, and over a socket. Green on all three = parity achieved. The wire backend needs no Docker and is on the default build:

```bash
./gradlew :seastar-server:test --tests 'com.tagadvance.seastar.server.WireCqlSessionTest'
```

## 7. Update the matrix

Add or move the statement's row in `docs/support-matrix.md`, and delete it from `UnsupportedStatements` if it was being rejected by name. Supporting a feature is deleting a row; rejecting one is adding a row.

## Notes

- Do not assume an existing handler is complete; several have `TODO`/`FIXME`/`UnsupportedOperationException` bodies (e.g. `CreateTableHandler` parses columns but does not persist them).
- If a needed `Volatile*` mutation method is missing (e.g. `addColumn`, `addRow`), add it to the interface + implementation before the handler can use it.
- Upgrading `cassandra-all` can rename the reflected fields; the inspector is the source of truth for current names, and `FieldBindingsTest` is what fails the build when they change.
