## What this is

SeaStar is a lightweight, in-memory implementation of the DataStax Java driver's `CqlSession` — essentially a mock/fake for Cassandra intended as a fast alternative to TestContainers for tests. The overriding design goal is **fidelity**: a query that fails against real Cassandra should fail in a similar fashion (same exception types) in SeaStar. Correctness of that mirroring takes precedence over performance, and startup time is the second priority.

This is an early prototype. Many code paths are stubbed with `TODO`/`FIXME`, throw `UnsupportedOperationException`, or are half-implemented (e.g. `CreateTableHandler` parses columns but does not yet persist them — note the stray `System.gc()` and commented-out `addColumn`). Do not assume a feature works just because a class for it exists; check for `UnsupportedOperationException` and unfinished handler bodies.

## Build & test

Requires JDK 17 (configured via Gradle toolchain; foojay resolver auto-downloads it).

All tests should be annotated with a short human-readable `@DisplayName`.

```bash
./gradlew build                 # compile + test
./gradlew :lib:test             # run all tests
./gradlew :lib:test --tests 'com.tagadvance.seastar.SeaStarCqlSessionTest'          # single class
./gradlew :lib:test --tests 'com.tagadvance.seastar.SeaStarCqlSessionTest.testSimpleSelect'  # single method
./gradlew :lib:publishToMavenLocal   # publish artifact locally
```

Everything lives in the single `lib` subproject. Tests are JUnit 5 (Jupiter) with Mockito. Configuration cache, parallel, and build caching are enabled in `gradle.properties`.

## Architecture

### Two dependencies that must not be confused
- **`java-driver-core` (client side)** — the DataStax driver whose public interfaces (`CqlSession`, `Statement`, `AsyncResultSet`, `KeyspaceMetadata`, `DriverContext`, …) SeaStar implements so existing client code can use it as a drop-in.
- **`cassandra-all` (server side)** — the actual Cassandra server library. SeaStar borrows its CQL parser: `QueryProcessor.parseStatement(query)` turns a CQL string into a `CQLStatement.Raw` parse tree. SeaStar never runs a Cassandra node; it only uses the parser.

### Request pipeline
Each SeaStar class is deliberately **analogous to** a driver-internal class (the Javadoc says so). The flow:

1. `SeaStarCqlSession.execute(request, resultType)` → `SeaStarRequestProcessorRegistry.processorFor(...)` picks a `SeaStarRequestProcessor` by matching the result type (`Statement.SYNC`/`ASYNC`, prepare sync/async). Registered in `SeaStarBuiltInRequestProcessors`.
2. Sync processors delegate to their async counterpart and block (`CompletableFutures.getUninterruptibly`).
3. `SeaStarCqlRequestHandler.handle()` extracts the query string + bound values (from `SimpleStatement` or `SeaStarBoundStatement`), parses it via Cassandra's `QueryProcessor`, then dispatches to a `CqlHandler` through `CqlHandlerRegistry`.
4. A `CqlHandler<T extends CQLStatement.Raw>` (`CreateKeyspaceHandler`, `CreateTableHandler`, `CreateTypeHandler`, `UseKeyspaceHandler`, `SelectHandler`) mutates or reads the in-memory model and returns a `CompletionStage<AsyncResultSet>`.

There are two parallel registries — do not conflate them: `SeaStarRequestProcessorRegistry` selects a *processor* by driver result type; `CqlHandlerRegistry` selects a *handler* by parsed statement type. Handlers are currently constructed inline in `SeaStarCqlRequestHandler`'s constructor (there's a TODO to move this out).

### Reflection into Cassandra internals
The `CQLStatement.Raw` parse-tree objects from `cassandra-all` expose most of their state only as package-private fields. Handlers read them via `Reflections.getDeclaredField(obj, "fieldName", Type.class)` (e.g. `ifNotExists`, `rawColumns`, `rawType`). This is central and fragile: **upgrading `cassandra-all` can silently break handlers** when field names change. `SeaStarRawType` wraps the reflected raw-type objects.

### Storage model: `SeaStar*` interfaces vs `Volatile*` implementations
Two layered abstractions:
- **`SeaStar*` interfaces** (`SeaStarKeyspace`, `SeaStarTable`, `SeaStarColumn`, `SeaStarRow`, `SeaStarUserDefinedType`, `SeaStarUdtValue`, `SeaStarDriverContext`) each extend the corresponding **driver metadata interface** (`KeyspaceMetadata`, `TableMetadata`, `Metadata`, etc.), so the same object serves as both mutable storage and the metadata the driver API exposes.
- **`Volatile*` classes** are the concrete in-memory implementations, backed by `ConcurrentHashMap` and, where needed, a `ReentrantReadWriteLock` (exposed via the `SeaStarReadWriteLock` mixin in `com.tagadvance.tools`). "Volatile" = lives only for the session, discarded on close.

`VolatileDriverContext` is the root: it is simultaneously the driver `DriverContext`, the cluster `Metadata`, and the owner of the keyspace map. `SeaStarCqlSession.getContext()` returns it, and tests reach into it (`context.newSeaStarKeyspace(...)`, `keyspace.newSeaStarTable(...)`) to populate data directly rather than via CQL.

### Identifiers
Everything is keyed by `CqlIdentifier`. Most internal APIs use `CqlIdentifier.fromInternal(name)` (case-sensitive, no quoting) — note this differs from `fromCql` (which interprets quoting/case rules). Be deliberate about which you use when adding lookups.

### Known correctness caveat noted by the author
SeaStar currently deserializes and re-serializes row data rather than storing it natively (the author calls this unnecessary but hasn't overridden all the default getters). Thread-safety is a stated invariant: everything should be thread-safe unless its documentation says otherwise (classes carry `@ThreadSafe`/`@NotThreadSafe` from `jcip-annotations`).

## Adding a new CQL statement type

1. Write a `CqlHandler<SomeRawStatement>` where `SomeRawStatement` is the `cassandra-all` parse-tree type (find it under `org.apache.cassandra.cql3.statements...`).
2. Implement `canProcess` (instanceof check) and `processCql` (read fields via `Reflections` if package-private; mutate the `Volatile*` model; return an `AsyncResultSet` via the `newAsyncResultSet` defaults on `CqlHandler`).
3. Register it in the `CqlHandlerRegistry` construction inside `SeaStarCqlRequestHandler`.
4. Match real Cassandra's failure behavior — throw the same driver exception type (`AlreadyExistsException`, `InvalidQueryException`, …) that a live cluster would.

---

[Minimize token use](https://raw.githubusercontent.com/drona23/claude-token-efficient/refs/heads/main/profiles/CLAUDE.coding.md):

## Output
- Return code first. Explanation after, only if non-obvious.
- No inline prose. Use comments sparingly - only where logic is unclear.
- No boilerplate unless explicitly requested.

## Code Rules
- Simplest working solution. No over-engineering.
- No abstractions for single-use operations.
- No speculative features or "you might also want..."
- Read the file before modifying it. Never edit blind.
- No docstrings or type annotations on code not being changed.
- No error handling for scenarios that cannot happen.
- Three similar lines is better than a premature abstraction.

## Review Rules
- State the bug. Show the fix. Stop.
- No suggestions beyond the scope of the review.
- No compliments on the code before or after the review.

## Debugging Rules
- Never speculate about a bug without reading the relevant code first.
- State what you found, where, and the fix. One pass.
- If cause is unclear: say so. Do not guess.

## Simple Formatting
- No em dashes, smart quotes, or decorative Unicode symbols.
- Plain hyphens and straight quotes only.
- Natural language characters (accented letters, CJK, etc.) are fine when the content requires them.
- Code output must be copy-paste safe.
