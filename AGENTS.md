## What this is

SeaStar is a lightweight, in-memory implementation of the DataStax Java driver's `CqlSession` — essentially a mock/fake for Cassandra intended as a fast alternative to TestContainers for tests. The overriding design goal is **fidelity**: a query that fails against real Cassandra should fail in a similar fashion (same exception types) in SeaStar. Correctness of that mirroring takes precedence over performance, and startup time is the second priority.

This is an early prototype. Many code paths are stubbed with `TODO`/`FIXME`, throw `UnsupportedOperationException`, or are half-implemented (e.g. `CreateTableHandler` parses columns but does not yet persist them — note the stray `System.gc()` and commented-out `addColumn`). Do not assume a feature works just because a class for it exists; check for `UnsupportedOperationException` and unfinished handler bodies.

## Build & test

Requires JDK 17 (configured via Gradle toolchain; foojay resolver auto-downloads it).

All tests should be annotated with a short human-readable `@DisplayName`.

### Testing strategy

`AbstractCqlSessionTest` is how the fidelity goal is measured: one suite, run twice against the same `CqlSession` API, once on a real Cassandra (`ContainerCqlSessionTest`, TestContainers) and once on SeaStar (`SeaStarCqlSessionTest`). Any behavioral divergence surfaces as a failure in one subclass but not the other.

So when adding or changing behavior, put the coverage in `AbstractCqlSessionTest` first, expressed only through the public driver API so both subclasses can run it. Add unit tests when appropriate, for what that suite cannot reach (internals, or error paths a live cluster will not produce), not as a substitute for it.

```bash
./gradlew build                 # compile + test; no Docker required
./gradlew :lib:test             # run all tests except the container suite
./gradlew :lib:containerTest    # run ContainerCqlSessionTest; needs Docker, skips without it
./gradlew :lib:test --tests 'com.tagadvance.seastar.SeaStarCqlSessionTest'          # single class
./gradlew :lib:test --tests 'com.tagadvance.seastar.SeaStarCqlSessionTest.testSimpleSelect'  # single method
./gradlew :lib:publishToMavenLocal   # publish artifact locally
```

Everything lives in the single `lib` subproject. Tests are JUnit 5 (Jupiter) with Mockito. Configuration cache, parallel, and build caching are enabled in `gradle.properties`.

### Benchmarks

Goal 2 — "minimize startup time to act as a viable alternative to TestContainers" — is measured, not asserted. The numbers, the hardware they were taken on, and the versions they pin to live in [benchmarks.md](benchmarks.md); it is the baseline against which the locking and query-engine changes are compared, so re-run these and update it when either lands.

Benchmarks live in their own source sets (`lib/src/jmh`, `lib/src/containerBench`). They are **not** on the default build and their classes are **not** in the published jar. All benchmark tasks are serialized against each other by a Gradle shared service, so listing several in one invocation is safe despite `org.gradle.parallel=true`.

```bash
./gradlew :lib:jmh                       # per-statement and scaling benchmarks (JMH), ~3 min
./gradlew :lib:jmh -PjmhIncludes='com.tagadvance.seastar.bench.StatementBenchmark.selectPoint'
./gradlew :lib:startupBenchmark          # cold/warm startup + the in-situ parser split, ~1 min
./gradlew :lib:startupSchemaBenchmark    # startup seeded with a 75-statement fixture schema
./gradlew :lib:parserCostBenchmark       # attributes the one-time cassandra-all parser cost
./gradlew :lib:parserEquivalenceCheck    # proves both parser entry points agree
./gradlew :lib:containerWarmBenchmark    # TestContainers baseline, cached image; needs Docker
./gradlew :lib:containerColdBenchmark    # same, but removes and re-pulls the image first
```

JMH is only used for the warm per-statement work. Startup, the parser breakdown and the container comparison fork a fresh JVM per sample instead (`ColdJvmBenchmark`): class loading is most of what they measure, so JMH's warmup would erase the very thing under test.

## Architecture

### Two dependencies that must not be confused
- **`java-driver-core` (client side)** — the DataStax driver whose public interfaces (`CqlSession`, `Statement`, `AsyncResultSet`, `KeyspaceMetadata`, `DriverContext`, …) SeaStar implements so existing client code can use it as a drop-in.
- **`cassandra-all` (server side)** — the actual Cassandra server library. SeaStar borrows its CQL parser: `QueryProcessor.parseStatement(query)` turns a CQL string into a `CQLStatement.Raw` parse tree. SeaStar never runs a Cassandra node; it only uses the parser.

### Request pipeline
Each SeaStar class is deliberately **analogous to** a driver-internal class (the Javadoc says so). The flow:

1. `SeaStarCqlSession.execute(request, resultType)` → `SeaStarRequestProcessorRegistry.processorFor(...)` picks a `SeaStarRequestProcessor` by matching the result type (`Statement.SYNC`/`ASYNC`, prepare sync/async). Registered in `SeaStarBuiltInRequestProcessors`.
2. Sync processors delegate to their async counterpart and block (`CompletableFutures.getUninterruptibly`).
3. `SeaStarCqlRequestHandler.handle()` extracts the query string + bound values (from `SimpleStatement` or `SeaStarBoundStatement`), parses it via Cassandra's `QueryProcessor`, then dispatches to a `CqlHandler` through `CqlHandlerRegistry`.
4. A `CqlHandler<T extends CQLStatement.Raw>` (`CreateKeyspaceHandler`, `CreateTableHandler`, `AlterTableHandler`, `CreateTypeHandler`, `UseKeyspaceHandler`, `SelectHandler`, …) has the statement translated (see below), then mutates or reads the in-memory model and returns a `CompletionStage<AsyncResultSet>`.

There are two parallel registries — do not conflate them: `SeaStarRequestProcessorRegistry` selects a *processor* by driver result type; `CqlHandlerRegistry` selects a *handler* by parsed statement type. Handlers are built once in `SeaStarCqlSession#buildHandlerRegistry` and shared across requests.

A statement no handler claims does **not** fall through as an internal error. `UnsupportedStatements` turns it into an `InvalidQueryException` that names the feature and quotes the query, and it holds the table of statement families SeaStar rejects on purpose — materialized views, UDFs and aggregates, triggers, roles and permissions, `DESCRIBE`. Adding a handler for one of those means deleting its row. [docs/support-matrix.md](docs/support-matrix.md) is the published version of that table and should be updated alongside it.

### The translation layer
Handlers do not read the parse tree. A statement is translated once, into a small model that names
only columns, values and operators, and the handler works from that:

- **`Targets`/`Target`** resolve the keyspace and table a statement names - the statement's own
  keyspace, else the session's, then the keyspace, then the table - and report the three failures
  (no keyspace, unknown keyspace, unknown table) identically for every statement. `Target` also owns
  the primary key and partition key name sets. Every handler that addresses a table starts here.
- **`Queries`/`Query`** translate a SELECT; **`Modifications`/`Modification`** translate an INSERT,
  UPDATE or DELETE, into `Restriction`, `Assignment` and `Condition`. `Restrictions`, `Conditions`
  and `Terms` do the pieces; `CqlOperator` is SeaStar's own operator enum.
- Only these classes import `org.apache.cassandra.*`, plus `Tokens`, which borrows Cassandra's
  `MurmurHash` to compute a partition token. A handler's remaining imports are `CQLStatement` and
  the `Raw` type it is generic over, both of which come from the `CqlHandler` interface rather than
  from reading a statement.

The split is deliberate: **translation says what the statement is, the handler says whether it is
allowed and what it does.** Resolving a column name to a position, a term to a value or an operator
to a `CqlOperator` is translation. ALLOW FILTERING, "a primary key part is missing", "a PRIMARY KEY
part was found in the SET part" and DISTINCT validation are the handler's, because they are rules
about the store rather than about the statement.

### Read-time ordering
A SELECT returns rows in the order a real node would: partitions by their Murmur3 token, rows within
a partition by the clustering columns in their declared order and direction. `RowOrdering` builds the
comparator for a table, `Tokens` hashes a partition key the way `Murmur3Partitioner` does, and
`ValueComparators` supplies the order Cassandra's `AbstractType` defines for a column's `DataType` -
which is not `Comparable`'s for `text`, `uuid` or `timeuuid`. Sorting happens in `SelectHandler`
after filtering and before LIMIT, never on insert: a token belongs to the partition key rather than
to the row, so there is no order to maintain on write.

A restriction becomes a row predicate in exactly one place (`Restriction#toPredicate`), so the
operators SeaStar does not evaluate yet - the range comparisons, CONTAINS, LIKE, IS NOT NULL - are
carried through translation and rejected there. Implementing them, and the collection forms of
`Assignment`, is a change to one file rather than to SELECT, UPDATE and DELETE separately.

### Reflection into Cassandra internals
The `CQLStatement.Raw` parse-tree objects from `cassandra-all` expose much of their state only as package-private fields. Where a public accessor exists, handlers use it (`ModificationStatement.Parsed#getConditions()`, `ColumnCondition.Raw#getValue()`, `QualifiedStatement#keyspace()`/`name()`, `Selectable.RawIdentifier#toFieldIdentifier()`, `CreateTableStatement.Raw#keyspace()`/`table()`, `CreateKeyspaceStatement.Raw#keyspaceName`). What is left has no accessor and no alternative, and lives in one place:

- **`FieldBindings`** holds every (declaring class, field name, expected type) triple, resolved once at class-init. Handlers call `FieldBindings.SOME_FIELD.require(raw)` for state a statement always carries, or `.find(raw)` where absence is a genuine answer. Nothing defaults silently: a required field that has been renamed throws a `ReflectionException` naming the field, the class and the cassandra-all version.
- **`FieldBindingsTest`** walks the table and fails the build if any binding stops resolving. That is the gate on a `cassandra-all` bump - upgrade deliberately, and expect this test to tell you what moved. `cassandra-all` is pinned to an exact version for the same reason.
- **`Reflections`** is the low-level resolver behind those bindings; `SeaStarRawType` wraps the parsed type objects.

Two standing caveats:

- **`setAccessible` works only because `cassandra-all` is a plain classpath jar** (unnamed module), so no `--add-opens` flags are needed. If Cassandra ever modularizes, this breaks.
- **`Raw#prepare(ClientState)` is not a way out.** It returns fully typed objects with public accessors, but resolves tables through the process-global `Schema.instance` singleton. Spiked against 5.0.8: `CreateTableStatement.parse(...).build()` fails with `InaccessibleObjectException` (`java.base` does not open `java.io` to the unnamed module), `Schema.instance.transform(...)` fails with `NoClassDefFoundError: TimeUUID$Generator`, and `raw.prepare(...)` throws `KeyspaceNotDefinedException`. Making it work means requiring `--add-opens` in every consumer's test JVM, ~300ms+ of forced `DatabaseDescriptor` init, and a mutable global shared by every session - which breaks the "Volatile = lives only for the session" model outright. Rejected; the trigger to revisit is Cassandra shipping an embeddable offline schema API, not a cleverer flag.

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
2. Implement `canProcess` (instanceof check) and `processCql`. Resolve the table through `Targets` rather than by hand, and read the statement in the translation layer rather than in the handler: extend `Queries`/`Modifications`, or add a translator beside them, and add a `FieldBinding` to `FieldBindings` for any state with no public accessor. The handler then mutates the `Volatile*` model and returns an `AsyncResultSet` via the `newAsyncResultSet` defaults on `CqlHandler`.
3. Register it in the `CqlHandlerRegistry` construction inside `SeaStarCqlSession#buildHandlerRegistry`.
4. Match real Cassandra's failure behavior — throw the same driver exception type (`AlreadyExistsException`, `InvalidQueryException`, …) that a live cluster would.
5. If the statement changes a table or a type, announce it through `SchemaChanges` so cached prepared statements naming it are re-resolved.
6. Update [docs/support-matrix.md](docs/support-matrix.md), and remove the statement from `UnsupportedStatements` if it was being rejected.

## Code Style
- Do not allow Optional as a field or parameter
- Exit statements like return or throw should have a newline before them unless the preceding character is an opening brace
- Favor functional code over procedural, e.g. Stream over loops, Optional over @Nullable
- Only/always use static wildcard imports for `org.junit.jupiter.api.Assertions.*`, `org.mockito.Mockito.*`, and `org.mockito.ArgumentMatchers.*`

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
