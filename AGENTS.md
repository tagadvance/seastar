## What this is

SeaStar is a lightweight, in-memory implementation of the DataStax Java driver's `CqlSession` — essentially a mock/fake for Cassandra intended as a fast alternative to TestContainers for tests. The overriding design goal is **fidelity**: a query that fails against real Cassandra should fail in a similar fashion (same exception types) in SeaStar. Correctness of that mirroring takes precedence over performance, and startup time is the second priority.

This is an early prototype. Many code paths are stubbed with `TODO`/`FIXME`, throw `UnsupportedOperationException`, or are half-implemented (e.g. `CreateTableHandler` parses columns but does not yet persist them — note the stray `System.gc()` and commented-out `addColumn`). Do not assume a feature works just because a class for it exists; check for `UnsupportedOperationException` and unfinished handler bodies.

## Build & test

Requires JDK 17 (configured via Gradle toolchain; foojay resolver auto-downloads it).

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

### The lock hierarchy

There are two locks, and a third that is not part of the tree. **Acquire them outermost first, and
never the other way round.**

```
context lock     VolatileDriverContext  - guards the keyspace map, and nothing else
  keyspace lock  VolatileKeyspace       - guards everything inside the keyspace
    (udt value)  VolatileUdtValue       - a detached value; below both, acquires neither
```

There is **no table lock and no row lock.** `VolatileTable`, `VolatileColumn` and
`VolatileUserDefinedType` return their keyspace's lock from `lock()`, and `VolatileRow` holds none at
all - it goes through its table's. A row's values are positionally tied to its table's column list,
so one lock over both is what keeps them consistent, and with only one lock there is no pair to take
in the wrong order. Two keyspaces do not contend.

The rules:

- **A mutation is `context.read` then `keyspace.write`.** Do not hand-roll it: `SeaStarTable#mutate`
  takes both, and `SeaStarTable#query` is the read-only counterpart (`context.read` +
  `keyspace.read`). Every DML handler goes through one of them.
- **Creating or dropping a keyspace takes `context.write`.** That is the only thing that does.
- **Never acquire a lock while holding an inner one.** `VolatileUdtValue` is where this can still be
  got wrong, because reading its type takes the keyspace lock: resolve whatever the type has to say
  *before* taking the value's own lock.
- **No read-to-write upgrades.** `ReentrantReadWriteLock` deadlocks on one. Downgrades are fine and
  are relied on - a handler holding `keyspace.write` calls methods that take `keyspace.read`.
- **Every field of every `Volatile*` class is annotated `@GuardedBy` or documented as immutable.**
  `jcip-annotations` is a `compileOnly` dependency for exactly this. Keep it that way when adding a
  field.
- **Getters hand out copies, not live views.** `getSeaStarKeyspaces`, `getSeaStarTables`,
  `getSeaStarUserDefinedTypes`, `rows()` and `partition(...)` all snapshot under the read lock. A
  lazy stream over the live storage escapes the lock and gives its consumer a
  `ConcurrentModificationException`.

One deliberate exception, and it is load-bearing: **`VolatileTable#statics(...)` writes to
`staticsByPartition` while its caller may hold only the read lock.** A row resolves its partition's
static cells while reading, and a read lock cannot be upgraded - taking the write lock there
deadlocks, and did. The map is a `ConcurrentHashMap` and `computeIfAbsent` is what makes creation
atomic. `VolatileRow#statics` caches the result in a single `volatile` field for the same reason: two
readers racing get the same `Cells` back, so the race is benign.

`ConcurrencyTest` is the regression test for all of this. It is a unit test, not part of
`AbstractCqlSessionTest`, because a live cluster cannot reach SeaStar's own locks. Everything in it
is time-bounded and fails on a latch: a hung Gradle test worker is far worse than a red test.

### Row storage and the partition index

A table stores its rows as a map from partition key to that partition's rows, which is how a node
stores them anyway. `SeaStarTable#partition(List)` reads one partition; `rows()` reads all of them.

`RestrictionRules#partitions` turns a WHERE clause into the partitions it reaches, or null when it
pins no partition key and has to be answered by a scan; `RestrictionRules#rows` is the two cases in
one call. SELECT, UPDATE and DELETE all go through it, and INSERT knows its partition outright. This
is what makes a point lookup and a bulk load stop being O(rows) - see [benchmarks.md](benchmarks.md).

### Identifiers
Everything is keyed by `CqlIdentifier`. Most internal APIs use `CqlIdentifier.fromInternal(name)` (case-sensitive, no quoting) — note this differs from `fromCql` (which interprets quoting/case rules). Be deliberate about which you use when adding lookups.

### Known correctness caveat noted by the author
SeaStar currently deserializes and re-serializes row data rather than storing it natively (the author calls this unnecessary but hasn't overridden all the default getters). Thread-safety is a stated invariant: everything should be thread-safe unless its documentation says otherwise (classes carry `@ThreadSafe`/`@NotThreadSafe` from `jcip-annotations`), and the lock hierarchy above is how that invariant is kept.

## Adding a new CQL statement type

1. Write a `CqlHandler<SomeRawStatement>` where `SomeRawStatement` is the `cassandra-all` parse-tree type (find it under `org.apache.cassandra.cql3.statements...`).
2. Implement `canProcess` (instanceof check) and `processCql`. Resolve the table through `Targets` rather than by hand, and read the statement in the translation layer rather than in the handler: extend `Queries`/`Modifications`, or add a translator beside them, and add a `FieldBinding` to `FieldBindings` for any state with no public accessor. The handler then mutates the `Volatile*` model and returns an `AsyncResultSet` via the `newAsyncResultSet` defaults on `CqlHandler`.
3. Register it in the `CqlHandlerRegistry` construction inside `SeaStarCqlSession#buildHandlerRegistry`.
4. Match real Cassandra's failure behavior — throw the same driver exception type (`AlreadyExistsException`, `InvalidQueryException`, …) that a live cluster would.
5. If the statement changes a table or a type, announce it through `SchemaChanges` so cached prepared statements naming it are re-resolved.
6. Update [docs/support-matrix.md](docs/support-matrix.md), and remove the statement from `UnsupportedStatements` if it was being rejected.

## Code Style

Java style lives in `~/git/.agents/java.md` — read it before writing code. Nothing in this project overrides it.
