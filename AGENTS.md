## What this is

SeaStar is a lightweight, in-memory implementation of the DataStax Java driver's `CqlSession` — essentially a mock/fake for Cassandra intended as a fast alternative to TestContainers for tests. The overriding design goal is **fidelity**: a query that fails against real Cassandra should fail in a similar fashion (same exception types) in SeaStar. Correctness of that mirroring takes precedence over performance, and startup time is the second priority.

This is a pre-1.0 library, and plenty of CQL is deliberately unimplemented rather than half-built - `UnsupportedOperationException` and `InvalidQueryException` naming the missing feature are expected outcomes, not bugs. Do not assume a feature works just because a class for it exists; [docs/support-matrix.md](docs/support-matrix.md) is the authoritative list of what is handled, what is rejected by name, and what is a deliberate fidelity trade-off (single page of results, no tombstones, batches not atomic, and the like).

## Build & test

Requires JDK 17 (configured via Gradle toolchain; foojay resolver auto-downloads it).

### Testing strategy

`AbstractCqlSessionTest` is how the fidelity goal is measured: one suite, run twice against the same `CqlSession` API, once on a real Cassandra (`ContainerCqlSessionTest`, TestContainers) and once on SeaStar (`SeaStarCqlSessionTest`). Any behavioral divergence surfaces as a failure in one subclass but not the other.

It lives in `seastar/src/testFixtures` rather than `src/test`, so a backend in another module can extend it. The fixture is for this build only — the test-fixtures variants are skipped in the publishing block. `ContainerCqlSessionTest` stays in `src/test`: it is a backend, not the suite.

So when adding or changing behavior, put the coverage in `AbstractCqlSessionTest` first, expressed only through the public driver API so both subclasses can run it. Add unit tests when appropriate, for what that suite cannot reach (internals, or error paths a live cluster will not produce), not as a substitute for it.

```bash
./gradlew build                          # compile + test; no Docker required
./gradlew :seastar:test                  # run all tests except the container suite
./gradlew :seastar:containerTest         # run ContainerCqlSessionTest; needs Docker, skips without it
./gradlew :seastar:test --tests 'com.tagadvance.seastar.SeaStarCqlSessionTest'          # single class
./gradlew :seastar:test --tests 'com.tagadvance.seastar.SeaStarCqlSessionTest.testSimpleSelect'  # single method
./gradlew publishToMavenLocal            # publish both artifacts locally
./gradlew :seastar:inspectRaw -Pquery="CREATE KEYSPACE foo WITH replication = {...}"
    # parses the CQL string with cassandra-all's own parser and prints its CQLStatement.Raw
    # class plus a reflective dump of its package-private fields - the first thing to run when
    # writing a handler for a new statement type, or investigating a FieldBindings failure
```

### Modules

| Module | What it is |
| --- | --- |
| `:seastar` | The in-memory `CqlSession`. Everything below under Architecture is this module. Published as `com.tagadvance:seastar`. |
| `:seastar-server` | A native-protocol listener that serves a `SeaStarCqlSession` over a socket, for clients that cannot swap their `CqlSession` in-process. Published as `com.tagadvance:seastar-server`. |

The dependency runs one way — `:seastar-server` depends on `:seastar` and never the reverse. Every type the server needs from the core is a deliberate addition to the core's public API, not a visibility bump made in passing; if that list grows past a handful, add a purpose-built accessor (the shape `SeaStarKeyspace#newSeaStarTable` already uses) rather than opening the model.

Both are versioned in lockstep from `build-logic/src/main/kotlin/seastar.java-conventions.gradle.kts`, the convention plugin holding everything the two modules share: toolchain, `-Xlint:all`, doclint, reproducible archives, and the whole publishing and signing block. A module's own build file carries only its description and its dependencies.

Tests are JUnit 5 (Jupiter) with Mockito. Configuration cache, parallel, and build caching are enabled in `gradle.properties`.

### Benchmarks

Goal 2 — "minimize startup time to act as a viable alternative to TestContainers" — is measured, not asserted. The numbers, the hardware they were taken on, and the versions they pin to live in [benchmarks.md](benchmarks.md); it is the baseline against which the locking and query-engine changes are compared, so re-run these and update it when either lands.

Benchmarks live in their own source sets (`seastar/src/jmh`, `seastar/src/containerBench`). They are **not** on the default build and their classes are **not** in the published jar. All benchmark tasks are serialized against each other by a Gradle shared service, so listing several in one invocation is safe despite `org.gradle.parallel=true`.

```bash
./gradlew :seastar:jmh                       # per-statement and scaling benchmarks (JMH), ~3 min
./gradlew :seastar:jmh -PjmhIncludes='com.tagadvance.seastar.bench.StatementBenchmark.selectPoint'
./gradlew :seastar:startupBenchmark          # cold/warm startup + the in-situ parser split, ~1 min
./gradlew :seastar:startupSchemaBenchmark    # startup seeded with a 75-statement fixture schema
./gradlew :seastar:parserCostBenchmark       # attributes the one-time cassandra-all parser cost
./gradlew :seastar:parserEquivalenceCheck    # proves both parser entry points agree
./gradlew :seastar:containerWarmBenchmark    # TestContainers baseline, cached image; needs Docker
./gradlew :seastar:containerColdBenchmark    # same, but removes and re-pulls the image first
```

JMH is only used for the warm per-statement work. Startup, the parser breakdown and the container comparison fork a fresh JVM per sample instead (`ColdJvmBenchmark`): class loading is most of what they measure, so JMH's warmup would erase the very thing under test.

## Architecture

### Three dependencies that must not be confused
- **`java-driver-core` (client side)** — the DataStax driver whose public interfaces (`CqlSession`, `Statement`, `AsyncResultSet`, `KeyspaceMetadata`, `DriverContext`, …) SeaStar implements so existing client code can use it as a drop-in.
- **`cassandra-all` (server side)** — the actual Cassandra server library. SeaStar borrows its CQL parser: `QueryProcessor.parseStatement(query)` turns a CQL string into a `CQLStatement.Raw` parse tree. SeaStar never runs a Cassandra node; it only uses the parser.
- **`native-protocol` (neither, and this is the one that gets misfiled)** — `com.datastax.oss:native-protocol`, a standalone codec for Cassandra's binary protocol: `Message` and its subtypes, `Frame`, `FrameCodec`, `Segment`, `RawType`, `ProtocolConstants`. It is not the driver's public API and it is not Cassandra; it is the wire format the two ends share, and it encodes and decodes in **both** directions. `:seastar-server` uses it to read requests and write responses, which is exactly what `FrameCodec.defaultServer(...)` exists for — **serving with it is its intended purpose, not a hack.** It arrives transitively through `java-driver-core` and `:seastar-server` declares it anyway, because an implicit transitive is not a contract.

Only `:seastar-server` sees the third. `:seastar` never names a protocol type: an in-process session has no wire, and `VolatileDriverContext#getProtocolVersion()` returning `ProtocolVersion.DEFAULT` is a codec setting rather than a claim about one (its javadoc says why).

### Request pipeline
There are two entry points. An in-process caller reaches `SeaStarCqlSession.execute(...)` directly; a
client on a socket reaches `SeaStarRequestDispatcher` in `:seastar-server`, which decodes a frame and
then makes that same `execute(...)` call. Everything in this section is the in-process path, which is
all of it bar the transport — see "Answering a statement over the wire" for what wraps it.

Each SeaStar class is deliberately **analogous to** a driver-internal class (the Javadoc says so). The flow:

1. `SeaStarCqlSession.execute(request, resultType)` → `SeaStarRequestProcessorRegistry.processorFor(...)` picks a `SeaStarRequestProcessor` by matching the result type (`Statement.SYNC`/`ASYNC`, prepare sync/async). Registered in `SeaStarBuiltInRequestProcessors`.
2. Sync processors delegate to their async counterpart and block (`CompletableFutures.getUninterruptibly`).
3. `SeaStarCqlRequestHandler.handle()` extracts the query string + bound values (from `SimpleStatement` or `SeaStarBoundStatement`), parses it via Cassandra's `QueryProcessor`, then dispatches to a `CqlHandler` through `CqlHandlerRegistry`.
4. A `CqlHandler<T extends CQLStatement.Raw>` (`CreateKeyspaceHandler`, `CreateTableHandler`, `AlterTableHandler`, `CreateTypeHandler`, `UseKeyspaceHandler`, `SelectHandler`, …) has the statement translated (see below), then mutates or reads the in-memory model and returns a `CompletionStage<AsyncResultSet>`.

There are two parallel registries — do not conflate them: `SeaStarRequestProcessorRegistry` selects a *processor* by driver result type; `CqlHandlerRegistry` selects a *handler* by parsed statement type. Handlers are built once in `SeaStarCqlSession#buildHandlerRegistry` and shared across requests.

**The async contract**: every request is answered entirely on the calling thread. `:seastar` never spawns a thread, so by the time `executeAsync` (or a handler's `CompletionStage<AsyncResultSet>`) returns, the work is already done — `whenComplete` and friends run inline rather than on a callback thread, and there is no interleaving to reason about between one session's requests. This is also why the sync processors can block on `CompletableFutures.getUninterruptibly` for free: the future is already complete when they call it.

`:seastar-server` is the one thing that has threads, and it is built so that the second half of that promise survives anyway. A Netty worker decodes a frame and hands it to a **single-threaded executor owned by the server** — the funnel — which is the only thread that ever touches the session. So a wire request is still answered on the thread that calls into the session, there is still no interleaving between one session's requests, and what changes is only that the calling thread is not the caller's. Concurrency there would buy throughput no test needs at the cost of a new source of flakiness, since a driver opens several connections to the same node. An in-process caller reaching `getContext()` directly is unaffected, and the model's own thread safety and lock hierarchy are unchanged either way.

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

### Compilation against driver internals (`:seastar-server`)
The server module carries a second, different unstable-API exposure. Do not confuse it with the one above:

- `com.datastax.oss.protocol.internal.*` — internal by package name, but server-side use is what native-protocol is *for*; `FrameCodec.defaultServer` says so. Low risk.
- `com.datastax.oss.driver.internal.core.protocol.*` — `ByteBufPrimitiveCodec`, `FrameDecoder`, `FrameEncoder`, and for v5 `BytesToSegmentDecoder`, `SegmentToFrameDecoder`, `FrameToSegmentEncoder`, `SegmentToBytesEncoder`. Genuinely internal and genuinely liable to move.

The important difference: this is **ordinary compilation against public classes**, so a driver bump that moves one of them fails the build at `compileJava`. The `cassandra-all` hazard is reflection, which compiles fine and only `FieldBindingsTest` catches. Do not reach for reflection here out of habit — if a driver type is not public, that is a signal, not an obstacle to route around.

The mitigation is the same either way: `java-driver-core` and `native-protocol` are pinned to exact versions.

### Protocol versions and framing (`:seastar-server`)

The listener serves **v4 and v5**, and refuses everything else in the shape that makes a driver
retry one version lower - `PROTOCOL_ERROR`, on the first request of the channel, with a message
containing the literal `"Invalid or unsupported protocol version"`. That wording is load-bearing;
`ProtocolInitHandler` looks for exactly that substring, and paraphrasing it makes an unconfigured
driver fail outright instead of stepping down.

- **The refusal happens at the header, not after decoding.** An unconfigured driver's first byte is
  66 (`DSE_V2`), and `FrameCodec` has codecs for v3-v6 only, so such a frame does not decode at all
  and there would be no stream to answer politely. `ProtocolVersionGate` is a `ByteToMessageDecoder`
  ahead of the frame decoder that reads the raw version byte, settles the connection's version, and
  removes itself. The refusal is written at the highest version the server speaks, which is what a
  node does and the only choice that is always decodable.
- **`Framing` owns both pipeline shapes and the switch between them.** Legacy is `bytes <-> frames`;
  v5 is `bytes <-> segments <-> frames`, with a CRC24 over each segment header and a CRC32 over each
  payload. All six handlers are the driver's own and every one is direction-agnostic - the direction
  is in the `FrameCodec` (`defaultServer`), not the handler - so **no framing code is written here**.
- **The switch is mid-stream, on the same connection, right after `READY`.** The
  `OPTIONS`/`STARTUP` exchange is legacy-framed at every version. The driver switches its side on
  *receiving* `READY`; the server switches on *having sent* it, so the ordering is: write the READY,
  then rearrange. Both are submitted to the channel's event loop from the funnel, in that order,
  which is what guarantees the READY is encoded before the pipeline changes under it. Do not hang
  the switch off the write's future - a completed future notifies inline, on the funnel.
- **A CRC mismatch ends the connection.** It is a `PROTOCOL_ERROR` naming the mismatch, then close:
  the corruption is in the framing rather than in a message, so there is no stream id to fail and no
  way to resynchronize. `SegmentFramingTest` corrupts a header and a payload deliberately, which is
  the only way to produce one over loopback.
- **Three things differ per version below the framing**, and they are the only ones: `PREPARE`
  carries a result metadata id from v5, `duration` stops being a custom type at v5, and a v5 request
  may name its own keyspace. The version comes off `SeaStarConnection`, which is why `RawTypes`,
  `Results` and `SystemTables` all take one.

### Answering a statement over the wire (`:seastar-server`)

`SeaStarRequestDispatcher` is the whole of it: a decoded `QUERY`, `PREPARE`, `EXECUTE` or `BATCH`
in, a response `Message` out, always on the funnel. Everything it needs from the core is public
driver API plus two purpose-built additions, `CqlStatementSummary` and
`SeaStarCqlSession#setKeyspace`.

- **A statement is summarized before it runs.** `CqlStatementSummary.of(metadata, keyspace, query)`
  says whether it selects a keyspace, changes the schema, or does neither - the three things the
  protocol distinguishes and an `AsyncResultSet` cannot. Before rather than after, because
  `DROP INDEX` names only the index and the table a driver has to be told about can only be found
  while the index is still there. It over-reports: a DDL statement that changes nothing is still a
  schema change, where a node would answer `VOID`, because comparing the schema before and after is
  not something a summary can do and a redundant metadata refresh is the harmless direction to be
  wrong in.
- **Everything else follows from the result set.** Columns means `ROWS`, no columns means `VOID`,
  and that is already right for `SELECT`, the modifications, `TRUNCATE`, `BATCH` and a lightweight
  transaction - `[applied]` is a column like any other, so LWTs need no special case. `Results` is
  where that decision lives.
- **`RawTypes` is the fiddly part.** Driver `DataType` to protocol `RawType`, recursive. Frozen-ness
  is not on the wire at all, and two types travel as Cassandra marshaller class names rather than
  as protocol ids: `duration`, which only became a primitive at v5, and `vector`, which has no id.
  Both are what the container sends and what the driver's own `DataTypes#custom` names explicitly on
  the way back in. `RawTypesTest` runs every supported type back through the driver's reader.
- **`Failures` is the inverse of the handlers.** They throw the driver exception a live cluster
  would have; this turns it back into the error code that reconstructs it. That is what keeps
  `assertThrows(InvalidQueryException.class, ...)` passing when a test moves onto a socket, and it
  is why a statement SeaStar does not implement has to arrive as `INVALID` naming the feature rather
  than as a `SERVER_ERROR`.
- **The keyspace is per connection, and the session's is set from it before every statement.** The
  protocol keeps a *name*, not a keyspace: selecting one that does not exist is allowed, dropping
  the selected keyspace leaves unqualified statements failing on it while qualified ones elsewhere
  keep working, and recreating it makes the connection work again. `USE` can express none of that,
  which is why `setKeyspace` exists.
- **A schema change is announced twice, on purpose.** The `SCHEMA_CHANGE` result goes back to the
  connection that ran the DDL; a `SCHEMA_CHANGE` event goes to every connection whose `REGISTER`
  asked for one, on a negative stream id. A driver needs both - the result is how the client that
  changed the schema finds out, the event is how every other client does. `SeaStarConnection` holds
  the registration, the dispatcher publishes from the funnel, and the write itself is Netty's, on
  the target channel's own event loop. `TOPOLOGY_CHANGE` and `STATUS_CHANGE` never fire.
- **Paging is not implemented and that is protocol-legal.** Rows metadata with no paging state means
  "last page". `page_size` is accepted and ignored; a paging state in a request is a
  `PROTOCOL_ERROR`, because ignoring one is an infinite loop in the client rather than a slow
  answer. Consistency, serial consistency, timestamps, `now_in_seconds` and tracing are all accepted
  and ignored, matching what the in-process statement settings already do with them.

### The system keyspaces (`seastar-server`)

A driver does not just open a socket and send CQL. Before `CqlSession.builder().build()` returns it
reads the cluster name out of `system.local`, then builds its whole idea of the node - datacenter,
rack, tokens, host id - from the same row, and with schema metadata on (the default) it assembles
its `Metadata` from `system_schema` and `system_virtual_schema` as well. `SystemQuery`,
`SystemTables` and the interception at the top of `SeaStarRequestDispatcher#query` are that, and the
whole of it is in the server module.

- **Answered here, not in the model, and that is the load-bearing decision.** `system.local` is a
  fact about the *listener*; making it a keyspace would put invented tables in front of every
  in-process user, for the benefit of one hardcoded row. `SystemSchemaTest` guards the other half of
  it: the `system_schema` projection is not registered with the context either.
- **Matched by query string.** There are six such queries, they are string literals in
  java-driver-core's own source (`ProtocolInitHandler`, `DefaultTopologyMonitor`,
  `SchemaAgreementChecker`, `Cassandra3SchemaQueries`), and they do not vary. `SystemQuery` is a
  regex over `SELECT <columns> FROM <ks>.<table> [WHERE ...]`; a `WHERE` clause is matched and then
  ignored. It is deliberately grubby and it is what Simulacron effectively does. Parsing it properly
  would mean putting `cassandra-all` on this module's classpath.
- **The columns came off a container, the values came from the driver.** `DESCRIBE TABLE` on
  `cassandra:5.0.8` says which columns exist; `DefaultTopologyMonitor#nodeInfoBuilder` and
  `PeerRowValidator` say which of them the driver dereferences. A column it reads that is missing is
  a `NullPointerException` in driver internals rather than a legible error, so the list is the
  container's in full.
- **`schema_version` moves on every DDL statement**, driven off the `CqlStatementSummary` the
  dispatcher already computes. It must be a real UUID and stable *between* changes: the driver
  compares `system.local`'s against every peer's after DDL and waits ten seconds before giving up,
  so getting it wrong is a ten-second pause per DDL statement followed by success, which nothing
  fails on. `DriverSessionTest` asserts DDL is fast for that reason.
- **`system_virtual_schema` answers empty rather than failing.** The driver runs its three queries in
  the same batch as the eight `system_schema` ones, and one failure abandons the whole refresh, so
  an error there costs a stock-configured session its metadata entirely.
- **`data_center`, `rack` and `cluster_name` are on the builder.** A datacenter that does not match
  the driver's `withLocalDatacenter(...)` leaves the node `IGNORED` - the session still builds, and
  the first statement fails saying only that no node was available.

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

### The `system_schema` projection

`SystemSchema.select(context, table)` reshapes the live model into the rows
`system_schema.keyspaces`, `.tables`, `.columns`, `.types` and `.indexes` carry on a real node, and
returns them as an ordinary `AsyncResultSet`. `.views`, `.functions` and `.aggregates` are always
empty - they are unsupported by design - but still describe their columns. It exists so that a
driver connecting over the wire can build its own `Metadata`; the core has no other use for it.

Two things about it are deliberate and easy to undo by accident:

- **It is a projection, not a keyspace.** Nothing is registered with the context, so
  `session.getMetadata().getKeyspaces()` is unchanged for an in-process user who never starts a
  server. `SystemSchemaTest` asserts that. Making `system_schema` a real queryable keyspace is a
  documented change to the core, not a refactor.
- **The type string is not `DataType#asCql`.** `system_schema` writes a user-defined type
  unqualified (`frozen<address>`); `asCql` qualifies it with its keyspace, and the driver's schema
  parser throws on the qualified form. `SystemSchema#cqlType` writes the recursion out for that
  reason. Every expectation in `SystemSchemaTest` came off a `cassandra:5.0.8` container.

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
6. If it changes the schema or selects a keyspace, add a branch to `StatementSummaries` so `CqlStatementSummary` says so. That is the only thing telling `:seastar-server` to answer `SCHEMA_CHANGE` or `SET_KEYSPACE` rather than `VOID`; miss it and the statement works in process and leaves a connected driver holding stale metadata. An index statement reports the table it indexes.
7. Update [docs/support-matrix.md](docs/support-matrix.md), and remove the statement from `UnsupportedStatements` if it was being rejected.

## Code Style

Java style lives in `~/git/.agents/java.md` — read it before writing code. Nothing in this project overrides it.
