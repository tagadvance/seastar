# Handover: the 1.0.0-alpha push

Working branch: `final-push` (branched from `main` at `ad92e9d`). Never push; never commit to `main`.

Read `TODO/*.txt` for the plans themselves. This file records what is DONE, what is NOT, and the
traps that have already cost time. Update it as waves land.

## State

Verify before trusting this file:

```bash
git log --oneline main..final-push
./gradlew build              # no Docker needed
./gradlew :lib:containerTest # needs Docker
```

Last verified: 213 tests green locally, 112 green on the container.

### Done

| Plan | Items | Notes |
| --- | --- | --- |
| j_plan | J1 J2 J4 J5 J6 | deps pinned exactly; artifacts named `seastar-*`; signing guarded; `-Xlint:all` on |
| g_plan | G1 | doclint clean; 22 of 27 warnings are unfixable shaded-Guava noise |
| h_plan | H2 | container suite is opt-in via `:lib:containerTest`, image pinned `cassandra:5.0.8` |
| f_plan | F8 (deps half) | gson + jakarta.annotation removed, jspecify declared, jcip -> compileOnly |
| c_plan | C2 C3 C4 | `FieldBindings` binding table + `FieldBindingsTest` version guard; translation layer |
| f_plan | F4 | shared `Targets` resolver |
| a_plan | A1-A11 | A1 fixed via F4; A6 drops the keyspace map on close (deliberate divergence); A11 fell out of D3 |
| d_plan | D1 D2 D3 D4 D8 | Murmur3 token + clustering order, ORDER BY; literals; bound-value typing; `RestrictionRules`; counters |
| i_plan | I1 I2 I3 | `benchmarks.md`, baseline at `1145dae` |
| j_plan | J7 | CI on JDK 17/21/25; container suite nightly and on demand |
| e_plan | E0 E1 E2 E3 E4 E5 | ALTER TABLE/KEYSPACE, DROP INDEX/TYPE; everything else rejected by name; `docs/support-matrix.md` |
| d_plan | D5 D6 D7 D9 | select clause (aggregates, token, writetime/ttl, cast, aliases, JSON both ways); static columns per partition; per-cell write time and TTL against an injectable clock; paging documented as a deliberate single page |

The full suite passes on JDK 17, 21 and 25 - verified locally, not assumed. That is the check
j_plan J7 wanted, because the handlers `setAccessible` into package-private cassandra-all fields and
a JDK bump is the most likely thing to break them. Run it with
`./gradlew test -PtestJavaVersion=25`.

### Not done

- **b_plan all**, **f_plan F1 F2 F3 F5 F6 F7**, **g_plan G2 G3 G4**, **h_plan H1 H3 H4 H5 H6**,
  **k_plan all**, **j_plan J3 J8**.

### Decisions already made - do not relitigate

- **Exception messages need not match Cassandra's byte for byte.** The bar is: the same exception
  type, semantically the same failure, and the same information conveyed (name the keyspace, table
  or column at fault). Matching wording exactly is welcome where it is free, but it is not worth
  contorting code or chasing across Cassandra's three different spellings of "keyspace does not
  exist". Assert on type and on the offending name being present; do not pin whole strings in tests
  unless the wording itself is the thing under test.

- **Locking**: one lock per keyspace (`context.read -> keyspace.write`), NOT the four-level
  hierarchy in b_plan B2. Drop the row lock entirely; that deletes the B1 deadlock rather than
  fixing it. This is l_plan L1's middle path.
- **Scope**: static columns (D6), counters (D4), TTL (D7) and JSON (D5) are all to be IMPLEMENTED,
  not rejected.
- **Unsupported statements are a table, not a pile of handlers.** `UnsupportedStatements` maps a
  parse-tree class to the feature name SeaStar reports, and `CqlHandlerRegistry` consults it when no
  handler claims a statement. Rejecting a new feature is one row; supporting it is deleting one.
  `docs/support-matrix.md` is the published form and moves with it.
- **`DESCRIBE` is rejected for a parse-tree reason, not a scope one.** `DescribeStatement`
  distinguishes its variants only by anonymous inner class ordinal and by the identity of a lambda
  field, so there is no honest way to tell `DESCRIBE KEYSPACES` from `DESCRIBE TABLES`. Revisit only
  if cassandra-all exposes the variant.
- **`SeaStarRequestProcessorRegistry` keeps throwing `IllegalArgumentException`**, deliberately:
  that is what the driver's own `RequestProcessorRegistry` throws, and reaching it means a caller
  asked for a result type nothing was registered for - a client-side programming error, not a query
  failure. Only the message changed. e_plan E0 asked for a driver exception there; that was the
  wrong call and the reasoning is in the method's javadoc.
- **API surface**: f_plan F1 in full - handlers, processors, registries and `Volatile*` all become
  internal or package-private. D7 added four things to it deliberately and they should survive that
  pass in some form: `SeaStarClock`, `SeaStarCqlSessionBuilder#withClock(java.time.Clock)`,
  `SeaStarDriverContext#getClock()` and a three-argument `VolatileDriverContext` constructor. The
  clock is how a test observes a TTL without sleeping, so it is user-facing however the rest is
  narrowed.
- **A tombstone is not stored.** D7 resolves two writes to a cell by their timestamps, which is what
  makes `USING TIMESTAMP` real, but a delete leaves nothing behind. A write stamped older than a
  delete that already happened is therefore applied rather than suppressed. Recorded in the support
  matrix; the fix is a tombstone per cell and it was judged not worth the storage for a fake.
- **c_plan C1** (`Raw#prepare(ClientState)`) is rejected with evidence, recorded in AGENTS.md.

### Open questions for Tag

- A6 drops the keyspace map on `close()`. A real cluster keeps metadata readable after close, so
  this is a deliberate fidelity trade, not a fix. Reversible if he prefers fidelity.
- g_plan G1 asks for a zero warning count. Unreachable: 22 of 27 come from
  `java-driver-guava-shaded`, which shades Guava but not the errorprone annotations it was compiled
  against. Choose `-Xlint:all,-classfile` or accept permanent noise.
- Cassandra uses a third error wording (`Keyspace '%s' doesn't exist`) for CREATE INDEX/CREATE TABLE
  specifically. F4 unified everything on the `ClientState` pair instead. Revisit deliberately.
- `getResultMetadataId()` now returns the SAME buffer instance on every call, because that is what
  the real driver does - a caller that consumes it drains it for the next one. Handing out a
  duplicate would be friendlier but would let code pass here and fail against a cluster. The
  container proved the driver's behavior; a defensive copy was the original guess and was wrong.

## Traps that have already cost time

1. **`~/.gitignore` line 11 is a bare `*`.** Every NEW file is invisible to `git status` and skipped
   by plain `git add`. `CqlParsers.java` was one merge away from being lost silently. Always
   `git add -f <new file>` and verify with `git ls-files --error-unmatch <path>`.
2. **Worktree agents branch from `main`, not from `final-push`.** One agent built against
   `Reflections.getDeclaredField(...)` after that API had been replaced by `FieldBindings`; the merge
   was textually clean and did not compile. Every worktree brief must start with
   `git reset --hard final-push` and verify with `git log --oneline -5`.
3. **A clean textual merge is not a clean semantic merge.** Build and run BOTH suites after every
   merge, never trust the diff.
4. ~~**`ContainerCqlSessionTest > DROP TABLE ...` flake**~~ - DIAGNOSED AND FIXED. It was never
   random: `auto_snapshot` is on by default, so `DROP TABLE` writes a snapshot before responding,
   which exceeds the driver's default `basic.request.timeout` of 2 seconds on a loaded container.
   A test written during e_plan reproduced it on every run until its `DROP TABLE` was removed.
   `ContainerCqlSessionTest` now raises the request and schema-agreement timeouts to 30s; starting
   the container with `auto_snapshot: false` would work too. This costs nothing in fidelity - a
   timeout is a transport concern, and SeaStar accepts and ignores timeouts anyway.
5. **Benchmarks must be serialized.** `org.gradle.parallel=true` lets two benchmark tasks share the
   cores and silently corrupts comparative runs; a Gradle shared service now enforces this.
6. **TestContainers drags in the 3.x DataStax driver**, whose Guava 19 and slf4j 1.7 shadow the
   pinned versions inside a JMH uber jar. The container probe needs its own source set.

## What the locking rework needs to know

D6 and D7 changed the storage, and l_plan L1 is about to change how it is locked. Three things
matter:

1. **`VolatileTable#statics(...)` takes no lock, on purpose.** A row resolves its partition's static
   cells while reading, under the table's *read* lock, and a `ReentrantReadWriteLock` read lock
   cannot be upgraded - taking a write lock there deadlocks, and it did. The map is a
   `ConcurrentHashMap` and `computeIfAbsent` is what makes creation atomic. If the row lock goes and
   the keyspace lock arrives, keep that property: whatever lock protects the column list must already
   be held by the caller, and the map must stay concurrent or the lookup must move outside the read
   path.
2. **A row's values are a `Cells`, not a `List<Object>`.** Values, write times and expiries move
   together through `insert`/`remove`, which is what keeps ALTER TABLE ADD/DROP in step. A partition's
   static cells are a `Cells` of the same width, so the same shift applies to them - see
   `VolatileTable#insertColumn`/`removeColumn`/`truncate`.
3. **Reads are no longer pure.** `VolatileRow#cellsOf` resolves and caches the partition's statics on
   first use, so the read path mutates two fields (`statics`, `staticsResolved`). It is idempotent
   and guarded by the row's own lock today; with the row lock gone it needs the keyspace lock or a
   volatile/final rework.

A partition-key index would help here too: `InsertHandler` now also scans the partition to drop a
static row when the first clustered row arrives, and `DeleteHandler` scans it to clear static cells.

## Known performance debt

- `InsertHandler` scans every existing row for a primary-key match on each insert, so bulk load is
  O(n^2) (~5e9 comparisons at 100k rows). Seeding a fixture goes through this path, so it works
  against goal 2. Needs a partition-key index.
- Every SELECT is a full scan: point lookup by full primary key costs 5.8 us at 10 rows, 63 us at
  1k, 12 749 us at 100k.
- D1's read-time sort roughly doubles a full scan (`selectAll` at 1k: 334 -> 624 us), because every
  returned row's partition key is re-encoded through `getBytesUnsafe`, which takes two locks and
  runs a codec. A point lookup is unaffected. Both this and the line above are the same missing
  partition key index; see the "After D1" section of `benchmarks.md`.

## Working agreement reminders

- Build and run the tests before reporting anything complete; paste real output.
- One logical change per commit, imperative subject under ~50 chars, no agent/tooling references,
  no co-author trailers. Stage only files you touched.
- Coverage goes in `AbstractCqlSessionTest` so both backends run it; the container is the authority
  on real Cassandra behavior. Every test needs a `@DisplayName`.
