# Handover: the 1.0.0-alpha push

`final-push` no longer exists as a branch - its history is merged into `main` and pushed to
`origin/main` already (confirmed identical SHAs as of this update). **`main` is now the
integration branch.** The repo is pre-1.0.0 (`version = "1.0.0-alpha"`, no release tag), so per the
working agreement committing straight to `main` is correct here; this update continued doing that.

Read `TODO/*.txt` for the plans themselves. This file records what is DONE, what is NOT, and the
traps that have already cost time. Update it as waves land.

## State

Verify before trusting this file:

```bash
git log --oneline -30      # everything below HEAD is what this update did
./gradlew build              # no Docker needed
./gradlew :lib:containerTest # needs Docker
```

Last verified: 293 tests green locally (282 baseline + 11 new from this update), build clean on a
full `./gradlew clean build`. Container suite not re-run this session (no Docker check performed
here beyond what CI does) - run it before trusting cross-backend fidelity claims.

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
| b_plan | B1-B7 | one lock per keyspace (l_plan L1's middle path, not B2's hierarchy); row lock and table lock gone; every `Volatile*` field `@GuardedBy` or immutable; `ConcurrencyTest` |
| l_plan | L1 | decided and implemented as above |
| f_plan | F1 F2 F3 F4b F5 F6 F7 F8 | see "f_plan closed out this update" below |

The full suite passes on JDK 17, 21 and 25 - verified locally, not assumed. That is the check
j_plan J7 wanted, because the handlers `setAccessible` into package-private cassandra-all fields and
a JDK bump is the most likely thing to break them. Run it with
`./gradlew test -PtestJavaVersion=25`.

### Not done

- **g_plan G2 G3 G4**, **h_plan H1 H3 H4 H5 H6**, **k_plan all**, **j_plan J3 J8**,
  **l_plan L2-L7**. (f_plan is fully closed out - see below.)

### f_plan closed out this update

All of F1-F8 landed in one sitting; commits `aa5f529`..`b4e261b`. Worth knowing before touching
this area again:

- **F1's package-private pass has two structural exceptions, not oversights.**
  `SeaStarAsyncResultSet` and `VolatileUserDefinedType` stay public because
  `com.tagadvance.seastar.handlers` constructs both directly (`CqlHandler#newAsyncResultSet`,
  `AppliedResultSets`, `SelectHandler` for the former; `CreateTypeHandler` for the latter), and
  Java has no way to let one package see a type while hiding it from everyone else. Fixing this for
  real means adding factory methods in `handlers` (mirroring how `SeaStarKeyspace#newSeaStarTable`
  already avoids callers needing `VolatileTable`) - not attempted here, flagged as the next step if
  the surface needs to shrink further.
- **The `handlers` package itself could not go package-private at all.**
  `SeaStarCqlSession#buildHandlerRegistry` constructs every individual handler
  (`InsertHandler`, `SelectHandler`, ...) and holds a `CqlHandlerRegistry` field, both from the
  `com.tagadvance.seastar` package - a different package than `handlers`. True package-private
  visibility would break that at the language level, not just as a style violation. Took f_plan
  F1's own documented fallback instead: classes stay public, `handlers/package-info.java` now
  states the package is internal and unstable. This was the plan's own escape hatch, not a
  deviation from it.
- **F4's "three copies of primaryKeyNames/resolveWhere" was already fixed** by earlier C4/D-plan
  work before this update started - `Target#primaryKeyNames()` is the only implementation, WHERE
  resolution goes solely through `RestrictionRules`. No changes needed.
- **F6 drew the accept-vs-reject line exactly where the plan proposed**: every transport setting on
  `SeaStarBoundStatement` and `SeaStarCqlSessionBuilder` (timeout, paging, consistency, tracing,
  auth, TLS, local datacenter, metrics, node distance) is now stored and handed back instead of
  throwing. Contact points and cloud secure-connect bundles still throw - accepting one would claim
  a connection target exists when SeaStar has none.
- **F8 uncovered a real bug while inlining Guava's `checkArgument`**: `SeaStarUdtValue`'s two call
  sites used `%d` in the message template, which Guava's formatter never substitutes (it only
  understands `%s`) - the literal text `%d` was going out in the exception message instead of the
  value. `SeaStarRow`'s own comment had already flagged the general risk (it used `%s` correctly
  and was never actually wrong); `VolatileRow` had no bug either, just switched from `%s` to `%d`
  for precision now that plain `String#formatted` supports it. Fixed as part of the inline, not
  filed separately.

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
- **API surface**: f_plan F1 landed - see "f_plan closed out this update" above for exactly what
  stayed public and why. D7's four additions survive as promised: `SeaStarClock`,
  `SeaStarCqlSessionBuilder#withClock(java.time.Clock)` and `SeaStarDriverContext#getClock()` are
  all still public. The fourth, a three-argument `VolatileDriverContext` constructor, no longer
  needs to be independently reachable - `VolatileDriverContext` itself is package-private now, and
  `withClock` is the sanctioned way to get the same effect from outside the library.
- **A tombstone is not stored.** D7 resolves two writes to a cell by their timestamps, which is what
  makes `USING TIMESTAMP` real, but a delete leaves nothing behind. A write stamped older than a
  delete that already happened is therefore applied rather than suppressed. Recorded in the support
  matrix; the fix is a tombstone per cell and it was judged not worth the storage for a fake.
- **c_plan C1** (`Raw#prepare(ClientState)`) is rejected with evidence, recorded in AGENTS.md.

### Open questions for Tag

- **DEFERRED, needs you: j_plan J3 (publishing repositories are dead).** Both configured
  repositories point at `s01.oss.sonatype.org`, which Central Portal replaced after OSSRH's 30 June
  2025 sunset - publishing to either URL will fail outright. Fixing this needs the current Central
  Portal publishing path (Gradle plugin vs. publisher API) and, more importantly, namespace
  verification for `com.tagadvance` under your account - that's an external, credentialed step I
  cannot do from here. Flagging rather than guessing at a plugin choice you'd have to unwind.
  j_plan calls this the long-pole item, not the code - worth starting the namespace verification
  early even before the Gradle side is settled.
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

## The locking rework: what landed

**The lock hierarchy is written up in `AGENTS.md`. Read it there, not here.** In short: one lock per
keyspace, `context.read -> keyspace.write` for every mutation, no table lock and no row lock. That
deletes B1's deadlock rather than ordering around it, which is l_plan L1's middle path and what
b_plan B3 independently recommended.

The three hazards the previous wave flagged, and what became of them:

1. **`VolatileTable#statics(...)` still takes no lock, on purpose,** and the reason is unchanged: a
   row resolves its partition's static cells while reading, and a read lock cannot be upgraded. The
   map stays a `ConcurrentHashMap` and `computeIfAbsent` is still what makes creation atomic. It is
   the one documented exception to "the keyspace lock guards everything".
2. **A row's values are still a `Cells`.** The partition index keys off them directly
   (`VolatileRow#storedValues`) rather than through `getObject`, which round-trips every value
   through its codec.
3. **The impure read was the sharpest edge, and it resolved cleanly.** `VolatileRow`'s two fields
   (`statics`, `staticsResolved`) collapsed into one `volatile @Nullable Cells`. Several readers may
   resolve at once now that there is no row lock, but they all get the same `Cells` instance back
   from the `computeIfAbsent`, so the race is benign and publishing the reference is all that is
   needed. Null means "not resolved yet" rather than "no statics", which is sound because a row only
   asks when the column it is reading is declared static.

## Known performance debt

The partition-key index landed with the lock rework; the numbers are in the last section of
`benchmarks.md`. What it fixed: bulk load through INSERT (100k rows now seed in 1.7 s rather than an
extrapolated ~19 minutes), and the point lookup, which no longer scales with the table at all -
12.7 us at 1 000 rows and 13.4 us at 100 000, a 1 900x improvement at 100k.

What is left:

- **A full scan did not improve, and got 4-11 % worse at small row counts.** Two causes, both
  deliberate: `rows()` copies under the read lock rather than handing out a lazy stream over live
  storage (b_plan B5 - a lazy one escapes the lock), and a map of partitions has worse locality than
  one array. At 100 000 rows the same change measures 13 % faster, so the cost is only visible where
  the scan is cheap anyway.
- **D1's read-time sort still costs a full scan its per-row key encoding.** The index removed the
  search, not the encoding: a query returning every row still puts every row's partition key through
  `getBytesUnsafe` and a codec. Iterating partitions in token order would remove it - the index now
  makes that possible, where before it was not - and that is the next thing to try if `selectAll`
  matters.
- **`VolatileRow#getObject` round-trips through the codec.** Overriding it to return the stored value
  was considered and rejected: a `blob` column would hand out its stored `ByteBuffer` rather than a
  duplicate, so a caller that consumed it would drain the stored value. The index sidesteps it with a
  package-private accessor instead.

## Working agreement reminders

- Build and run the tests before reporting anything complete; paste real output.
- One logical change per commit, imperative subject under ~50 chars, no agent/tooling references,
  no co-author trailers. Stage only files you touched.
- Coverage goes in `AbstractCqlSessionTest` so both backends run it; the container is the authority
  on real Cassandra behavior. Every test needs a `@DisplayName`.
