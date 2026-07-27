# CQL support matrix

Every CQL statement type Cassandra 5.0 parses, and what SeaStar does with it. There is no third
category: a statement is either handled or it fails with a driver exception that names the feature
SeaStar does not implement, so "SeaStar is broken" and "SeaStar does not do that" are never the same
answer.

Verified against `cassandra:5.0.8`. `AbstractCqlSessionTest` runs the same assertions against SeaStar
and against a real node in a container, so anything marked **yes** below agrees with a cluster on the
cases that suite covers.

## Data

| Statement | Supported | Notes |
| --- | --- | --- |
| `SELECT` | yes | Rows come back in partition-token and clustering order. `ORDER BY`, `LIMIT`, `DISTINCT` and `ALLOW FILTERING` are implemented; the range, `CONTAINS`, `LIKE` and `IS NOT NULL` operators are parsed and rejected. Every query is a full scan - there is no partition index yet. `GROUP BY` and `PER PARTITION LIMIT` are rejected. |
| `SELECT` clause | yes | Column aliases, `count(*)`, `count`, `min`, `max`, `sum`, `avg`, `token`, `writetime`, `ttl`, `cast` and `SELECT JSON`. A cast converts between the numeric types and to text; any other pair is rejected. Element selection (`m['k']`), field selection, slices and arithmetic in the select clause are rejected by name. |
| `INSERT` | yes | Including `IF NOT EXISTS`, `INSERT ... JSON` with `DEFAULT NULL`/`DEFAULT UNSET`, and `USING TTL`/`USING TIMESTAMP`. Bulk load is O(n^2) in the number of rows. |
| `UPDATE` | yes | Including `IF` conditions and `USING TTL`/`USING TIMESTAMP`. |
| `DELETE` | yes | Including `IF EXISTS` and `USING TIMESTAMP`. |
| `BATCH` | yes | Children are applied in order. **Not atomic and not isolated**: a child that fails partway through leaves the earlier ones applied, where a cluster rejects the whole batch first. A batch-level `USING` is rejected; write it on each child. |
| `TRUNCATE` | yes | |
| `USE` | yes | A keyspace that does not exist fails with `InvalidQueryException`, as on a cluster. |

## Schema

| Statement | Supported | Notes |
| --- | --- | --- |
| `CREATE KEYSPACE` | yes | `IF NOT EXISTS`; the replication map is stored with the fully qualified strategy class a cluster reports. |
| `ALTER KEYSPACE` | yes | Replication and `durable_writes`; an option the statement does not name keeps its value. Neither changes an answer - SeaStar stores one copy of every row in one process - but both read back. |
| `DROP KEYSPACE` | yes | `IF EXISTS`; clears the session keyspace when it was the one dropped. |
| `CREATE TABLE` | yes | `IF NOT EXISTS`, clustering order, static columns, collections, tuples, vectors and UDT columns. `COMPACT STORAGE` and column masks are rejected. `WITH` options are accepted and not stored. |
| `ALTER TABLE ... ADD` | yes | One column or several, `IF NOT EXISTS`. Existing rows gain a null in the new column, which lands where a cluster keeps it: after the primary key, alphabetically among the rest. |
| `ALTER TABLE ... DROP` | yes | One column or several, `IF EXISTS`. Discards the values that column held, so re-adding it brings it back empty - as on a cluster. A primary key column is refused. |
| `ALTER TABLE ... RENAME` | yes | Primary key columns only, as on Cassandra 5. A column with a dependent index is refused. `RENAME IF EXISTS` does **not** forgive a missing column, matching 5.0.8. |
| `ALTER TABLE ... ALTER ... TYPE` | n/a | Rejected, because Cassandra 5 rejects it: *Altering column types is no longer supported*. |
| `ALTER TABLE ... WITH` | accepted | Applied as a no-op. SeaStar models no table options at all - `TableMetadata#getOptions()` is always empty, however the table was created - so accepting keeps schema scripts running over settings that cannot change a result. |
| `ALTER TABLE ... DROP COMPACT STORAGE` | no | Rejected. No SeaStar table can have compact storage, because `CREATE TABLE ... WITH COMPACT STORAGE` is rejected. |
| `DROP TABLE` | yes | `IF EXISTS`. |
| `CREATE TYPE` | yes | `IF NOT EXISTS`. |
| `ALTER TYPE` | yes | `ADD` and `RENAME`; altering a field's type is rejected, as on Cassandra 5. Prepared statements naming the type are re-resolved. |
| `DROP TYPE` | yes | `IF EXISTS`. Refuses a type another type or a table column still names, counting nested references (`list<frozen<t>>` holds `t`). |
| `CREATE INDEX` | yes | `IF NOT EXISTS`, single column, named or derived `<table>_<column>_idx`. An indexed column can be queried without `ALLOW FILTERING`; the index is metadata, not an access path - the query is still a scan. A custom index class, `WITH OPTIONS` and a collection target (`KEYS(m)`, `ENTRIES(m)`, `FULL(m)`) are rejected. |
| `DROP INDEX` | yes | `IF EXISTS`. A keyspace that does not exist reads as an index that does not exist, as on a cluster. |

## Not implemented

Each of these fails with `InvalidQueryException` whose message names the feature and quotes the
query. `SeaStarCqlSessionTest` pins that behavior; it cannot go in the shared suite, because a real
node runs most of them.

The exception type is the one a live 5.0 node gives for a feature that is switched off rather than
missing - `CREATE MATERIALIZED VIEW` on a default node answers *Materialized views are disabled.
Enable in cassandra.yaml to use.* and `CREATE FUNCTION` answers *User-defined functions are disabled
in cassandra.yaml*, both `InvalidQueryException`. SeaStar's reason differs, so the message says so.

| Statement | Why not |
| --- | --- |
| `CREATE`/`ALTER`/`DROP MATERIALIZED VIEW` | A view is a derived table that has to be maintained on every write. Not for 1.0.0-alpha. `KeyspaceMetadata#getViews()` is always empty, which is what a cluster answers for a keyspace without views. |
| `CREATE`/`DROP FUNCTION` | A user-defined function needs a script engine to run its body. `KeyspaceMetadata#getFunctions()` is always empty. |
| `CREATE`/`DROP AGGREGATE` | Rests on user-defined functions. `KeyspaceMetadata#getAggregates()` is always empty. |
| `CREATE`/`DROP TRIGGER` | A trigger loads a class by name and runs it inside the server. |
| `CREATE`/`ALTER`/`DROP ROLE`, `GRANT`, `REVOKE`, `LIST ROLES`, `LIST PERMISSIONS`, identity statements | SeaStar has no authentication or authorization model, so there is nothing for a permission to restrict. Note that a default Cassandra node - which also has no auth configured - answers these with `UnauthorizedException` rather than `InvalidQueryException`. |
| `DESCRIBE ...` | Not a scope decision but a parse-tree one: `DescribeStatement` tells `DESCRIBE KEYSPACES` from `DESCRIBE TABLES` only by which anonymous inner class it is, and `DESCRIBE TABLE` only by the identity of an opaque lambda field. There is no honest way to read the variant back out, and keying off `$1` versus `$2` would break silently on any cassandra-all upgrade. `TableMetadata#describe()` works, so the same text is reachable through the metadata API. |

## Cells: write time, TTL and static columns

Every cell carries the microsecond timestamp it was written at and, where a statement gave one, the
second it stops being readable. Both read back through `writetime()` and `ttl()`.

- **Expiry is lazy and clock-driven.** Nothing is reaped and no timer runs; a TTL is evaluated on
  read against the session's clock. `SeaStarCqlSessionBuilder#withClock(java.time.Clock)` supplies
  it, and `SeaStarClock` is a clock a test moves with `advance(Duration)` - so a test asserting on
  expiry does not sleep. The default is `Clock.systemUTC()`, which behaves like a cluster.
- **`INSERT ... USING TTL` expires the row, not just its columns.** The row marker takes the TTL, so
  the row disappears rather than leaving a primary key with nothing under it, exactly as on a
  cluster. `UPDATE ... USING TTL` writes no marker, so the row survives its columns.
- **Two writes to one cell are resolved by timestamp.** A write stamped older than the value already
  stored is discarded, and so is a `DELETE ... USING TIMESTAMP` older than what it would remove.
- **A static column is one cell per partition.** Writing it through one row is visible from all of
  them, a static-only `INSERT` may leave the clustering key out and reads back with a null one, and
  a partition-wide `DELETE` takes the static cells with it. An `UPDATE` that writes only static
  columns may not restrict a clustering column, as on a cluster.

## Paging

**SeaStar does not page, deliberately.** A cluster pages because a result may not fit in memory and
because the rows are on another machine; SeaStar's rows are already in this process, so a page
boundary would be an invention with nothing behind it.

`setPageSize` is accepted and has no effect, `AsyncResultSet#hasMorePages()` is always false, and
`fetchNextPage()` always throws `IllegalStateException` - which is what the driver's contract says an
implementation does when there is no next page. Every client idiom still terminates and returns every
row: `while (rs.hasMorePages())` runs zero times, `rs.all()`, `rs.iterator()` and `rs.currentPage()`
return the lot. What is not reproduced is code that asserts on the page boundary itself - a page
count, a page size being respected, or `fetchNextPage()` returning something.
`AbstractCqlSessionTest` pins the idioms on both backends.

## Known gaps within supported statements

These are fidelity gaps rather than missing statements; a query runs but SeaStar's answer can differ
from a cluster's.

- **Table options are not modelled.** `TableMetadata#getOptions()` is always empty.
- **A tombstone is not stored.** A delete resolves against what is there at the time; it leaves
  nothing behind, so a write stamped older than a delete that already happened is applied rather than
  suppressed.
- **Two writes at the same timestamp resolve to the later statement.** A cluster compares the
  serialized values and keeps the greater.
- **Deleting a partition's last clustered row also takes its static columns.** A cluster leaves the
  static row behind, readable with a null clustering key.
- **`UPDATE` cannot create a partition from static columns alone.** `UPDATE t SET s = 'x' WHERE
  pk = 1` on a partition with no rows writes nothing, where a cluster creates a static row.
- **Batches are not atomic or isolated.** See `BATCH` above.
- **Secondary indexes are metadata only.** They make a query legal without `ALLOW FILTERING`; they do
  not make it faster.
- **`getTokenMap()` is empty.** SeaStar is one node with no token ring, although read order does
  follow real Murmur3 token order.
- **Closing a session discards its keyspaces.** A real cluster keeps its metadata readable after a
  session closes; SeaStar's storage *is* the session. Deliberate, so that a leaked session fails
  loudly.
- **A prepared statement with no bind or result columns is not evicted by a schema change.** It has
  no column list to go stale, so nothing is lost by it - but a caller re-preparing the same string
  gets the cached instance.
