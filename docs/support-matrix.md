# CQL support matrix

Every CQL statement type Cassandra 5.0 parses, and what SeaStar does with it. There is no third
category: a statement is either handled or it fails with a driver exception that names the feature
SeaStar does not implement, so "SeaStar is broken" and "SeaStar does not do that" are never the same
answer.

Verified against `cassandra:5.0.8`. The fidelity suite - the `Abstract*FidelityTest` groups under
`seastar/src/testFixtures` - runs the same assertions three ways -
against SeaStar in process, against a real node in a container, and against SeaStar over a socket
through `seastar-server` - so anything marked **yes** below agrees with a cluster on the cases that
suite covers, and agrees with itself across the wire.

A statement's own values - `SimpleStatement.newInstance(cql, ...)` and
`SimpleStatement.builder(cql).addNamedValue(...)` - are bound to its bind markers the way a prepared
statement's are, in process and over the wire alike. A marker written `:name` is addressed by that
name, an anonymous `?` by the column it binds, and `getVariableDefinitions()` names them the same
way. The two value forms are counted as a node counts them: positional values against the marker
count, named ones with one lookup per marker - so a marker no name accounts for is refused, a name no
marker claims is ignored, and one value feeds every marker sharing its name. A discrepancy is
*Invalid amount of bind variables*, and a null in a primary key part is *Invalid null value in
condition for column x*, both being what a node answers.

Every marker a statement carries is a variable, not only the ones standing in for a column. A
`USING` clause, an `IF` condition, a multi-column relation, an element or field of a collection or
UDT column, a `LIMIT` and an `INSERT ... JSON` document all bind, and `getVariableDefinitions()`
names and types them as a node does: `[ttl]` is an `int`, `[timestamp]` a `bigint`, `[json]` and
`[limit]` their own, and `idx(l)`, `value(l)`, `key(m)`, `value(m)` and `u.field` are typed by what
they address rather than by the whole column. A bind index follows the order the markers are written
in, so a `USING` clause written ahead of `SET` and `WHERE` binds ahead of them.

## Data

| Statement | Supported | Notes |
| --- | --- | --- |
| `SELECT` | yes | Rows come back in partition-token and clustering order. `ORDER BY`, `LIMIT`, `DISTINCT` and `ALLOW FILTERING` are implemented. The range operators are implemented under Cassandra's restriction rules - a column may carry a lower and an upper bound in one query, and a multi-column relation (`(ck1, ck2) > (1, 2)`) compares lexicographically - and so are `CONTAINS` and `CONTAINS KEY`; `LIKE` and `IS NOT NULL` are parsed and rejected. A query that pins the whole partition key walks one partition; anything else is a full scan. `GROUP BY` and `PER PARTITION LIMIT` are rejected. |
| `SELECT` clause | yes | Column aliases, `count(*)`, `count`, `min`, `max`, `sum`, `avg`, `token`, `writetime`, `ttl`, `cast` and `SELECT JSON`. A cast converts between the numeric types and to text; any other pair is rejected. Element selection (`m['k']`), field selection, slices and arithmetic in the select clause are rejected by name. |
| `INSERT` | yes | Including `IF NOT EXISTS`, `INSERT ... JSON` with `DEFAULT NULL`/`DEFAULT UNSET`, and `USING TTL`/`USING TIMESTAMP`. An insert reads only the partition it writes to, so a bulk load scales linearly. |
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

## The native protocol (`seastar-server`)

Everything above is CQL, and it answers the same whether a statement arrives in process or over a
socket. This section is the **transport**, which is a separate set of trade-offs with its own
supported list, its own refusals and its own deliberate divergences. A reader who only ever swaps a
`CqlSession` in process can skip it. A reader who points a driver at the listener should not: the
interesting limits down here are not the ones in the CQL tables above.

```java
final var session = SeaStarCqlSession.builder().build();
try (final var server = SeaStarProtocolServer.builder().session(session).build().start()) {
    // server.port() is an ephemeral port on the loopback address
}
```

A stock `CqlSession` connects to it with no configuration but a contact point and
`withLocalDatacenter("datacenter1")`, negotiating its way to protocol v5.

### Handled

Every client-to-server message the protocol defines, and nothing is left over.

| Message | What it does |
| --- | --- |
| `STARTUP` | always answered `READY`. No authentication is required and none is offered. |
| `OPTIONS` | `SUPPORTED` with `CQL_VERSION=3.4.7`, `PROTOCOL_VERSIONS=[4/v4, 5/v5]` and an empty compression list. |
| `AUTH_RESPONSE` | `AUTH_SUCCESS`. Credentials a client sends anyway are accepted unexamined. |
| `QUERY` | the statement, run against the session. Positional, named and null values all work. `USE` comes back as `SET_KEYSPACE`, DDL as `SCHEMA_CHANGE`, a `SELECT` or an LWT as `ROWS`, everything else as `VOID`. |
| `PREPARE` | `PREPARED` carrying bind and result metadata. The id is `MD5(keyspace + query)`, the digest a real node computes, so preparing the same statement twice - or against a node - gives the same id. |
| `EXECUTE` | the bound statement. An id this server never issued is `UNPREPARED` carrying the id back. |
| `BATCH` | children applied in order; prepared ids and query strings may be mixed in one batch. |
| `REGISTER` | recorded per connection. See [Server events](#server-events-seastar-server). |

### Rejected by name

Each of these fails loudly rather than being quietly dropped, so a client that needs one finds out.

| Feature | What the client gets |
| --- | --- |
| Compression | `STARTUP` naming `snappy` or `lz4` is a `PROTOCOL_ERROR` naming it, not a silent fallback to none. It buys nothing on a loopback socket. |
| A paging state in a request | `PROTOCOL_ERROR`, with a node's own wording. This server never issues one, and ignoring it would answer page one for ever. |
| An event type `REGISTER` does not define | `PROTOCOL_ERROR` reading *Invalid value '&lt;name&gt;' for Type*, which is a node's own wording. Accepting a subscription that can never be honoured is worse. A name is resolved case-insensitively, as on a node, and one unknown name rejects the whole message. |
| Protocol v3, v6-beta, DSE v1 and v2 | the `PROTOCOL_ERROR` that makes a driver retry one version lower. See [Protocol versions](#protocol-versions-seastar-server). |
| TLS | not implemented at all - there is no `STARTTLS`-style negotiation to refuse, and a client configured for SSL fails in its own handshake. This is a loopback test socket. |

### Accepted and ignored

Present in a request, read, and deliberately without effect. Nothing here changes an answer, because
there is one replica in one process and no I/O to bound.

`consistency`, `serial_consistency`, the request's `default_timestamp` and `now_in_seconds`, the
tracing flag (no tracing id is ever fabricated), `skip_metadata` (full column metadata is always
sent), `page_size`, custom payloads, and a `WHERE` clause on a system table. Response warnings are
always empty.

Note that this is the protocol's timestamp, not CQL's: `INSERT ... USING TIMESTAMP` is implemented
and does resolve writes, as the [Data](#data) table says.

### Fidelity trades

Deliberate, and each is expanded in the section named.

- **Every answer is one page.** `page_size` is ignored - see [Paging](#paging).
- **`TOPOLOGY_CHANGE` and `STATUS_CHANGE` never fire.** One node, up for as long as the server is
  bound - see [Server events](#server-events-seastar-server).
- **One node, no peers, one synthetic token.** `system.peers` and `system.peers_v2` are empty, and
  `system.local` reports the single minimum token so this node owns the whole ring - see
  [The system keyspaces](#the-system-keyspaces-seastar-server).
- **`system_schema` is a projection, not a stored keyspace.** It is generated from the model on every
  query, so an in-process user sees no system keyspaces at all.
- **Table options over the wire are Cassandra 5.0.8 defaults, not stored state.** SeaStar models no
  table options, so the projection writes what a plain `CREATE TABLE` would have written - which
  means **`system_schema.tables.comment` is always the empty string**, whatever `WITH comment = '...'`
  said, and a client reading table options over the wire disagrees with in-process `getOptions()`,
  which is empty. Fixing it is a change to the model, not to the projection.
- **A `BATCH` is not atomic or isolated.** Inherited from the core and restated here, because a
  client on a socket has no reason to read the CQL tables: a child that fails partway through leaves
  the earlier ones applied, where a node rejects the whole batch first.
- **A DDL statement is answered from memory, and the wait is the driver's.** One second per statement
  by default - see the note at the end of
  [The system keyspaces](#the-system-keyspaces-seastar-server).
- **`system_auth`, `system_traces` and `system_distributed` answer `INVALID`.** Nothing on a driver's
  connect path reads them; a harness that queries one notices.

### Not proven

Honest gaps in the evidence rather than in the behaviour. Recorded so that "tested" is not assumed.

- **A frame over the 64 MiB ceiling is untested, and at v5 there is no ceiling.** `maxFrameLength` is
  enforced by the driver's `FrameEncoder` and `FrameDecoder`, both of which the pipeline replaces when
  it switches to segments; neither `FrameToSegmentEncoder` nor `SegmentToFrameDecoder` takes a limit,
  so a v5 connection will reassemble a frame of any size and run out of heap rather than refuse it.
  That is the driver's own pipeline behaving as it does on the client side too, not something this
  server chose. Proving anything about it means allocating 64 MiB.

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
`AbstractCellFidelityTest` pins the idioms on every backend.

Over the wire (`seastar-server`) this is protocol-legal rather than a divergence - a node is always
free to return everything, and result metadata with no paging state is what says "last page", so a
driver reads the answer as complete and stops asking. Two consequences:

- **`page_size` in a request is accepted and ignored.** A client that sets it and expects three
  round trips gets one.
- **A paging state in a request is refused with `PROTOCOL_ERROR`**, not ignored. It can only have
  come from this server and this server never issues one; ignoring it would answer page one forever,
  which is an infinite loop in the client rather than a slow answer.

## The system keyspaces (`seastar-server`)

A driver queries the node about itself before `CqlSession.builder().build()` returns, and again on
every schema change. `seastar-server` answers those queries itself rather than from the model, so
**an in-process user who never starts a server sees no system keyspaces at all** - `getKeyspaces()`
holds only what was created.

| Keyspace | Over the wire | In process |
| --- | --- | --- |
| `system.local` | one row describing the listener | not present |
| `system.peers`, `system.peers_v2` | empty, with full column metadata - one node has no peers | not present |
| `system_schema.*` | all eight tables, projected live from the model | not present; `SystemSchema.select` is a projection, not a keyspace |
| `system_virtual_schema.*` | all three tables, empty - SeaStar has no virtual tables | not present |
| `system_auth`, `system_traces`, `system_distributed`, anything else | `InvalidQueryException` naming the table | not present |

What `system.local` reports: `partitioner` is Murmur3, which is what the core actually hashes
partition keys with; `release_version` is `5.0.8`, matching the `cassandra-all` pin; `tokens` is the
single token that owns the whole ring; `host_id` is generated once per server. The datacenter, the
rack and the cluster name default to `datacenter1`, `rack1` and `SeaStar`, and are settable on
`SeaStarProtocolServer.builder()`. **The datacenter has to match the driver's
`withLocalDatacenter(...)`**: a mismatch leaves the node `IGNORED`, the session still builds, and
the first statement fails saying only that no node was available.

`schema_version` is a real UUID that changes when a DDL statement runs, which is what lets the
driver's schema-agreement check pass immediately instead of waiting out its ten-second timeout on
every DDL statement.

Everything else a node keeps in `system` is absent, and a `WHERE` clause on one of these tables is
matched and then ignored - `system.local` has one row, so restricting it would be a predicate that
is always true.

**A DDL-heavy schema setup costs a second per statement, and it is the driver, not SeaStar.** The
driver holds a DDL statement's answer until the metadata refresh it triggered has finished, and
debounces that refresh by `advanced.metadata.schema.debouncer.window`, one second by default. Every
statement is answered from memory here, so that window is the whole of the wait. A harness seeding
a large schema should shorten it - the fidelity suite's own wire backend went from 190 s to under
7 s doing exactly that - or turn schema metadata off if it does not read `getMetadata()`.

## Protocol versions (`seastar-server`)

| Version | What the listener does |
| --- | --- |
| v5 | served, with segment framing from the message after `READY` |
| v4 | served, legacy framing throughout |
| v3, v6-beta, DSE v1/v2 | refused with the `PROTOCOL_ERROR` that makes a driver retry one version lower |

A driver that was never told which version to use starts at DSE v2 and walks down; it is refused
three times and settles on v5. One that is pinned to v4 is served at v4, and one pinned to v3 or v6
fails with `UnsupportedProtocolVersionException` after the refusal, exactly as it would against a
node that does not speak them. `OPTIONS` advertises `4/v4, 5/v5`.

What v5 changes, and it is only these three things: everything after `READY` travels in segments
carrying a CRC24 over the header and a CRC32 over the payload, which are checked - a mismatch is a
`PROTOCOL_ERROR` naming it, and the connection ends, because a byte stream that has been corrupted
once cannot be resynchronized; `PREPARE` answers with a result metadata id where v4 sends none; and
`duration` is described by its own protocol code rather than as a Cassandra marshaller class name. A
request may also name its own keyspace, which is honoured for that statement and leaves the
connection's own selection alone.

A segment carries at most 128 KiB - 1, so a frame larger than that is split across several and
reassembled by the far side. **Both directions are exercised.** The server reaches the split path
easily, because [paging](#paging) is deliberately not implemented and a node may legally answer with
every row it has; a client reaches it with a large bound value or a large batch. The slicing and the
reassembly are the driver's own `SegmentBuilder` and `SegmentToFrameDecoder` in both directions - what
is proven here is that they are wired up the right way round, and that the content survives.

Compression is refused at both versions - `STARTUP` asking for one is a `PROTOCOL_ERROR` naming it
rather than a silent fallback. It buys nothing on a loopback socket.

## Server events (`seastar-server`)

`REGISTER` is honoured, and a DDL statement produces both halves of what a driver expects: the
`SCHEMA_CHANGE` result on the connection that ran it, and a `SCHEMA_CHANGE` event on every
connection registered for one. That is what keeps a second client's metadata current when it was
not the one that changed the schema.

| Event | When it fires |
| --- | --- |
| `SCHEMA_CHANGE` | every DDL statement that actually changes something |
| `TOPOLOGY_CHANGE` | never - there is one node and no membership to change |
| `STATUS_CHANGE` | never - the node is up for as long as the server is bound |

Registering for the latter two is accepted and correctly produces nothing. An event may be sent at
any time, so nothing orders it against the result of the statement that caused it - a client tells
them apart by stream id, which is negative on an event.

Naming an event type that does not exist is a `PROTOCOL_ERROR` reading *Invalid value '&lt;name&gt;'
for Type*, which is a `cassandra:5.0.8` node's own wording, quoting the name in the case it was sent
in. Three things about it were captured from a container rather than reasoned about: a name is
resolved **case-insensitively**, so `schema_change` registers and is then honoured; the refusal does
not upper-case the name it quotes; and one unknown name rejects the **whole** message, so a
`REGISTER` naming `SCHEMA_CHANGE` beside a type that does not exist registers nothing at all.

## Known gaps within supported statements

These are fidelity gaps rather than missing statements; a query runs but SeaStar's answer can differ
from a cluster's.

- **Table options are not modelled.** `TableMetadata#getOptions()` is always empty. Over the wire the
  same tables read back as Cassandra 5.0.8 defaults instead, and `comment` is always empty - see
  [The native protocol](#the-native-protocol-seastar-server).
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
- **A bind marker inside a collection literal is accepted.** `VALUES (1, [?, ?])` binds each element;
  a node refuses the statement outright - *bind variables are not supported inside collection
  literals*. SeaStar is the more permissive of the two here.
- **A marker binding a whole list or tuple is not described the way a node describes it.** `ck IN ?`,
  `(ck) = ?`, `(ck) IN ?` and `IF v IN ?` bind one marker to a collection of values, which a node
  names `in(ck)` or `(ck)` and types `list<int>` or `frozen<tuple<int>>`; SeaStar either types it as
  the column or reports no variables for the statement at all. Nothing is lost by it, because SeaStar
  refuses every one of those forms at execute - but `prepare` succeeds on them, as it does on a node,
  so the metadata is reachable.
- **A bound value is checked against the column's Java type rather than against its bytes.** A value
  the type cannot hold is an `InvalidQueryException`, where a node reports the byte length it was sent
  and there are no bytes in process. It differs in the one direction where a node's check is weaker
  than a type check: an `int` bound to a `text` column travels as four bytes a node stores as text,
  and SeaStar refuses it.
- **A null compared to a column outside the primary key is accepted.** `WHERE v = null ALLOW
  FILTERING` and `l CONTAINS null` match nothing; a node refuses both with *Unsupported null value
  for column v*. A null in a primary key part is refused, as on a node.
- **`getTokenMap()` is empty.** SeaStar is one node with no token ring, although read order does
  follow real Murmur3 token order.
- **Closing a session discards its keyspaces.** A real cluster keeps its metadata readable after a
  session closes; SeaStar's storage *is* the session. Deliberate, so that a leaked session fails
  loudly.
- **A prepared statement with no bind or result columns is not evicted by a schema change.** It has
  no column list to go stale, so nothing is lost by it - but a caller re-preparing the same string
  gets the cached instance.
- **Over the wire, a DDL statement that changes nothing still reports a schema change.** A node
  answers `CREATE TABLE IF NOT EXISTS` on a table that already exists with `VOID`, because it
  compares the schema before and after. `seastar-server` sends `SCHEMA_CHANGE`, which costs a
  connected driver one redundant metadata refresh; the alternative would risk leaving it holding
  stale metadata.
- **Over the wire, a prepared statement is never evicted.** A node has a bounded cache and answers
  an id it has forgotten with `UNPREPARED` so the client re-prepares. `seastar-server` remembers
  every id for the life of the session, so that path is never exercised against it.
- **Over the wire, a prepared statement's result metadata id never changes.** A node reached at v5
  answers an `EXECUTE` run after `ALTER TABLE` with a new identifier and the `METADATA_CHANGED`
  flag, so the client can update its copy. SeaStar sends the same identifier throughout. Nothing is
  lost by it, because `skip_metadata` is ignored and full column metadata is sent on every answer -
  which the driver prefers over anything it holds locally - so a prepared `SELECT *` run after a
  column is added still describes and returns the new column.
