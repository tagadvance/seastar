# Changelog

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). SeaStar is pre-1.0, so
any release may still change the public API; from `1.0.0` onward this file is where that gets
called out explicitly.

## [1.0.0-alpha] - Unreleased

Initial alpha release. Public API: `SeaStarCqlSession` (via `.builder()...build()`),
`SeaStarCqlSessionBuilder`, `SeaStarDriverContext`, the `SeaStar{Keyspace,Table,Column,Row,
UserDefinedType,UdtValue}` model interfaces, `SystemSchema` and `CqlStatementSummary`. Everything
else is internal - see [AGENTS.md](AGENTS.md) for the architecture and
[docs/support-matrix.md](docs/support-matrix.md) for what CQL is and isn't supported.

The `seastar-server` artifact ships one public type, `SeaStarProtocolServer`, which serves a
session over Cassandra's native protocol for clients that cannot be pointed at an in-process one.
It speaks protocol v4 and v5 - a driver that was never told which to use negotiates its way to v5,
with the CRC-checked segment framing that introduced - and answers `QUERY`, `PREPARE`, `EXECUTE` and
`BATCH`: rows and their column
metadata, `SET_KEYSPACE` for `USE`, `SCHEMA_CHANGE` for DDL, and the error code that rebuilds the
same driver exception an in-process caller would have caught. The keyspace is tracked per
connection, as on a real node. Paging is not implemented - deliberately, and legally: every answer
is one page, `page_size` is ignored and a paging state in a request is refused.

`REGISTER` is honoured: a DDL statement produces a `SCHEMA_CHANGE` result on the connection that ran
it and a `SCHEMA_CHANGE` event on every connection registered for one, so a second client watching
the same server keeps its metadata current. `TOPOLOGY_CHANGE` and `STATUS_CHANGE` never fire, there
being one node that is up for as long as the server is bound.

It answers the system keyspaces a driver reads on its way in - `system.local`, `system.peers`,
`system.peers_v2`, all of `system_schema` and all of `system_virtual_schema` - so an ordinary
`CqlSession` connects to it with no configuration beyond the contact point and the datacenter, and
builds its own schema metadata. None of them exist in the model: an in-process user who never starts
a server sees no system keyspaces. The datacenter, the rack and the cluster name the listener
reports are settable on its builder.
