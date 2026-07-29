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
It speaks protocol v4 and answers `QUERY`, `PREPARE`, `EXECUTE` and `BATCH`: rows and their column
metadata, `SET_KEYSPACE` for `USE`, `SCHEMA_CHANGE` for DDL, and the error code that rebuilds the
same driver exception an in-process caller would have caught. The keyspace is tracked per
connection, as on a real node. Paging is not implemented - deliberately, and legally: every answer
is one page, `page_size` is ignored and a paging state in a request is refused. The system tables a
driver queries while connecting are not answered yet, so a full `CqlSession` does not open against
it.
