# Changelog

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). SeaStar is pre-1.0, so
any release may still change the public API; from `1.0.0` onward this file is where that gets
called out explicitly.

## [1.0.0-alpha] - Unreleased

Initial alpha release. Public API: `SeaStarCqlSession` (via `.builder()...build()`),
`SeaStarCqlSessionBuilder`, `SeaStarDriverContext`, the `SeaStar{Keyspace,Table,Column,Row,
UserDefinedType,UdtValue}` model interfaces, and `SystemSchema`. Everything else is internal - see
[AGENTS.md](AGENTS.md) for the architecture and [docs/support-matrix.md](docs/support-matrix.md)
for what CQL is and isn't supported.
