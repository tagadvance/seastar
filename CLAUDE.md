See [AGENTS.md](AGENTS.md) for project overview, build/test commands, architecture, and code style. Those instructions apply here in full.

## Claude-specific

- **`add-cql-statement` skill** (`.claude/skills/add-cql-statement/`) — invoke when asked to add support for, handle, or implement a CQL statement type (INSERT, DELETE, DROP, ALTER, CREATE INDEX, …). It covers inspecting the parse tree (`./gradlew :lib:inspectRaw -Pquery="<CQL>"`), matching real Cassandra failure behavior, writing the `CqlHandler`, and wiring it into `CqlHandlerRegistry`.