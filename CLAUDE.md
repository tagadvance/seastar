See [AGENTS.md](AGENTS.md) for project overview, build/test commands, architecture, and code style. Those instructions apply here in full.

## Claude-specific

- **`add-cql-statement` skill** (`.agents/skills/add-cql-statement/`, surfaced to Claude Code via the `.claude/skills` symlink) — invoke when asked to add support for, handle, or implement a CQL statement type (INSERT, DELETE, DROP, ALTER, CREATE INDEX, …). It covers inspecting the parse tree (`./gradlew :seastar:inspectRaw -Pquery="<CQL>"`), matching real Cassandra failure behavior, writing the `CqlHandler`, and wiring it into `CqlHandlerRegistry`.