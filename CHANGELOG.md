# Changelog

All notable changes to MarciDB are documented here. New entries below this line
are generated from commit history by [`scripts/gen-changelog.sh`](scripts/gen-changelog.sh)
when cutting a release.

## v0.2.2 - 2026-06-15

Baseline entry summarizing the project up to this point.

### Features

- Schema-first model DSL: relations, nested structs, and enums with payload fields (including fields shared between variants)
- Fully typed TypeScript client with result types inferred from the `select` shape, including discriminated unions for enums
- Secondary indexes with a query planner: range scans, `$order` by index, keyset pagination (`$cursor`), unique indexes
- Aggregations: `count` / `$sum` / `$avg` / `$min` / `$max`, including aggregates over relations counted by index keys
- Atomic batch transactions via `db.$transaction([...])` with `ref(...)` to feed a generated id into later operations
- Migrations as reviewable `.march` files: `marcidb generate` diffs the schema, `marcidb migrate push` ships them and the server applies the new ones against an applied ledger
- Optional, opt-in index modules: vector search (`@custom(vector, …)`, nightly toolchain) and full-text search (`@custom(fulltext, …)`, stable)
- Compact binary row format with zero-copy field access
- Standalone HTTP server (`marcidb-server`): schema-agnostic, multi-database, with a `$sync` endpoint that applies a `schema.marci` directly over HTTP

### Build System

- Docker image published as `den59k/marcidb-server` in two flavours: `full` (vector + full-text) and `core` (engine only)
- Release pipeline: `scripts/release.sh` plus a GitHub Actions workflow building Windows `.exe` and Linux binaries (full + core)
