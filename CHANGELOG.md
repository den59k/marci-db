# Changelog

All notable changes to MarciDB are documented here. New entries below this line
are generated from commit history by [`scripts/gen-changelog.sh`](scripts/gen-changelog.sh)
when cutting a release.

## v0.4.0 - 2026-06-16

### Features

- add marcidb-ffi package (6e39939)

## v0.3.0 - 2026-06-15

### Features

- add changelog (845aeed)
- add insert and remove elements feature to fulltext search (a89197f)
- add build feature in Dockerfile and docs (9668da5)
- add fulltext and vector to ts and API (ba0bceb)
- update vector index and add benchmarks (400786e)
- add full text search prototype (b7e9b8a)
- add marci-vector support (13e1716)
- add drop field migration feature (9c05b11)
- add argv for marcidb-server (728e36e)
- complete migration (6c41cf2)
- migration update. Convert comments to english (0af5864)
- update migration (2b36485)
- new migration system protype (329e456)
- refactor todos (9350537)
- remove unwrap from schema parse (dabf657)
- update migration tool (fd00785)
- add  method (a9d9c88)
- update readme (190c5b5)
- update cli (f2d458d)
- add multiserver feature (fdd5ac0)
- complete migration engine protype (f414ede)
- add migation engine protype (0756cd8)
- complete transaction (87945fc)
- add union syntax for enum (82fd761)
- update adaptive indexes and README (f90626d)
- add row caching (3571b3f)
- complete aggregation (3cc605d)
- add aggregation feature (44ce106)
- add cursor feature (1b4e04c)
- add  and (898e4c7)
- update type generators (17df2f6)
- add only_id_required optimizer (783f04e)
- update binaries (4bfe95f)
- update binary (6300d86)
- add starts with feature (00c9d08)

### Bug Fixes

- fix cascade delete (349cca5)
- fix enum migrations (b8363ff)
- fix some bugs (d23614c)
- fix get_max_id method on start (29241bb)
- fix findOne with filter (875d2e1)
- fix package prefix (f78ec86)
- fix where bug (52f9f97)

### Refactoring

- refactor marci_db file (e209e6d)
- move parse snapshot to marcidb-schema (2d229ba)
- extract march parsing from marci_db (eeb32ff)
- extract migration parser from marci_db (086ed3a)

### Documentation

- update readme (537c783)

### Chores

- v0.2.2 (c5f5693)
- v0.1.1 (d547d19)
- v0.1.0 (fe56524)

### Styles

- format updates (285aef3)

### Other Changes

- v0.2.0 (661046f)
- Feat: add transaction prototype (449f3f4)

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
