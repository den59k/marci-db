# MarciDB

A schema-first NoSQL database written in Rust, with relations, secondary indexes and a fully typed TypeScript client — no ORM layer in between.

```
model User {
    name    String
    posts   Post[]  @bind(Post.author)
}

model Post {
    title   String
    views   UInt    @index
    author  User?
}
```

```ts
const posts = await db.post.findMany({
  title: true,
  author: { name: true },
  $where: { views: { $gt: 100 } },
  $order: { views: "desc" },
  $limit: 20,
})
```

## Features

- **Schema-first**: relations, nested structs and enums with payload fields (including fields shared between variants: `pro | business { ... }`) are part of the schema, not application code
- **Typed TS client**: result types are inferred from the `select` shape, including discriminated unions for enums
- **Secondary indexes** with a query planner: range scans, `$order` by index, keyset pagination (`$cursor`)
- **Aggregations**: `count` / `$sum` / `$avg` / `$min` / `$max`, including aggregates over relations inside a select (`posts: { $count: true }` — counted by index keys, without reading rows)
- **Atomic batch transactions**: `db.$transaction([...])` applies several operations as all-or-nothing, with `ref("0.id")` to feed a generated id into later operations
- **Migrations as files**: `marcidb generate` diffs the schema into a `.march` file (a reviewable action list + a snapshot of the resulting schema); `marcidb migrate push` ships your migrations and the server applies the new ones, tracking an applied ledger — adding fields and indexes without rewriting existing rows
- **JSON fields**: a `Json` column stores any JSON value in a compact binary (JSONB-style) format — no fixed sub-schema — and filters by path: `$where: { meta: { "address.city": "Tokyo", "age": { $gt: 18 } } }`. Reads ride the binary transport like any other field
- **Compact binary row format** with zero-copy field access — filters and aggregates read only the bytes they need
- **Binary read transport**: `findMany`/`findFirst` results come back as a compact binary buffer instead of JSON — automatic in the embedded client, opt-in over HTTP (JSON stays the default for curl). It removes the serialize/parse tax on row-heavy reads: **~5–6× faster** large/nested reads embedded, **~1.4–1.6×** over HTTP (with a smaller payload). See [Benchmarks](docs/BENCHMARKS.md)

## Quick start

1. Create `schema.marci` in your project root.

2. Generate the typed client **and** a migration file from the schema:

```bash
npm install marcidb-client
npx marcidb generate
```

3. Run the server. It is schema-agnostic and hosts multiple databases — the schema is applied via migrations, not read from a file:

```bash
docker run -d -p 3000:3000 -v marcidb-data:/app/data ghcr.io/den59k/marcidb-server:latest
# or, from the repo: cargo run -p marcidb-server --release
```

Vector and full-text search are optional modules, off by default. Build with their cargo features to include them — `cargo +nightly build --release -p marcidb-server --features "vector fulltext"` (vector needs nightly; full-text builds on stable). See [Custom indexes](docs/CUSTOM-INDEXES.md#building-the-server-with-the-modules).

4. Push your schema to a database (created on first push):

```bash
npx marcidb migrate push myapp
```

5. Connect — the database name is part of the URL:

```ts
import { marcidb } from "marcidb-client"
const db = marcidb("http://localhost:3000/myapp")

const posts = await db.post.findMany({ title: true, author: { name: true } })
```

## Docker

The server is published to the GitHub Container Registry as
[`ghcr.io/den59k/marcidb-server`](https://github.com/den59k/marci-db/pkgs/container/marcidb-server):

```bash
docker run -d --name marcidb -p 3000:3000 -v marcidb-data:/app/data ghcr.io/den59k/marcidb-server:latest
```

Two image flavours are published: `:latest` / `:full` include the vector and
full-text modules, while `:core` is the core engine only (smaller, no
optional modules). Version-pinned tags follow the same pattern — `:X.Y.Z-full`,
`:X.Y.Z-core`.

- listens on `0.0.0.0:$PORT` (default `3000`)
- stores databases under `/app/data` — mount a volume to persist them across container recreation
- schema-agnostic and multi-database; a database is created on its first migration push:

```bash
npx marcidb migrate push myapp --url http://localhost:3000
```

No Node toolchain? The `$sync` endpoint applies a `schema.marci` directly over plain HTTP — the server diffs and applies it, creating the database if absent (for databases not managed by migration files):

```bash
curl -X POST http://localhost:3000/myapp/\$sync --data-binary @schema.marci
```

## Documentation

- [TypeScript API](docs/API.md) — schema DSL, queries, updates, aggregations
- [HTTP API](docs/HTTP-API.md) — the raw server protocol
- [Internals](docs/INTERNALS.md) — storage format, query pipeline, optimizations
- [Benchmarks](docs/BENCHMARKS.md)
- [Releasing](docs/RELEASING.md) — how releases, binaries and Docker images are built

## Status

Experimental. The storage format is not stabilized yet — do not use for data you cannot regenerate. Migrations cover adding fields, adding/dropping indexes and creating/dropping models; dropping fields, renames and field reordering are not supported yet.

## License

MIT
