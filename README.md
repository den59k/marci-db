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
- **Declarative migrations**: edit the schema, `marcidb generate` diffs it into a migration file; the server applies it on push — adding fields and indexes without rewriting existing rows
- **Compact binary row format** with zero-copy field access — filters and aggregates read only the bytes they need

## Quick start

1. Create `schema.marci` in your project root.

2. Generate the typed client **and** a migration file from the schema:

```bash
npm install marcidb-client
npx marcidb generate
```

3. Run the server. It is schema-agnostic and hosts multiple databases — the schema is applied via migrations, not read from a file:

```bash
docker run -d -p 3000:3000 -v marcidb-data:/app/data den59k/marcidb-server:latest
# or, from the repo: cargo run -p marcidb-server --release
```

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

The server is published as [`den59k/marcidb-server`](https://hub.docker.com/r/den59k/marcidb-server):

```bash
docker run -d --name marcidb -p 3000:3000 -v marcidb-data:/app/data den59k/marcidb-server:latest
```

- listens on `0.0.0.0:$PORT` (default `3000`)
- stores databases under `/app/data` — mount a volume to persist them across container recreation
- schema-agnostic and multi-database; a database is created on its first migration push:

```bash
npx marcidb migrate push myapp --url http://localhost:3000
# equivalently, over raw HTTP:
curl -X POST http://localhost:3000/myapp/\$migrate --data-binary @schema.marci
```

No Node toolchain at all? `$init` applies a schema directly from plain text — a one-shot bootstrap that **wipes any existing data** and recreates the database to match (accepts any schema, no migration files):

```bash
curl -X POST http://localhost:3000/myapp/\$init --data-binary @schema.marci
```

## Documentation

- [TypeScript API](docs/API.md) — schema DSL, queries, updates, aggregations
- [HTTP API](docs/HTTP-API.md) — the raw server protocol
- [Internals](docs/INTERNALS.md) — storage format, query pipeline, optimizations
- [Benchmarks](docs/BENCHMARKS.md)

## Status

Experimental. The storage format is not stabilized yet — do not use for data you cannot regenerate. Migrations cover adding fields, adding/dropping indexes and creating/dropping models; dropping fields, renames and field reordering are not supported yet.

## License

MIT
