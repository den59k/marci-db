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
// тип результата выведен из запроса — без кодогенерации моделей вручную
```

## Features

- **Schema-first**: relations, nested structs and enums with payload fields (including fields shared between variants: `pro | business { ... }`) are part of the schema, not application code
- **Typed TS client**: result types are inferred from the `select` shape, including discriminated unions for enums
- **Secondary indexes** with a query planner: range scans, `$order` by index, keyset pagination (`$cursor`)
- **Aggregations**: `count` / `$sum` / `$avg` / `$min` / `$max`, including aggregates over relations inside a select (`posts: { $count: true }` — counted by index keys, without reading rows)
- **Atomic batch transactions**: `db.$transaction([...])` applies several operations as all-or-nothing, with `ref("0.id")` to feed a generated id into later operations
- **Compact binary row format** with zero-copy field access — filters and aggregates read only the bytes they need

## Quick start

1. Create `schema.marci` in your project root
2. Generate the typed client:

```bash
npm install marcidb-client
npx marcidb generate
```

3. Start the server (reads `schema.marci`, stores data in `./data`, listens on `127.0.0.1:3000`):

```bash
cargo run -p marcidb-server --release
```

4. Connect:

```ts
import { marcidb } from "marcidb-client"
const db = marcidb("http://localhost:3000")
```

## Documentation

- [TypeScript API](docs/API.md) — schema DSL, queries, updates, aggregations
- [HTTP API](docs/HTTP-API.md) — the raw server protocol
- [Internals](docs/INTERNALS.md) — storage format, query pipeline, optimizations
- [Benchmarks](docs/BENCHMARKS.md)

## Status

Experimental. The storage format is not stabilized yet and there is no schema migration mechanism — do not use for data you cannot regenerate.

## License

MIT
