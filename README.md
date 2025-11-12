# MarciDB

MarciDB is an experimental NoSQL database written in Rust. It can run as a lightweight **server** or be **embedded** directly in your app. Data is stored on top of CanopyDB (B-trees), with a schema-first model inspired by Prisma and efficient binary on-disk layout.

## Why MarciDB

* **Two modes:** run as a server or embed as a library.
* **Schema-first:** clear relations, derived fields, and ordered collections.
* **Fast storage:** CanopyDB backend with prefix scans and composite keys.
* **Simple API:** JSON for server requests today; binary format planned for embedded.

## Features

* Models, structs, one-to-many and many-to-many relations
* Automatic direct/reverse indexes for relations
* Derived fields (virtual, no duplication)
* Ordered lists via sorted keys (`@sorted`) or append-only lists
* Transactions and prefix/range queries through CanopyDB

## Modes

### Server mode

* Start with: `cargo run`
* Default port: `http://localhost:3000`
* No separate config yet (data directory defaults to `./data`)

### Embedded mode

* Link the library directly (FFI/WASM planned).
* JSON remains for testing; a compact binary format will be used for production embeddings.

## Quick start (Server)

### Insert a user

**POST** `http://localhost:3000/User/insert`

```json
{
  "name": "Alice",
  "surname": null
}
```

### Insert a post (with foreign key)

**POST** `http://localhost:3000/Post/insert`

```json
{
  "title": "Post first",
  "createdAt": "2025-11-12T07:02:17.150Z",
  "author": { "id": 1 }
}
```

### Find many posts

**POST** `http://localhost:3000/Post/findMany`

```json
{
  "id": true,
  "title": true,
  "author": true,
  "images": true
}
```

**Response**

```json
[
  {
    "id": 1,
    "author": {
      "id": 1,
      "name": "Alice",
      "surname": null
    },
    "images": [],
    "title": "First post"
  }
]
```

> Notes
> • Endpoints use JSON bodies.
> • Relations are resolved from indexes; derived fields are virtual.

## Data & Indexing Model (overview)

* **Direct index**: `<A_id><B_id>` for a relation A → B.
* **Reverse index**: `<B_id><A_id>` for efficient traversal the other way.
* **Derived fields**: computed from the opposite side’s index; no duplication in documents.
* **Ordered lists**: keys may encode order for automatic sorted iteration.

## Status

Alpha. Interfaces may change; expect breaking changes while we iterate.

## Roadmap

* Embedded binary wire format for TS/FFI
* Query operators and filters for `findMany`
* Sorted lists (`@sorted`) and append-only lists
* Migrations and schema versioning
* CLI and documentation site

## License

MIT
