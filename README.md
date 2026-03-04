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

---

## Full API Reference

All endpoints follow the pattern `/{ModelName}/{action}`. On error the server returns a non-200 status with a plain-text message.

---

### `POST /{Model}/insert`

Inserts a new document. Auto-generates `id` if the model uses an auto-increment key.

**Returns:** the assigned `id` as a JSON number.

```json
POST /User/insert
{ "name": "Bob", "surname": "Smith" }
```

```json
POST /Post/insert
{
  "title": "Hello world",
  "createdAt": "2025-11-12T07:02:17.150Z",
  "author": { "id": 1 }
}
```

> DateTime fields accept either an **ISO-8601 string** (`"2025-11-12T07:02:17.150Z"`) or a **Unix timestamp in milliseconds** (integer).

---

### `GET /{Model}/findMany`

Returns **all** documents with every field selected. No request body needed.

```
GET /User/findMany
```

**Response:**
```json
[
  { "id": 1, "name": "Alice", "surname": null },
  { "id": 2, "name": "Bob",   "surname": "Smith" }
]
```

---

### `POST /{Model}/findMany`

Returns documents with explicit field selection and optional filtering via `$where`.

#### Field selection

Pass `true` for each field you want in the response. Omitted fields are excluded.

```json
POST /Post/findMany
{
  "id": true,
  "title": true,
  "author": true
}
```

#### Filtering with `$where`

Add a `$where` key alongside the field-selection object. A plain scalar value is an implicit `$eq`.

```json
POST /User/findMany
{
  "id": true,
  "name": true,
  "$where": { "name": "Alice" }
}
```

**Supported comparison operators:**

| Operator | Meaning               |
|----------|-----------------------|
| `$eq`    | Equal                 |
| `$ne`    | Not equal             |
| `$gt`    | Greater than          |
| `$lt`    | Less than             |
| `$ge`    | Greater than or equal |
| `$le`    | Less than or equal    |

**Range example — posts from 2025:**
```json
POST /Post/findMany
{
  "id": true,
  "title": true,
  "createdAt": true,
  "$where": {
    "createdAt": {
      "$ge": "2025-01-01T00:00:00.000Z",
      "$lt": "2026-01-01T00:00:00.000Z"
    }
  }
}
```

Multiple operators on the same field are ANDed: `{ "$ge": 10, "$le": 99 }` means `10 ≤ value ≤ 99`.

#### Logical operators `$and` / `$or`

Both accept an array of condition objects. Conditions are evaluated with index-aware ordering for efficiency.

```json
POST /User/findMany
{
  "id": true,
  "name": true,
  "$where": {
    "$and": [
      { "name": { "$ne": null } },
      { "id": { "$gt": 5 } }
    ]
  }
}
```

```json
POST /Post/findMany
{
  "id": true,
  "title": true,
  "$where": {
    "$or": [
      { "title": "Breaking news" },
      { "title": "Hello world" }
    ]
  }
}
```

#### Filtering by a relation field (`ModelRef`)

```json
POST /Post/findMany
{
  "id": true,
  "title": true,
  "author": true,
  "$where": { "author": { "$eq": 1 } }
}
```

#### Filtering a list relation (`ModelRefList`) — `$all`

Find records connected to **all** of the listed related ids.

```json
POST /Post/findMany
{
  "id": true,
  "title": true,
  "tags": true,
  "$where": {
    "tags": { "$all": [{ "id": 3 }, { "id": 7 }] }
  }
}
```

Shorthand forms that are equally valid:
```json
"$where": { "tags": [3, 7] }   // array of ids
"$where": { "tags": 3 }        // single id
```

#### Filtering by nested struct fields

```json
POST /Order/findMany
{
  "id": true,
  "address": true,
  "$where": {
    "address": { "city": "Berlin" }
  }
}
```

#### Vector similarity search (`$close`)

For fields annotated with `@vectorIndex` (Cosine or Euclidean), use `$close` to find nearest neighbours.

| Parameter    | Type      | Default | Description                               |
|--------------|-----------|---------|-------------------------------------------|
| `$close`     | `float[]` | —       | Query vector (dimension must match field)  |
| `$take`      | integer   | `10`    | Maximum number of results to return        |
| `$threshold` | float     | `0.0`   | Minimum similarity score to include        |

```json
POST /Document/findMany
{
  "id": true,
  "title": true,
  "$where": {
    "embedding": {
      "$close": [0.1, 0.4, 0.9, 0.2],
      "$take": 5,
      "$threshold": 0.75
    }
  }
}
```

Vector search can be combined with scalar `$where` conditions — scalar filters run first (using indexes), then vector search narrows the candidate set.

---

### `POST /{Model}/update`

Updates an existing document identified by `id`. Only the provided fields are changed; omitted fields stay unchanged.

**Returns:** the updated document's `id`.

```json
POST /User/update
{ "id": 1, "name": "Alicia" }
```

```json
POST /Post/update
{
  "id": 3,
  "title": "Updated title",
  "author": { "id": 2 }
}
```

---

### `POST /{Model}/delete`

Deletes a document by `id` and removes all associated index entries.

**Returns:** the deleted document's `id`.

```json
POST /User/delete
{ "id": 1 }
```

---

### `POST /{Model}/index`

Rebuilds the vector index for every field on the model that carries a `@vectorIndex` attribute. Call this after bulk inserts; the index is rewritten from scratch.

**Returns:** `{ "ok": true }`

```
POST /Document/index
(no body required)
```

---

## CLI (`marcidb_cli.py`)

`marcidb_cli.py` is a Python 3 command-line client that wraps every HTTP endpoint.

### Requirements

```bash
pip install requests
```

### Usage

```
python marcidb_cli.py <command> <Model> [<json-data>]
```

The client connects to `http://127.0.0.1:3000` by default.

### Commands

| Command    | HTTP                   | Endpoint            | Body required |
|------------|------------------------|---------------------|---------------|
| `insert`   | POST                   | `/{Model}/insert`   | Yes           |
| `findmany` | GET (no body) / POST   | `/{Model}/findMany` | Optional      |
| `update`   | POST                   | `/{Model}/update`   | Yes           |
| `delete`   | POST                   | `/{Model}/delete`   | Yes           |
| `index`    | POST                   | `/{Model}/index`    | No            |

The command name is **case-insensitive**: `findMany`, `findmany`, `FINDMANY` all work.

### Examples

```bash
# Insert
python marcidb_cli.py insert User "{'name': 'Alice', 'surname': null}"
python marcidb_cli.py insert Post "{'title': 'Hello', 'createdAt': '2025-11-12T07:02:17.150Z', 'author': {'id': 1}}"

# Find all (no filter)
python marcidb_cli.py findmany User
python marcidb_cli.py findmany Post

# Find with field selection
python marcidb_cli.py findmany Post "{'id': true, 'title': true, 'author': true}"

# Find with $where — exact match
python marcidb_cli.py findmany User "{'id': true, 'name': true, '$where': {'name': 'Alice'}}"

# Find with $where — range
python marcidb_cli.py findmany Post "{'id': true, '$where': {'createdAt': {'$ge': '2025-01-01T00:00:00.000Z'}}}"

# Find with $and
python marcidb_cli.py findmany User "{'id': true, '$where': {'$and': [{'id': {'$gt': 1}}, {'name': {'$ne': null}}]}}"

# Find with $or
python marcidb_cli.py findmany Post "{'id': true, 'title': true, '$where': {'$or': [{'title': 'Hello'}, {'title': 'World'}]}}"

# Find by relation
python marcidb_cli.py findmany Post "{'id': true, 'title': true, 'author': true, '$where': {'author': {'$eq': 1}}}"

# Find by many-to-many $all
python marcidb_cli.py findmany Post "{'id': true, 'tags': true, '$where': {'tags': {'$all': [3, 7]}}}"

# Vector similarity search
python marcidb_cli.py findmany Document "{'id': true, 'title': true, '$where': {'embedding': {'$close': [0.1, 0.4, 0.9, 0.2], '$take': 5}}}"

# Update
python marcidb_cli.py update User "{'id': 1, 'name': 'Alicia'}"
python marcidb_cli.py update Post "{'id': 3, 'title': 'New title'}"

# Delete
python marcidb_cli.py delete User "{'id': 1}"
python marcidb_cli.py delete Post "{'id': 3}"

# Rebuild vector index
python marcidb_cli.py index Document
```

### JSON quoting tips


The script also accepts single-quoted JSON (e.g. `{'name': 'Alice'}`) as a convenience, as long as there are no mixed quote styles inside values.

---

## Data & Indexing Model (overview)

* **Direct index**: `<A_id><B_id>` for a relation A → B.
* **Reverse index**: `<B_id><A_id>` for efficient traversal the other way.
* **Derived fields**: computed from the opposite side's index; no duplication in documents.
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
