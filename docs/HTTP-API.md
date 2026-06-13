# MarciDB HTTP API

The raw protocol of `marcidb-server`. The [TypeScript client](API.md) is a thin wrapper over these endpoints — request bodies are the same JSON objects as the client's query/insert/update arguments, so the shapes documented in [API.md](API.md) apply here verbatim.

## Server

The server is schema-agnostic and hosts **multiple databases** under one data directory. A database is created on its first migration push — the schema is applied via migrations (see [Migrations](#migrations)), not read from a file.

```bash
docker run -d -p 3000:3000 -v marcidb-data:/app/data den59k/marcidb-server:latest
# or, from the repo: cargo run -p marcidb-server --release
```

- listens on `0.0.0.0:$PORT` (default `3000`)
- stores each database under `<data>/<db>` (`./data` locally, `/app/data` in the image)

## Endpoints

Every path starts with the **database name**: `/:db/...`. The database is created by its first `$migrate`; data endpoints on an unknown database return `404`. All endpoints are `POST` with a JSON body (except `$migrate`, whose body is the schema text). `:model` is the model name exactly as in the schema (case-sensitive: `User`, not `user`).

| Endpoint | Body | Response |
|---|---|---|
| `POST /:db/$migrate` | schema text (`.marci`) | empty — applies the schema, creating the db if absent |
| `POST /:db/:model/findMany` | query object | JSON array |
| `POST /:db/:model/findFirst` | query object | object or `null` |
| `POST /:db/:model/insert` | insert object | id object |
| `POST /:db/:model/update/:id` | update object | empty body |
| `POST /:db/:model/delete/:id` | — | empty body |
| `POST /:db/:model/count` | `{ "$where"?: ... }` (or `{}`) | bare number |
| `POST /:db/:model/aggregate` | aggregate object | object with requested keys |
| `POST /:db/$transaction` | array of operations | array of results — see [Transactions](#transactions) |

### Examples

```bash
# query (database "myapp")
curl -X POST http://localhost:3000/myapp/Post/findMany \
  -H "Content-Type: application/json" \
  -d '{ "title": true, "author": { "name": true }, "$where": { "views": { "$gt": 100 } }, "$limit": 20 }'
# → [ { "title": "...", "author": { "name": "..." } }, ... ]

# insert
curl -X POST http://localhost:3000/myapp/User/insert \
  -H "Content-Type: application/json" \
  -d '{ "name": "Alice" }'
# → { "id": 1 }

# update and delete — id in the path
curl -X POST http://localhost:3000/myapp/User/update/1 -H "Content-Type: application/json" -d '{ "name": "Alicia" }'
curl -X POST http://localhost:3000/myapp/User/delete/1

# count and aggregate
curl -X POST http://localhost:3000/myapp/User/count -H "Content-Type: application/json" -d '{}'
# → 42
curl -X POST http://localhost:3000/myapp/Post/aggregate \
  -H "Content-Type: application/json" \
  -d '{ "$count": true, "$max": "views" }'
# → { "count": 42, "max": 1337 }
```

## Id encoding in the URL

`update/:id` and `delete/:id` take the primary key in the path:

- single-field id — the plain value: `/myapp/User/update/42`, `/myapp/User/update/559d7a0c-ec2e-4926-99ad-eb6f4a70c789`
- composite id — `field=value` pairs joined with `&` (url-encoded): `/myapp/ChatUser/delete/chat=1&user=2`

In request **bodies** ids are always objects: `{ "id": 42 }`, `{ "chat": { "id": 1 }, "user": { "id": 2 } }` — including `$cursor` and relation connects.

## Aggregates over relations

Aggregate keys inside a relation select work over HTTP exactly as in the client:

```json
{ "name": true, "posts": { "$count": true, "$max": "views" } }
```

returns `"posts": { "count": 3, "max": 25 }` instead of an array.

## Migrations

`POST /:db/$migrate` applies a schema to the database. The body is the **schema text** (the contents of `schema.marci`), not JSON. The server diffs it against the database's current schema and applies the change atomically; if the database does not exist, it is created.

```bash
curl -X POST http://localhost:3000/myapp/\$migrate --data-binary @schema.marci
```

The server reconstructs each database's schema from its own storage on open, so migrations survive restarts. v1 supports adding fields, adding/dropping indexes and creating/dropping models; dropping a field, renaming, or reordering fields is rejected with `400`. Usually you push via the CLI rather than raw curl:

```bash
npx marcidb migrate push myapp --url http://localhost:3000
```

## Transactions

`POST /:db/$transaction` applies a list of operations in a **single atomic transaction**: either all of them commit, or none do. The body is a JSON array of operation objects; the response is a JSON array of results — one per operation, in the same order.

Each operation is `{ "model", "action", <payload> }`. The payload field and the result mirror the matching single-op endpoint:

| `action` | payload | result |
|---|---|---|
| `insert` | `data` (insert object) | id object |
| `update` | `id` + `data` (update object) | `null` |
| `delete` | `id` | `true` / `false` |
| `findFirst` | `query` | object or `null` |
| `findMany` | `query` | array |
| `count` | `query` (`{ "$where"?: ... }`, or `{}`) | number |
| `aggregate` | `query` (aggregate object) | object |

Two differences from the single-op routes: `id` is passed in the **body** as an id object (`{ "id": 42 }`), not in the URL; and reads see the writes of earlier operations in the same batch (read-your-writes).

### References between operations

`{ "$ref": "<i>.<path>" }` is replaced, before the operation runs, with a value from the result of operation `i` — usually a generated id. `"0.id"` is the `id` field of operation `0`'s result; `"0"` is the whole result, `"1.author.id"` a nested path.

```bash
curl -X POST 'http://localhost:3000/myapp/$transaction' \
  -H "Content-Type: application/json" \
  -d '[
    { "model": "User", "action": "insert", "data": { "name": "Alice" } },
    { "model": "Post", "action": "insert",
      "data": { "title": "Hi", "author": { "id": { "$ref": "0.id" } } } }
  ]'
# → [ { "id": 1 }, { "id": 1 } ]   (User #1 and Post #1 — each model has its own id sequence)
```

### Atomicity and errors

On the first failing operation the whole transaction is rolled back — including operations that already succeeded — and the message names the failing operation by index:

```
batch op #1: Insert(UniqueViolation("User.email", ...))
```

Parse, constraint and `$ref` errors are `400`; a storage failure on commit is `500`. Auto-increment counters are **not** rolled back (gaps are possible, as with SQL sequences).

## Errors

Errors are returned as plain-text messages with a status code:

| Status | When |
|---|---|
| `400 Bad Request` | invalid JSON, unknown field, malformed query/insert/update, bad `:id` syntax, missing aggregate keys, unsupported migration |
| `404 Not Found` | unknown database, model or action |
| `500 Internal Server Error` | internal failures |

Constraint violations (unique, foreign key, duplicate id) are currently reported as `400` with the error in the message text; a structured error format is planned. For `POST /$transaction` the message is prefixed with the failing operation's index (`batch op #N: ...`) and the whole batch is rolled back — see [Transactions](#atomicity-and-errors).
