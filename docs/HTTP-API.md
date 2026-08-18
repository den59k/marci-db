# MarciDB HTTP API

The raw protocol of `marcidb-server`. The [TypeScript client](API.md) is a thin wrapper over these endpoints — request bodies are the same JSON objects as the client's query/insert/update arguments, so the shapes documented in [API.md](API.md) apply here verbatim.

## Server

The server is schema-agnostic and hosts **multiple databases** under one data directory. A database is created on its first migration push — the schema is applied via migrations (see [Migrations](#migrations)), not read from a file.

```bash
docker run -d -p 3000:3000 -v marcidb-data:/app/data ghcr.io/den59k/marcidb-server:latest
# or, from the repo: cargo run -p marcidb-server --release -- --port 3000
```

**Configuration** — each option is a CLI flag or an environment variable; precedence is **flag > env > default**:

| Flag | Env | Default | |
|---|---|---|---|
| `--host <HOST>` | `MARCI_HOST` | `0.0.0.0` | bind address (`localhost` is accepted) |
| `--port <PORT>` | `PORT` | `3000` | listen port |
| `--data <DIR>` | `MARCI_DATA` | `./data` | data directory (`/app/data` in the image) |
| `--token <TOKEN>` | `MARCI_TOKEN` | *(unset)* | when set, every request must carry `Authorization: Bearer <TOKEN>` (else `401`); `GET /$health` stays open |

`--help` / `--version` print and exit. Each database is stored under `<data>/<db>`.

`GET /$health` → `{"ok":true,"version":"0.11.4"}` — liveness probe (docker healthcheck), never authenticated; `version` since 0.11.4 (hosts use it to know `$sync?plan=1` exists).
`DELETE /:db` → drops a database: closes the handle and removes its directory (`404` if absent). Meant for hosts that own many databases (per-tenant / per-environment DBs); there is no undo.

## Endpoints

Every path starts with the **database name**: `/:db/...`. The database is created by its first `$migrate` (or `$sync`); data endpoints on an unknown database return `404`. All endpoints are `POST` with a JSON body (except `$sync`, whose body is the raw schema text). `:model` is the model name exactly as in the schema (case-sensitive: `User`, not `user`).

| Endpoint | Body | Response |
|---|---|---|
| `POST /:db/$migrate` | JSON `[{ id, ops }]` — migration files | `{ "applied": [...ids] }` — replays new migrations, creating the db if absent |
| `POST /:db/$sync` | schema text (`.marci`) | empty — diffs & applies the schema directly, creating the db if absent |
| `POST /:db/$sync?plan=1` | schema text (`.marci`) | `{ ops: [...], destructive }` — dry run: the diff `$sync` would apply, nothing committed, db not created |
| `POST /:db/:model/findMany` | query object (`{}` = id + all scalars) | JSON array |
| `POST /:db/:model/findFirst` | query object | object or `null` |
| `POST /:db/:model/insert` | insert object | id object |
| `POST /:db/:model/update/:id` | update object | empty body |
| `POST /:db/:model/updateMany` | `{ "$where"?: ..., "data": ... }` | bare number — rows matched |
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
# a query object without field keys (only $-clauses, or {}) selects id + every scalar field — at any nesting level
curl -X POST http://localhost:3000/myapp/Post/findMany -H "Content-Type: application/json" -d '{ "$limit": 5 }'

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

A database evolves through **migration files**. Locally, `marcidb generate` diffs `schema.marci` against the replayed history and writes a `NNNN_name.march` file — a list of **self-contained actions** (each carrying its field definition). `POST /:db/$migrate` applies actions to the server.

`$migrate` is a **dumb executor**: its body is migration-action text, and the server lays those actions onto its current state and applies them — no ledger, no deciding what to skip. Choosing *which* actions to send is the **client's** job:

1. `GET /:db/$snapshot` returns the server's current materialized snapshot (`""` if the database doesn't exist yet).
2. `marci-migrate plan` replays the local `.march` files from empty until a step matches the server's snapshot, then prints the unapplied tail.
3. That tail is POSTed to `$migrate`.

```bash
# the CLI wraps all three steps:
npx marcidb migrate push myapp --url http://localhost:3000
```

```
# POST /myapp/$migrate   (Content-Type: text/plain — the planned actions)
add field User.age UInt @slot(12)
add index User.email
# → 200 OK
```

Properties:
- **Reviewable** — each action shows exactly what changes; an accidental drop is visible before you push.
- **Idempotent & ordered — client-side** — `plan` only emits the unapplied tail, so re-pushing sends nothing; if the server's schema isn't a point in your history, `plan` reports drift.
- **Survives restarts** — the materialized schema lives in the database, reconstructed on open.

v1 actions cover creating/dropping entities, adding fields, and adding/dropping indexes; dropping a field, type changes, or removing an enum variant are rejected with `400`.

### Checking for drift — `$snapshot`

`GET /:db/$snapshot` returns the server's current materialized snapshot. `marcidb migrate check` runs the same planner: an empty plan means up-to-date, a non-empty plan means pending migrations, and a planning failure means the server has drifted from your history:

```bash
npx marcidb migrate check myapp --url http://localhost:3000
```

### Bootstrapping from a schema — `$sync`

`POST /:db/$sync` is an **HTTP-only** escape hatch for when you have no migration files (a fresh setup, CI, a bare HTTP client). The body is the **schema text** (the contents of `schema.marci`), not JSON. Instead of replaying migrations, the server diffs its stored schema against the pushed one and applies the difference, creating the database if absent.

```bash
curl -X POST http://localhost:3000/myapp/\$sync --data-binary @schema.marci
```

**Dry run — `?plan=1`.** `POST /:db/$sync?plan=1` with the same body computes the diff and answers it as JSON without committing anything (a missing database is planned against an empty schema and is *not* created):

```json
{ "destructive": true,
  "ops": [ { "op": "dropField", "entity": "Task", "field": "text", "destructive": true, "text": "drop field Task.text" },
           { "op": "createEntity", "entity": "Note", "destructive": false, "text": "create entity Note" },
           { "op": "addIndex", "entity": "Note", "field": "text", "unique": false, "destructive": false, "text": "add index Note.text" } ] }
```

`op` ∈ `createEntity | dropEntity | addField | dropField | alterField | addIndex | dropIndex`; `destructive` marks data loss (`dropEntity`, `dropField`); `text` is the action as a `.march` line. Entities are the *materialized* names (a struct field appears as `Parent.field`). An incompatible change answers the same `400` the real `$sync` would. Use it to show/confirm what a schema push does to a database that holds data.

It is deliberately **not** in the CLI — it doesn't touch the migration ledger, so mixing it with `$migrate` on one database desyncs the bookkeeping, and a schema that omits a model diffs to a `drop model` (data loss). Use it only for databases that aren't managed by migration files. It accepts the same changes as `$migrate` and rejects the same ones with `400`.

## Transactions

`POST /:db/$transaction` applies a list of operations in a **single atomic transaction**: either all of them commit, or none do. The body is a JSON array of operation objects; the response is a JSON array of results — one per operation, in the same order.

Each operation is `{ "model", "action", <payload> }`. The payload field and the result mirror the matching single-op endpoint:

| `action` | payload | result |
|---|---|---|
| `insert` | `data` (insert object) | id object |
| `update` | `id` + `data` (update object) | `null` |
| `updateMany` | `query` (`{ "$where"?: ... }`) + `data` | number — rows matched |
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
| `400 Bad Request` | invalid JSON, unknown field, malformed query/insert/update, bad `:id` syntax, missing aggregate keys, unsupported or diverged migration |
| `404 Not Found` | unknown database, model or action |
| `500 Internal Server Error` | internal failures |

Constraint violations (unique, foreign key, duplicate id) are currently reported as `400` with the error in the message text; a structured error format is planned. For `POST /$transaction` the message is prefixed with the failing operation's index (`batch op #N: ...`) and the whole batch is rolled back — see [Transactions](#atomicity-and-errors).
