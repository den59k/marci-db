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

Every path starts with the **database name**: `/:db/...`. The database is created by its first `$migrate` (or `$sync`); data endpoints on an unknown database return `404`. All endpoints are `POST` with a JSON body (except `$sync`, whose body is the raw schema text, the [journal](#journals) routes, and the [backup](#backups) routes). `:model` is the model name exactly as in the schema (case-sensitive: `User`, not `user`).

| Endpoint | Body | Response |
|---|---|---|
| `POST /:db/$migrate` | JSON `[{ id, ops }]` — migration files | `{ "applied": [...ids] }` — replays new migrations, creating the db if absent |
| `POST /:db/$sync` | schema text (`.marci`) | empty — diffs & applies the schema directly, creating the db if absent |
| `POST /:db/$sync?plan=1` | schema text (`.marci`) | `{ ops: [...], destructive }` — dry run: the diff `$sync` would apply, nothing committed, db not created |
| `POST /:db/:model/findMany` | query object (`{}` = id + all scalars) | JSON array |
| `POST /:db/:model/findFirst` | query object | object or `null` |
| `POST /:db/:model/insert` | insert object | id object |
| `POST /:db/:model/update/:id` | update object | empty body (`404` when the row does not exist) |
| `POST /:db/:model/upsert` | `{ "key": ..., "data": ..., "onInsert"?: ... }` | id object — the row `key` names (the id, or the fields of one unique index) is updated with `data`, or inserted as key + data (+ `onInsert`) — see [Upsert](API.md#upsert) |
| `POST /:db/:model/updateMany` | `{ "$where"?: ..., "data": ... }` | bare number — rows matched |
| `POST /:db/:model/deleteMany` | `{ "$where"?: ... }` | bare number — rows deleted (cascades apply; a `Restrict` on any row fails the whole call) |
| `POST /:db/:model/delete/:id` | — | empty body (`404` when the row does not exist) |
| `POST /:db/:model/count` | `{ "$where"?: ... }` (or `{}`) | bare number |
| `POST /:db/:model/aggregate` | aggregate object | object with requested keys |
| `POST /:db/$transaction` | array of operations | array of results — see [Transactions](#transactions) |
| `POST /:db/:model/$journal/:name` | `{ "on": "delete" }` | `{ "created": bool }` — see [Journals](#journals) |
| `GET /:db/:model/$journal/:name` | — (`?after=&limit=&wait=`) | JSON array of entries |
| `DELETE /:db/:model/$journal/:name` | — | `{ "dropped": bool }` |
| `GET /:db/$backup` | — | the database as a `.marcibin` file (`application/x-marcidb-backup`) — see [Backups](#backups) |
| `POST /:db/$restore` | a `.marcibin` file (`?into=<name>` restores under another name) | `{ "restored", "bytes", "trees", "entries", "schemaVersion", "createdAt" }` — replaces the database |

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
| `upsert` | `key` + `data` (+ `onInsert`) | id object |
| `updateMany` | `query` (`{ "$where"?: ... }`) + `data` | number — rows matched |
| `deleteMany` | `query` (`{ "$where"?: ... }`) | number — rows deleted |
| `delete` | `id` | `null` |
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
batch op #1: User.email: the value is already taken (by the row with id 1)
```

The status and `kind` are the failing operation's (a constraint is `409`, a missing row `404`, a bad payload or `$ref` `400`); a storage failure on commit is `500`. Auto-increment counters are **not** rolled back (gaps are possible, as with SQL sequences).

## Journals

A journal is a named log of a model's deleted rows, written in the deleting transaction — cascades included (see the [TypeScript API](API.md#journals) for the semantics).

```bash
# create (a second call with the same "on" changes nothing; another "on" is a 400)
curl -X POST localhost:3000/myapp/File/\$journal/stored-files -d '{ "on": "delete" }'
# → { "created": true }

# read: the oldest entries kept
curl 'localhost:3000/myapp/File/$journal/stored-files?limit=100'
# → [ { "seq": 41, "op": "delete", "row": { "id": 7, "blob": "a1", "size": 1024 } }, ... ]

# confirm everything up to seq 42 — those entries are dropped — and get the next ones;
# hold the request for up to 30 s when there are none
curl 'localhost:3000/myapp/File/$journal/stored-files?after=42&wait=30'

# drop the journal
curl -X DELETE localhost:3000/myapp/File/\$journal/stored-files
```

- `name` is 1–64 characters of `A-Z a-z 0-9 _ -`.
- `after` is the only way an entry leaves a journal. A reader that failed repeats its request with the same `after` and gets the same entries; confirming twice is harmless.
- `limit` defaults to 100 (at most 1000), `wait` is in seconds (at most 60). A waiting request holds no lock on the database.
- Reading or confirming a journal that does not exist is a `404`.

## Backups

`GET /:db/$backup` streams a consistent copy of the whole database; `POST /:db/$restore` puts one back.
From the command line, `marcidb backup` / `marcidb restore` wrap both (they send `--token` / `MARCI_TOKEN`
as the bearer token, like every `marcidb` command):

```bash
npx marcidb backup myapp --url http://localhost:3000 --out backups/
# → Backed up 'myapp' to backups/myapp-20261012-030000.marcibin (48213 bytes)

npx marcidb restore myapp backups/myapp-20261012-030000.marcibin --into myapp_check   # a copy beside it
npx marcidb restore myapp backups/myapp-20261012-030000.marcibin --yes                # replace myapp

npx marcidb inspect backups/myapp-20261012-030000.marcibin        # header, schema, trees — offline
```

Over plain HTTP:

```bash
curl -fsS -H "Authorization: Bearer $MARCI_TOKEN" -o myapp.marcibin 'http://localhost:3000/myapp/$backup'
curl -fsS -H "Authorization: Bearer $MARCI_TOKEN" --data-binary @myapp.marcibin 'http://localhost:3000/myapp/$restore'
# → { "restored": "myapp", "bytes": 48213, "trees": 14, "entries": 1532, "schemaVersion": 7, "createdAt": "2026-10-12T03:00:00Z" }
```

**What is in a backup.** Everything the database stores, byte for byte: the rows, every index (including
`@unique` and `@custom` ones), the relation trees, the journals with their unconfirmed entries, and the schema
with its version. Ids continue where they stopped after a restore. The file is not compressed (`zstd` it if
you need to); it is checksummed per tree and as a whole, so a damaged or truncated file is refused instead
of half-restored. The response carries `Content-Disposition: attachment; filename="<db>-<YYYYMMDD-HHMMSS>.marcibin"`
with the time of the snapshot.

**Consistency.** The backup is one snapshot, taken when the request arrives: writes committed after that
point are not in it, however long the download takes.

**What waits.** Nothing: reads, writes and migrations of the same database all go on while a backup runs —
the backup keeps seeing the snapshot it started from. A `$restore` of that database waits for the backup
to finish (it counts as an in-flight request). During a long backup the data file can grow: pages freed by
concurrent writes are kept for the snapshot and reused once it ends.

**Restore** replaces the database in place. The upload is stored next to the databases and restored into a
staging directory while the live database keeps serving — plan for free disk space of about twice the
backup. Then the swap: requests to that database get `503` (`kind: "unavailable"`) for its duration;
requests already running finish first (up to 30 s — otherwise the restore is abandoned with a `503` and
nothing changes). A malformed or damaged file is a `400` and changes nothing. `?into=<name>` restores under
another name instead — created if absent, replaced if it exists. The restored database is at the schema
version of the backup; push your migrations afterwards (`marcidb migrate push`) to bring it forward.
A second restore of the same name while one runs is a `503`. `DELETE /:db` is unaffected.

**Scheduling.** The server has no scheduler of its own — run the CLI from cron, a systemd timer or a
Kubernetes CronJob:

```bash
# crontab -e — 03:00 every night, keep 14 days
MARCI_TOKEN=change-me
0 3 * * * npx marcidb backup myapp --url http://localhost:3000 --out /var/backups/marcidb/ && find /var/backups/marcidb -name '*.marcibin' -mtime +14 -delete
```

`marcidb backup` writes to `<file>.tmp` and renames it only when the download is complete; a server-side
failure mid-copy aborts the response, so a broken download never looks like a finished file.

A database directory that no process has open can also be backed up, restored and inspected locally,
without a server: `marci-migrate backup <db-dir> <out>`, `marci-migrate restore <db-dir> <file> [--overwrite]`,
`marci-migrate inspect <file | db-dir>` (`marcidb inspect` runs the latter).

## Errors

An error answers with a JSON body and the status its kind means:

```json
{ "error": "Order.dish: no row with id \"nothing\" to point at", "kind": "foreign_key", "field": "Order.dish" }
```

| `kind` | Status | When |
|---|---|---|
| `unique` | `409` | a `@unique` value is taken (`field`; the message names the row holding it) |
| `foreign_key` | `409` | a relation points at an id with no row behind it (`field`) |
| `restrict` | `409` | the delete is refused by a `Restrict` relation (`field` = the relation pointing at the row) |
| `duplicate_id` | `409` | a row with the given id exists |
| `not_found` | `404` | unknown database, model or action; the row to update or delete does not exist |
| `bad_request` | `400` | invalid JSON, unknown or missing field, malformed query/insert/update, bad `:id` syntax, missing aggregate keys, unsupported or diverged migration |
| `unauthorized` | `401` | the token is missing or wrong |
| `unavailable` | `503` | the database is being restored, or stayed busy past the restore's wait — retry |
| `internal` | `500` | a fault of the server |

`kind` is what a client switches on; `error` is a message for a person; `field` is present when the failure is about
one field (`Model.field`). For `POST /$transaction` the message is prefixed with the failing operation's index
(`batch op #N: ...`) and the whole batch is rolled back — see [Transactions](#atomicity-and-errors).
