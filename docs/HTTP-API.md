# MarciDB HTTP API

The raw protocol of `marcidb-server`. The [TypeScript client](API.md) is a thin wrapper over these endpoints — request bodies are the same JSON objects as the client's query/insert/update arguments, so the shapes documented in [API.md](API.md) apply here verbatim.

## Server

```bash
cargo run -p marcidb-server --release
```

- reads `schema.marci` from the working directory
- stores data in `./data`
- listens on `http://127.0.0.1:3000`

## Endpoints

All endpoints are `POST` with a JSON body. `:model` is the model name exactly as in the schema (case-sensitive: `User`, not `user`).

| Endpoint | Body | Response |
|---|---|---|
| `POST /:model/findMany` | query object | JSON array |
| `POST /:model/findFirst` | query object | object or `null` |
| `POST /:model/insert` | insert object | id object |
| `POST /:model/update/:id` | update object | empty body |
| `POST /:model/delete/:id` | — | empty body |
| `POST /:model/count` | `{ "$where"?: ... }` (or `{}`) | bare number |
| `POST /:model/aggregate` | aggregate object | object with requested keys |

### Examples

```bash
# query
curl -X POST http://localhost:3000/Post/findMany \
  -H "Content-Type: application/json" \
  -d '{ "title": true, "author": { "name": true }, "$where": { "views": { "$gt": 100 } }, "$limit": 20 }'
# → [ { "title": "...", "author": { "name": "..." } }, ... ]

# insert
curl -X POST http://localhost:3000/User/insert \
  -H "Content-Type: application/json" \
  -d '{ "name": "Alice" }'
# → { "id": 1 }

# update and delete — id in the path
curl -X POST http://localhost:3000/User/update/1 -H "Content-Type: application/json" -d '{ "name": "Alicia" }'
curl -X POST http://localhost:3000/User/delete/1

# count and aggregate
curl -X POST http://localhost:3000/User/count -H "Content-Type: application/json" -d '{}'
# → 42
curl -X POST http://localhost:3000/Post/aggregate \
  -H "Content-Type: application/json" \
  -d '{ "$count": true, "$max": "views" }'
# → { "count": 42, "max": 1337 }
```

## Id encoding in the URL

`update/:id` and `delete/:id` take the primary key in the path:

- single-field id — the plain value: `/User/update/42`, `/User/update/559d7a0c-ec2e-4926-99ad-eb6f4a70c789`
- composite id — `field=value` pairs joined with `&` (url-encoded): `/ChatUser/delete/chat=1&user=2`

In request **bodies** ids are always objects: `{ "id": 42 }`, `{ "chat": { "id": 1 }, "user": { "id": 2 } }` — including `$cursor` and relation connects.

## Aggregates over relations

Aggregate keys inside a relation select work over HTTP exactly as in the client:

```json
{ "name": true, "posts": { "$count": true, "$max": "views" } }
```

returns `"posts": { "count": 3, "max": 25 }` instead of an array.

## Errors

Errors are returned as plain-text messages with a status code:

| Status | When |
|---|---|
| `400 Bad Request` | invalid JSON, unknown field, malformed query/insert/update, bad `:id` syntax, missing aggregate keys |
| `404 Not Found` | unknown model or action |
| `500 Internal Server Error` | internal failures |

Constraint violations (unique, foreign key, duplicate id) are currently reported as `400` with the error in the message text; a structured error format is planned.
