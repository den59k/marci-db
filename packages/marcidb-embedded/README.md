# marcidb-embedded

Run [MarciDB](https://github.com/den59k/marci-db) **in-process** from Bun (primary) and Node.js
(via FFI) — no server, no network. Ideal for fast, ephemeral integration tests.

It pairs with the generated `marcidb-client`: open an embedded database, then hand its `transport`
to the same `marcidb()` client you'd use over HTTP.

## Install

```sh
bun add marcidb-embedded marcidb-client
# or
npm install marcidb-embedded marcidb-client
```

The native library (the full build — vector + full-text indexes included) ships prebuilt per
platform. Override its location with the `MARCIDB_LIB` environment variable if needed.

## Usage

```ts
import { openTestDatabase } from "marcidb-embedded";
import { marcidb } from "./marcidb-client"; // your generated client

const schema = `
  model User {
    name String
    age  Int
  }
`;

// Fresh temp-dir database, fsync off, schema applied, auto-removed on close().
const db = await openTestDatabase(schema);
const client = marcidb(db.transport);

await client.user.insert({ name: "Alice", age: 30 });
const users = await client.user.findMany({ name: true, age: true });

db.close();
```

For a persistent database, use `openDatabase(dir, { disableFsync })` and apply your schema with
`db.$sync(schemaText)` (declarative) or `db.$migrate(migrationText)` (imperative).

## API

- `openDatabase(dir, options?)` → `{ transport, $sync, $migrate, $snapshot, reindexAll, close, closed }`
- `openTestDatabase(schema?, options?)` → as above, plus `path`; backed by a temp dir (fsync off),
  removed on `close()`.
- `transport` — `{ exec(op), batch(ops) }`, the object you pass to `marcidb(...)`.
- Errors surface as `MarciEmbeddedError` with a `.kind` of `bad_request | not_found | internal`.

Data crosses the FFI boundary as JSON. A handle is single-threaded — don't share it across Worker
threads (calls from one runtime's event loop are already serialized).

## Building the native library locally

The library is `marcidb-ffi` in the workspace. The full variant needs the nightly toolchain
(vector SIMD):

```sh
./build-lib.sh win    # or: linux | mac  — stages native/marcidb-<platform>.<ext>
```

Run the tests (Node and Bun) with `npm test` / `npm run test:bun`.
