# marcidb-embedded

Run [MarciDB](https://github.com/den59k/marci-db) **in-process** from Bun (primary) and Node.js
(via FFI) — no server, no network. Ideal for fast, ephemeral integration tests, and usable for
small single-process production apps.

It pairs with the generated `marcidb-client`: open an embedded database and hand it straight to the
same `marcidb()` client you'd use over HTTP — `marcidb(db)`.

## Install

```sh
bun add marcidb-embedded marcidb-client
# or
npm install marcidb-embedded marcidb-client
```

The native library (the full build — vector + full-text indexes included) ships prebuilt for
linux-x64, macOS arm64, and win32-x64. On other platforms, build it yourself (see the end) and
point `MARCIDB_LIB` at the result.

## Generating the typed client

The typed `marcidb()` client is generated from your `.marci` schema. The default flow generates into
`node_modules` and you import it from `marcidb-client` (works on both Node and Bun):

```sh
npx marcidb generate schema.marci
```

```ts
import { marcidb } from "marcidb-client";
```

> If you prefer to keep the generated client in your repo, generate it into your project and import it
> by path instead — also works on both runtimes:
> ```sh
> npx marcidb generate schema.marci src/db   # writes src/db/{index.js,index.d.ts}
> ```
> ```ts
> import { marcidb } from "./db/index.js";
> ```

---

## Example 1 — test / development database (`$sync`)

For tests, use `openTestDatabase`: a fresh temp-dir database with fsync off, removed on `close()`.
`$sync` applies your `.marci` schema declaratively (idempotent — safe to call every run).

```ts
import { openTestDatabase } from "marcidb-embedded";
import { marcidb } from "./db/index.js";

const schema = `
  model User {
    name  String
    email String?
    age   Int
  }
`;

const db = await openTestDatabase(schema); // temp dir + fsync off + $sync(schema)
const client = marcidb(db);

await client.user.insert({ name: "Alice", email: "alice@example.com", age: 30 });
const users = await client.user.select({ name: true, age: true });
console.log(users); // [{ name: "Alice", age: 30 }]

db.close(); // also removes the temp directory
```

A typical test helper:

```ts
import { beforeEach, afterEach } from "vitest"; // or your runner of choice

let db, client;
beforeEach(async () => {
  db = await openTestDatabase(schema);
  client = marcidb(db);
});
afterEach(() => db.close());
```

`$sync` is **declarative**: it diffs your schema against the DB and applies the difference. Great
for tests, but note a model you remove from the schema is dropped (with its data) — for production,
use migrations.

---

## Example 2 — production database with migrations

For production, author versioned `.march` migrations and apply the unapplied ones on startup with
**`db.migrate(dir)`** — a built-in, idempotent, drift-aware migrator (the embedded equivalent of
`marci-migrate` over HTTP).

**Author migrations** whenever the schema changes:

```sh
npx marcidb generate schema.marci src/db --name init        # → migrations/0000_init.march
# …edit schema.marci…
npx marcidb generate schema.marci src/db --name add_email   # → migrations/0001_add_email.march
```

`marcidb generate` regenerates the typed client *and* writes the next numbered migration.

**Apply migrations on startup** — one call, safe to run every time:

```ts
import { openDatabase } from "marcidb-embedded";
import { marcidb } from "./db/index.js";

const db = openDatabase("./data");

const { applied, total } = await db.migrate("./migrations");
console.log(`migrations: ${applied} applied, ${total} total`);

const client = marcidb(db);
await client.user.insert({ name: "Bob", email: "bob@example.com", age: 40 });

db.close();
```

`db.migrate(dir)` reads the current schema, finds where the DB sits in the migration history, and
applies only the migrations after that point:

- a **fresh** DB applies the whole history;
- an **up-to-date** DB applies nothing (returns `{ applied: 0 }`);
- a **newly-added** migration applies incrementally;
- if the DB matches **no point** in the history (e.g. it was changed outside these migrations, or a
  migration file went missing), it **throws** a `MarciEmbeddedError` (`kind: "bad_request"`) rather
  than guessing — surfacing the drift instead of corrupting state.

This makes it safe to call on every startup. Migrations are expected to be append-only and never
reordered (the normal workflow).

---

## Backups

`db.backup(path)` writes a consistent backup of the whole database to a `.marcibin` file — rows, indexes,
relations, journals and the schema, byte for byte:

```ts
const info = await db.backup(`./backups/app-${Date.now()}.marcibin`);
// { bytes: 48213, trees: 14, entries: 1532, schemaVersion: 7, createdAt: "2026-10-12T03:00:00Z" }
```

- **The snapshot point is fixed when you call it**: writes made after `backup()` returns its promise are not
  in the file.
- **The copy runs on a native thread**, not on the JS thread: the app keeps reading and writing while it
  runs, and the promise settles when the file is complete. The file is written under a temporary name and
  renamed into place only then — a crash mid-way never leaves a file that looks finished.
- **Nothing waits for it**: reads, writes and schema changes (`$sync`, `migrate()`) all proceed while a
  backup runs; the backup keeps seeing its own snapshot.
- `close()` waits for running backups; their promises still settle with the real result.

There is no built-in scheduler — use a timer:

```ts
const DAY = 24 * 60 * 60 * 1000;
setInterval(() => {
  db.backup(`./backups/app-${new Date().toISOString().replace(/[:.]/g, "-")}.marcibin`)
    .catch((e) => console.error("backup failed", e));
}, DAY);
```

**Restore at startup**, in place of `openDatabase`, then bring the schema forward:

```ts
import { openDatabase, restoreDatabase } from "marcidb-embedded";

// e.g. RESTORE_FROM=./backups/app-2026-10-12.marcibin for the one start that should restore
const db = process.env.RESTORE_FROM
  ? restoreDatabase("./data", process.env.RESTORE_FROM, { overwrite: true })
  : openDatabase("./data");
await db.migrate("./migrations"); // a restored database is at the backup's schema version
```

`restoreDatabase(dir, file, options?)` restores into `dir` and returns the opened database. Without
`overwrite: true` it refuses a `dir` that already holds a database; with it, the old database is
replaced — but never one that is open (by this process or another): close it first. A damaged or
truncated file is refused and leaves `dir` untouched. The restore is synchronous (it runs at startup).

A backup file can be read without the app: `npx marcidb inspect file.marcibin` shows the schema and the
trees, `--tree User` the rows.

---

## Maintenance port

An embedded database lives inside your app's process, and its directory is locked for as long as the app
has it open: no other process can open it. So a cron backup, a maintenance script or a `marcidb` command
cannot reach a running app's data — unless the app lets them in. `db.listen()` does that: the app serves
**the same HTTP API as `marcidb-server`**, for this one database, on a local port.

```ts
const db = openDatabase("./data");
await db.migrate("./migrations");

const port = db.listen({ port: 3999, token: process.env.MARCI_TOKEN });
console.log(port.url); // http://127.0.0.1:3999/data
```

`listen({ port, host?, token?, name? })` returns `{ port, host, url, close() }`:

- `port` — `0` picks a free one (read it back from `port`).
- `host` — `127.0.0.1` by default. Any other (`0.0.0.0`, a LAN address) **requires a `token`**.
- `token` — every request must carry `Authorization: Bearer <token>`; `GET /$health` stays open.
- `name` — the database name in the URL (`/<name>/…`); defaults to the directory's basename
  (`openDatabase("./data")` → `data`). The CLI and the HTTP client need it, so it is in `url`.

It is synchronous (the port is bound when it returns), one port per database. `port.close()` stops it;
`db.close()` does too. Both block the JS thread while in-flight requests finish — at most 5 s, then the
rest are aborted; idle connections close at once. Requests are served on native threads beside the app's
own calls, over the same database: what a script writes through the port, the app reads at once.

From outside the process, everything works as against a server:

```sh
# crontab: a nightly backup of the running app
0 3 * * * npx marcidb backup data --url http://127.0.0.1:3999 --token "$MARCI_TOKEN" --out /var/backups/app/
npx marcidb migrate check data --url http://127.0.0.1:3999 --token "$MARCI_TOKEN"
```

```ts
// scripts/fix-names.ts — a one-off maintenance script against the live app
import { marcidb } from "../src/db/index.js";

const db = marcidb("http://127.0.0.1:3999/data", { token: process.env.MARCI_TOKEN });
const fixed = await db.user.where({ name: "" }).updateMany({ name: "anonymous" });
console.log(`${fixed} users renamed`);
```

The typed client sends `token` on every request; leave it out for a port opened without one.

**What the port refuses**, with a `400` that says why: `$restore` (in place or `?into=`) and
`DELETE /<name>` — the app process owns the database, and replacing it under the app at runtime is not
something the port does; restore at startup with [`restoreDatabase`](#backups). Any other database name
is a `404`. Schema changes (`$migrate`, `$sync`) **are** allowed and change the schema the app sees at
once — the app's generated client may then be behind the database, as with a server.

**Security**: the port gives full read and write access to the database. Keep it on loopback (the
default), and set a `token` for anything else.

---

## Reading a schema file portably

`openTestDatabase(schema)` / `$sync(schema)` take the schema **text**. Read it with `fs` so it works
on both runtimes:

```ts
import { readFileSync } from "node:fs";
const schema = readFileSync(new URL("./schema.marci", import.meta.url), "utf8");
```

Bun also supports `import schema from "./schema.marci" with { type: "text" }`, but that's Bun-only —
Node doesn't support text import attributes.

---

## API

- `openDatabase(dir, options?)` → `EmbeddedDatabase`
- `openTestDatabase(schema?, options?)` → `EmbeddedDatabase` + `path`; backed by a temp dir
  (fsync off), removed on `close()`. Applies `schema` via `$sync` if given.
- `options`: `{ disableFsync?: boolean }` — fsync off is faster but durability-unsafe; intended for
  tests (`openTestDatabase` sets it automatically).

`EmbeddedDatabase` — *is* a transport (`exec`/`batch`), so pass it to `marcidb(db)`, plus:
- `transport` — back-compat alias of `db` itself (`marcidb(db.transport)` still works).
- `$sync(schemaText)` — declarative schema sync (`.marci` text). **await it.**
- `migrate(migrationsDir)` — **idempotent, drift-aware** migrator: applies the unapplied `.march`
  files (sorted by name) in order; safe to call on every startup. Returns `{ applied, total }`,
  throws on drift. The recommended way to run migrations.
- `$migrate(migrationText)` — apply a *single* migration's `.march` actions (low-level; not
  idempotent — prefer `migrate`). **await it.**
- `$snapshot()` — current materialized schema snapshot text.
- `reindexAll()` — rebuild every model's `@custom` (vector / full-text) indexes.
- `backup(path)` — consistent backup to a `.marcibin` file on a native thread; resolves to
  `{ bytes, trees, entries, schemaVersion, createdAt }` (see [Backups](#backups)).
- `listen({ port, host?, token?, name? })` — serve the server's HTTP API for this database on a local
  port; returns `{ port, host, url, close() }` (see [Maintenance port](#maintenance-port)).
- `close()` — release the database (idempotent; stops the maintenance port, waits for running backups).
  `closed` — boolean.

`restoreDatabase(dir, backupPath, options?)` → `EmbeddedDatabase` — restore a backup into `dir` and open it;
`options`: `{ disableFsync?, overwrite? }`.

Errors surface as `MarciEmbeddedError`: `.kind` is what to switch on — a constraint (`unique`, `foreign_key`, `restrict`, `duplicate_id`), `not_found`, `bad_request` or `internal` —, `.field` the `Model.field` a constraint is about (`"Order.dish"`), the message is for a person (`Order.dish: no row with id "nothing" to point at`).

Notes:
- Always `await` `$sync` / `$migrate` — they return promises; an un-awaited rejection on error is
  silent.
- Data crosses the FFI boundary as JSON. Use a handle from the thread that opened it — don't share it
  across Worker threads (calls from one runtime's event loop are already serialized). Backups and the
  maintenance port run on their own native threads and need nothing from you.

---

## Building the native library locally

The library is `marcidb-ffi` in the workspace. The full variant (vector + full-text) needs the
nightly toolchain (vector SIMD):

```sh
./build-lib.sh win    # or: linux | mac  — stages native/marcidb-<platform>.<ext>
```

Run the tests (Node and Bun) with `npm test` / `npm run test:bun`.
