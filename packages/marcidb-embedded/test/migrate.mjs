// Tests the built-in idempotent migrator db.migrate(dir) against the local package + freshly-built lib.
// Run under both: node test/migrate.mjs / bun test/migrate.mjs
import assert from "node:assert";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { openDatabase, MarciEmbeddedError } from "../src/index.js";
import { marcidb } from "./.gen/index.js";

const runtime = typeof Bun !== "undefined" ? `bun ${Bun.version}` : `node ${process.version}`;
console.log(`[migrate] runtime: ${runtime}`);

const M0 =
  "create entity User\nadd field User.id UInt @id @default(autoincrement())\nadd field User.name String @slot(4)\nadd field User.age Int @slot(8)";
const M1 = "add field User.email String @nullable @slot(12)";

// Lay out a migrations directory.
const migrations = fs.mkdtempSync(path.join(os.tmpdir(), "marci-migs-"));
fs.writeFileSync(path.join(migrations, "0000_init.march"), M0);

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "marci-data-"));
let db = openDatabase(dataDir);

// Fresh DB, only M0 present → applies 1.
let r = await db.migrate(migrations);
assert.deepEqual(r, { applied: 1, total: 1 }, `fresh with M0: ${JSON.stringify(r)}`);

// Re-run, nothing new → applies 0 (idempotent).
r = await db.migrate(migrations);
assert.deepEqual(r, { applied: 0, total: 1 }, `re-run: ${JSON.stringify(r)}`);

// Add a second migration → applies exactly 1 more.
fs.writeFileSync(path.join(migrations, "0001_add_email.march"), M1);
r = await db.migrate(migrations);
assert.deepEqual(r, { applied: 1, total: 2 }, `incremental: ${JSON.stringify(r)}`);

// The new column is usable through the typed client.
const client = marcidb(db);
await client.user.insert({ name: "Bob", email: "bob@x.io", age: 40 });
const rows = await client.user.findMany({ name: true, email: true });
assert.equal(rows[0].email, "bob@x.io");

// Drift: a truncated history (DB is ahead) throws.
const truncated = fs.mkdtempSync(path.join(os.tmpdir(), "marci-trunc-"));
fs.writeFileSync(path.join(truncated, "0000_init.march"), M0);
await assert.rejects(
  () => db.migrate(truncated),
  (e) => e instanceof MarciEmbeddedError && e.kind === "bad_request",
  "truncated history should be drift",
);

db.close();
console.log("[migrate] ALL PASSED");
