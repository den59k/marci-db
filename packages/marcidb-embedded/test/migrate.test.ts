// The built-in idempotent migrator `db.migrate(dir)`: fresh apply, idempotent re-run, incremental apply,
// usage of a migration-added column through the client, and drift detection.
import { test, expect, beforeAll, afterAll } from "bun:test";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";

import { openDatabase, MarciEmbeddedError, type EmbeddedDatabase } from "../dist/index.js";
import { generateClient, schemaPath } from "./helpers.ts";

const M0 =
  "create entity User\nadd field User.id UInt @id @default(autoincrement())\nadd field User.name String @slot(4)\nadd field User.age Int @slot(8)";
const M1 = "add field User.email String @nullable @slot(12)";

const migrations = fs.mkdtempSync(path.join(os.tmpdir(), "marci-migs-"));
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "marci-data-"));

let db: EmbeddedDatabase;
let client: any;

beforeAll(async () => {
  fs.writeFileSync(path.join(migrations, "0000_init.march"), M0);
  const mod = await generateClient(schemaPath("schema.marci"));
  db = openDatabase(dataDir);
  client = mod.marcidb(db);
});
afterAll(() => db?.close());

test("fresh DB applies the one available migration", async () => {
  expect(await db.migrate(migrations)).toEqual({ applied: 1, total: 1 });
});

test("re-run is idempotent (applies 0)", async () => {
  expect(await db.migrate(migrations)).toEqual({ applied: 0, total: 1 });
});

test("a new migration applies incrementally (exactly 1 more)", async () => {
  fs.writeFileSync(path.join(migrations, "0001_add_email.march"), M1);
  expect(await db.migrate(migrations)).toEqual({ applied: 1, total: 2 });
});

test("the migration-added column round-trips through the client (JSON fallback for the stale field)", async () => {
  await client.user.insert({ name: "Bob", email: "bob@x.io", age: 40 });
  const rows = await client.user.findMany({ name: true, email: true });
  expect(rows[0].email).toBe("bob@x.io");
});

test("a truncated history (DB ahead) is detected as drift", async () => {
  const truncated = fs.mkdtempSync(path.join(os.tmpdir(), "marci-trunc-"));
  fs.writeFileSync(path.join(truncated, "0000_init.march"), M0);
  const err = await db.migrate(truncated).then(() => null, (e) => e);
  expect(err).toBeInstanceOf(MarciEmbeddedError);
  expect((err as MarciEmbeddedError).kind).toBe("bad_request");
});
