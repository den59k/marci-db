// The binary read path on a *migrated* database — the case that bit in production: a client generated from
// `schema.marci` (declaration order) against a database whose schema came from `.march` migrations (slot
// order), with a field declared early but added late. Rows go over the wire in name order and the client
// decodes them in name order, so the two can't disagree; the handshake fingerprint ignores slots, so the fast
// path really engages (not a silent JSON fallback). Both are pinned here.
import { test, expect, beforeAll, afterAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { openDatabase, type EmbeddedDatabase } from "../dist/index.js";
import { generateClient, schemaPath, clientSchemaHash } from "./helpers.ts";

// `active` is declared second, but migration 0001 adds it last (slot 16, after title/flag/note).
const M0 =
  "create entity Item\nadd field Item.id UInt @id @default(autoincrement())\nadd field Item.title String @slot(4)\nadd field Item.flag Bool @slot(8) @default(false)\nadd field Item.note String @nullable @slot(12)";
const M1 = "add field Item.active Bool @slot(16) @default(true)";

const migrations = fs.mkdtempSync(path.join(os.tmpdir(), "marci-migs-order-"));
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "marci-data-order-"));

let db: EmbeddedDatabase;
let binaryClient: any;
let jsonClient: any;
let binaryAnswers = 0; // how many reads the engine actually answered in binary

beforeAll(async () => {
  fs.writeFileSync(path.join(migrations, "0000_init.march"), M0);
  fs.writeFileSync(path.join(migrations, "0001_add_active.march"), M1);
  db = openDatabase(dataDir);
  expect(await db.migrate(migrations)).toEqual({ applied: 2, total: 2 });

  const { marcidb } = await generateClient(schemaPath("migrated.marci"));
  // A counting transport: the parity below is meaningless if everything quietly went over JSON.
  binaryClient = marcidb({
    exec: db.exec,
    batch: db.batch,
    queryBinary: async (op: any, hash: string) => {
      const out = await db.queryBinary(op, hash);
      if (out) binaryAnswers++;
      return out;
    },
  });
  jsonClient = marcidb({ exec: db.exec, batch: db.batch });

  await jsonClient.item.insert({ title: "one", flag: true, active: false });
  await jsonClient.item.insert({ title: "two", flag: false, active: true, note: "hello" });
});
afterAll(() => {
  db?.close();
  for (const dir of [migrations, dataDir]) try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best-effort */ }
});

test("same-typed fields declared and slotted in different orders do not swap", async () => {
  const select = { id: true, active: true, flag: true };
  const bin = await binaryClient.item.findMany(select);
  expect(bin).toEqual(await jsonClient.item.findMany(select));
  expect(bin).toEqual([
    { id: 1, active: false, flag: true },
    { id: 2, active: true, flag: false },
  ]);
});

test("a full select decodes (no cursor desync on the late-added field)", async () => {
  const select = { id: true, title: true, active: true, flag: true, note: true };
  const bin = await binaryClient.item.findMany(select);
  expect(bin).toEqual(await jsonClient.item.findMany(select));
  expect(bin[1]).toEqual({ id: 2, title: "two", active: true, flag: false, note: "hello" });
});

test("the reads above really went over binary — the fingerprint ignores slot order", async () => {
  expect(binaryAnswers).toBeGreaterThan(0);
  // And directly: the client's fingerprint is accepted by the migrated database …
  const op = { model: "Item", action: "findMany", query: { id: true, title: true } };
  expect(await db.queryBinary(op, clientSchemaHash(schemaPath("migrated.marci")))).toBeInstanceOf(Uint8Array);
  // … while a client from another schema (or none, i.e. one predating the handshake) gets the JSON fallback.
  expect(await db.queryBinary(op, "0000000000000000")).toBeNull();
  expect(await db.queryBinary(op)).toBeNull();
});
