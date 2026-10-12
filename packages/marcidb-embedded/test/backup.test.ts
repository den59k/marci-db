// Backups of an embedded database: `db.backup(path)` copies on a native thread while the app keeps
// reading and writing, `restoreDatabase(dir, file)` opens what was backed up, and an open or occupied
// directory is never overwritten by accident.
import { test, expect, beforeAll, afterAll } from "bun:test";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";

import { openDatabase, openTestDatabase, restoreDatabase, MarciEmbeddedError, type TestDatabase } from "../dist/index.js";
import { generateClient, schemaPath } from "./helpers.ts";

const SCHEMA = fs.readFileSync(schemaPath("schema.marci"), "utf8");
const work = fs.mkdtempSync(path.join(os.tmpdir(), "marci-backup-"));

let db: TestDatabase;
let mod: any;

beforeAll(async () => {
  mod = await generateClient(schemaPath("schema.marci"));
  db = await openTestDatabase(SCHEMA);
  const users = Array.from({ length: 500 }, (_, i) => ({ model: "User", action: "insert", data: { name: `user${i}`, age: i } }));
  await db.batch(users);
});
afterAll(() => {
  db?.close();
  fs.rmSync(work, { recursive: true, force: true });
});

test("a backup restores into a database the typed client reads", async () => {
  const file = path.join(work, "round-trip.marcibin");
  const info = await db.backup(file);
  expect(info.bytes).toBe(fs.statSync(file).size);
  expect(info.entries).toBeGreaterThanOrEqual(500);
  expect(info.schemaVersion).toBeGreaterThanOrEqual(1);
  expect(info.createdAt).toMatch(/Z$/);
  expect(fs.readdirSync(work).filter((f) => f.endsWith(".tmp"))).toEqual([]);

  const restored = restoreDatabase(path.join(work, "restored"), file, { disableFsync: true });
  try {
    const query = { name: true, age: true, $where: { age: { $gte: 495 } } };
    const original = await mod.marcidb(db).user.findMany(query);
    expect(original.length).toBe(5);
    expect(await mod.marcidb(restored).user.findMany(query)).toEqual(original);
    expect(await restored.$snapshot()).toBe(await db.$snapshot());
  } finally {
    restored.close();
  }
});

test("writes made while a backup runs succeed and stay out of it", async () => {
  const file = path.join(work, "during.marcibin");
  const before = await db.exec({ model: "User", action: "count", query: {} });
  const running = db.backup(file);
  await db.exec({ model: "User", action: "insert", data: { name: "late", age: -1 } });
  expect(await db.exec({ model: "User", action: "count", query: {} })).toBe(before + 1);
  await running;

  const restored = restoreDatabase(path.join(work, "during"), file, { disableFsync: true });
  try {
    expect(await restored.exec({ model: "User", action: "count", query: {} })).toBe(before);
  } finally {
    restored.close();
  }
});

test("close() waits for a running backup and its promise still resolves", async () => {
  const temp = await openTestDatabase(SCHEMA);
  await temp.batch(Array.from({ length: 200 }, (_, i) => ({ model: "User", action: "insert", data: { name: `t${i}`, age: i } })));
  const file = path.join(work, "closing.marcibin");
  const running = temp.backup(file);
  temp.close();
  const info = await running;
  expect(info.bytes).toBe(fs.statSync(file).size);
  await expect(temp.backup(path.join(work, "never.marcibin"))).rejects.toBeInstanceOf(MarciEmbeddedError);
});

test("a database that is open is not restored over; overwrite replaces a closed one", async () => {
  const file = path.join(work, "over.marcibin");
  await db.backup(file);

  const dir = path.join(work, "live");
  const live = openDatabase(dir, { disableFsync: true });
  await live.$sync("model Other {\n  v String\n}");

  const occupied = (() => { try { restoreDatabase(dir, file); } catch (e) { return e; } })();
  expect(occupied).toBeInstanceOf(MarciEmbeddedError);
  expect((occupied as Error).message).toContain("overwrite");

  const open = (() => { try { restoreDatabase(dir, file, { overwrite: true }); } catch (e) { return e; } })();
  expect(open).toBeInstanceOf(MarciEmbeddedError);
  expect((open as Error).message).toContain("is open");
  live.close();

  const replaced = restoreDatabase(dir, file, { overwrite: true, disableFsync: true });
  try {
    expect(await replaced.$snapshot()).toBe(await db.$snapshot());
    expect(await replaced.exec({ model: "User", action: "count", query: {} })).toBeGreaterThanOrEqual(500);
  } finally {
    replaced.close();
  }
  expect(fs.readdirSync(work).filter((f) => f.includes(".restoring") || f.includes(".old-"))).toEqual([]);
});
