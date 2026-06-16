// Smoke test for the embedded transport (raw `db.exec`/`db.batch`, no generated client).
import { test, expect, beforeAll, afterAll } from "bun:test";
import { openTestDatabase, MarciEmbeddedError, type TestDatabase } from "../dist/index.js";

const SCHEMA = `
  model User {
    name String
    age  Int
  }
`;

let db: TestDatabase;
let firstId: any;

beforeAll(async () => {
  db = await openTestDatabase(SCHEMA);
});
afterAll(() => db?.close());

test("insert returns an id", async () => {
  firstId = await db.exec({ model: "User", action: "insert", data: { name: "Alice", age: 30 } });
  expect(firstId).not.toBeNull();
  await db.exec({ model: "User", action: "insert", data: { name: "Bob", age: 25 } });
});

test("findMany + count", async () => {
  const rows = await db.exec({ model: "User", action: "findMany", query: { name: true, age: true } });
  expect(rows.length).toBe(2);
  expect(await db.exec({ model: "User", action: "count", query: {} })).toBe(2);
});

test("update persists, read back via findFirst", async () => {
  await db.exec({ model: "User", action: "update", id: firstId, data: { age: 31 } });
  const alice = await db.exec({ model: "User", action: "findFirst", query: { name: true, age: true, $where: { name: "Alice" } } });
  expect(alice.age).toBe(31);
});

test("atomic transaction via the db.transport alias", async () => {
  const results = await db.transport.batch([
    { model: "User", action: "insert", data: { name: "C", age: 1 } },
    { model: "User", action: "insert", data: { name: "D", age: 2 } },
  ]);
  expect(results.length).toBe(2);
  expect(await db.exec({ model: "User", action: "count", query: {} })).toBe(4);
});

test("delete returns true and drops the count", async () => {
  expect(await db.exec({ model: "User", action: "delete", id: firstId })).toBe(true);
  expect(await db.exec({ model: "User", action: "count", query: {} })).toBe(3);
});

test("snapshot mentions the model", async () => {
  const snap = await db.$snapshot();
  expect(snap).toContain("User");
});

test("unknown model rejects with a typed not_found error", async () => {
  const err = await db.exec({ model: "Ghost", action: "findMany", query: {} }).catch((e) => e);
  expect(err).toBeInstanceOf(MarciEmbeddedError);
  expect((err as MarciEmbeddedError).kind).toBe("not_found");
});
