// End-to-end test of the *generated* typed client over the embedded transport (`marcidb(db)`), including a
// @custom(fulltext) search and an atomic $transaction. `marcidb(db)` uses the binary read fast path.
import { test, expect, beforeAll, afterAll } from "bun:test";
import fs from "node:fs";

import { openTestDatabase, type TestDatabase } from "../dist/index.js";
import { generateClient, schemaPath } from "./helpers.ts";

let db: TestDatabase;
let client: any;

beforeAll(async () => {
  const schema = fs.readFileSync(schemaPath("schema.marci"), "utf8");
  const mod = await generateClient(schemaPath("schema.marci"));
  db = await openTestDatabase(schema);
  client = mod.marcidb(db); // db is itself the transport
});
afterAll(() => db?.close());

test("typed CRUD through the generated client", async () => {
  const id = await client.user.insert({ name: "Alice", age: 30 });
  expect(id).not.toBeNull();
  const users = await client.user.findMany({ name: true, age: true });
  expect(users.length).toBe(1);
  expect(users[0].name).toBe("Alice");
});

test("atomic $transaction built from lazy ops", async () => {
  const res = await client.$transaction([
    client.user.insert({ name: "B", age: 1 }),
    client.user.insert({ name: "C", age: 2 }),
  ]);
  expect(res.length).toBe(2);
  expect(await client.user.count()).toBe(3);
});

test("@custom(fulltext) search through the generated client", async () => {
  await client.doc.insert({ title: "A", body: "the quick brown fox" });
  await client.doc.insert({ title: "B", body: "lazy dogs sleeping" });
  const hits = await client.doc.findMany({ title: true, $where: { body: { $search: "quick fox" } } });
  expect(hits.length).toBe(1);
  expect(hits[0].title).toBe("A");
});
