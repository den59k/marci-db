// Runtime-agnostic smoke test for marcidb-embedded. Run under both Bun and Node:
//   bun  test/smoke.mjs
//   node test/smoke.mjs
import assert from "node:assert";
import { openTestDatabase, MarciEmbeddedError } from "../src/index.js";

const runtime = typeof Bun !== "undefined" ? `bun ${Bun.version}` : `node ${process.version}`;
console.log(`[smoke] runtime: ${runtime}`);

const SCHEMA = `
  model User {
    name String
    age  Int
  }
`;

const db = await openTestDatabase(SCHEMA);
console.log(`[smoke] opened ${db.path}`);

try {
  // insert
  const id = await db.exec({ model: "User", action: "insert", data: { name: "Alice", age: 30 } });
  assert.ok(id != null, "insert should return an id");
  await db.exec({ model: "User", action: "insert", data: { name: "Bob", age: 25 } });

  // findMany
  const rows = await db.exec({ model: "User", action: "findMany", query: { name: true, age: true } });
  assert.equal(rows.length, 2, "expected 2 rows");

  // count
  const n = await db.exec({ model: "User", action: "count", query: {} });
  assert.equal(n, 2, "count should be 2");

  // update + findFirst
  await db.exec({ model: "User", action: "update", id, data: { age: 31 } });
  const alice = await db.exec({
    model: "User",
    action: "findFirst",
    query: { name: true, age: true, $where: { name: "Alice" } },
  });
  assert.equal(alice.age, 31, "update should persist");

  // atomic transaction (array form) — via the back-compat `db.transport` alias
  const results = await db.transport.batch([
    { model: "User", action: "insert", data: { name: "C", age: 1 } },
    { model: "User", action: "insert", data: { name: "D", age: 2 } },
  ]);
  assert.equal(results.length, 2, "transaction returns one result per op");
  assert.equal(await db.exec({ model: "User", action: "count", query: {} }), 4);

  // delete
  const deleted = await db.exec({ model: "User", action: "delete", id });
  assert.equal(deleted, true, "delete returns true");
  assert.equal(await db.exec({ model: "User", action: "count", query: {} }), 3);

  // snapshot
  const snap = await db.$snapshot();
  assert.ok(snap.includes("User"), "snapshot mentions the model");

  // error envelope → typed error
  await assert.rejects(
    () => db.exec({ model: "Ghost", action: "findMany", query: {} }),
    (err) => err instanceof MarciEmbeddedError && err.kind === "not_found",
    "unknown model should reject with not_found",
  );

  console.log("[smoke] ALL PASSED");
} finally {
  db.close();
}
