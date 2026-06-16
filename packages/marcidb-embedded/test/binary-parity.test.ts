// Parity gate for the binary read transport: every query runs through two clients over the *same* DB — one
// with the binary fast path on (`marcidb(db)`, since `db` exposes `queryBinary`) and one forced onto JSON (a
// transport without `queryBinary`) — and their results must be deep-equal. This is the contract that lets
// binary be on by default.
import { test, expect, beforeAll, afterAll } from "bun:test";
import fs from "node:fs";

import { openTestDatabase, type TestDatabase } from "../dist/index.js";
import { generateClient, schemaPath } from "./helpers.ts";

let db: TestDatabase;
let binaryClient: any;
let jsonClient: any;
const ids: Record<string, any> = {};

beforeAll(async () => {
  const schema = fs.readFileSync(schemaPath("parity.marci"), "utf8");
  const { marcidb } = await generateClient(schemaPath("parity.marci"));
  db = await openTestDatabase(schema);
  binaryClient = marcidb(db); // binary path (db.queryBinary exists)
  jsonClient = marcidb({ exec: db.exec, batch: db.batch }); // no queryBinary → JSON path

  // seed (writes identical for both; go through JSON)
  ids.u1 = await jsonClient.user.insert({ name: "Alice", age: 30, rating: 0.1, score: 3.141592653589793, active: true, seen: 1700000000000 });
  ids.u2 = await jsonClient.user.insert({ name: "Bob", age: 25, score: 2.5, active: false }); // rating/seen null
  await jsonClient.user.insert({ name: "Carol", age: 40, rating: 1.5, score: 0.0, active: true, seen: 0 });

  await jsonClient.post.insert({ title: "Hello", views: 10, author: ids.u1 });
  await jsonClient.post.insert({ title: "World", views: 20, author: ids.u1 });
  await jsonClient.post.insert({ title: "Orphan", views: 0 }); // author null
  await jsonClient.post.insert({ title: "Bob's", views: 5, author: ids.u2 });

  await jsonClient.account.insert({ label: "free", kind: "basic" });
  await jsonClient.account.insert({ label: "team", kind: "pro", seats: 9 });
});
afterAll(() => db?.close());

test("the embedded db really exposes the binary fast path", () => {
  expect(typeof db.queryBinary).toBe("function");
});

// Each query must match deep-equal across both transports.
const cases: [string, (c: any) => Promise<any>][] = [
  ["user all scalars", (c) => c.user.findMany({ id: true, name: true, age: true, rating: true, score: true, active: true, seen: true })],
  ["user projection (no id)", (c) => c.user.findMany({ name: true, age: true })],
  ["user empty select", (c) => c.user.findMany({})],
  ["user findFirst by id", (c) => c.user.findFirst({ id: true, name: true, score: true, $where: { id: ids.u1.id } })],
  ["user findFirst null", (c) => c.user.findFirst({ id: true, $where: { id: 99999 } })],
  ["user where + order", (c) => c.user.findMany({ id: true, name: true, $where: { active: true }, $order: { age: "desc" } })],
  ["post nested author obj", (c) => c.post.findMany({ id: true, title: true, views: true, author: { id: true, name: true, active: true } })],
  ["post nested author=true", (c) => c.post.findMany({ title: true, author: true })],
  ["post nested null author", (c) => c.post.findMany({ title: true, author: { name: true } })],
  ["user to-many posts", (c) => c.user.findMany({ id: true, name: true, posts: { id: true, title: true, views: true } })],
  ["user to-many + author", (c) => c.user.findMany({ name: true, posts: { title: true, author: { name: true } } })],
  // Enum-payload model: binary gates out → JSON fallback. Still must be correct & identical.
  ["account (enum fallback)", (c) => c.account.findMany({ id: true, label: true, kind: true })],
];

for (const [label, run] of cases) {
  test(`parity: ${label}`, async () => {
    const [bin, json] = await Promise.all([run(binaryClient), run(jsonClient)]);
    expect(bin).toEqual(json);
  });
}

test("binary really decoded concrete values (not both empty)", async () => {
  const alice = await binaryClient.user.findFirst({ name: true, rating: true, score: true, active: true, seen: true, $where: { id: ids.u1.id } });
  expect(alice).toEqual({ name: "Alice", rating: 0.1, score: 3.141592653589793, active: true, seen: 1700000000000 });
});
