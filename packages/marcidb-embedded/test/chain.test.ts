// The query builder (`db.user.where(…).order(…).limit(…).select(…)`) over the *generated* client and the embedded
// engine: clause merging, the empty-select rule, sub-queries in a select (builder / count / aggregate), the object
// form merged with the chain, keyset paging, and batching builders in a $transaction. `marcidb(db)` takes the
// binary read fast path, so this also checks the decoder agrees with the filled projections.
import { test, expect, beforeAll, afterAll } from "bun:test";
import fs from "node:fs";

import { openTestDatabase, type TestDatabase } from "../dist/index.js";
import { generateClient, schemaPath } from "./helpers.ts";

let db: TestDatabase;
let client: any;
let alice: any, bob: any;

beforeAll(async () => {
  const schema = fs.readFileSync(schemaPath("chain.marci"), "utf8");
  const mod = await generateClient(schemaPath("chain.marci"));
  db = await openTestDatabase(schema);
  client = mod.marcidb(db);
  alice = await client.user.insert({ name: "Alice", age: 30 });
  bob = await client.user.insert({ name: "Bob", age: 25 });
  await client.user.insert({ name: "Carol", age: 41 });
  for (const [title, views] of [["a1", 10], ["a2", 20], ["a3", 30]]) await client.post.insert({ title, views, author: alice });
  await client.post.insert({ title: "b1", views: 5, author: bob });
});
afterAll(() => db?.close());

test("the root query and clauses: no select = id + scalars, where/order/limit/skip", async () => {
  const all = await client.user;
  expect(all.map((u: any) => Object.keys(u).sort())).toEqual([["age", "id", "name"], ["age", "id", "name"], ["age", "id", "name"]]);

  const adults = await client.user.where({ age: { $gte: 30 } }).order("age", "desc");
  expect(adults.map((u: any) => u.name)).toEqual(["Carol", "Alice"]);

  const paged = await client.user.order({ age: "asc" }).skip(1).limit(1);
  expect(paged.map((u: any) => u.name)).toEqual(["Alice"]);

  // where().where() ANDs
  const both = await client.user.where({ age: { $gte: 26 } }).where({ name: { $startsWith: "A" } });
  expect(both.map((u: any) => u.name)).toEqual(["Alice"]);

  expect((await client.user.where({ name: "Bob" }).first()).age).toBe(25);
  expect(await client.user.where({ name: "Nobody" }).first()).toBeNull();
});

test("select shapes: nested shape, builder sub-query, count and aggregate sub-queries", async () => {
  const users = await client.user.order("name").select({
    name: true,
    posts: client.post.order("views", "desc").limit(2).select({ title: true }),
    total: undefined,
  });
  expect(users).toEqual([
    { name: "Alice", posts: [{ title: "a3" }, { title: "a2" }] },
    { name: "Bob", posts: [{ title: "b1" }] },
    { name: "Carol", posts: [] },
  ]);

  const counted = await client.user.order("name").select({ name: true, posts: client.post.where({ views: { $gt: 5 } }).count() });
  expect(counted).toEqual([
    { name: "Alice", posts: { count: 3 } },
    { name: "Bob", posts: { count: 0 } },
    { name: "Carol", posts: { count: 0 } },
  ]);

  const agg = await client.user.where({ name: "Alice" }).select({ posts: client.post.aggregate({ $count: true, $sum: "views" }) });
  expect(agg).toEqual([{ posts: { count: 3, sum: 60 } }]);

  // a sub-query without a select → the relation's scalars; a to-one sub-query
  const scalars = await client.user.where({ name: "Bob" }).select({ posts: client.post.limit(5) });
  expect(scalars[0].posts).toEqual([{ id: expect.anything(), title: "b1", views: 5 }]);
  const authors = await client.post.where({ title: "b1" }).select({ title: true, author: client.user.select({ name: true }) });
  expect(authors).toEqual([{ title: "b1", author: { name: "Bob" } }]);

  // select() with only clauses / nothing = scalars — at every level, plain object form too
  expect(Object.keys((await client.post.select({ $limit: 1 }))[0]).sort()).toEqual(["id", "title", "views"]);
  expect(Object.keys((await client.post.select())[0]).sort()).toEqual(["id", "title", "views"]);
  const nestedEmpty = await client.user.where({ name: "Bob" }).findMany({ name: true, posts: { $limit: 1 } });
  expect(nestedEmpty).toEqual([{ name: "Bob", posts: [{ id: expect.anything(), title: "b1", views: 5 }] }]);
});

test("the object form merges with the chain", async () => {
  const rows = await client.post.where({ views: { $gte: 20 } }).findMany({ title: true, $order: { views: "asc" } });
  expect(rows).toEqual([{ title: "a2" }, { title: "a3" }]);
  const first = await client.post.order("views", "desc").findFirst({ title: true, $where: { author: { name: "Alice" } } });
  expect(first).toEqual({ title: "a3" });
  expect(await client.post.where({ author: { name: "Alice" } }).count({ $where: { views: { $lt: 30 } } })).toBe(2);
});

test("keyset paging with after()", async () => {
  const page1 = await client.post.order("views", "asc").limit(2);
  expect(page1.map((p: any) => p.title)).toEqual(["b1", "a1"]);
  const page2 = await client.post.order("views", "asc").limit(2).after(page1[1].id);
  expect(page2.map((p: any) => p.title)).toEqual(["a2", "a3"]);
  const page2b = await client.post.order("views", "asc").limit(2).after({ id: page1[1].id });
  expect(page2b).toEqual(page2);
});

test("builders are immutable and batch into $transaction", async () => {
  const base = client.post.where({ author: { name: "Alice" } });
  const top = base.order("views", "desc").limit(1);
  const [n, rows, one] = await client.$transaction([base.count(), top.select({ title: true }), top.first()]);
  expect(n).toBe(3);
  expect(rows).toEqual([{ title: "a3" }]);
  expect(one.title).toBe("a3");
  // `base` was not mutated by the derived queries
  expect((await base).length).toBe(3);
});

test("updateMany(data) and deleteMany() take the filter from the chain", async () => {
  expect(await client.post.where({ author: { name: "Alice" } }).where({ views: { $lt: 25 } }).updateMany({ views: { $increment: 100 } })).toBe(2);
  const bumped = await client.post.where({ views: { $gt: 100 } }).order("title").select({ title: true });
  expect(bumped).toEqual([{ title: "a1" }, { title: "a2" }]);
  // the deprecated two-argument form still works (its $where ANDs with the chain)
  expect(await client.post.where({ author: { name: "Alice" } }).updateMany({ $where: { views: { $gt: 100 } } }, { views: 1 })).toBe(2);

  // deleteMany: refuses without a where; counts deleted rows; batches into $transaction
  expect(() => client.post.deleteMany()).toThrow(/needs a where/);
  expect(await client.post.where({ views: 1 }).deleteMany()).toBe(2);
  expect((await client.post.order("title")).map((p: any) => p.title)).toEqual(["a3", "b1"]);
  const [n, left] = await client.$transaction([client.post.where({ title: "b1" }).deleteMany(), client.post.count()]);
  expect([n, left]).toEqual([1, 1]);
  // .where({}) is the explicit "every row"
  expect(await client.post.where({}).deleteMany()).toBe(1);
  expect(await client.post.count()).toBe(0);
});
