// Parity gate for the binary read transport: every query is run through two clients over the *same* DB —
// one with the binary fast path on (`marcidb(db)`, since `db` exposes `queryBinary`) and one forced onto
// JSON (a transport without `queryBinary`) — and their results must be deep-equal. This is the contract
// that lets binary be on by default. Runs under both Node and Bun.
import assert from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { openTestDatabase } from "../src/index.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, "..", "..", "..");
const genDir = path.join(HERE, ".gen-parity");
const schemaPath = path.join(HERE, "parity.marci");
const schema = fs.readFileSync(schemaPath, "utf8");

// Generate the typed client for parity.marci, then point its runtime import at the local build.
fs.mkdirSync(genDir, { recursive: true });
execFileSync("cargo", ["run", "-q", "-p", "marcidb-ts", "--", schemaPath, genDir], { cwd: REPO, stdio: "inherit" });
const idxPath = path.join(genDir, "index.js");
const runtimeRel = path
  .relative(genDir, path.join(REPO, "packages", "marcidb-client", "runtime", "index.js"))
  .replace(/\\/g, "/");
fs.writeFileSync(idxPath, fs.readFileSync(idxPath, "utf8").replace("marcidb-client/runtime", runtimeRel));

const { marcidb } = await import(`file://${idxPath.replace(/\\/g, "/")}`);

const runtime = typeof Bun !== "undefined" ? `bun ${Bun.version}` : `node ${process.version}`;
console.log(`[binary-parity] runtime: ${runtime}`);

const db = await openTestDatabase(schema);
let checks = 0;

try {
  // `marcidb(db)` uses the binary path (db.queryBinary exists); a transport without queryBinary stays JSON.
  const binaryClient = marcidb(db);
  const jsonClient = marcidb({ exec: db.exec, batch: db.batch });

  // Sanity: the two clients really do take different code paths.
  assert.ok(typeof db.queryBinary === "function", "embedded db exposes queryBinary");

  // ── seed data (writes are identical for both clients; go through JSON) ──
  const u1 = await jsonClient.user.insert({ name: "Alice", age: 30, rating: 0.1, score: 3.141592653589793, active: true, seen: 1700000000000 });
  const u2 = await jsonClient.user.insert({ name: "Bob", age: 25, score: 2.5, active: false }); // rating/seen null
  const u3 = await jsonClient.user.insert({ name: "Carol", age: 40, rating: 1.5, score: 0.0, active: true, seen: 0 });

  await jsonClient.post.insert({ title: "Hello", views: 10, author: u1 });
  await jsonClient.post.insert({ title: "World", views: 20, author: u1 });
  await jsonClient.post.insert({ title: "Orphan", views: 0 }); // author null
  await jsonClient.post.insert({ title: "Bob's", views: 5, author: u2 });

  await jsonClient.account.insert({ label: "free", kind: "basic" });
  await jsonClient.account.insert({ label: "team", kind: "pro", seats: 9 });

  // ── query matrix: each must match byte-for-byte (deep-equal) across both transports ──
  const cases = [
    ["user all scalars",        () => (c) => c.user.findMany({ id: true, name: true, age: true, rating: true, score: true, active: true, seen: true })],
    ["user projection (no id)", () => (c) => c.user.findMany({ name: true, age: true })],
    ["user empty select",       () => (c) => c.user.findMany({})],
    ["user findFirst by id",    () => (c) => c.user.findFirst({ id: true, name: true, score: true, $where: { id: u1.id } })],
    ["user findFirst null",     () => (c) => c.user.findFirst({ id: true, $where: { id: 99999 } })],
    ["user where + order",      () => (c) => c.user.findMany({ id: true, name: true, $where: { active: true }, $order: { age: "desc" } })],
    ["post nested author obj",  () => (c) => c.post.findMany({ id: true, title: true, views: true, author: { id: true, name: true, active: true } })],
    ["post nested author=true", () => (c) => c.post.findMany({ title: true, author: true })],
    ["post nested null author", () => (c) => c.post.findMany({ title: true, author: { name: true } })],
    ["user to-many posts",      () => (c) => c.user.findMany({ id: true, name: true, posts: { id: true, title: true, views: true } })],
    ["user to-many + author",   () => (c) => c.user.findMany({ name: true, posts: { title: true, author: { name: true } } })],
    // Enum-payload model: binary gates out → JSON fallback. Still must be correct & identical.
    ["account (enum fallback)", () => (c) => c.account.findMany({ id: true, label: true, kind: true })],
  ];

  for (const [label, make] of cases) {
    const run = make();
    const [bin, json] = await Promise.all([run(binaryClient), run(jsonClient)]);
    assert.deepStrictEqual(bin, json, `parity failed: ${label}\n binary=${JSON.stringify(bin)}\n   json=${JSON.stringify(json)}`);
    checks++;
    console.log(`  ✓ ${label}`);
  }

  // Spot-check a couple of concrete values so we're sure binary really decoded (not both empty).
  const alice = await binaryClient.user.findFirst({ name: true, rating: true, score: true, active: true, seen: true, $where: { id: u1.id } });
  assert.deepStrictEqual(alice, { name: "Alice", rating: 0.1, score: 3.141592653589793, active: true, seen: 1700000000000 });
  checks++;

  console.log(`[binary-parity] OK — ${checks} checks passed (${cases.length} matrix cases)`);
} finally {
  db.close();
  fs.rmSync(genDir, { recursive: true, force: true });
}
