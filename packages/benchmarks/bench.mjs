// Benchmark: marcidb-embedded vs SQLite (better-sqlite3, plus the runtime built-in: bun:sqlite / node:sqlite).
//
// Fairness notes:
//  - Every engine is disk-backed with durability OFF (marcidb: disableFsync; SQLite: synchronous=OFF, WAL),
//    so we measure engine + binding overhead, not fsync.
//  - SQLite uses prepared statements (its idiomatic fast path) and runs synchronously.
//  - marcidb uses its real typed client (`marcidb(db)`), which is async (a Promise per op) and JSON-over-FFI —
//    exactly how an app uses it. The async + JSON cost is the point of the comparison.
//  - One full suite is run as warmup (discarded) before the measured run, so the JIT is hot.
//
// Run:  node bench.mjs   |   bun bench.mjs    (env: N=rows, READS=, POINTS=, UPDATES=, COUNTS=)
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { performance } from "node:perf_hooks";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { openDatabase } from "marcidb-embedded";
import { marcidb } from "marcidb-client";
import BetterSqlite3 from "better-sqlite3";

const isBun = typeof Bun !== "undefined";
const RUNTIME = isBun ? `bun ${Bun.version}` : `node ${process.version}`;
const HERE = path.dirname(fileURLToPath(import.meta.url));
const SCHEMA = readFileSync(path.join(HERE, "schema.marci"), "utf8");

const N = Number(process.env.N ?? 20000);
const READS = Number(process.env.READS ?? 50);
const POINTS = Number(process.env.POINTS ?? N);
const COUNTS = Number(process.env.COUNTS ?? 500);
const UPDATES = Number(process.env.UPDATES ?? N);
// relational (nested select) + index-backed filter
const REL_POSTS = Number(process.env.REL_POSTS ?? 10000);
const REL_AUTHORS = Number(process.env.REL_AUTHORS ?? 100);
const REL_READS = Number(process.env.REL_READS ?? 20);
const IDX_ROWS = Number(process.env.IDX_ROWS ?? 20000);
const IDX_READS = Number(process.env.IDX_READS ?? 5000);
const INDEX_SCHEMA = "model User {\n  name String\n  age Int @index\n  email String\n}";

function rows(n, tag = "") {
  const out = new Array(n);
  for (let i = 0; i < n; i++) out[i] = { name: `${tag}user${i}`, age: 18 + (i % 60), email: `${tag}user${i}@example.com` };
  return out;
}
const DATA = rows(N);
const randId = () => 1 + Math.floor(Math.random() * N);
const tmp = (prefix) => fs.mkdtempSync(path.join(os.tmpdir(), prefix));
const safeRm = (dir) => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* Windows may hold the file briefly */ } };

// ───────────────────────────── adapters ─────────────────────────────
async function marcidbAdapter() {
  const dir = tmp("bench-marci-");
  const db = openDatabase(dir, { disableFsync: true });
  await db.$sync(SCHEMA);
  const client = marcidb(db);
  return {
    name: "marcidb-embedded", async: true,
    insertOne: (r) => client.user.insert(r),
    insertMany: (rs) => client.$transaction(rs.map((r) => client.user.insert(r))),
    selectAll: () => client.user.findMany({ name: true, age: true, email: true }),
    pointById: (id) => client.user.findFirst({ name: true, age: true, $where: { id } }),
    count: () => client.user.count(),
    updateById: (id, age) => client.user.update({ id }, { age }),
    cleanup: () => { db.close(); safeRm(dir); },
  };
}

function sqliteAdapter(name, db, dir) {
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = OFF");
  db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)");
  const insert = db.prepare("INSERT INTO users (name, age, email) VALUES (?, ?, ?)");
  const selectAll = db.prepare("SELECT id, name, age, email FROM users");
  const byId = db.prepare("SELECT id, name, age, email FROM users WHERE id = ?");
  const countStmt = db.prepare("SELECT COUNT(*) AS c FROM users");
  const upd = db.prepare("UPDATE users SET age = ? WHERE id = ?");
  return {
    name, async: false,
    insertOne: (r) => insert.run(r.name, r.age, r.email),
    insertMany: (rs) => { for (const r of rs) insert.run(r.name, r.age, r.email); },
    selectAll: () => selectAll.all(),
    pointById: (id) => byId.get(id),
    count: () => countStmt.get().c,
    updateById: (id, age) => upd.run(age, id),
    cleanup: () => { db.close(); if (dir) safeRm(dir); },
  };
}

async function buildAdapters() {
  const list = [await marcidbAdapter()];
  // better-sqlite3 is the cross-runtime baseline — but it doesn't load on Bun (oven-sh/bun#4290), so skip it there.
  try {
    const d = tmp("bench-bsq-");
    list.push(sqliteAdapter("better-sqlite3", new BetterSqlite3(path.join(d, "db.sqlite")), d));
  } catch (e) {
    if (!buildAdapters.warned) { console.log(`  (skipping better-sqlite3: ${e.code || e.message})`); buildAdapters.warned = true; }
  }
  if (isBun) {
    const { Database } = await import("bun:sqlite");
    const d = tmp("bench-bun-"); list.push(sqliteAdapter("bun:sqlite", new Database(path.join(d, "db.sqlite")), d));
  } else {
    const { DatabaseSync } = await import("node:sqlite");
    const d = tmp("bench-node-"); list.push(sqliteAdapter("node:sqlite", new DatabaseSync(path.join(d, "db.sqlite")), d));
  }
  return list;
}

// ───────────────────────────── timing ─────────────────────────────
async function timeOps(a, iters, op) {
  const t0 = performance.now();
  if (a.async) { for (let i = 0; i < iters; i++) await op(i); }
  else { for (let i = 0; i < iters; i++) op(i); }
  return performance.now() - t0;
}
async function timeOnce(fn) { const t0 = performance.now(); await fn(); return performance.now() - t0; }

// ───────────────────────────── suite ─────────────────────────────
async function suite() {
  const adapters = await buildAdapters();
  const results = [];
  const add = (title, out) => results.push({ title, out });

  // bulk insert (empty → N)
  { const out = []; for (const a of adapters) { const ms = await timeOnce(() => a.insertMany(DATA)); out.push({ engine: a.name, ms, ops: N / (ms / 1000) }); } add(`Bulk insert (${N} rows, one transaction)`, out); }

  // select all
  { const out = []; for (const a of adapters) { let last; const ms = await timeOps(a, READS, async () => { last = await a.selectAll(); }); if (last.length !== N) throw new Error(`${a.name} selectAll=${last.length}`); out.push({ engine: a.name, ms, ops: READS / (ms / 1000) }); } add(`Select all (${N} rows × ${READS})`, out); }

  // point by id
  { const out = []; for (const a of adapters) { const ms = await timeOps(a, POINTS, () => a.pointById(randId())); out.push({ engine: a.name, ms, ops: POINTS / (ms / 1000) }); } add(`Point query by id (${POINTS})`, out); }

  // count
  { const out = []; for (const a of adapters) { const ms = await timeOps(a, COUNTS, () => a.count()); out.push({ engine: a.name, ms, ops: COUNTS / (ms / 1000) }); } add(`Count (${COUNTS})`, out); }

  // update by id
  { const out = []; for (const a of adapters) { const ms = await timeOps(a, UPDATES, () => a.updateById(randId(), 99)); out.push({ engine: a.name, ms, ops: UPDATES / (ms / 1000) }); } add(`Update by id (${UPDATES})`, out); }

  // single insert (append N more, one at a time)
  { const out = []; for (const a of adapters) { const more = rows(N, "s"); let i = 0; const ms = await timeOps(a, N, () => a.insertOne(more[i++])); out.push({ engine: a.name, ms, ops: N / (ms / 1000) }); } add(`Single insert (${N} rows, one at a time)`, out); }

  for (const a of adapters) a.cleanup();
  return results;
}

// Open one fresh sqlite db file per available engine (skips better-sqlite3 on Bun). Returns [{name, db, dir}].
async function openSqlites(tag) {
  const out = [];
  try { const d = tmp(tag); out.push({ name: "better-sqlite3", db: new BetterSqlite3(path.join(d, "db.sqlite")), dir: d }); } catch { /* not on Bun */ }
  if (isBun) { const { Database } = await import("bun:sqlite"); const d = tmp(tag); out.push({ name: "bun:sqlite", db: new Database(path.join(d, "db.sqlite")), dir: d }); }
  else { const { DatabaseSync } = await import("node:sqlite"); const d = tmp(tag); out.push({ name: "node:sqlite", db: new DatabaseSync(path.join(d, "db.sqlite")), dir: d }); }
  return out;
}

// ── Nested select: read posts with their author decoded — marcidb's one-query graph vs SQLite JOIN/N+1 ──
async function relationalBench() {
  const authors = rows(REL_AUTHORS, "a");
  const posts = Array.from({ length: REL_POSTS }, (_, i) => ({ title: `post${i}`, authorId: 1 + Math.floor(Math.random() * REL_AUTHORS) }));
  const ok = (r) => { if (r.length !== REL_POSTS || !r[0].author || r[0].author.name === undefined) throw new Error("bad nested result"); };
  const out = [];

  // marcidb — one query, author graph decoded (shared authors hit the include cache)
  {
    const dir = tmp("bench-rel-marci-");
    const db = openDatabase(dir, { disableFsync: true });
    await db.$sync(SCHEMA);
    const client = marcidb(db);
    await client.$transaction(authors.map((a) => client.user.insert(a)));
    await client.$transaction(posts.map((p) => client.post.insert({ title: p.title, author: { id: p.authorId } })));
    const ms = await timeOps({ async: true }, REL_READS, async () => ok(await client.post.findMany({ title: true, author: { name: true, email: true } })));
    out.push({ engine: "marcidb-embedded", ms, ops: REL_READS / (ms / 1000) });
    db.close(); safeRm(dir);
  }

  // SQLite — JOIN then reshape to the same nested shape; plus the N+1 trap a naive ORM falls into
  const sqlites = await openSqlites("bench-rel-sql-");
  for (let si = 0; si < sqlites.length; si++) {
    const { name, db, dir } = sqlites[si];
    db.exec("PRAGMA journal_mode = WAL"); db.exec("PRAGMA synchronous = OFF");
    db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)");
    db.exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER)");
    const insU = db.prepare("INSERT INTO users (name, age, email) VALUES (?, ?, ?)");
    const insP = db.prepare("INSERT INTO posts (title, author_id) VALUES (?, ?)");
    for (const a of authors) insU.run(a.name, a.age, a.email);
    for (const p of posts) insP.run(p.title, p.authorId);

    const joinStmt = db.prepare("SELECT p.title AS title, u.name AS a_name, u.email AS a_email FROM posts p LEFT JOIN users u ON u.id = p.author_id");
    const ms = await timeOps({ async: false }, REL_READS, () => {
      const flat = joinStmt.all();
      const shaped = new Array(flat.length);
      for (let i = 0; i < flat.length; i++) { const r = flat[i]; shaped[i] = { title: r.title, author: { name: r.a_name, email: r.a_email } }; }
      ok(shaped);
    });
    out.push({ engine: `${name} (JOIN)`, ms, ops: REL_READS / (ms / 1000) });

    // N+1 — for the first engine only, to illustrate the trap marcidb's nested select avoids
    if (si === 0) {
      const allPosts = db.prepare("SELECT title, author_id FROM posts");
      const byId = db.prepare("SELECT name, email FROM users WHERE id = ?");
      const ms2 = await timeOps({ async: false }, REL_READS, () => {
        const ps = allPosts.all();
        const shaped = new Array(ps.length);
        for (let i = 0; i < ps.length; i++) { const a = byId.get(ps[i].author_id); shaped[i] = { title: ps[i].title, author: { name: a.name, email: a.email } }; }
        ok(shaped);
      });
      out.push({ engine: `${name} (N+1)`, ms: ms2, ops: REL_READS / (ms2 / 1000) });
    }
    db.close(); safeRm(dir);
  }
  return { title: `Nested select — ${REL_POSTS} posts + author{name,email}, ${REL_AUTHORS} authors (×${REL_READS})`, out };
}

// ── Index-backed filter: WHERE age=? over IDX_ROWS rows; marcidb @index vs SQLite CREATE INDEX ──
async function indexBench() {
  const data = rows(IDX_ROWS, "i").map((r, i) => ({ ...r, age: 18 + (i % 60) }));
  const randAge = () => 18 + Math.floor(Math.random() * 60);
  const out = [];

  {
    const dir = tmp("bench-idx-marci-");
    const db = openDatabase(dir, { disableFsync: true });
    await db.$sync(INDEX_SCHEMA);
    const client = marcidb(db);
    await client.$transaction(data.map((u) => client.user.insert(u)));
    // sanity: an equality filter must return ~IDX_ROWS/60 rows (≈index hit), not a wrong/zero count
    const sample = await client.user.findMany({ name: true, $where: { age: 30 } });
    if (sample.length < 1) throw new Error(`marcidb indexed where returned ${sample.length}`);
    const ms = await timeOps({ async: true }, IDX_READS, async () => { await client.user.findMany({ name: true, $where: { age: randAge() } }); });
    out.push({ engine: "marcidb-embedded", ms, ops: IDX_READS / (ms / 1000) });
    db.close(); safeRm(dir);
  }

  for (const { name, db, dir } of await openSqlites("bench-idx-sql-")) {
    db.exec("PRAGMA journal_mode = WAL"); db.exec("PRAGMA synchronous = OFF");
    db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)");
    const ins = db.prepare("INSERT INTO users (name, age, email) VALUES (?, ?, ?)");
    for (const u of data) ins.run(u.name, u.age, u.email);
    db.exec("CREATE INDEX idx_users_age ON users(age)");
    const sel = db.prepare("SELECT id, name FROM users WHERE age = ?");
    const ms = await timeOps({ async: false }, IDX_READS, () => { sel.all(randAge()); });
    out.push({ engine: name, ms, ops: IDX_READS / (ms / 1000) });
    db.close(); safeRm(dir);
  }
  return { title: `Index-backed filter — WHERE age=? over ${IDX_ROWS} rows (×${IDX_READS}, ≈${Math.round(IDX_ROWS / 60)} rows/match)`, out };
}

async function everything() {
  return [...(await suite()), await relationalBench(), await indexBench()];
}

const fmt = (n) => Math.round(n).toLocaleString("en-US");

console.log(`MarciDB embedded vs SQLite — ${RUNTIME}`);
console.log(`flat N=${fmt(N)}  nested ${fmt(REL_POSTS)} posts/${REL_AUTHORS} authors  index ${fmt(IDX_ROWS)} rows`);

await everything(); // warmup (discarded)
const results = await everything();

for (const { title, out } of results) {
  console.log(`\n## ${title}`);
  const w = Math.max(...out.map((r) => r.engine.length));
  const best = Math.max(...out.map((r) => r.ops));
  for (const r of out) {
    const rel = (r.ops / best);
    console.log(`  ${r.engine.padEnd(w)}  ${(r.ms.toFixed(1) + " ms").padStart(11)}  ${(fmt(r.ops) + " ops/s").padStart(17)}  ${rel === 1 ? "(fastest)" : `${rel.toFixed(2)}x`}`);
  }
}
console.log("\ndone.");
