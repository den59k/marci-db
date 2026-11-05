// `op.watch(callback, { error })` over the generated client, on both transports: the embedded engine (the
// FFI poll) and `marcidb-server` over HTTP (the WebSocket). What is pinned: the first call comes at once, a
// commit that changes the result calls again, a signal whose re-read gives the same result calls nothing,
// signals during a read make one more read, `close()` stops it, a refused op goes to `error`, only reads
// can be watched, a migration reaches it — and over HTTP a server restart is invisible to the caller.
import { test, expect, beforeAll, afterAll, describe } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync, spawn, type ChildProcess } from "node:child_process";

import { openTestDatabase, type TestDatabase } from "../dist/index.js";
import { generateClient, schemaPath } from "./helpers.ts";

const REPO = path.resolve(import.meta.dir, "..", "..", "..");
const SERVER_EXE = path.join(REPO, "target", "debug", process.platform === "win32" ? "marcidb-server.exe" : "marcidb-server");
const PORT = 39828;
const RESTART_PORT = 39829;
const SCHEMA = fs.readFileSync(schemaPath("watch.marci"), "utf8");

let mod: any;
let embedded: TestDatabase;
const servers: ChildProcess[] = [];
const dataDirs: string[] = [];
const clients: Record<string, any> = {};
const sync: Record<string, (name: string, schema: string) => Promise<any>> = {};

/** Starts `marcidb-server` on `port` over `dataDir` and waits for its health check. */
async function startServer(port: number, dataDir: string): Promise<ChildProcess> {
  const server = spawn(SERVER_EXE, ["--port", String(port), "--data", dataDir], { stdio: "ignore" });
  servers.push(server);
  for (let i = 0; ; i++) {
    try { if ((await fetch(`http://127.0.0.1:${port}/$health`)).ok) return server; } catch {}
    if (i > 200) throw new Error("marcidb-server did not start");
    await new Promise((r) => setTimeout(r, 25));
  }
}

async function httpSync(port: number, name: string, schema: string) {
  const res = await fetch(`http://127.0.0.1:${port}/${name}/$sync`, { method: "POST", body: schema });
  if (!res.ok) throw new Error(`$sync failed: ${await res.text()}`);
}

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "marci-watch-"));
  dataDirs.push(dir);
  return dir;
}

beforeAll(async () => {
  mod = await generateClient(schemaPath("watch.marci"));

  embedded = await openTestDatabase(SCHEMA);
  clients.embedded = mod.marcidb(embedded);
  const extra: TestDatabase[] = [];
  sync.embedded = async (_name, schema) => {
    const db = await openTestDatabase(SCHEMA);
    extra.push(db);
    await db.$sync(schema);
    return { client: mod.marcidb(db), migrate: (s: string) => db.$sync(s) };
  };
  afterAll(() => extra.forEach((db) => db.close()));

  const build = spawnSync("cargo", ["build", "-q", "-p", "marcidb-server"], { cwd: REPO, stdio: "inherit" });
  if (build.status !== 0) throw new Error("marcidb-server build failed");
  await startServer(PORT, tempDir());
  await httpSync(PORT, "watch", SCHEMA);
  clients.http = mod.marcidb(`http://127.0.0.1:${PORT}/watch`);
  sync.http = async (name, schema) => {
    await httpSync(PORT, name, schema);
    return { client: mod.marcidb(`http://127.0.0.1:${PORT}/${name}`), migrate: (s: string) => httpSync(PORT, name, s) };
  };
}, 120_000);

afterAll(() => {
  embedded?.close();
  for (const server of servers) server.kill();
  for (const dir of dataDirs) fs.rmSync(dir, { recursive: true, force: true });
});

/** Waits until `ready()` holds (or fails after `ms`). */
async function until(ready: () => boolean, ms = 4000) {
  const deadline = Date.now() + ms;
  while (!ready()) {
    if (Date.now() > deadline) throw new Error("timed out waiting");
    await new Promise((r) => setTimeout(r, 10));
  }
}

const quiet = (ms = 300) => new Promise((r) => setTimeout(r, ms));

for (const transport of ["embedded", "http"]) {
  describe(transport, () => {
    test("the first call comes at once; a commit that changes the result calls again", async () => {
      const db = clients[transport];
      const seen: any[] = [];
      const w = db.player.where({ room: "first" }).select({ name: true }).watch((rows: any) => seen.push(rows));
      await until(() => seen.length === 1);
      expect(seen[0]).toEqual([]);

      await db.player.insert({ name: "ann", room: "first", score: 1 });
      await until(() => seen.length === 2);
      expect(seen[1]).toEqual([{ name: "ann" }]);

      // first() and count() are watched the same way.
      const one: any[] = [];
      const n: number[] = [];
      const w1 = db.player.where({ room: "first", name: "bob" }).first().watch((row: any) => one.push(row));
      const w2 = db.player.where({ room: "first" }).count().watch((count: number) => n.push(count));
      await until(() => one.length === 1 && n.length === 1);
      expect([one[0], n[0]]).toEqual([null, 1]);
      await db.player.insert({ name: "bob", room: "first", score: 2 });
      await until(() => seen.length === 3 && one.length === 2 && n.length === 2);
      expect(seen[2]).toEqual([{ name: "ann" }, { name: "bob" }]);
      expect(one[1]).toEqual({ id: expect.anything(), name: "bob", room: "first", score: 2 });
      expect(n[1]).toBe(2);
      for (const x of [w, w1, w2]) x.close();
    });

    test("a signal whose re-read gives the same result calls nothing", async () => {
      const db = clients[transport];
      const ann = await db.player.insert({ name: "ann", room: "same", score: 0 });
      const names: any[] = [];
      const sums: any[] = [];
      const w1 = db.player.where({ room: "same" }).select({ name: true }).watch((rows: any) => names.push(rows));
      const w2 = db.player.where({ room: "same" }).aggregate({ $sum: "score" }).watch((agg: any) => sums.push(agg));
      await until(() => names.length === 1 && sums.length === 1);

      // One commit signals both: the sum changed, the names did not.
      await db.player.update(ann, { score: 5 });
      await until(() => sums.length === 2);
      expect(sums[1]).toEqual({ sum: 5 });
      await quiet();
      expect(names.length).toBe(1);

      // A write outside the filter: not even a signal.
      await db.player.insert({ name: "zed", room: "elsewhere", score: 9 });
      await quiet();
      expect([names.length, sums.length]).toEqual([1, 2]);
      w1.close();
      w2.close();
    });

    test("close() stops it", async () => {
      const db = clients[transport];
      const seen: any[] = [];
      const w = db.player.where({ room: "closed" }).watch((rows: any) => seen.push(rows));
      await until(() => seen.length === 1);
      w.close();
      w.close();
      await db.player.insert({ name: "ann", room: "closed", score: 1 });
      await quiet();
      expect(seen.length).toBe(1);
    });

    test("a refused op goes to error; only reads can be watched", async () => {
      const db = clients[transport];
      const errors: any[] = [];
      const calls: any[] = [];
      const w = db.player.aggregate({ $sum: "nope" }).watch((r: any) => calls.push(r), { error: (e: unknown) => errors.push(e) });
      await until(() => errors.length === 1);
      expect(errors[0].kind).toBe("bad_request");
      expect(String(errors[0].message)).toContain("nope");
      expect(calls).toEqual([]);
      w.close();

      const insert = db.player.insert({ name: "never", room: "w", score: 0 });
      expect(() => insert.watch(() => {})).toThrow("only reads can be watched");
      expect(await db.player.where({ name: "never" }).count()).toBe(0);
    });

    test("a migration reaches the watch: a read it breaks goes to error", async () => {
      const { client, migrate } = await sync[transport]!(`migrated_${transport}`, SCHEMA);
      const seen: any[] = [];
      const errors: any[] = [];
      const w = client.player.where({ room: "m" }).order("score", "desc").select({ name: true })
        .watch((rows: any) => seen.push(rows), { error: (e: unknown) => errors.push(e) });
      await until(() => seen.length === 1);

      await migrate(SCHEMA.replace("  score  Int\n", ""));
      await until(() => errors.length === 1);
      expect(String(errors[0].message)).toContain("score");
      w.close();
    });
  });
}

test("signals during a read make one more read, not one each", async () => {
  // A transport whose signal the test holds: the coalescing is the query layer's, the same for every transport.
  let listener: any;
  let reads = 0;
  const transport = {
    exec: (op: any) => { reads++; return embedded.exec(op); },
    batch: (ops: any[]) => embedded.batch(ops),
    watch: (_op: any, l: any) => { listener = l; return { ready: Promise.resolve(), close() {} }; },
  };
  const db = mod.marcidb(transport);
  const seen: number[] = [];
  const w = db.player.where({ room: "coalesce" }).count().watch((n: number) => seen.push(n));
  await until(() => seen.length === 1);
  expect(reads).toBe(1);

  for (let i = 0; i < 5; i++) listener.changed();
  await quiet(100);
  expect(reads).toBe(3); // the first signal reads; the four during it, one more
  expect(seen).toEqual([0]); // and neither read changed anything

  await embedded.exec({ model: "Player", action: "insert", data: { name: "ann", room: "coalesce", score: 0 } });
  listener.changed();
  await until(() => seen.length === 2);
  expect(seen[1]).toBe(1);
  w.close();
  listener.changed();
  await quiet(100);
  expect(reads).toBe(4);
});

test("http: a server restart is re-subscribed and re-read, invisible to the caller", async () => {
  const dir = tempDir();
  let server = await startServer(RESTART_PORT, dir);
  await httpSync(RESTART_PORT, "r", SCHEMA);
  const db = mod.marcidb(`http://127.0.0.1:${RESTART_PORT}/r`);
  const seen: any[] = [];
  const errors: any[] = [];
  const w = db.player.where({ room: "r" }).select({ name: true }).watch((rows: any) => seen.push(rows), { error: (e: unknown) => errors.push(e) });
  await until(() => seen.length === 1);

  server.kill();
  await new Promise((r) => server.once("exit", r));
  await quiet(400);
  server = await startServer(RESTART_PORT, dir);

  // Re-subscribed: a write after the restart reaches the old watch; the re-read in between changed nothing.
  await quiet(1500);
  expect(seen.length).toBe(1);
  await db.player.insert({ name: "ann", room: "r", score: 0 });
  await until(() => seen.length === 2, 8000);
  expect(seen[1]).toEqual([{ name: "ann" }]);
  expect(errors).toEqual([]);
  w.close();
}, 20_000);

test("http: a watch the server never confirms fails with the reason", async () => {
  const errors: any[] = [];
  const db = mod.marcidb(`http://127.0.0.1:${PORT}/no_such_db`);
  const w = db.player.watch(() => {}, { error: (e: unknown) => errors.push(e) });
  await until(() => errors.length === 1);
  expect(errors[0].kind).toBe("not_found");
  w.close();
});
