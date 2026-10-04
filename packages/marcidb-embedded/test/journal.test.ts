// `db.<model>.$journal(name, { on })` over the generated client, on both transports: the embedded engine
// and the real `marcidb-server` over HTTP. What is pinned: a cascade reaches the journal, the loop confirms
// what it got past and nothing else (a `break` and a throw both leave the rest for the next reader), a
// waiting loop is woken by a commit, and `drop()` ends the journal.
import { test, expect, beforeAll, afterAll, describe } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync, spawn, type ChildProcess } from "node:child_process";

import { openTestDatabase, type TestDatabase } from "../dist/index.js";
import { generateClient, schemaPath } from "./helpers.ts";

const REPO = path.resolve(import.meta.dir, "..", "..", "..");
const PORT = 39818;

let embedded: TestDatabase;
let server: ChildProcess;
let dataDir: string;
const clients: Record<string, any> = {};

beforeAll(async () => {
  const schema = fs.readFileSync(schemaPath("journal.marci"), "utf8");
  const mod = await generateClient(schemaPath("journal.marci"));

  embedded = await openTestDatabase(schema);
  clients.embedded = mod.marcidb(embedded);

  const build = spawnSync("cargo", ["build", "-q", "-p", "marcidb-server"], { cwd: REPO, stdio: "inherit" });
  if (build.status !== 0) throw new Error("marcidb-server build failed");
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "marci-journal-"));
  const exe = path.join(REPO, "target", "debug", process.platform === "win32" ? "marcidb-server.exe" : "marcidb-server");
  server = spawn(exe, ["--port", String(PORT), "--data", dataDir], { stdio: "ignore" });
  const origin = `http://127.0.0.1:${PORT}`;
  for (let i = 0; ; i++) {
    try { if ((await fetch(`${origin}/$health`)).ok) break; } catch {}
    if (i > 100) throw new Error("marcidb-server did not start");
    await new Promise((r) => setTimeout(r, 50));
  }
  const synced = await fetch(`${origin}/journal/$sync`, { method: "POST", body: schema });
  if (!synced.ok) throw new Error(`$sync failed: ${await synced.text()}`);
  clients.http = mod.marcidb(`${origin}/journal`);
}, 120_000);

afterAll(() => {
  embedded?.close();
  server?.kill();
  if (dataDir) fs.rmSync(dataDir, { recursive: true, force: true });
});

/** Reads the journal through, without waiting. */
async function drain(journal: AsyncIterable<any>): Promise<any[]> {
  const out = [];
  for await (const change of journal) out.push(change);
  return out;
}

for (const transport of ["embedded", "http"]) {
  describe(transport, () => {
    test("a cascade is journaled; the loop confirms what it got past", async () => {
      const db = clients[transport];
      const name = "files";
      // Opened before the deletes; read after them.
      const journal = db.file.$journal(name, { on: "delete", wait: false });
      expect(await drain(journal)).toEqual([]);

      const ann = await db.user.insert({ name: "ann" });
      const post = await db.post.insert({ title: "hi", author: ann });
      const avatar = await db.file.insert({ blob: "avatar", size: 1, user: ann });
      await db.file.insert({ blob: "photo-1", size: 2, post });
      await db.file.insert({ blob: "photo-2", size: 3, post });
      const loose = await db.file.insert({ blob: "loose", size: 4 });

      // One delete: the user's file, and the files of the user's posts.
      await db.user.delete(ann);
      await db.file.delete(loose);

      // Stop after the first entry: only that one is confirmed.
      const seen: string[] = [];
      for await (const change of db.file.$journal(name, { on: "delete", wait: false })) {
        expect(change.op).toBe("delete");
        seen.push(change.row.blob);
        if (seen.length === 2) break; // the body of the second did not finish
      }
      expect(seen.length).toBe(2);

      // A loop that throws confirms nothing more.
      const failed = (async () => {
        for await (const _ of db.file.$journal(name, { on: "delete", wait: false })) throw new Error("boom");
      })();
      expect(failed).rejects.toThrow("boom");
      await failed.catch(() => {});

      const rest = await drain(db.file.$journal(name, { on: "delete", wait: false }));
      expect(rest.length).toBe(3);
      expect([seen[0], ...rest.map((c) => c.row.blob)].sort()).toEqual(["avatar", "loose", "photo-1", "photo-2"]);
      expect(rest[0].row).toEqual({ id: expect.anything(), blob: rest[0].row.blob, size: expect.any(Number) });
      expect(avatar.id).toBeDefined();

      // Read through and confirmed: nothing is left.
      expect(await drain(db.file.$journal(name, { on: "delete", wait: false }))).toEqual([]);
    });

    test("a waiting loop is woken by a commit; drop() ends the journal", async () => {
      const db = clients[transport];
      const journal = db.file.$journal("waiting", { on: ["delete"] });
      const first = (async () => {
        for await (const change of journal) return change.row.blob;
      })();

      await new Promise((r) => setTimeout(r, 150));
      const file = await db.file.insert({ blob: "late", size: 1 });
      await db.file.delete(file);
      expect(await first).toBe("late");

      await journal.drop();
      // The name is free again: a new journal of it starts empty.
      expect(await drain(db.file.$journal("waiting", { on: "delete", wait: false }))).toEqual([]);
      await db.file.$journal("waiting", { on: "delete" }).drop();
    });

    test("an operation that is not journaled is refused", async () => {
      const db = clients[transport];
      expect(drain(db.file.$journal("bad", { on: "update", wait: false }))).rejects.toThrow("only 'delete' is supported");
    });
  });
}
