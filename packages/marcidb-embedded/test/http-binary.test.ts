// HTTP binary transport: the wire-level counterpart of `binary-parity.test.ts`. It launches the real
// `marcidb-server`, syncs the parity schema, and exercises the `marcidb-client` HTTP transport's binary read
// path against it. (It lives in the embedded test suite because that's where the codegen + runtime harness is
// already wired up; the feature under test is client↔server, not embedded.)
//
// Three things are pinned here:
//   1. Parity — every query deep-equals between the binary HTTP client and a forced-JSON client over one DB.
//   2. Negotiation — the server returns binary only for `Accept: <binary>` + a matching `X-Marci-Schema`;
//      a stale/absent hash, or no Accept (curl), falls back to JSON, still correct.
//   3. Handshake agreement — the hash the client bakes in equals the server's fingerprint of the same schema.
import { test, expect, beforeAll, afterAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync, spawn, type ChildProcess } from "node:child_process";

import { generateClient, schemaPath, clientSchemaHash } from "./helpers.ts";
import { requestBinary } from "../../marcidb-client/runtime/index.js";

const REPO = path.resolve(import.meta.dir, "..", "..", "..");
const DB = "parity";
const PORT = 39817;
const BINARY_MEDIA_TYPE = "application/x-marcidb-rows";

let server: ChildProcess;
let dataDir: string;
let base: string; // `${origin}/${DB}`
let binaryClient: any;
let jsonClient: any;
let serverHash: string;
const ids: Record<string, any> = {};

/** FNV-1a (64-bit), hex — a reference reimplementation of the Rust `schema_fingerprint`, used to derive the
 *  server's hash from its `$snapshot` text and cross-check that the two implementations agree. */
function fnv1a(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let hash = 0xcbf29ce484222325n;
  const mask = (1n << 64n) - 1n;
  for (const b of bytes) {
    hash = (hash ^ BigInt(b)) & mask;
    hash = (hash * 0x100000001b3n) & mask;
  }
  return hash.toString(16).padStart(16, "0");
}

/** A transport that always takes the plain-JSON path (no `queryBinary`) — the parity oracle. */
function jsonTransport(baseUrl: string): any {
  const post = async (route: string, body?: any) => {
    const res = await fetch(`${baseUrl}/${route}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
    return res.json();
  };
  return {
    exec(op: any) {
      switch (op.action) {
        case "findMany": return post(`${op.model}/findMany`, op.query);
        case "findFirst": return post(`${op.model}/findFirst`, op.query);
        case "insert": return post(`${op.model}/insert`, op.data);
        case "count": return post(`${op.model}/count`, op.query ?? {});
        case "aggregate": return post(`${op.model}/aggregate`, op.query);
        default: throw new Error(`jsonTransport: unhandled action '${op.action}'`);
      }
    },
    batch(ops: any[]) { return post(`$transaction`, ops); },
  };
}

beforeAll(async () => {
  // Build + launch the server (debug — fast to compile, the binary path is identical to release).
  const b = spawnSync("cargo", ["build", "-q", "-p", "marcidb-server"], { cwd: REPO, stdio: "inherit" });
  if (b.status !== 0) throw new Error(`marcidb-server build failed (exit ${b.status})`);
  const exe = path.join(REPO, "target", "debug", process.platform === "win32" ? "marcidb-server.exe" : "marcidb-server");

  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "marci-http-"));
  server = spawn(exe, ["--host", "127.0.0.1", "--port", String(PORT), "--data", dataDir], { stdio: "ignore" });

  const origin = `http://127.0.0.1:${PORT}`;
  base = `${origin}/${DB}`;

  // Wait until the server accepts connections (any HTTP status means it's up).
  for (let i = 0; i < 200; i++) {
    try { await fetch(`${origin}/${DB}/$snapshot`); break; } catch { await Bun.sleep(50); }
    if (i === 199) throw new Error("server did not become ready");
  }

  // Sync the schema over HTTP, then derive the server's fingerprint from its snapshot.
  const schema = fs.readFileSync(schemaPath("parity.marci"), "utf8");
  const sync = await fetch(`${base}/$sync`, { method: "POST", body: schema });
  if (!sync.ok) throw new Error(`$sync failed: ${sync.status} ${await sync.text()}`);
  serverHash = fnv1a(await (await fetch(`${base}/$snapshot`)).text());

  const { marcidb } = await generateClient(schemaPath("parity.marci"));
  binaryClient = marcidb(base);           // HTTP transport → binary fast path (httpTransport has queryBinary)
  jsonClient = marcidb(jsonTransport(base)); // forced JSON — the oracle

  // Seed (writes go over JSON either way).
  ids.u1 = await jsonClient.user.insert({ name: "Alice", age: 30, rating: 0.1, score: 3.141592653589793, active: true, seen: 1700000000000 });
  ids.u2 = await jsonClient.user.insert({ name: "Bob", age: 25, score: 2.5, active: false });
  await jsonClient.user.insert({ name: "Carol", age: 40, rating: 1.5, score: 0.0, active: true, seen: 0 });

  await jsonClient.post.insert({ title: "Hello", views: 10, author: ids.u1 });
  await jsonClient.post.insert({ title: "World", views: 20, author: ids.u1 });
  await jsonClient.post.insert({ title: "Orphan", views: 0 });
  await jsonClient.post.insert({ title: "Bob's", views: 5, author: ids.u2 });

  await jsonClient.account.insert({ label: "free", kind: "basic" });
  await jsonClient.account.insert({ label: "team", kind: "pro", seats: 9 });
});

afterAll(() => {
  server?.kill();
  if (dataDir) try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
});

// ── handshake agreement ──────────────────────────────────────────────────────────────────────

test("client's baked SCHEMA_HASH matches the server's fingerprint of the same schema", () => {
  expect(clientSchemaHash(schemaPath("parity.marci"))).toBe(serverHash);
});

// ── parity (binary HTTP vs forced JSON) ──────────────────────────────────────────────────────

const cases: [string, (c: any) => Promise<any>][] = [
  ["user all scalars", (c) => c.user.findMany({ id: true, name: true, age: true, rating: true, score: true, active: true, seen: true })],
  ["user projection (no id)", (c) => c.user.findMany({ name: true, age: true })],
  ["user findFirst by id", (c) => c.user.findFirst({ id: true, name: true, score: true, $where: { id: ids.u1.id } })],
  ["user findFirst null", (c) => c.user.findFirst({ id: true, $where: { id: 99999 } })],
  ["user where + order", (c) => c.user.findMany({ id: true, name: true, $where: { active: true }, $order: { age: "desc" } })],
  ["post nested author obj", (c) => c.post.findMany({ id: true, title: true, views: true, author: { id: true, name: true, active: true } })],
  ["post nested null author", (c) => c.post.findMany({ title: true, author: { name: true } })],
  ["user to-many posts", (c) => c.user.findMany({ id: true, name: true, posts: { id: true, title: true, views: true } })],
  // Enum-payload model → binary gates out server-side (shape unsupported) → JSON fallback. Still identical.
  ["account (enum fallback)", (c) => c.account.findMany({ id: true, label: true, kind: true })],
];

for (const [label, run] of cases) {
  test(`parity: ${label}`, async () => {
    const [bin, json] = await Promise.all([run(binaryClient), run(jsonClient)]);
    expect(bin).toEqual(json);
  });
}

test("binary client decoded concrete values (not silently empty)", async () => {
  const alice = await binaryClient.user.findFirst({ name: true, rating: true, score: true, active: true, seen: true, $where: { id: ids.u1.id } });
  expect(alice).toEqual({ name: "Alice", rating: 0.1, score: 3.141592653589793, active: true, seen: 1700000000000 });
});

// ── negotiation (the server's content-type decision) ─────────────────────────────────────────

test("matching hash + supported shape → server returns binary", async () => {
  const out = await requestBinary(`${base}/User/findMany`, { id: true, name: true, age: true }, serverHash);
  expect(out).toBeInstanceOf(Uint8Array);
  expect((out as Uint8Array)[0]).toBe(1); // BINARY_VERSION
});

test("stale schema hash → server falls back to JSON", async () => {
  const out = await requestBinary(`${base}/User/findMany`, { id: true, name: true }, "0000000000000000");
  expect(out).not.toBeInstanceOf(Uint8Array);
  expect((out as { json: any }).json).toBeInstanceOf(Array);
});

test("unsupported shape (enum payload) → server falls back to JSON even with a matching hash", async () => {
  const out = await requestBinary(`${base}/Account/findMany`, { id: true, label: true, kind: true }, serverHash);
  expect(out).not.toBeInstanceOf(Uint8Array);
  expect((out as { json: any }).json).toBeInstanceOf(Array);
});

test("curl smoke: no Accept header → JSON, never binary", async () => {
  const res = await fetch(`${base}/User/findMany`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ id: true, name: true }),
  });
  expect(res.ok).toBe(true);
  expect(res.headers.get("content-type") ?? "").not.toContain(BINARY_MEDIA_TYPE);
  const body = await res.json();
  expect(body).toBeInstanceOf(Array);
});
