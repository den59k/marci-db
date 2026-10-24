// HTTP binary transport: the wire-level counterpart of `binary-parity.test.ts`. It launches the real
// `marcidb-server`, syncs the parity schema, and exercises the `marcidb-client` HTTP transport's binary read
// path against it. (It lives in the embedded test suite because that's where the codegen + runtime harness is
// already wired up; the feature under test is client↔server, not embedded.)
//
// Three things are pinned here:
//   1. Parity — every query deep-equals between the binary HTTP client and a forced-JSON client over one DB.
//   2. Negotiation — the server returns binary only for `Accept: <binary>` + a matching `X-Marci-Schema`;
//      a stale/absent hash, or no Accept (curl), falls back to JSON, still correct.
//   3. Handshake agreement — the hash the client bakes in is the one the server (which rebuilt its schema
//      from the stored snapshot) accepts, so the fast path actually engages.
import { test, expect, beforeAll, afterAll, describe } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync, spawn, type ChildProcess } from "node:child_process";

import { generateClient, schemaPath, clientSchemaHash } from "./helpers.ts";
import { MarciError, requestBinary } from "../../marcidb-client/runtime/index.js";

const REPO = path.resolve(import.meta.dir, "..", "..", "..");
const DB = "parity";
const PORT = 39817;
const BINARY_MEDIA_TYPE = "application/x-marcidb-rows";

let server: ChildProcess;
let dataDir: string;
let base: string; // `${origin}/${DB}`
let binaryClient: any;
let jsonClient: any;
let clientHash: string; // the fingerprint the generated client bakes in (`SCHEMA_HASH`)
const ids: Record<string, any> = {};

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

  // Sync the schema over HTTP.
  const schema = fs.readFileSync(schemaPath("parity.marci"), "utf8");
  const sync = await fetch(`${base}/$sync`, { method: "POST", body: schema });
  if (!sync.ok) throw new Error(`$sync failed: ${sync.status} ${await sync.text()}`);

  const { marcidb } = await generateClient(schemaPath("parity.marci"));
  clientHash = clientSchemaHash(schemaPath("parity.marci"));
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

test("the client's baked SCHEMA_HASH is accepted by a server that rebuilt its schema from the snapshot", async () => {
  // The server went schema text → `$sync` → stored snapshot → live schema; the client went schema text →
  // codegen. Binary coming back proves both ends fingerprint the same schema the same way.
  const out = await requestBinary(`${base}/User/findMany`, { id: true, name: true }, clientHash);
  expect(out).toBeInstanceOf(Uint8Array);
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
  const out = await requestBinary(`${base}/User/findMany`, { id: true, name: true, age: true }, clientHash);
  expect(out).toBeInstanceOf(Uint8Array);
  expect((out as Uint8Array)[0]).toBe(2); // BINARY_VERSION
});

test("stale schema hash → server falls back to JSON", async () => {
  const out = await requestBinary(`${base}/User/findMany`, { id: true, name: true }, "0000000000000000");
  expect(out).not.toBeInstanceOf(Uint8Array);
  expect((out as { json: any }).json).toBeInstanceOf(Array);
});

test("unsupported shape (enum payload) → server falls back to JSON even with a matching hash", async () => {
  const out = await requestBinary(`${base}/Account/findMany`, { id: true, label: true, kind: true }, clientHash);
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

// ── a server started with --token ─────────────────────────────────────────────────────────────

describe("a server started with --token", () => {
  const TOKEN = "s3cret";
  let tokenServer: ChildProcess;
  let tokenDir: string;
  let tokenBase: string;

  beforeAll(async () => {
    const exe = path.join(REPO, "target", "debug", process.platform === "win32" ? "marcidb-server.exe" : "marcidb-server");
    tokenDir = fs.mkdtempSync(path.join(os.tmpdir(), "marci-http-token-"));
    tokenServer = spawn(exe, ["--host", "127.0.0.1", "--port", String(PORT + 1), "--data", tokenDir, "--token", TOKEN], { stdio: "ignore" });
    const origin = `http://127.0.0.1:${PORT + 1}`;
    tokenBase = `${origin}/${DB}`;
    for (let i = 0; i < 200; i++) {
      try { await fetch(`${origin}/$health`); break; } catch { await Bun.sleep(50); }
      if (i === 199) throw new Error("token server did not become ready");
    }
    const schema = fs.readFileSync(schemaPath("parity.marci"), "utf8");
    const sync = await fetch(`${tokenBase}/$sync`, { method: "POST", body: schema, headers: { Authorization: `Bearer ${TOKEN}` } });
    if (!sync.ok) throw new Error(`$sync failed: ${sync.status} ${await sync.text()}`);
  });

  afterAll(() => {
    tokenServer?.kill();
    if (tokenDir) try { fs.rmSync(tokenDir, { recursive: true, force: true }); } catch { /* best-effort */ }
  });

  test("the typed client carries its token, the binary read path included", async () => {
    const { marcidb } = await generateClient(schemaPath("parity.marci"));
    const client = marcidb(tokenBase, { token: TOKEN });
    await client.user.insert({ name: "Tia", age: 7, score: 1, active: true });
    expect(await client.user.findMany({ name: true, age: true })).toEqual([{ name: "Tia", age: 7 }]);

    const binary = await requestBinary(`${tokenBase}/User/findMany`, { name: true, age: true }, clientHash, { token: TOKEN });
    expect(binary).toBeInstanceOf(Uint8Array);

    const err = await marcidb(tokenBase).user.findMany({ name: true }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(MarciError);
    expect((err as MarciError).kind).toBe("unauthorized");
    expect((err as MarciError).message).toBe("missing or invalid bearer token");
  });
});
