// The maintenance port: `db.listen()` serves the server's HTTP API for the open database, so `fetch`, the
// typed HTTP client and the CLI reach the live data while the app keeps using it in-process.
import { test, expect, beforeAll, afterAll } from "bun:test";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";

import { openTestDatabase, restoreDatabase, MarciEmbeddedError } from "../dist/index.js";
import { MarciError } from "../../marcidb-client/runtime/index.js";
import { clientSchemaHash, generateClient, schemaPath } from "./helpers.ts";

const SCHEMA = fs.readFileSync(schemaPath("schema.marci"), "utf8");
const work = fs.mkdtempSync(path.join(os.tmpdir(), "marci-listen-"));
const count = { model: "User", action: "count", query: {} };

let mod: any;

beforeAll(async () => {
  mod = await generateClient(schemaPath("schema.marci"));
});
afterAll(() => fs.rmSync(work, { recursive: true, force: true }));

/** A refused call's error, for asserting on it. */
function refusal(f: () => unknown): MarciEmbeddedError {
  try {
    f();
  } catch (e) {
    expect(e).toBeInstanceOf(MarciEmbeddedError);
    return e as MarciEmbeddedError;
  }
  throw new Error("expected the call to be refused");
}

test("the typed HTTP client reads what the app wrote in-process, and the other way round", async () => {
  const db = await openTestDatabase(SCHEMA);
  const srv = db.listen({ port: 0, name: "t" });
  try {
    expect(srv.port).toBeGreaterThan(0);
    expect(srv.url).toBe(`http://127.0.0.1:${srv.port}/t`);
    const health = await fetch(new URL("/$health", srv.url));
    expect(health.status).toBe(200);
    expect((await health.json()).ok).toBe(true);

    await mod.marcidb(db).user.insert({ name: "Ann", age: 30 });
    const overHttp = mod.marcidb(srv.url);
    expect(await overHttp.user.findMany({ name: true, age: true })).toEqual([{ name: "Ann", age: 30 }]);
    await overHttp.user.insert({ name: "Bob", age: 40 });
    expect(await db.exec(count)).toBe(2);
  } finally {
    srv.close();
    db.close();
  }
});

test("a backup taken over the port restores with restoreDatabase; replacing the database is refused", async () => {
  const db = await openTestDatabase(SCHEMA);
  const srv = db.listen({ port: 0, name: "t" });
  try {
    await db.batch(Array.from({ length: 50 }, (_, i) => ({ model: "User", action: "insert", data: { name: `u${i}`, age: i } })));
    const res = await fetch(`${srv.url}/$backup`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("application/x-marcidb-backup");
    const file = path.join(work, "port.marcibin");
    fs.writeFileSync(file, new Uint8Array(await res.arrayBuffer()));

    const restored = restoreDatabase(path.join(work, "from-port"), file, { disableFsync: true });
    try {
      expect(await restored.exec(count)).toBe(50);
    } finally {
      restored.close();
    }

    const restore = await fetch(`${srv.url}/$restore`, { method: "POST", body: fs.readFileSync(file) });
    expect(restore.status).toBe(400);
    expect((await restore.json()).error).toContain("restoreDatabase");
    const other = await fetch(`http://127.0.0.1:${srv.port}/other/User/count`, { method: "POST", body: "{}" });
    expect(other.status).toBe(404);
    expect(await db.exec(count)).toBe(50);
  } finally {
    srv.close();
    db.close();
  }
});

test("srv.close() closes the port, and db.close() with a port open (and a kept-alive client) returns promptly", async () => {
  const db = await openTestDatabase(SCHEMA);
  const first = db.listen({ port: 0, name: "t" });
  first.close();
  first.close(); // idempotent
  await expect(fetch(new URL("/$health", first.url))).rejects.toThrow();

  const second = db.listen({ port: 0, name: "t" });
  expect((await fetch(new URL("/$health", second.url))).status).toBe(200); // leaves a pooled connection
  const started = performance.now();
  db.close();
  expect(performance.now() - started).toBeLessThan(2000);
  await expect(fetch(new URL("/$health", second.url))).rejects.toThrow();
  second.close(); // after db.close(): nothing left to stop
}, 20000);

test("listen checks its options, and a token guards every route but the health check", async () => {
  const db = await openTestDatabase(SCHEMA);
  try {
    expect(refusal(() => db.listen({ port: 0, name: "a/b" })).kind).toBe("bad_request");
    expect(refusal(() => db.listen({ port: 0, host: "0.0.0.0", name: "t" })).message).toContain("token");

    // The default name is the directory's.
    const named = db.listen({ port: 0 });
    expect(named.url).toBe(`http://127.0.0.1:${named.port}/${path.basename(db.path)}`);
    expect(refusal(() => db.listen({ port: 0, name: "t" })).message).toContain("already listens");
    named.close();

    const srv = db.listen({ port: 0, name: "t", token: "s3cret" });
    expect((await fetch(new URL("/$health", srv.url))).status).toBe(200);
    expect((await fetch(`${srv.url}/User/count`, { method: "POST", body: "{}" })).status).toBe(401);
    const authed = await fetch(`${srv.url}/User/count`, { method: "POST", body: "{}", headers: { Authorization: "Bearer s3cret" } });
    expect(await authed.json()).toBe(0);
    srv.close();
  } finally {
    db.close();
  }
});

/** Records every request's headers and its response's content type, until `restore()`. */
function spyFetch() {
  const original = globalThis.fetch;
  const seen: { url: string; headers: Record<string, string>; contentType: string }[] = [];
  globalThis.fetch = (async (input: any, init?: any) => {
    const res = await original(input, init);
    seen.push({ url: String(input), headers: { ...(init?.headers ?? {}) }, contentType: res.headers.get("content-type") ?? "" });
    return res;
  }) as typeof fetch;
  return { seen, restore: () => { globalThis.fetch = original; } };
}

/** The headers the HTTP transport sends without options — unchanged by the token option. */
const JSON_HEADERS = { "Content-Type": "application/json" };
const binaryHeaders = () => ({
  "Content-Type": "application/json",
  Accept: "application/x-marcidb-rows, application/json",
  "X-Marci-Schema": clientSchemaHash(schemaPath("schema.marci")),
});

test("marcidb(url, { token }) sends the token on the JSON and the binary path; without it, nothing changes", async () => {
  const db = await openTestDatabase(SCHEMA);
  const srv = db.listen({ port: 0, name: "t", token: "s3cret" });
  const spy = spyFetch();
  try {
    const client = mod.marcidb(srv.url, { token: "s3cret" });
    await client.user.insert({ name: "Ann", age: 30 });
    expect(await client.user.findMany({ name: true, age: true })).toEqual([{ name: "Ann", age: 30 }]);

    const [insert, read] = spy.seen;
    expect(insert!.headers).toEqual({ ...JSON_HEADERS, Authorization: "Bearer s3cret" });
    expect(read!.headers).toEqual({ ...binaryHeaders(), Authorization: "Bearer s3cret" });
    expect(read!.contentType).toBe("application/x-marcidb-rows"); // the select really took the binary path

    // No token, or a wrong one: the server's refusal, typed — on either path.
    for (const refused of [mod.marcidb(srv.url), mod.marcidb(srv.url, { token: "wrong" })]) {
      for (const call of [() => refused.user.insert({ name: "Eve", age: 1 }), () => refused.user.findMany({ name: true, age: true })]) {
        const err = await call().catch((e: unknown) => e);
        expect(err).toBeInstanceOf(MarciError);
        expect((err as MarciError).kind).toBe("unauthorized");
        expect((err as MarciError).status).toBe(401);
        expect((err as MarciError).message).toBe("missing or invalid bearer token");
      }
    }
    expect(await db.exec(count)).toBe(1);
    srv.close();

    // Without a token the requests are exactly what they were before the option existed.
    const open = db.listen({ port: 0, name: "t" });
    spy.seen.length = 0;
    const plain = mod.marcidb(open.url);
    await plain.user.insert({ name: "Bob", age: 40 });
    expect((await plain.user.findMany({ name: true })).length).toBe(2);
    expect(spy.seen.map((r) => r.headers)).toEqual([JSON_HEADERS, binaryHeaders()]);
    open.close();
  } finally {
    spy.restore();
    srv.close();
    db.close();
  }
});

test("options with a transport object are refused", async () => {
  const db = await openTestDatabase(SCHEMA);
  try {
    expect(() => mod.marcidb(db, { token: "s3cret" })).toThrow(TypeError);
    expect(() => mod.marcidb(db)).not.toThrow();
  } finally {
    db.close();
  }
});
