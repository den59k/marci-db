// Unit tests for the binary decoder-compiler (`createDecoderRegistry` in marcidb-client/runtime) — the
// fiddliest part of the binary transport. Pure JS: hand-built MODELS + hand-built buffers, no FFI, no native
// lib. Covers the shape cache, the support gate (`build` → null), and the byte-level reader.
import { test, expect } from "bun:test";
import path from "node:path";

const runtimeUrl = `file://${path.resolve(import.meta.dir, "..", "..", "marcidb-client", "runtime", "index.js").replace(/\\/g, "/")}`;
const { createDecoderRegistry } = (await import(runtimeUrl)) as any;

// Mirror of the codegen `MODELS` metadata, hand-written so the test is independent of codegen.
const MODELS = {
  User: [
    { n: "id", k: "key", t: "u64" },
    { n: "name", k: "body", t: "str" },
    { n: "age", k: "body", t: "i64" },
    { n: "rating", k: "body", t: "f64" },
    { n: "active", k: "body", t: "bool" },
    { n: "secret", k: "body", t: null }, // unsupported scalar → gate falls back
    { n: "author", k: "one", m: "Post" },
    { n: "posts", k: "many", m: "Post" },
  ],
  Post: [
    { n: "id", k: "key", t: "u64" },
    { n: "title", k: "body", t: "str" },
    { n: "blob", k: "body", t: null }, // unsupported, for nested-propagation test
  ],
};

// ── byte buffer builder matching the wire format (little-endian) ──
function buf() {
  const bytes: number[] = [];
  const dv = new DataView(new ArrayBuffer(8));
  const push = (n: number) => { for (let i = 0; i < n; i++) bytes.push(dv.getUint8(i)); };
  return {
    u8: (v: number) => bytes.push(v & 0xff),
    u32(v: number) { dv.setUint32(0, v, true); push(4); },
    i64(v: number | bigint) { dv.setBigInt64(0, BigInt(v), true); push(8); },
    u64(v: number | bigint) { dv.setBigUint64(0, BigInt(v), true); push(8); },
    f64(v: number) { dv.setFloat64(0, v, true); push(8); },
    str(s: string) { const e = new TextEncoder().encode(s); this.u32(e.length); for (const b of e) bytes.push(b); },
    done: () => Uint8Array.from(bytes),
  };
}

const { getDecoder, decodeBuffer } = createDecoderRegistry(MODELS);

test("shape cache & projection key", () => {
  const a = getDecoder("User", { name: true });
  expect(getDecoder("User", { name: true })).toBe(a); // same shape → cached same fn
  expect(getDecoder("User", { age: true, name: true })).toBe(getDecoder("User", { name: true, age: true })); // key order irrelevant
  expect(getDecoder("User", { name: true, $where: { age: 1 }, $order: { age: "asc" }, $limit: 5 })).toBe(a); // shape-irrelevant keys
  expect(getDecoder("User", { name: true, age: true })).not.toBe(a); // different projection → different fn
});

test("support gate falls back to JSON (null) for shapes binary can't decode", () => {
  expect(getDecoder("User", { secret: true })).toBeNull(); // unsupported scalar
  expect(getDecoder("User", { secret: true })).toBeNull(); // …still null on the cached call
  expect(getDecoder("User", { name: true })).not.toBeNull(); // unselected unsupported field is fine
  expect(getDecoder("User", { name: true, posts: { $count: true } })).toBeNull(); // aggregate include
  expect(getDecoder("User", { name: true, author: { blob: true } })).toBeNull(); // nested unsupported propagates
  expect(getDecoder("User", { name: true, author: { title: true } })).not.toBeNull(); // nested supported
  expect(getDecoder("Nope", { x: true })).toBeNull(); // unknown model
  // Stale client: selecting a field the descriptors don't know (e.g. added by a later migration) → JSON,
  // and it must not collide with the {name} cache entry.
  expect(getDecoder("User", { name: true, addedLater: true })).toBeNull();
  expect(getDecoder("User", { name: true })).not.toBeNull();
});

test("scalars + null tags (findMany over 2 rows)", () => {
  const b = buf();
  b.u8(1); b.u32(2); // version, row_count
  // row 0: id=1, name="Alice", age=30, rating=0.5, active=true
  b.u64(1); b.u8(1); b.str("Alice"); b.u8(1); b.i64(30); b.u8(1); b.f64(0.5); b.u8(1); b.u8(1);
  // row 1: id=2, name="Bob", age=-5, rating=null, active=false
  b.u64(2); b.u8(1); b.str("Bob"); b.u8(1); b.i64(-5); b.u8(0); b.u8(1); b.u8(0);
  const dec = getDecoder("User", { id: true, name: true, age: true, rating: true, active: true });
  expect(decodeBuffer(dec, b.done(), true)).toEqual([
    { id: 1, name: "Alice", age: 30, rating: 0.5, active: true },
    { id: 2, name: "Bob", age: -5, rating: null, active: false },
  ]);
});

test("nested to-one (present/null) + to-many", () => {
  const dec = getDecoder("User", { id: true, name: true, author: { id: true, title: true }, posts: { id: true, title: true } });
  const b = buf();
  b.u8(1); b.u32(2);
  // row 0: author present, two posts
  b.u64(1); b.u8(1); b.str("Zoe");
  b.u8(1); b.u64(10); b.u8(1); b.str("P1");
  b.u32(2); b.u64(20); b.u8(1); b.str("P2"); b.u64(21); b.u8(1); b.str("P3");
  // row 1: author null, zero posts
  b.u64(2); b.u8(1); b.str("Max");
  b.u8(0);
  b.u32(0);
  expect(decodeBuffer(dec, b.done(), true)).toEqual([
    { id: 1, name: "Zoe", author: { id: 10, title: "P1" }, posts: [{ id: 20, title: "P2" }, { id: 21, title: "P3" }] },
    { id: 2, name: "Max", author: null, posts: [] },
  ]);
});

test("findFirst: 0 rows → null, 1 row → object", () => {
  const dec = getDecoder("User", { id: true, name: true });
  const empty = buf(); empty.u8(1); empty.u32(0);
  expect(decodeBuffer(dec, empty.done(), false)).toBeNull();
  const one = buf(); one.u8(1); one.u32(1); one.u64(7); one.u8(1); one.str("Solo");
  expect(decodeBuffer(dec, one.done(), false)).toEqual({ id: 7, name: "Solo" });
});

test("version mismatch is a hard error", () => {
  const dec = getDecoder("User", { id: true });
  const bad = buf(); bad.u8(2); bad.u32(0); // version 2
  expect(() => decodeBuffer(dec, bad.done(), true)).toThrow(/unsupported binary result version 2/);
});

test("empty projection → empty objects", () => {
  const dec = getDecoder("User", {});
  expect(dec).not.toBeNull();
  const b = buf(); b.u8(1); b.u32(2); // 2 rows, no field bytes
  expect(decodeBuffer(dec, b.done(), true)).toEqual([{}, {}]);
});
