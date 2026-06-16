// Unit tests for the binary decoder-compiler (`createDecoderRegistry` in marcidb-client/runtime) — the
// fiddliest part of the binary transport. Pure JS: hand-built MODELS + hand-built buffers, no FFI, no native
// lib. Covers the shape cache, the support gate (`build` → null), and the byte-level reader. Node + Bun.
import assert from "node:assert";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const runtimeUrl = `file://${path.resolve(HERE, "..", "..", "marcidb-client", "runtime", "index.js").replace(/\\/g, "/")}`;
const { createDecoderRegistry } = await import(runtimeUrl);

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
  const bytes = [];
  const dvbuf = new ArrayBuffer(8);
  const dv = new DataView(dvbuf);
  const push = (n) => { for (let i = 0; i < n; i++) bytes.push(dv.getUint8(i)); };
  return {
    u8: (v) => bytes.push(v & 0xff),
    u32(v) { dv.setUint32(0, v, true); push(4); },
    i64(v) { dv.setBigInt64(0, BigInt(v), true); push(8); },
    u64(v) { dv.setBigUint64(0, BigInt(v), true); push(8); },
    f64(v) { dv.setFloat64(0, v, true); push(8); },
    str(s) { const e = new TextEncoder().encode(s); this.u32(e.length); for (const b of e) bytes.push(b); },
    done: () => Uint8Array.from(bytes),
  };
}

let n = 0;
const ok = (label) => { n++; console.log(`  ✓ ${label}`); };

const { getDecoder, decodeBuffer } = createDecoderRegistry(MODELS);

// ── 1. shape cache & projection key ──
{
  const a = getDecoder("User", { name: true });
  assert.strictEqual(getDecoder("User", { name: true }), a, "same shape → cached same fn");
  assert.strictEqual(getDecoder("User", { age: true, name: true }), getDecoder("User", { name: true, age: true }), "select key order is irrelevant");
  assert.strictEqual(getDecoder("User", { name: true, $where: { age: 1 }, $order: { age: "asc" }, $limit: 5 }), a, "shape-irrelevant keys don't change the decoder");
  assert.notStrictEqual(getDecoder("User", { name: true, age: true }), a, "different projection → different fn");
  ok("shape cache & projection key");
}

// ── 2. support gate (build → null → JSON fallback) ──
{
  assert.strictEqual(getDecoder("User", { secret: true }), null, "unsupported scalar selected → null");
  assert.strictEqual(getDecoder("User", { secret: true }), null, "…and still null on the cached second call");
  assert.notStrictEqual(getDecoder("User", { name: true }), null, "…but unselected unsupported field is fine");
  assert.strictEqual(getDecoder("User", { name: true, posts: { $count: true } }), null, "aggregate include → null");
  assert.strictEqual(getDecoder("User", { name: true, author: { blob: true } }), null, "nested unsupported propagates → null");
  assert.notStrictEqual(getDecoder("User", { name: true, author: { title: true } }), null, "nested supported is fine");
  assert.strictEqual(getDecoder("Nope", { x: true }), null, "unknown model → null");
  ok("support gate");
}

// ── 3. scalars + null tags (findMany over 2 rows) ──
{
  const b = buf();
  b.u8(1); b.u32(2); // version, row_count
  // row 0: id=1, name="Alice", age=30, rating=0.5, active=true
  b.u64(1); b.u8(1); b.str("Alice"); b.u8(1); b.i64(30); b.u8(1); b.f64(0.5); b.u8(1); b.u8(1);
  // row 1: id=2, name="Bob", age=-5, rating=null, active=false
  b.u64(2); b.u8(1); b.str("Bob"); b.u8(1); b.i64(-5); b.u8(0); b.u8(1); b.u8(0);
  const dec = getDecoder("User", { id: true, name: true, age: true, rating: true, active: true });
  assert.deepStrictEqual(decodeBuffer(dec, b.done(), true), [
    { id: 1, name: "Alice", age: 30, rating: 0.5, active: true },
    { id: 2, name: "Bob", age: -5, rating: null, active: false },
  ]);
  ok("scalars + null tag (findMany)");
}

// ── 4. nested to-one (present + null) and to-many ──
{
  const dec = getDecoder("User", { id: true, name: true, author: { id: true, title: true }, posts: { id: true, title: true } });
  // row 0: author present, two posts
  const b = buf();
  b.u8(1); b.u32(2);
  b.u64(1); b.u8(1); b.str("Zoe");
  b.u8(1); b.u64(10); b.u8(1); b.str("P1");          // author present
  b.u32(2); b.u64(20); b.u8(1); b.str("P2"); b.u64(21); b.u8(1); b.str("P3"); // posts[2]
  // row 1: author null, zero posts
  b.u64(2); b.u8(1); b.str("Max");
  b.u8(0);                                            // author null
  b.u32(0);                                           // posts[]
  assert.deepStrictEqual(decodeBuffer(dec, b.done(), true), [
    { id: 1, name: "Zoe", author: { id: 10, title: "P1" }, posts: [{ id: 20, title: "P2" }, { id: 21, title: "P3" }] },
    { id: 2, name: "Max", author: null, posts: [] },
  ]);
  ok("nested to-one (present/null) + to-many");
}

// ── 5. findFirst: 0 rows → null, 1 row → object ──
{
  const dec = getDecoder("User", { id: true, name: true });
  const empty = buf(); empty.u8(1); empty.u32(0);
  assert.strictEqual(decodeBuffer(dec, empty.done(), false), null, "findFirst, 0 rows → null");
  const one = buf(); one.u8(1); one.u32(1); one.u64(7); one.u8(1); one.str("Solo");
  assert.deepStrictEqual(decodeBuffer(dec, one.done(), false), { id: 7, name: "Solo" }, "findFirst, 1 row → object");
  ok("findFirst 0/1 rows");
}

// ── 6. version mismatch is a hard error ──
{
  const dec = getDecoder("User", { id: true });
  const bad = buf(); bad.u8(2); bad.u32(0); // version 2
  assert.throws(() => decodeBuffer(dec, bad.done(), true), /unsupported binary result version 2/);
  ok("version mismatch throws");
}

// ── 7. empty projection decodes empty objects (matches the engine emitting {} for an empty select) ──
{
  const dec = getDecoder("User", {});
  assert.notStrictEqual(dec, null, "empty select still builds a decoder");
  const b = buf(); b.u8(1); b.u32(2); // 2 rows, no field bytes
  assert.deepStrictEqual(decodeBuffer(dec, b.done(), true), [{}, {}]);
  ok("empty projection → empty objects");
}

console.log(`[decoder-unit] OK — ${n} unit groups passed`);
