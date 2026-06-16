# PLAN — Binary transport for query results

Replace JSON-over-FFI for **query results** with a compact binary format decoded by **shape-specialized,
cached, JIT-friendly functions** on the TS side, reusing the engine's **existing slot-based row encoding**
on the Rust side.

## Why

Benchmarks ([docs/BENCHMARKS.md](docs/BENCHMARKS.md)) show marcidb wins batched writes and count, ties
single-row writes, but every **read** pays a JSON tax — `select all` ~5–8× slower than SQLite, point reads
~2×, nested select (beats N+1, loses to a hand-tuned JOIN), index filter ~2–3×. Two costs stack up:

1. The result is **serialized to JSON in Rust and parsed in JS**, O(rows) on both sides.
2. On the Rust side it's even serialized **twice** — the decode layer builds a JSON string, which is parsed
   into a `serde_json::Value` ([`batch.rs`](marcidb/src/batch.rs) `json_value(...)`) and then re-serialized
   into the FFI envelope ([`marcidb-ffi/src/lib.rs`](marcidb-ffi/src/lib.rs) `ok_envelope`).

Binary removes both. Writes/inputs are tiny and marcidb already wins there, so **this plan is read-only**:
`findMany` / `findFirst` result decoding. Inputs (op descriptors, insert/update payloads) and small results
(`insert` id, `count`, `aggregate`) stay JSON for now.

## Core idea

- **The `select` is the schema for the result buffer.** A decoder is a function of the *result shape*
  (projection + nesting), not of the model. `findMany({name:true})` and
  `findMany({name:true, author:{email:true}})` have different layouts.
- **Canonical field order = schema slot order**, known to both sides independently (engine from the schema,
  TS from per-model metadata the codegen emits). This avoids depending on JS object key order or JSON
  round-tripping the select's key order — the encoder writes in slot order, the decoder reads in slot order,
  they agree by construction.
- **Compile once per shape, cache, reuse.** On a `findMany(select)`, derive a shape key → look up a cached
  decoder → compile only on a miss (recursively for nested relations). The hot per-row loop is then
  monomorphic straight-line code the JIT fully optimizes. Compiling per call would pay compilation *and*
  hand the JIT a cold function every time — the cache is the whole point. Apps have a small fixed set of
  shapes, so the cache warms instantly.
- **Engine reuses the existing row decode**, emitting binary instead of a JSON string — same slot walk
  ([`query_op.rs`](marcidb/src/query_op.rs) `decode_row`, [`json_parsers.rs`](marcidb/src/json_parsers.rs)
  `decode_document`), new sink.

---

## Part 1 — Row-sequential binary results (MVP)

Related rows are **repeated** inline (no dedup yet). This captures most of the win and proves the
encoder/decoder contract and the codec-cache machinery.

### Wire format (result buffer)

```
[u8 version][u32 row_count][row 0][row 1] … [row N-1]
```

Each **row** = the selected fields in **slot order**, decoded by a forward cursor (rows are *not*
fixed-width because of strings, so no random indexing — a sequential walk, which is exactly the
"apply the decoder to each element" loop):

- **Null bitmap** (ceil(nullable_selected_fields / 8) bytes) for the nullable fields in this shape.
- **Fixed-width scalars** (`Int`/`UInt` of each width, `Float`, `Bool`, `Byte[N]`): raw little-endian.
- **Variable-width** (`String`, byte blobs): `u32 len` + bytes (UTF-8 for strings).
- **Enum-with-payload**: `u8 variant tag` + the variant's payload fields in slot order (see
  [[marcidb-enum-semantics]]).
- **Nested to-one relation**: presence is covered by the null bitmap; if present, the related row encoded
  inline (recursively, per its sub-shape).
- **Nested to-many / list**: `u32 count` + that many sub-rows inline.

Little-endian fixed; `version` byte gates future format changes.

### FFI / envelope

New entry `marci_query_binary(handle, op_json) -> ptr` returning a length-framed blob:

```
[u8 status]  status 0 = ok  → [u32 len][binary result buffer]
             status 1 = err → [utf-8 message]      (kind carried as today)
```

Keep `marci_exec` (JSON) for everything else; `marci_query_binary` is read-only. The TS side reads the
status byte and either decodes binary or throws a `MarciEmbeddedError`.

### Engine (Rust, `marcidb` crate)

- New `binary_encode.rs`: given a `QueryOp` + the stored row, walk the **same** select the JSON path walks
  and write the layout above into a `Vec<u8>`. Reuse the slot-reading logic; swap the JSON sink for a byte
  sink. This also removes the double-serialize (no string → `Value` → re-serialize).
- A `query_binary(db, &QueryOp) -> Vec<u8>` engine entry that runs the query and encodes the result set.
- `marcidb-ffi`: `marci_query_binary` wraps it (panic-isolated, status-framed).

### TS side (`marcidb-client` + `marcidb-embedded`)

- **Codegen metadata** (`marcidb-ts`): emit a per-model **field descriptor** — slot order, type+width,
  nullability, and relation target model — that the decoder-compiler consumes. Static, generated once.
- **Decoder-compiler** (runtime): `compileDecoder(model, select) -> (view, cursor) => object`, recursive
  for nested relations, built via `new Function` for tight JIT code (Node/Bun, no CSP concerns). Cache keyed
  by a canonical shape key (`model` + structurally-normalized select).
- **Transport capability**: the embedded transport gains `queryBinary(op) -> Uint8Array`. HTTP transport
  does not implement it (stays JSON).
- **Client wiring** (`prefix.js`): `findMany`/`findFirst` use the binary path **iff** `transport.queryBinary`
  exists — compile/cache the decoder from the select, call `queryBinary`, loop-decode `row_count` rows into
  an array (or 0/1 for `findFirst`). Otherwise fall back to `transport.exec` (JSON). Decode lives in the
  client (it has the metadata); the transport only moves bytes.

### Type coverage (the bulk of the work)

The binary path must handle **every** `FieldType` the JSON path does, with **identical** output:
`Int`/`UInt` widths, `Float`, `Bool`, `String`, `Byte[N]` + `@format(uuid/hex)`, dates/timestamps, refs
(`@id` shapes incl. composite ids), lists, structs (synthesized child models), and enum-with-payload.
Ship incrementally (scalars → nullable → strings → nested-repeat → enums/lists/structs); fall back to JSON
for any shape not yet supported, so rollout is safe.

> 64-bit `Int`/`UInt` exceed JS `number` (f64) precision. The JSON path has the same limitation (JSON
> numbers are f64), so Part 1 keeps parity (`Number`); revisit `BigInt` for exact 64-bit later.

### Correctness — parity is mandatory

A **cross-check** harness runs each query through both transports and asserts deep-equal JSON-vs-binary
results. Wire it over the existing engine/embedded tests and the benchmark queries; this is the gate for
turning binary on by default.

### Benchmark

Re-run [packages/benchmarks](packages/benchmarks) with binary on; expect the read rows (`select all`,
point, nested, index filter) to close most of the gap to SQLite. Update [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

### Part 1 checklist — **DONE**

- [x] `binary_encode.rs` + `query_binary` engine entry — 8 primitives + nullable + strings + nested
      repeat (to-one/to-many). Formats/enums/lists/composite-keys are gated to JSON fallback via
      `shape_supported` (a clean follow-up slice), not yet covered.
- [x] `marci_query_binary` FFI (status-framed `[u32 n][u8 status][payload]` blob, panic-isolated) +
      `marci_free_buffer`. status 2 = "unsupported" → client falls back to JSON.
- [x] Codegen: per-model field descriptors (`MODELS`) emitted into the generated client.
- [x] Decoder-compiler + shape-keyed cache. **Built with closure composition, not `new Function`** — it
      already removes the JSON tax (direct typed reads); `new Function` is a later perf lever if profiling
      asks for it. Lives in `marcidb-client` runtime (`createDecoderRegistry`).
- [x] Embedded transport `queryBinary`; `findMany`/`findFirst` binary path with JSON fallback (`prefix.js`).
- [x] JSON-vs-binary parity cross-check tests (gate) — `test/binary-parity.mjs`, green on Node + Bun.
- [x] Benchmark + BENCHMARKS.md update — `test/binary-bench.mjs`; row-heavy reads **~5–6×**, index **~1.8×**,
      point read a wash. **Verdict: Part 2 deferred** (Part 1 already closes the nested gap even under heavy
      author sharing).

---

## Part 2 — Relation dictionaries (dedup shared related rows)

Beats JSON on exactly the **relational reads** we benchmarked. **The engine already de-dups *decoding*** via
the include cache ([`process_query_one.rs`](marcidb/src/query_op/process_query_one.rs)) — a shared author is
read + decoded once. But on a cache hit it returns `cached.clone()`, so each parent still gets its **own
copy**: the **wire bytes and the JS objects are duplicated** today (JSON has no back-references; Part 1
binary inlines copies too). Part 2 lifts that existing dedup onto the wire — emit each unique related record
**once** + an index per parent — which buys a smaller payload *and* shared JS object identity on top of the
decode savings already in place. The include cache already holds the unique decoded set keyed by id, so it's
nearly free to source the dictionary from it; and its adaptive self-disable on unique relations is exactly
the case where a dictionary gives no benefit (skip it, inline).

### Wire format extension

For each relation in the shape, emit a **dictionary** section once, then reference by index from each parent
row:

```
… per relation: [u32 dict_count][unique row 0]…[unique row K-1] …
… parent row: … [u32 dict_index] (instead of an inlined related row; sentinel for null) …
```

### Engine

The include cache becomes the dictionary source: collect unique related rows by id, emit each once, and
write the parent's index. No extra decode cost (the cache already de-dups decoding).

### Decoder

Build the dictionary's objects **once** (decode each unique related row), then each parent reads an index and
points at the **shared object reference** → smaller payload *and* less JS allocation (object identity is
shared, like the include cache does server-side). Shape key still derives from the select, same cache.

### Part 2 checklist

- [ ] Format: per-relation dictionary + parent index references (+ null sentinel)
- [ ] Engine: emit dictionaries from the include cache
- [ ] Decoder: build dict once, share object references by index
- [ ] Parity cross-check (same results as Part 1 / JSON)
- [ ] Benchmark: nested select vs JOIN — target beating it

---

## Risks / open questions

- **`new Function` vs closure composition** — `new Function` gives the best JIT but is eval-like (fine for
  Node/Bun embedded; a closure-composed fallback covers CSP'd browser bundlers if ever needed).
- **Endianness** fixed little-endian; `version` byte for evolution; mismatched version → hard error.
- **Shape-key canonicalization** must be stable (normalize select key order / boolean-vs-object forms) so the
  cache hits and matches the engine's slot-order encoding.
- **Graceful fallback** to JSON for unsupported shapes/types during rollout — never silently wrong.
- **HTTP transport** stays JSON in this plan; a binary HTTP mode (Accept negotiation) is a later option.

## Out of scope (later)

- Binary **input** encoding (op descriptors, insert/update payloads) — small; revisit if profiling shows it.
- Binary for `aggregate`/`count`/write results — tiny payloads.
- Streaming / lazy partial materialization (would need a per-row offset table).
- 64-bit `BigInt` exactness (keep JSON parity for now).
