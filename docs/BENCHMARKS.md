# Benchmarks

Two kinds of benchmarks live in this repo:

- **Engine micro-benchmarks** (Rust, `cargo bench`) — for tracking relative performance between MarciDB
  versions. See [Engine micro-benchmarks](#engine-micro-benchmarks).
- **Cross-database comparison** — marcidb-embedded vs SQLite, through the in-process (FFI) bindings. See
  [marcidb-embedded vs SQLite](#marcidb-embedded-vs-sqlite).

---

## Engine micro-benchmarks

Core-engine benchmarks live in [`crates/marcidb/benches/`](../crates/marcidb/benches/); the `@custom` index modules each
carry their own:

```bash
cargo bench -p marcidb                 # engine
cargo bench -p marci_vector_index      # vector search (needs nightly — portable_simd)
cargo bench -p marci_fulltext_index    # full-text search
```

Numbers below were measured on a development machine (Windows 11, release build) and are meant for
**tracking relative changes between MarciDB versions**, not as absolute claims. Re-run locally before
drawing conclusions.

### Include cache (`include_cache.rs`)

Query: `findMany` over 10 000 posts with a nested relation select — `{ title, author: { id, name } }`.

The include cache stores decoded related records per query execution, so a `User` shared by many posts is
read and decoded once. The cache disables itself adaptively if the first 256 lookups produce no repeats
(unique relations), so the worst case pays almost nothing.

| Scenario | Before cache | With cache | Effect |
|---|---|---|---|
| 100 authors shared by 10k posts | 4.90 ms | **2.95 ms** | −40% |
| 10k unique authors (worst case) | 5.56 ms | 5.68 ms | +2% (noise) |

### Index modules (`@custom`)

Both build the index in a single `$reindex` and then time a search operator. Deterministic synthetic
corpora (fixed-seed LCG), 5 000 rows each.

#### Vector search (`crates/marci-vector-index/benches/vector_search.rs`)

5 000 random embeddings, dim 128, `@custom(vector, cosine)`.

| Operation | Time |
|---|---|
| `$reindex` (build cluster tree) | ~56 ms |
| `$near` search, k=10 | ~0.94 ms / query |

#### Full-text search (`crates/marci-fulltext-index/benches/fulltext_search.rs`)

5 000 documents, ~24 words each, mixed English/Russian vocabulary, `@custom(fulltext)`.

| Operation | Time |
|---|---|
| `$reindex` (build inverted index) | ~65 ms |
| `$search`, 1 term | ~0.56 ms / query |
| `$search`, 3 terms | ~1.5 ms / query |

> These measure the batch `$reindex` build + query-many shape. Full-text is also maintained live on each
> write (insert/update/delete), so in normal use `$reindex` is only needed to backfill pre-existing rows;
> vector remains `$reindex`-only.

---

## marcidb-embedded vs SQLite

In-process (FFI) MarciDB vs SQLite, on Bun and Node. The harness lives in
[`packages/benchmarks/`](../packages/benchmarks/); see [Reproducing](#reproducing) to run it yourself.

> **TL;DR.** As of **0.6** the embedded read path is **binary** (no JSON), and it changes the picture.
> marcidb-embedded **wins batched writes (~2–2.5×) and counts**, and — the big shift — now **wins large
> `select all` (~1.3× on Node) and nested select (beats a hand-written JOIN on Node)**, the cases that were
> *5–8× behind* on the old JSON transport. It **ties** single-row updates. It still trails on **single-row
> point reads** (~2.5× — a tiny result where binary framing doesn't pay off), and on Bun, whose
> exceptionally fast `bun:sqlite` keeps the lead on `select all` and the index filter. Writes/inputs stay
> JSON (tiny, already winning). See [Binary transport](#binary-transport-how-it-works) for the isolated
> before/after.

### Setup

- **Machine:** AMD Ryzen 9 9900X (12C/24T), 32 GB, Windows 11 (10.0.26200)
- **Runtimes:** Node 24.15.0, Bun 1.3.14
- **Versions:** marcidb-embedded 0.6.1 · marcidb-client 0.6.1 · better-sqlite3 11.10.0 · `node:sqlite` (Node 24 built-in) · `bun:sqlite` (Bun 1.3 built-in) — embedded reads use the **binary transport** (default since 0.6)
- **Dataset:** 20,000 rows, `User { name String, age Int, email String }` (+ autoincrement id). The relational section adds a `Post { title, author User? }` relation (10k posts, 100 shared authors); the index section uses an indexed `age` (`@index` / `CREATE INDEX`).

#### Methodology / fairness

- **Durability off for everyone** — marcidb `disableFsync`; SQLite `PRAGMA synchronous = OFF` + `journal_mode = WAL`. All engines are disk-backed (a temp dir), *not* `:memory:`. We're measuring engine + binding overhead, not fsync.
- **SQLite uses prepared statements** (its idiomatic fast path) and runs **synchronously**.
- **marcidb uses its real typed client** (`marcidb(db)`): **async** (a `Promise` per op), with **read results over the binary transport** and writes/inputs over JSON — exactly how an app uses it. The async + per-op cost is the thing being compared.
- One full suite is run as **warmup and discarded** before the measured run (hot JIT).
- Numbers vary ~±10–15 % run-to-run; treat the **relative factors** as the signal, not the absolute ops/s.

`ops/s` is the logical operation per second: one **row** for inserts/updates/point-reads, one **call** for
count, and one **full 20k-row read** for `select all`.

### Results — Node 24 (marcidb vs better-sqlite3 vs node:sqlite)

| Operation | marcidb-embedded | better-sqlite3 | node:sqlite | marcidb vs best SQLite |
| --- | ---: | ---: | ---: | :--- |
| **Bulk insert** (20k in 1 txn) | **290k ops/s** | 127k | 136k | **2.1× faster** ✅ |
| **Select all** 20k rows (×50) | **172 reads/s** (≈5.8 ms/read) | 88 | 131 | **1.3× faster** ✅ |
| **Count** (×500) | **277k ops/s** | 224k | 232k | **1.2× faster** ✅ |
| **Update by id** (×20k) | 161k ops/s | 161k | 167k | 0.96× (≈tie) |
| **Single insert** (×20k) | 114k ops/s | 121k | 131k | 0.87× |
| **Point query by id** (×20k) | 183k ops/s | 401k | 456k | 0.40× (≈2.5× slower) |

### Results — Bun 1.3 (marcidb vs bun:sqlite)

> `better-sqlite3` does not load on Bun ([oven-sh/bun#4290](https://github.com/oven-sh/bun/issues/4290)),
> so the comparison there is against the built-in `bun:sqlite`.

| Operation | marcidb-embedded | bun:sqlite | marcidb vs bun:sqlite |
| --- | ---: | ---: | :--- |
| **Bulk insert** (20k in 1 txn) | **337k ops/s** | 135k | **2.5× faster** ✅ |
| **Count** (×500) | **433k ops/s** | 257k | **1.7× faster** ✅ |
| **Update by id** (×20k) | **168k ops/s** | 163k | 1.03× (≈tie) |
| **Single insert** (×20k) | 122k ops/s | 135k | 0.90× |
| **Select all** 20k rows (×50) | 200 reads/s (≈5.0 ms/read) | 243 (≈4.1 ms/read) | 0.82× |
| **Point query by id** (×20k) | 219k ops/s | 545k | 0.40× (≈2.5× slower) |

On Bun the read gaps shrank too (`select all` was **0.12×** on JSON, now **0.82×**), but `bun:sqlite` is an
unusually fast native binding, so it keeps a slim lead on `select all` and the index filter where Node's
SQLite bindings don't.

### Relational & index-backed reads (marcidb's strengths)

**Nested select** — read 10,000 posts, each with its `author { name, email }` decoded. marcidb does it in
**one query** (the relation graph is decoded server-side; shared authors hit the include cache). SQLite has
no native nesting, so the app either writes a `JOIN` and reshapes the flat rows to the nested shape, or — the
naive-ORM path — issues one query per post (**N+1**). Each read = the full 10k-post result.

| Node (×20) | reads/s | vs marcidb |
| --- | ---: | :--- |
| **marcidb-embedded** (1 query) | **232** | — |
| node:sqlite (JOIN + reshape) | 212 | 0.91× — **marcidb wins** ✅ |
| better-sqlite3 (JOIN + reshape) | 179 | 0.77× — **marcidb wins** ✅ |
| better-sqlite3 (**N+1**) | 46 | 0.20× |

| Bun (×20) | reads/s | vs marcidb |
| --- | ---: | :--- |
| **marcidb-embedded** (1 query) | 232 | — |
| bun:sqlite (JOIN + reshape) | 327 | 1.4× faster |
| bun:sqlite (**N+1**) | 56 | 0.24× |

This is the clearest binary-transport win: on JSON the nested read lost to a JOIN (and barely beat N+1); on
binary it **beats a hand-written JOIN on Node** outright (232 vs 212/179) — one query, no JOINs to write, no
N+1 risk. On Bun it trails `bun:sqlite`'s JOIN but still beats the N+1 pattern ~4×.

**Index-backed filter** — `findMany({ $where: { age: ? } })` returning ~333 of 20,000 rows, the field
indexed (`@index` vs `CREATE INDEX`). marcidb **uses the index** (a full scan would be ~100× slower); the
remaining gap is materializing the ~333 result rows.

| Index filter | marcidb | better-sqlite3 | node:sqlite | bun:sqlite |
| --- | ---: | ---: | ---: | ---: |
| **Node** (×5000) | 7.0k ops/s | 6.3k | 8.7k | — |
| **Bun** (×5000) | 7.1k ops/s | — | — | 13.1k |

(Binary lifted this from ~2–3× behind to **beating better-sqlite3** on Node and within ~0.8× of `node:sqlite`;
`bun:sqlite` stays ahead.)

### Reading the results

- **Batched writes win.** A `$transaction` of 20k inserts is **one** FFI call carrying one JSON array; SQLite pays a JS→native crossing per `stmt.run()`. marcidb amortizes the boundary and comes out ~2–2.5× ahead.
- **Count wins.** A single integer crosses the boundary, and the engine counts a B-tree cheaply.
- **Large reads now win (Node) / are competitive (Bun).** `select all` and nested select used to serialize 20k rows to JSON in Rust *and* parse them in JS every pass — O(rows) the binary transport removes. With binary, `select all` leads on Node and nested select beats a hand-written JOIN; on Bun, `bun:sqlite`'s very fast row materialization keeps a slim lead.
- **Single-row writes tie / trail slightly.** One small op each way; marcidb is within ~10–15 %.
- **Point reads stay ~2.5× slower.** For a *single* tiny row, binary doesn't pay: the per-call cost is the `Promise` + FFI round-trip + framing a one-row buffer, which is on par with (Node) or slightly worse than the old `JSON.parse` of one small object. SQLite returns a native object from a prepared statement synchronously. This is the one read binary doesn't help.

### Binary transport (how it works)

The embedded read path encodes `findMany`/`findFirst` results as a compact **binary** buffer (no JSON),
decoded by a shape-specialized, cached decoder on the JS side — reusing the engine's slot-row encoding and
dropping the old double-serialize (decode → JSON string → `Value` → envelope). It's on automatically for
`marcidb(db)`; shapes it doesn't cover yet (formats, enums, lists, composite keys) fall back to JSON
transparently. The same encoder backs an opt-in **binary HTTP** mode (below). A JSON-vs-binary parity test
([`test/binary-parity.test.ts`](../packages/marcidb-embedded/test/binary-parity.test.ts)) gates correctness.

Isolating the transport — the **same DB**, the same query through binary vs forced-JSON (harness:
[`test/binary-bench.ts`](../packages/marcidb-embedded/test/binary-bench.ts)):

| Read | Node — JSON → binary | Bun — JSON → binary |
| --- | ---: | ---: |
| **Select all** (20k rows ×50) | 28 → 156 reads/s — **5.6×** | 30 → 160 — **5.3×** |
| **Nested select** (10k posts + author ×20) | 38 → 227 reads/s — **5.9×** | 42 → 219 — **5.3×** |
| **Index filter** WHERE age=? (×5000) | 3.9k → 7.1k ops/s — **1.8×** | 4.2k → 7.1k — **1.7×** |
| **Point query by id** (×20k) | 242k → 206k ops/s — 0.85× | 255k → 262k — 1.03× |

The row-heavy reads — exactly the JSON-tax cases that were 5–8× behind SQLite — gain **~5–6×**, which is what
flips them to wins/ties against SQLite above. The single-row point read is a wash: the result is tiny, so
framing/decoder overhead roughly cancels the saved `JSON.parse` (it even dips ~15% under Node's koffi, where
reading the buffer costs an extra decode; under Bun's zero-copy `toArrayBuffer` it's neutral).

**Part 2 (relation-dictionary dedup) — deferred.** The nested-select case repeats each shared author inline
(no dedup) yet still reaches ~5.3–5.9×, even with 100 authors shared across 10,000 posts (heavy duplication
— Part 2's best case). Since Part 1 already closes the gap there, the extra wire/object dedup isn't worth its
complexity yet; revisit only if a real workload shows a relation-heavy result still bottlenecked on payload
size or JS allocation.

### Binary over HTTP (`marcidb-server`)

The same encoder is available over HTTP as an **opt-in** mode, in parallel with JSON. The client advertises
`Accept: application/x-marcidb-rows` plus a schema fingerprint (`X-Marci-Schema`); the server replies binary
only when the fingerprint matches the DB's current schema **and** the shape is supported, otherwise JSON — so
curl and any non-negotiating client are unaffected. Reads only (`findMany`/`findFirst`).

Here the network round-trip is in the mix, so the win is narrower than embedded and concentrated on **large
reads**, where binary is both **smaller on the wire** and skips `JSON.parse`/double-serialize. Same running
server, binary HTTP client vs forced-JSON (harness:
[`test/http-bench.ts`](../packages/marcidb-embedded/test/http-bench.ts); 20k users, 10k posts / 100 authors):

| Read | Node — JSON → binary | Bun — JSON → binary |
| --- | ---: | ---: |
| **Select all** (20k rows ×50) | 72 → 102 reads/s — **1.41×** | 80 → 128 — **1.60×** |
| **Nested select** (10k posts + author ×50) | 124 → 172 reads/s — **1.38×** | 135 → 202 — **1.50×** |
| **Index filter** WHERE age=? (×2000) | 2.0k → 2.5k ops/s — **1.24×** | 3.6k → 3.9k — **1.08×** |
| **Point query by id** (×2000) | 3.7k → 4.2k ops/s — **1.14×** | 16.2k → 13.0k — 0.80× |

The `select all` body is **1.27× smaller** on the wire (957 KB vs 1.22 MB) — repeated field names dropped,
compact scalars. Large/nested reads gain ~1.4–1.6×; the point read is a wash or a slight loss (a tiny payload
where binary framing doesn't pay, and on Bun the very fast `fetch`+`JSON.parse` already win). The HTTP
handshake (`X-Marci-Schema`) keeps a stale client *correct* — a fingerprint mismatch transparently serves
JSON, never wrong bytes — verified by [`test/http-binary.test.ts`](../packages/marcidb-embedded/test/http-binary.test.ts).

### Caveats

- One machine, Windows, single process. Absolute numbers will differ elsewhere; the **relative factors** are the takeaway.
- Durability is **off** for all engines — this is not a durability/crash-safety comparison.
- The relational test is a single one-level relation (`Post → User`) with one indexed field. Deeper graphs, multi-index queries, ordering/pagination, aggregates, and `@custom` (vector/full-text) indexes aren't exercised here and have their own cost profiles.
- marcidb's client is **async** by design; a synchronous embedded API (the FFI calls are already synchronous) could narrow the per-call gap independently of transport.

### Reproducing

```sh
cd packages/benchmarks
npm install
npm run gen          # generate the marcidb client from schema.marci
npm run bench        # Node
npm run bench:bun    # Bun
```

Tune the workload with env vars: `N` (rows), `READS`, `POINTS`, `COUNTS`, `UPDATES`, and for the relational
/ index sections `REL_POSTS`, `REL_AUTHORS`, `REL_READS`, `IDX_ROWS`, `IDX_READS`. Example:
`N=50000 REL_POSTS=50000 node bench.mjs`.

---

## Still planned: other databases & scenarios

The SQLite comparison above covers the core CRUD shapes. Still to add:

- **MongoDB** — the reference point for document/NoSQL query semantics (nested selects, aggregation
  pipeline vs MarciDB aggregates).
- **PostgreSQL** — only for the client-server mode (`marcidb-server`), as the upper bound of a mature
  server database.

MySQL is intentionally omitted — for these scenarios it answers the same question as PostgreSQL.

Scenarios not yet covered by the SQLite harness (which currently does bulk/single insert, update, count,
point reads, full-table read, one-level nested select, and indexed equality filter):

- indexed **range** filter + `$order` + `$limit` (here we only do indexed equality)
- keyset pagination over a large table
- aggregate fast paths vs `aggregate()` / `SELECT … GROUP BY`
- insert throughput for nested writes (rows with relations)
- deeper relation graphs (2+ levels)
