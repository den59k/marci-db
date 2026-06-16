# Benchmarks

Two kinds of benchmarks live in this repo:

- **Engine micro-benchmarks** (Rust, `cargo bench`) — for tracking relative performance between MarciDB
  versions. See [Engine micro-benchmarks](#engine-micro-benchmarks).
- **Cross-database comparison** — marcidb-embedded vs SQLite, through the in-process (FFI) bindings. See
  [marcidb-embedded vs SQLite](#marcidb-embedded-vs-sqlite).

---

## Engine micro-benchmarks

Core-engine benchmarks live in [`marcidb/benches/`](../marcidb/benches/); the `@custom` index modules each
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

#### Vector search (`marci-vector-index/benches/vector_search.rs`)

5 000 random embeddings, dim 128, `@custom(vector, cosine)`.

| Operation | Time |
|---|---|
| `$reindex` (build cluster tree) | ~56 ms |
| `$near` search, k=10 | ~0.94 ms / query |

#### Full-text search (`marci-fulltext-index/benches/fulltext_search.rs`)

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

> **TL;DR.** marcidb-embedded is competitive with SQLite and *faster* on batched writes and counts, ~2×
> slower on single-row point reads, and **~5–8× slower on large result sets** (`select all`). On
> **relational reads** it returns the whole object graph in one query — **faster than the N+1 pattern** a
> naive ORM falls into, though a hand-written JOIN is faster; **index-backed filters use the `@index`** (no
> scan). The read gaps are all the same thing: results are serialized to JSON across the FFI boundary.
> That's the motivation for the upcoming binary transport.

### Setup

- **Machine:** AMD Ryzen 9 9900X (12C/24T), 32 GB, Windows 11 (10.0.26200)
- **Runtimes:** Node 24.15.0, Bun 1.3.14
- **Versions:** marcidb-embedded 0.2.1 · marcidb-client 0.3.1 · better-sqlite3 11.10.0 · `node:sqlite` (Node 24 built-in) · `bun:sqlite` (Bun 1.3 built-in)
- **Dataset:** 20,000 rows, `User { name String, age Int, email String }` (+ autoincrement id). The relational section adds a `Post { title, author User? }` relation (10k posts, 100 shared authors); the index section uses an indexed `age` (`@index` / `CREATE INDEX`).

#### Methodology / fairness

- **Durability off for everyone** — marcidb `disableFsync`; SQLite `PRAGMA synchronous = OFF` + `journal_mode = WAL`. All engines are disk-backed (a temp dir), *not* `:memory:`. We're measuring engine + binding overhead, not fsync.
- **SQLite uses prepared statements** (its idiomatic fast path) and runs **synchronously**.
- **marcidb uses its real typed client** (`marcidb(db)`), which is **async** (a `Promise` per op) and **JSON-over-FFI** — exactly how an app uses it. The async + serialization cost is the thing being compared.
- One full suite is run as **warmup and discarded** before the measured run (hot JIT).
- Numbers vary ~±10–15 % run-to-run; treat the **relative factors** as the signal, not the absolute ops/s.

`ops/s` is the logical operation per second: one **row** for inserts/updates/point-reads, one **call** for
count, and one **full 20k-row read** for `select all`.

### Results — Node 24 (marcidb vs better-sqlite3 vs node:sqlite)

| Operation | marcidb-embedded | better-sqlite3 | node:sqlite | marcidb vs best SQLite |
| --- | ---: | ---: | ---: | :--- |
| **Bulk insert** (20k in 1 txn) | **292k ops/s** | 118k | 132k | **2.2× faster** ✅ |
| **Count** (×500) | **399k ops/s** | 244k | 231k | **1.6× faster** ✅ |
| **Update by id** (×20k) | 153k ops/s | 157k | 165k | 0.93× (≈tie) |
| **Single insert** (×20k) | 110k ops/s | 123k | 132k | 0.84× |
| **Point query by id** (×20k) | 238k ops/s | 451k | 449k | 0.53× (1.9× slower) |
| **Select all** 20k rows (×50) | 28 reads/s (≈36 ms/read) | 90 | 134 (≈7.5 ms/read) | **0.21× (≈5× slower)** ⚠️ |

### Results — Bun 1.3 (marcidb vs bun:sqlite)

> `better-sqlite3` does not load on Bun ([oven-sh/bun#4290](https://github.com/oven-sh/bun/issues/4290)),
> so the comparison there is against the built-in `bun:sqlite`.

| Operation | marcidb-embedded | bun:sqlite | marcidb vs bun:sqlite |
| --- | ---: | ---: | :--- |
| **Bulk insert** (20k in 1 txn) | **343k ops/s** | 124k | **2.8× faster** ✅ |
| **Count** (×500) | **362k ops/s** | 253k | **1.4× faster** ✅ |
| **Update by id** (×20k) | 146k ops/s | 143k | 1.0× (tie) |
| **Single insert** (×20k) | 118k ops/s | 130k | 0.91× |
| **Point query by id** (×20k) | 230k ops/s | 486k | 0.47× (2.1× slower) |
| **Select all** 20k rows (×50) | 27 reads/s (≈37 ms/read) | 221 (≈4.5 ms/read) | **0.12× (≈8× slower)** ⚠️ |

### Relational & index-backed reads (marcidb's strengths)

**Nested select** — read 10,000 posts, each with its `author { name, email }` decoded. marcidb does it in
**one query** (the relation graph is decoded server-side; shared authors hit the include cache). SQLite has
no native nesting, so the app either writes a `JOIN` and reshapes the flat rows to the nested shape, or — the
naive-ORM path — issues one query per post (**N+1**). Each read = the full 10k-post result.

| Node (×20) | reads/s | vs marcidb |
| --- | ---: | :--- |
| **marcidb-embedded** (1 query) | 53 | — |
| node:sqlite (JOIN + reshape) | 217 | 4.1× faster |
| better-sqlite3 (JOIN + reshape) | 186 | 3.5× faster |
| better-sqlite3 (**N+1**) | 46 | **0.9× — marcidb wins** ✅ |

| Bun (×20) | reads/s | vs marcidb |
| --- | ---: | :--- |
| **marcidb-embedded** (1 query) | 46 | — |
| bun:sqlite (JOIN + reshape) | 371 | 8.1× faster |
| bun:sqlite (**N+1**) | 56 | 1.2× (≈tie) |

marcidb gives you the nested graph with **no JOINs to write and no N+1 risk** — it beats the N+1 pattern on
Node and ties it on Bun (where `bun:sqlite`'s point reads are exceptionally fast). A hand-tuned JOIN is
faster because marcidb serializes the nested result to JSON — the same read tax binary transport targets.

**Index-backed filter** — `findMany({ $where: { age: ? } })` returning ~333 of 20,000 rows, the field
indexed (`@index` vs `CREATE INDEX`). At ≈0.3 ms/query this confirms marcidb **uses the index** (a full scan
would be ~100× slower); the remaining gap is decoding the ~333 result rows.

| Index filter | marcidb | better-sqlite3 | node:sqlite | bun:sqlite |
| --- | ---: | ---: | ---: | ---: |
| **Node** (×5000) | 4.0k ops/s | 6.3k | 8.9k | — |
| **Bun** (×5000) | 4.1k ops/s | — | — | 13.4k |

(≈2–3× slower — index lookup is comparable; the gap is again per-row result serialization.)

### Reading the results

- **Batched writes win.** A `$transaction` of 20k inserts is **one** FFI call carrying one JSON array; SQLite pays a JS→native crossing per `stmt.run()`. marcidb amortizes the boundary and comes out 2–3× ahead.
- **Count wins.** A single integer crosses the boundary — negligible serialization — and the engine counts a B-tree cheaply.
- **Single-row writes tie.** One small op each way; the JSON for one row is tiny, so marcidb is within ~10–15 %.
- **Point reads are ~2× slower.** Per call marcidb pays: build an op descriptor → `JSON.stringify` → FFI → Rust decodes the row to a JSON string → `JSON.parse` → unwrap the envelope, plus a `Promise`. SQLite returns a native object from a prepared statement synchronously.
- **Large reads are the weak spot (~5–8×).** `select all` serializes 20,000 rows to JSON in Rust **and** parses them in JS, every pass. That's O(rows) work SQLite skips by handing back native values. On Bun the gap is widest because `bun:sqlite` is especially fast at materializing rows.

### Why — and what binary transport fixes

Every operation currently crosses the FFI boundary as **JSON**, and a query result is even serialized
*twice* on the Rust side (the decode layer builds a JSON string, which is parsed into a `Value` and then
re-serialized into the result envelope). For small payloads this is dwarfed by fixed costs and marcidb's
batching wins. For large result sets it dominates.

A binary/columnar transport (the next task) targets exactly the read paths — `select all`, point reads,
nested select, and index filters: returning rows in a typed binary layout (or zero-copy buffers) removes the
double JSON pass and most of the per-row overhead, which should close most of the read gap while keeping the
batched-write advantage.

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
