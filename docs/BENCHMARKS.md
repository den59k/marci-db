# Benchmarks

Core-engine benchmarks live in [`marcidb/benches/`](../marcidb/benches/); the `@custom` index modules each
carry their own:

```bash
cargo bench -p marcidb                 # engine
cargo bench -p marci_vector_index      # vector search (needs nightly — portable_simd)
cargo bench -p marci_fulltext_index    # full-text search
```

Numbers below were measured on a development machine (Windows 11, release build) and are meant for **tracking relative changes between MarciDB versions**, not as absolute claims. Re-run locally before drawing conclusions.

## Include cache (`include_cache.rs`)

Query: `findMany` over 10 000 posts with a nested relation select — `{ title, author: { id, name } }`.

The include cache stores decoded related records per query execution, so a `User` shared by many posts is read and decoded once. The cache disables itself adaptively if the first 256 lookups produce no repeats (unique relations), so the worst case pays almost nothing.

| Scenario | Before cache | With cache | Effect |
|---|---|---|---|
| 100 authors shared by 10k posts | 4.90 ms | **2.95 ms** | −40% |
| 10k unique authors (worst case) | 5.56 ms | 5.68 ms | +2% (noise) |

## Index modules (`@custom`)

Both build the index in a single `$reindex` and then time a search operator. Deterministic synthetic
corpora (fixed-seed LCG), 5 000 rows each.

### Vector search (`marci-vector-index/benches/vector_search.rs`)

5 000 random embeddings, dim 128, `@custom(vector, cosine)`.

| Operation | Time |
|---|---|
| `$reindex` (build cluster tree) | ~56 ms |
| `$near` search, k=10 | ~0.94 ms / query |

### Full-text search (`marci-fulltext-index/benches/fulltext_search.rs`)

5 000 documents, ~24 words each, mixed English/Russian vocabulary, `@custom(fulltext)`.

| Operation | Time |
|---|---|
| `$reindex` (build inverted index) | ~65 ms |
| `$search`, 1 term | ~0.56 ms / query |
| `$search`, 3 terms | ~1.5 ms / query |

> Both indexes are batch-built (`$reindex`); incremental maintenance is not wired yet, so these measure
> the build-once / query-many shape.

## Planned: comparison with other databases

The comparison set, in order of priority:

1. **SQLite** (`better-sqlite3`, with and without Drizzle) — the primary target: same embedded niche, and the ORM-on-top setup is the DX-equivalent of MarciDB's typed client. This is the benchmark that matters.
2. **MongoDB** — the reference point for document/NoSQL query semantics (nested selects, aggregation pipeline vs MarciDB aggregates).
3. **PostgreSQL** — only for the client-server mode (`marcidb-server`), as the upper bound of a mature server database.

MySQL is intentionally omitted — for these scenarios it answers the same question as PostgreSQL.

Planned scenarios:

- point reads by id, batched and single
- `findMany` with an indexed range filter + order + limit
- nested relation select (the include-cache scenario above)
- keyset pagination over a large table
- `count` / aggregate fast paths vs `SELECT count(*)` / `aggregate()`
- insert throughput (single and nested with relations)

> Note: meaningful comparison with SQLite requires the in-process (FFI) mode — over HTTP the network/JSON overhead dominates. The comparison will be added together with the embedded bindings.
