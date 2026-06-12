# Benchmarks

All benchmarks live in [`marcidb/benches/`](../marcidb/benches/) and run with:

```bash
cargo bench -p marcidb
```

Numbers below were measured on a development machine (Windows 11, release build) and are meant for **tracking relative changes between MarciDB versions**, not as absolute claims. Re-run locally before drawing conclusions.

## Include cache (`include_cache.rs`)

Query: `findMany` over 10 000 posts with a nested relation select — `{ title, author: { id, name } }`.

The include cache stores decoded related records per query execution, so a `User` shared by many posts is read and decoded once. The cache disables itself adaptively if the first 256 lookups produce no repeats (unique relations), so the worst case pays almost nothing.

| Scenario | Before cache | With cache | Effect |
|---|---|---|---|
| 100 authors shared by 10k posts | 4.90 ms | **2.95 ms** | −40% |
| 10k unique authors (worst case) | 5.56 ms | 5.68 ms | +2% (noise) |

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
