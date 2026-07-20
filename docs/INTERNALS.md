# MarciDB Internals

How MarciDB stores data and executes queries. This is a design document for contributors; nothing here is needed to *use* the database.

## Storage layout

MarciDB is built on [canopydb](https://crates.io/crates/canopydb) — an embedded transactional key-value store with ordered B-trees. The schema maps onto trees as follows:

- **one tree per model** — `User`, `Post`, `Project.users` (nested structs become their own entities with a dotted name)
- **one tree per secondary index** — `index_User.age`
- **one tree per list relation** — `User.posts->Post.author` (the binding tree)

Everything below builds on a single property of these trees: **keys are ordered**, and range/prefix scans are cheap.

### Primary keys

| Schema | Key bytes |
|---|---|
| autoincrement `id` | `u64` big-endian — tree order == insertion order |
| `Byte[16] @id @format(uuid)` | 16 raw bytes |
| composite (`chat @id`, `user @id`) | concatenation of the parts |
| nested struct list (`Project.users`) | `parent_id ++ own_id` |

The last row is the important one: **children of a struct list live under their parent's key prefix** in their own tree. "All children of X" is a prefix scan, "children count" is a prefix key count — no join machinery exists or is needed.

Variable-length key parts (string ids) are terminated with `\0` so that concatenated composite keys remain unambiguous.

### Row format

A row body is a small header followed by an offset table and the payload:

```
[version: u8] [reserved: u8] [payload_offset: u16]
[offset_0: u32] [offset_1: u32] ... (one slot per body field)
[payload bytes...]
```

- each body field has a fixed slot in the offset table, assigned at schema parse time
- `offset == 0` means **null** — nulls occupy 4 bytes of offset table and nothing else
- the *end* of a field is the next non-zero offset (or, for fixed-size types, `offset + size`)

This buys the central performance property: **zero-copy field access**. `get_data(entity, field, id, body)` returns a byte slice directly into the stored value — no deserialization, no allocation. Filters, sort keys, aggregations and index maintenance all read only the bytes they need. Decoding a full document happens exactly once per *returned* row, at the very end of the pipeline.

Values are stored big-endian, so numeric bytes are also directly comparable where the encoding allows (see below).

A `Json` field occupies one body slot holding a self-contained **JSONB blob** — tagged values (`null`/`bool`/`int`/`uint`/`double`/`string`/`array`/`object`) with sorted-key and array offset tables. The blob is opaque to the row framing (variable-length, like a string), so it needed no change to the row format. Its internal offset tables are what let a `$where` path filter binary-search to a nested key and decode just that leaf, without walking the whole document. Over the binary read transport a `Json` field is sent as its decoded JSON text (the client `JSON.parse`s it), so one tested codec serves both sides.

### Enums with payload

`enum Role { viewer, admin { sign String } }` stores the variant as a `u16` and **injects variant fields into the model itself** as ordinary body fields with an *existence condition* (`field exists iff role == admin`). Readers check the condition against the row bytes; writers skip fields whose condition fails. On update, switching the variant clears the other variants' fields (otherwise stale bytes would "resurrect" on switching back) and rejects writes into inactive variants.

### Secondary indexes

An index entry is a key-only record:

```
index key = encoded(value) ++ row_id        (no entry at all if the value is null)
```

- **sparse**: null values are not indexed at all. This makes `UNIQUE` allow multiple nulls for free (SQL semantics) and enables the O(1) null-count trick below — but it also means an index scan cannot be used to *order* a nullable field (rows would be lost), which the planner respects.
- **order-preserving encodings**: lexicographic byte order of the index key must equal the logical order of values. Unsigned ints are stored as-is (big-endian); signed ints flip the sign bit; floats flip the sign bit when positive and *all* bits when negative. Strings get a `\0` terminator so `"ab" < "abc"` holds after the id is appended.
- the appended `row_id` makes every index key unique and provides a deterministic tie-break — the same total order that in-memory sorting reproduces (see sort keys).

### Relations

Three binding kinds, chosen at schema resolution:

- **CurrentId** — struct children: the child's key *is* `parent_id ++ ...`; no extra structure.
- **FieldValue** — many-to-one (`Post.author`): the referenced id is stored in the post's body. Following it is a single `get`.
- **IndexTree** — list relations (`User.posts`): a binding tree with keys `parent_id ++ child_id`. Children of a parent = prefix scan, already in child-id order.

Reverse dependencies (`rev_dependencies`) drive cascades and `SetNull` on delete.

## Query pipeline

```
JSON → parse (+plan) → QueryOp → execute → decode
```

`QueryOp` is self-contained: select mask (bitmask over fields), full `filter`, access path (`prefix_key`), sort/limit/cursor, nested includes (recursively more `QueryOp`s or aggregate ops).

### The correctness invariant

The single most load-bearing design decision: **`filter` always contains the complete `$where`, and is re-checked for every row**. `prefix_key` is only an accelerator. Consequences:

- the planner can never produce a wrong result, only a slow one — heuristics are safe by construction
- residual conditions need no special handling: scan any tree, the filter sorts it out
- execution-time adaptivity (caches, fast paths) composes freely

### The planner

Runs at parse time, purely rule-based, no maintained statistics ("simpler, but reliable"):

1. **Access path from `$where`** — among all indexed conditions, pick the most selective by static priority: exact primary key → unique eq → primary-key prefix → eq → `$startsWith` → two-sided range → half-open range. A lower and an upper bound on the same numerically-indexed field are first fused into one bounded range, whether they were written as two operators on one field or as two conditions under `$and`. (`Or`/`Not` never produce an access path; the filter handles them.)
2. **`$order` resolution**:
   - by primary key → native tree order, `desc` = reverse scan (canopydb iterators are double-ended)
   - by an indexed, non-nullable, non-variant field → scan the *sort* index; if `$where` already ranges over the same index, reuse that range; if `$where` chose a different index and `$limit` is present, prefer the sort index (early exit beats selectivity)
   - everything else → `post_sort`: filter first, sort in memory, then slice
3. **`$cursor` without `$order`** pins the scan to primary-key order (drops a conflicting where-index — correctness of the cursor wins).

Plan-time data probing (`tree.len()`, bounded range counting) is intentionally used only at execution time so far; the parse layer has no storage handle.

### Execution

All scan paths produce a lazy `(id, body)` stream feeding one collector:

```
scan → cursor gate → filter → skip → decode … early exit on $limit
```

- bodies of rows beyond `$limit` are never read or decoded
- `post_sort` materializes `(sort_key, id, body)` for matching rows, sorts by bytes, then decodes only the requested slice
- **sort keys** are built with the *same* encoders as index keys (`0x00 ++ encoded_value ++ id`, null → `0xFF ++ id`), so in-memory order is byte-identical to index-scan order — asc puts nulls last, desc puts them first, and cursors work identically on both strategies

### Keyset pagination

`$cursor: { id }` is exclusive and resolves per strategy:

- primary-key order: the id *is* the tree key → `range(id+ε ..)`, zero extra reads
- index scan: one `get` of the cursor row → rebuild its index key → continue the range from it
- in-memory sort: build the cursor row's sort key, drop rows `<=` it before sorting

`skip` exists but is honest O(n); the cursor is the intended tool.

## Optimizations

### Aggregation fast paths

The aggregate executor checks, in order, whether it can avoid reading rows at all:

| Query | Cost | How |
|---|---|---|
| `count()` | O(1) | `tree.len()` (canopydb stores `num_keys` in tree metadata) |
| `count` + single indexed condition | O(range) keys only | the filter is *fully covered* by the range — count index keys, never touch rows |
| `count` + `{field: null}` / `{$not: null}` (indexed field) | O(1) | sparse index ⇒ `not-null = index.len()`, `null = table.len() − index.len()` |
| `$min`/`$max` (indexed, non-nullable, no filter) | O(log n) | first/last index key, decoded back through the inverse of the order-preserving encoding |
| relation `$count` (no `$where`) | O(children) keys only | prefix key count in the binding tree |

Everything else falls into a scan that still reads only the aggregated field's bytes per row (`get_data`), accumulating ints exactly in `i128` and floats in `f64`.

### Include cache

A `findMany` with `author: {...}` over 10k posts and 100 distinct authors used to fetch and decode the same author ~100× each. The execution context now keeps a per-query cache:

```
(identity of the nested query, related row id) → decoded result
```

- scoped to one query execution (a read-transaction snapshot — no invalidation problem exists)
- applied only to `FieldValue` (many-to-one) includes — the only binding where related rows are actually shared
- caches the *decode output* generically (`U`), not JSON specifically — when the output format becomes binary, the cache carries binary
- **adaptive**: if the first 256 lookups produce zero repeats, the cache disables itself for that include and frees its memory — unique relations pay ~nothing

Measured effect (see [BENCHMARKS.md](BENCHMARKS.md)): −40% on shared relations, +2% (noise) on the adversarial all-unique case.

### Other notable details

- **`only_id_required`**: filters touching only key fields are evaluated against the key bytes without fetching the body.
- **findFirst** reuses the `findMany` machinery with a hard limit of 1 whenever sorting/cursor/skip are involved; plain lookups keep their exact-`get` fast path.
- **Write path**: defaults are applied in two phases — counters first (at offsets relative to the original buffer), then the parent-id splice that shifts them into place. Index maintenance is incremental: updates delete/insert only the entries of fields that actually changed.

## The decode boundary

Everything in the pipeline is generic over the decode output `U`: the storage layer hands `(id, body, includes)` to a single callback, which currently renders JSON strings. This boundary is deliberate — the planned embedded mode replaces JSON with the binary row format itself plus generated (JIT-compiled) parsing functions on the client side. The pipeline, planner, caches and fast paths are all unaffected by that swap.
