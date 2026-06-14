# marci-vector-index

Vector search for [MarciDB](../README.md), as a pluggable `@custom` index module. It gives a `Float[N]`
field an on-disk approximate-nearest-neighbour index and answers `$near` queries — useful for semantic
search over embeddings, geospatial lookups, recommendations, and similar.

This crate is the thin **adapter** that bridges MarciDB's `IndexProvider` SPI to the pure clustering
algorithm in [`marci-vector`](../marci-vector/README.md) (which has no MarciDB dependency).

```
model Place {
    name String
    loc  Float[2]     @custom(vector, euclidean)
}

model Doc {
    title     String
    embedding Float[1536]  @custom(vector, cosine)
}
```

```ts
const docs = await db.doc.findMany({
  title: true,
  $where: { embedding: { $near: { vector: queryEmbedding, k: 10 } } },
})
```

## Metrics

The argument selects the distance metric:

| Declaration | Metric |
|---|---|
| `@custom(vector, euclidean)` (or `l2`, or no arg) | Euclidean distance. |
| `@custom(vector, cosine)` | Cosine similarity — vectors are L2-normalized at index **and** query time. |

The field must be a fixed-size float list, `Float[N]`; `N` is the embedding dimension.

## Querying

`$near` (alias `$search`, accepted on any `@custom` field) takes the query vector and a few options:

```ts
{ embedding: { $near: { vector: [/* N numbers */], k: 10, threshold: 0.0 } } }
```

| Field | Meaning |
|---|---|
| `vector` | The query vector (length must equal the field's `N`). Required. |
| `k` | Max neighbours to return. Default `10`. |
| `threshold` | Relative distance-gap cutoff: results are truncated where the distance to the next neighbour jumps by more than `threshold`. `0` (default) disables it. |

Results come back ordered nearest-first. The query's `$limit` further trims them, and other `$where`
conditions are applied as a post-filter over the candidate set.

Over the raw HTTP API:

```bash
curl -X POST http://localhost:3000/app/Doc/findMany \
  -H 'Content-Type: application/json' \
  --data-binary '{"title":true,"$where":{"embedding":{"$near":{"vector":[0.1,0.2, ...],"k":10}}}}'
```

## Building the index

The cluster tree is built in a batch from current data via the `$reindex` endpoint (not incrementally on
each write — v1). Call it after a bulk import or whenever the vectors have changed:

```bash
curl -X POST http://localhost:3000/app/Doc/$reindex     # one model
curl -X POST http://localhost:3000/app/$reindex          # every model in the DB
```

## Enabling it

> **Nightly required.** The underlying `marci-vector` uses `std::simd` (`portable_simd`), so building this
> module needs a nightly toolchain (`rustup default nightly`). It is opt-in precisely so the default
> MarciDB build stays on stable.

In the server, compile the module in behind its cargo feature:

```bash
cargo run -p marcidb-server --features vector
```

When embedding MarciDB as a library, register the provider on the database:

```rust
use std::sync::Arc;
use marcidb::{MarciDB, ProviderRegistry};
use marci_vector_index::VectorIndexProvider;

let mut registry = ProviderRegistry::new();
registry.register(Box::new(VectorIndexProvider::new()));
let db = MarciDB::open(path).with_providers(Arc::new(registry));
```

## How it works

- **Index**: a recursive k-means **cluster tree** (see [`marci-vector`](../marci-vector/README.md) for the
  algorithm), stored entirely in MarciDB's KV tree — search walks the tree by key prefix without loading the
  whole index into RAM.
- **Endianness bridge**: the model column stores `Float[N]` big-endian (MarciDB's encoding), while the index
  stores raw little-endian `f32` blocks (what `marci-vector` reads back). The adapter decodes BE on rebuild
  and reads/writes LE in the index.
- **Search**: descend the tree from the root, prune distant clusters, keep a top-`k` heap; SIMD-accelerated
  distance via `marci-vector`.

## Limitations (basic version)

- Batch `$reindex` only; incremental maintenance on insert/update/delete is not wired yet, so the index can
  go stale between reindexes.
- Indexes a single `Float[N]` field per `@custom` declaration.
- `$near` runs standalone plus a post-filter; it can't yet be combined with arbitrary `$order` (results stay
  in distance order) and is rejected inside `$or` / `$not`.

See [docs/CUSTOM-INDEXES.md](../docs/CUSTOM-INDEXES.md) for the `@custom` index SPI and how to author a
provider of your own, and [`marci-vector`](../marci-vector/README.md) for the indexing algorithm itself.

## License

MIT
