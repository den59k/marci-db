# Custom index modules (`@custom`)

MarciDB indexes are pluggable. Beyond the built-in value/number indexes (`@index`, `@unique`), a field can
carry a **module index** declared with `@custom(<provider>, <args>)`. A module (a separate crate) implements
the `IndexProvider` trait; the engine handles schema/migration/lifecycle and dispatches build and search to
the provider. The bundled example is the vector index (`marci_vector_index`); full-text search would be added
the same way, with no change to the engine or server cores.

```
embedding Float[1536]  @custom(vector, cosine)
body      String       @custom(fulltext, english)   # a future module
```

`<provider>` selects the provider by name; `<args>` is the raw remainder, parsed by the provider itself —
the schema layer stays provider-agnostic.

## How it fits together

```
schema parse  →  FieldIndex::Custom { name, args, tree_name }   (one KV tree per custom index)
migration     →  creates/drops the index tree (empty); never backfills
$reindex      →  provider.rebuild(scan, store)        ← batch population (v1)
findMany $near →  provider.search(payload, store) → ranked ids → rows fetched in rank order
```

- One `@custom` index = one KV tree. A provider that needs several structures (e.g. postings + stats)
  multiplexes them in that one tree by key prefix.
- v1 populates indexes via the explicit `$reindex` endpoint (a vector index needs global clustering, so
  batch rebuild is the right model). The `on_insert/on_update/on_delete` hooks are reserved for incremental
  maintenance (full-text's natural model) and default to no-op.
- A DB with a `@custom` field opens and serves normal CRUD even if the module isn't compiled in — only
  `$reindex` and `$near` require the provider to be registered.

## Implementing a provider

Add a crate depending on `marcidb` (for the SPI) and your algorithm. Implement `IndexProvider`:

```rust
use marcidb::{Field, IndexProvider, IndexTree, ProviderError, RowScan, SearchHit};

pub struct MyProvider;

impl IndexProvider for MyProvider {
    fn name(&self) -> &str { "myindex" }                 // matches @custom(myindex, …)

    fn validate(&self, field: &Field, args: &str) -> Result<(), ProviderError> {
        // Reject unsupported field types / bad args here (surfaced as HTTP 400).
        Ok(())
    }

    fn rebuild(&self, field: &Field, args: &str, scan: RowScan<'_>, store: &mut IndexTree<'_>)
        -> Result<(), ProviderError>
    {
        // `store` is already cleared. Iterate the model's rows and write index entries.
        for row in scan {
            let (id, value) = row?;            // value: Option<&field bytes> (None = null)
            if let Some(value) = value {
                store.insert(&id, &value)?;    // key/value are raw bytes you choose
            }
        }
        Ok(())
    }

    fn search(&self, field: &Field, args: &str, payload: &serde_json::Value, store: &IndexTree<'_>)
        -> Result<Vec<SearchHit>, ProviderError>
    {
        // Interpret the raw `$near`/`$search` payload, scan `store`, return ranked row ids.
        // SearchHit { id, score } — lower score = better (returned to the client in this order).
        Ok(vec![])
    }
}
```

Storage handles given to you:

- `IndexTree` — `insert` / `get` / `remove` / `clear` / `prefix(p)` / `iter()` over byte keys and values,
  scoped to this index's tree and the current transaction.
- `RowScan` — iterator of `(id, Option<field_value>)` over the model's rows (for `rebuild`).
- `RowRef` (passed to the reserved incremental hooks) — `id` + body + a `field(other_field)` accessor for
  sibling columns.

The ids you store and return are the model's primary-key bytes: `search` returns them, and the engine
fetches each row by id (skipping any that were deleted since the last reindex).

## Registering and serving

Register the provider into a `ProviderRegistry` and install it on every DB the host opens:

```rust
let mut registry = marcidb::ProviderRegistry::new();
registry.register(Box::new(MyProvider));
let registry = std::sync::Arc::new(registry);

let db = marcidb::MarciDB::open(path).with_providers(registry.clone());
```

In the server, wire it behind a cargo feature in `build_providers()` (see `marcidb-server/src/main.rs`) so
the default build stays dependency-light. The HTTP surface is generic — no new route is needed:

- `POST /:db/:model/$reindex` — rebuild that model's custom indexes (`{ "ok": true, "indexed": N }`).
- `POST /:db/$reindex` — rebuild every model's custom indexes.
- `POST /:db/:model/findMany` with `{ "$where": { "<field>": { "$near": <payload> } } }` — ranked search;
  the `<payload>` is handed verbatim to your provider's `search`.

## The vector module (reference)

`marci_vector_index::VectorIndexProvider` (`name = "vector"`) indexes a `Float[N]` field.

- Args: `euclidean` (default) or `cosine` (cosine L2-normalizes points at index and query time).
- `$near` payload: `{ "vector": [..N floats..], "k": 10, "threshold": 0.0 }`.
- Enable in the server with `cargo run -p marcidb-server --features vector` (pulls in `marci_vector`, which
  needs the nightly `portable_simd` feature).

## v1 limitations

- Population is via `$reindex`; there is no automatic incremental maintenance yet (the hooks exist but the
  engine does not call them on writes in v1).
- `$near`/`$search` runs standalone plus a residual post-filter; it cannot yet be combined with arbitrary
  `$order` (results stay in rank order) and is rejected inside `$or`/`$not`.
- One index spans a single field; a multi-field index (e.g. full-text over title+body) needs a row-level
  hook and is a later extension.
