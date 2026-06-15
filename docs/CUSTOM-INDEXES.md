# Custom index modules

MarciDB indexes are pluggable. Beyond the built-in value/number indexes (`@index`, `@unique`), a field can
carry a **module index**: a module (a separate crate) implements the `IndexProvider` trait, and the engine
handles schema/migration/lifecycle and dispatches build and search to it. The bundled modules are vector
(`marci_vector_index`) and full-text (`marci_fulltext_index`).

```
embedding Float[1536]  @vector(cosine)
body      String       @fulltext(english)
```

A module index is declared with `@<provider>(<args>)`, named after the provider — **any attribute that
matches no built-in is parsed as a module index** (so a new module just picks an attribute name; the schema
layer stays provider-agnostic). `<args>` is the raw remainder, parsed by the provider itself. The explicit
form `@custom(<provider>, <args>)` is equivalent — useful if a provider name would collide with a built-in
attribute. (A typo like `@indx` is no longer a *parse* error, but it isn't silent either: `$sync` / `$migrate`
validates every newly-added module index against the registered providers and rejects an unknown one — the
provider must be compiled in to apply a schema that uses it.)

## How it fits together

```
schema parse  →  FieldIndex::Custom { name, args, tree_name }   (one KV tree per custom index)
migration     →  creates/drops the index tree (empty); never backfills
$reindex      →  provider.rebuild(scan, store)        ← batch (re)build / backfill
insert/update/delete →  provider.on_insert / on_update / on_delete (id, old/new, store)
                                                      ← live maintenance (opt-in)
findMany $near →  provider.search(payload, store) → ranked ids → rows fetched in rank order
```

- One `@custom` index = one KV tree. A provider that needs several structures (e.g. postings + stats)
  multiplexes them in that one tree by key prefix.
- A provider chooses its maintenance model via `maintains_incrementally()`. A provider that returns `true`
  (full-text) is fed every write through the `on_insert/on_update/on_delete` hooks, in the row's own
  transaction, so a search reflects writes immediately. A provider that returns `false` (the default —
  vector, whose global clustering can't be updated per-point cheaply) is built only by `$reindex`; the
  engine never touches its tree on the write path, so those writes stay zero-cost. Either way `$reindex`
  remains the way to **backfill** rows that predate the index (it's empty after the migration that adds it)
  and to rebuild after an analyzer/args change.
- A DB with a module index **opens** and serves normal CRUD even if the module isn't compiled in. Only
  **changing** the schema (`$sync` / `$migrate`), `$reindex`, and `$near` require the provider — so a DB
  built on one server runs on a leaner build, but you must recompile with the module before migrating it.

## Implementing a provider

Add a crate depending on `marcidb` (for the SPI) and your algorithm. Implement `IndexProvider`:

```rust
use marcidb::{Field, IndexProvider, IndexTree, ProviderError, RowScan, SearchHit};

pub struct MyProvider;

impl IndexProvider for MyProvider {
    fn name(&self) -> &str { "myindex" }                 // matches @myindex(…) / @custom(myindex, …)

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
- `RowRef` (passed to the incremental `on_*` hooks) — `id` + body + a `field(other_field)` accessor for
  sibling columns. The hooks also receive the changed field's `old`/`new` bytes directly (an `on_update`
  carries both, so a provider can remove the old entries and write the new ones; a null↔value transition
  arrives as `old`/`new` = `None`).

The ids you store and return are the model's primary-key bytes: `search` returns them, and the engine
fetches each row by id (skipping any stale entries — e.g. a row a batch-only provider hasn't reindexed yet).

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

## Bundled modules

### Vector — `marci_vector_index::VectorIndexProvider` (`name = "vector"`)

Indexes a `Float[N]` field for approximate nearest-neighbour search.

- Args: `euclidean` (default) or `cosine` (cosine L2-normalizes points at index and query time).
- `$near` payload: `{ "vector": [..N floats..], "k": 10, "threshold": 0.0 }`.
- Enable with `cargo +nightly run -p marcidb-server --features vector` (pulls in `marci_vector`, which uses
  `portable_simd` — hence nightly). See [Building the server with the modules](#building-the-server-with-the-modules).

### Full-text — `marci_fulltext_index::FullTextProvider` (`name = "fulltext"`)

Indexes a `String` field for ranked text search (inverted index + tf·idf), with Snowball stemming.

- Args (language): `multi` (default), `english`, or `russian`. `multi` stems each token by script —
  Cyrillic → Russian, otherwise English — so one field handles mixed Russian/English text.
- `$search`/`$near` payload: a plain string `"quick brown fox"`, or `{ "query": "...", "limit": 50 }`.
- OR semantics over query terms; results ranked best-first; the query's `$limit` further trims them.
- Enable with `cargo run -p marcidb-server --features fulltext` (stable toolchain). See [Building the server with the modules](#building-the-server-with-the-modules).

```
body String @fulltext            # auto RU+EN
note String @fulltext(russian)   # force Russian stemming
```

It is also the worked example of a non-vector module: a `String` field, **live-maintained** through the
`on_*` hooks (with `$reindex` for backfill), with the provider owning its own key layout (posting list + a
doc-count stats key, multiplexed by a tag byte in one tree).

## Building the server with the modules

A plain `cargo build -p marcidb-server` includes no modules. Turn them on with cargo features. **Full-text
builds on stable; the vector module needs nightly** (it uses `std::simd` / `portable_simd`), so any build
that includes `vector` does too:

```bash
# both modules — vector pulls in portable_simd, so use the nightly toolchain
cargo +nightly build --release -p marcidb-server --features "vector fulltext"

# full-text only — stable is fine
cargo build --release -p marcidb-server --features fulltext
```

The binary lands at `target/release/marcidb-server`. (`--features "vector fulltext"`, `--features vector,fulltext`,
and `--features vector --features fulltext` are all equivalent. Use `rustup default nightly` instead of the
`+nightly` prefix if you prefer.)

The repository's [`Dockerfile`](../Dockerfile) already builds the server image with both modules (on a nightly
base): `docker build -t marcidb-server .` from the workspace root.

## Limitations

- Live maintenance is opt-in per provider (`maintains_incrementally()`): full-text is maintained on every
  write; vector is `$reindex`-only because its clustering can't be updated per-point cheaply.
- `$reindex` is still needed once after **adding** an index to a table with existing rows (the migration
  creates the tree empty), and after an analyzer/args change; live maintenance covers everything after.
- `$near`/`$search` runs standalone plus a residual post-filter; it cannot yet be combined with arbitrary
  `$order` (results stay in rank order) and is rejected inside `$or`/`$not`.
- One index spans a single field; a multi-field index (e.g. full-text over title+body) needs a row-level
  hook and is a later extension.
- The full-text module has no stop-word list (common words are down-weighted by idf instead) and stems via
  Snowball (English + Russian); other languages / analyzers are straightforward additions.
