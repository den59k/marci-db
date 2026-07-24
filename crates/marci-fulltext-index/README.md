# marci-fulltext-index

Full-text search for [MarciDB](../README.md), as a pluggable `@custom` index module. It gives a `String`
field a ranked text index (Snowball stemming + an on-disk inverted index with tf·idf scoring), and works for
**English and Russian out of the box** — including a single field that mixes both scripts.

```
model Post {
    title String
    body  String  @fulltext            # auto: Russian + English
    note  String  @fulltext(russian)   # force Russian stemming
}
```

```ts
const posts = await db.post.findMany({
  title: true,
  $where: { body: { $search: "быстрый поиск" } },   // or "quick search", or a mix
})
```

## Languages

The argument selects the analyzer language:

| Declaration | Behaviour |
|---|---|
| `@fulltext` / `@fulltext(multi)` | **Default.** Each token is stemmed by script — Cyrillic → Russian, otherwise English. One field handles mixed Russian/English text. |
| `@fulltext(english)` | English stemmer only. |
| `@fulltext(russian)` | Russian stemmer only. |

Stemming means queries match inflected forms: `running` ↔ `runs`, `кошку` ↔ `Кошки`, `машину` ↔ `машина`.

## Querying

`$search` (alias `$near`, accepted on any `@custom` field) takes either a plain query string or an object:

```ts
{ body: { $search: "quick brown fox" } }
{ body: { $search: { query: "quick brown fox", limit: 20 } } }
```

Semantics: a document matches if it contains **any** query term (OR); results are ordered most-relevant
first, scored by `Σ tf·idf` over the matching terms. The query's `$limit` further trims the result.

Over the raw HTTP API:

```bash
curl -X POST http://localhost:3000/blog/Post/findMany \
  -H 'Content-Type: application/json' \
  --data-binary '{"title":true,"$where":{"body":{"$search":"быстрый поиск"}}}'
```

## Building the index

The index is **live**: an insert/update/delete maintains it in the same transaction as the row, so a
`$search` reflects writes immediately — no `$reindex` after ordinary writes. You still call `$reindex` once to
**backfill** rows that predate the index (adding `@fulltext` to a table creates the tree empty), and to
rebuild after a bulk import bypassing the API or a language/args change:

```bash
curl -X POST http://localhost:3000/blog/Post/$reindex     # one model
curl -X POST http://localhost:3000/blog/$reindex          # every model in the DB
```

## Enabling it

In the server, compile the module in behind its cargo feature (stable toolchain — no nightly):

```bash
cargo run -p marcidb-server --features fulltext
```

When embedding MarciDB as a library, register the provider on the database:

```rust
use std::sync::Arc;
use marcidb::{MarciDB, ProviderRegistry};
use marci_fulltext_index::FullTextProvider;

let mut registry = ProviderRegistry::new();
registry.register(Box::new(FullTextProvider::new()));
let db = MarciDB::open(path).with_providers(Arc::new(registry));
```

## How it works

- **Analyzer**: Unicode word tokenizer (`is_alphanumeric` boundaries) → lowercase → [Snowball][snowball]
  stemmer ([`rust-stemmers`]). In `multi` mode the stemmer is chosen per token by script.
- **Index** (one KV tree, multiplexed by a tag byte):
  - posting: `0x01 <term> 0x00 <id>` → term frequency (`u32` BE)
  - stats:   `0x00 'N'` → document count (`u64` BE)
  Because a stemmed term never contains `0x00`, the separator delimits term from id and keeps a short
  term's prefix scan from matching a longer term.
- **Ranking**: OR over query terms, `Σ tf · idf` (BM25 idf), best first.

[snowball]: https://snowballstem.org/
[`rust-stemmers`]: https://crates.io/crates/rust-stemmers

## Limitations (basic version)

- No stop-word list — common words are down-weighted by idf rather than dropped.
- Live on single-field writes; `$reindex` is still needed to backfill pre-existing rows when the index is
  first added (or after a language/args change).
- Indexes a single `String` field; a multi-field index (e.g. title + body as one document) is a future
  extension.

See [docs/CUSTOM-INDEXES.md](../docs/CUSTOM-INDEXES.md) for the `@custom` index SPI and how to author a
provider of your own.

## License

MIT
