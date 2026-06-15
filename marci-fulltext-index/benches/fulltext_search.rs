//! Full-text index benchmark: inverted-index build (`$reindex`) and `$search` latency over a synthetic
//! mixed English/Russian corpus. Run: `cargo bench -p marci_fulltext_index`
//!
//! Deterministic (fixed-seed LCG) so runs are comparable.

use std::sync::Arc;
use std::time::Instant;

use marcidb::{MarciDB, ProviderRegistry, decode_document, parse_insert, parse_query};
use marci_fulltext_index::FullTextProvider;
use serde_json::json;

const N: usize = 5_000; // indexed documents
const WORDS_PER_DOC: usize = 24;

const VOCAB: &[&str] = &[
    // English
    "data", "index", "search", "query", "vector", "engine", "schema", "record", "relation", "field",
    "quick", "brown", "fox", "lazy", "dog", "river", "mountain", "forest", "ocean", "light",
    // Russian
    "книга", "машина", "кошка", "собака", "город", "море", "солнце", "ветер", "дорога", "музыка",
];

/// Small linear-congruential generator — keeps the benchmark deterministic and dependency-free.
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self { Lcg(seed.wrapping_mul(2862933555777941757).wrapping_add(3037000493)) }
    fn next(&mut self, n: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) as usize % n
    }
}

fn registry() -> Arc<ProviderRegistry> {
    let mut reg = ProviderRegistry::new();
    reg.register(Box::new(FullTextProvider::new()));
    Arc::new(reg)
}

fn document(rng: &mut Lcg) -> String {
    (0..WORDS_PER_DOC).map(|_| VOCAB[rng.next(VOCAB.len())]).collect::<Vec<_>>().join(" ")
}

fn bench_query(db: &MarciDB, label: &str, query: &str, iters: u32) {
    let entity = db.get_model("Doc").unwrap();
    let q = parse_query(&db.schema, entity, &json!({
        "title": true,
        "$where": { "body": { "$search": query } }
    })).unwrap();

    let mut hits = 0;
    for _ in 0..5 { hits = db.find_many(&q, |ctx| decode_document(ctx).unwrap()).unwrap().len(); } // warmup

    let start = Instant::now();
    for _ in 0..iters { db.find_many(&q, |ctx| decode_document(ctx).unwrap()).unwrap(); }
    let per = start.elapsed() / iters;
    println!("  {:<32} {:>12.3?}/query  ({} hits)", label, per, hits);
}

fn main() {
    const SCHEMA: &str = "model Doc {\n  title String\n  body String @fulltext\n}";

    let dir = tempfile::tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap()).with_providers(registry());

    // Bulk-insert N documents in a single transaction.
    let mut rng = Lcg::new(1);
    {
        let entity = db.get_model("Doc").unwrap();
        let tx = db.begin_write().unwrap();
        for i in 0..N {
            let op = parse_insert(&db.schema, entity, &json!({ "title": format!("doc {}", i), "body": document(&mut rng) })).unwrap();
            tx.insert_item(entity, &op).unwrap();
        }
        tx.commit().unwrap();
    }

    println!("\n  marci-fulltext-index — {} docs, ~{} words each (EN+RU)\n  {}", N, WORDS_PER_DOC, "-".repeat(56));

    // Index build (inverted index).
    let entity = db.get_model("Doc").unwrap();
    let start = Instant::now();
    db.reindex_entity(entity).unwrap();
    println!("  {:<32} {:>12.3?}", "reindex (build inverted index)", start.elapsed());

    bench_query(&db, "$search 1 term (en)", "data", 500);
    bench_query(&db, "$search 1 term (ru)", "книга", 500);
    bench_query(&db, "$search 3 terms", "river mountain forest", 500);
    bench_query(&db, "$search 3 terms (ru)", "город море солнце", 500);
    println!();
}
