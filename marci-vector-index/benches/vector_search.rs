//! Vector index benchmark: cluster-tree build (`$reindex`) and `$near` search latency over a synthetic
//! corpus of random embeddings. Run: `cargo bench -p marci_vector_index`
//!
//! Deterministic (fixed-seed LCG) so runs are comparable; vectors are pseudo-random in [-1, 1].

use std::sync::Arc;
use std::time::Instant;

use marcidb::{MarciDB, ProviderRegistry, decode_document, parse_insert, parse_query};
use marci_vector_index::VectorIndexProvider;
use serde_json::{Value, json};

const DIM: usize = 128;
const N: usize = 5_000; // indexed vectors
const QUERIES: usize = 200; // search queries timed

/// Small linear-congruential generator — keeps the benchmark deterministic and dependency-free.
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self { Lcg(seed.wrapping_mul(2862933555777941757).wrapping_add(3037000493)) }
    fn step(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }
    /// A pseudo-random vector of `DIM` floats in [-1, 1).
    fn vector(&mut self) -> Vec<f32> {
        (0..DIM).map(|_| ((self.step() >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0).collect()
    }
}

fn registry() -> Arc<ProviderRegistry> {
    let mut reg = ProviderRegistry::new();
    reg.register(Box::new(VectorIndexProvider::new()));
    Arc::new(reg)
}

fn main() {
    let schema = format!("model Item {{\n  name String\n  embedding Float[{}] @custom(vector, cosine)\n}}", DIM);

    let dir = tempfile::tempdir().unwrap();
    let db = MarciDB::new(&schema, dir.path().to_str().unwrap()).with_providers(registry());

    // Bulk-insert N vectors in a single transaction (keeps setup off the hot path).
    let mut rng = Lcg::new(1);
    {
        let entity = db.get_model("Item").unwrap();
        let tx = db.begin_write().unwrap();
        for i in 0..N {
            let arr: Vec<Value> = rng.vector().iter().map(|f| json!(f)).collect();
            let op = parse_insert(&db.schema, entity, &json!({ "name": format!("item {}", i), "embedding": arr })).unwrap();
            tx.insert_item(entity, &op).unwrap();
        }
        tx.commit().unwrap();
    }

    println!("\n  marci-vector-index — {} vectors, dim {}\n  {}", N, DIM, "-".repeat(52));

    // Index build (cluster tree).
    let entity = db.get_model("Item").unwrap();
    let start = Instant::now();
    db.reindex_entity(entity).unwrap();
    println!("  {:<34} {:>12.3?}", "reindex (build cluster tree)", start.elapsed());

    // Pre-parse search queries so parsing is excluded from the timing (type inferred — QueryOp unnamed).
    let queries: Vec<_> = (0..QUERIES).map(|i| {
        let mut q = Lcg::new(1_000 + i as u64);
        let arr: Vec<Value> = q.vector().iter().map(|f| json!(f)).collect();
        parse_query(&db.schema, entity, &json!({
            "name": true,
            "$where": { "embedding": { "$near": { "vector": arr, "k": 10 } } }
        })).unwrap()
    }).collect();

    // Warmup.
    for q in queries.iter().take(10) {
        db.find_many(q, |ctx| decode_document(ctx).unwrap()).unwrap();
    }

    let start = Instant::now();
    let mut hits = 0usize;
    for q in &queries {
        hits += db.find_many(q, |ctx| decode_document(ctx).unwrap()).unwrap().len();
    }
    let per = start.elapsed() / queries.len() as u32;
    println!("  {:<34} {:>12.3?}/query  (k=10, {} hits avg)", "$near search", per, hits / queries.len());
    println!();
}
