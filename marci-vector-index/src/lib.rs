//! `@custom(vector, …)` index provider: adapts the pure `marci_vector` clustering algorithm to MarciDB's
//! `IndexProvider` SPI. Register [`VectorIndexProvider`] into a `ProviderRegistry` to give a `Float[N]` field
//! an on-disk approximate-nearest-neighbour index, populated via `$reindex` and queried via `$near`.
//!
//! Endianness note: the model stores `Float[N]` values big-endian (MarciDB's column encoding), while the
//! index tree stores raw little-endian `f32` blocks (`bytemuck` cast, what `marci_vector` reads back). The
//! two representations are bridged here: [`VectorIndexProvider::rebuild`] decodes BE row data, and the
//! `ReadCluster`/`WriteCluster` bridge below reads/writes LE index data.

use marcidb::{
    Field, FieldType, IndexProvider, IndexTree, PrimitiveFieldType, ProviderError, RowScan, SearchHit,
};
use marci_vector::{CustomDistance, ReadCluster, WriteCluster, bytes_to_point, point_to_bytes};
use serde_json::Value;

/// Branching factor of the cluster tree (k-means `k` per level). Matches the value the legacy server used.
const DEFAULT_K: usize = 8;

pub struct VectorIndexProvider;

impl VectorIndexProvider {
    pub fn new() -> Self { VectorIndexProvider }
}

impl Default for VectorIndexProvider {
    fn default() -> Self { Self::new() }
}

impl IndexProvider for VectorIndexProvider {
    fn name(&self) -> &str { "vector" }

    fn validate(&self, field: &Field, args: &str) -> Result<(), ProviderError> {
        VectorConfig::parse(args)?;
        vector_dim(field)?;
        Ok(())
    }

    fn rebuild(&self, field: &Field, args: &str, scan: RowScan<'_>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        let cfg = VectorConfig::parse(args)?;
        let dim = vector_dim(field)?;

        let mut coordinates: Vec<(Vec<u8>, Vec<f32>)> = Vec::new();
        for row in scan {
            let (id, value) = row?;
            let Some(value) = value else { continue }; // null embedding — not indexable
            let mut point = decode_be_f32(&value);
            if point.len() != dim {
                return Err(ProviderError::Invalid(format!(
                    "vector index on '{}': expected {} dimensions, got {}", field.full_name, dim, point.len()
                )));
            }
            if cfg.normalize { l2_normalize(&mut point); }
            coordinates.push((id, point));
        }

        if coordinates.is_empty() {
            return Ok(()); // nothing to cluster (store was already cleared by the engine)
        }
        self.create_cluster(store, &coordinates, cfg.distance);
        Ok(())
    }

    fn search(&self, field: &Field, args: &str, payload: &Value, store: &IndexTree<'_>) -> Result<Vec<SearchHit>, ProviderError> {
        let cfg = VectorConfig::parse(args)?;
        let dim = vector_dim(field)?;
        let SearchParams { mut vector, k, threshold } = SearchParams::parse(payload)?;
        if vector.len() != dim {
            return Err(ProviderError::Invalid(format!(
                "$near on '{}': query vector has {} dimensions, expected {}", field.full_name, vector.len(), dim
            )));
        }
        if cfg.normalize { l2_normalize(&mut vector); }

        let hits = self.find_nearest_points(store, &vector, k, cfg.distance, threshold);
        Ok(hits.into_iter().map(|(id, dist)| SearchHit { id, score: dist }).collect())
    }
}

// ─────────────────────────── storage bridge: marci_vector ⇄ IndexTree ───────────────────────────

impl<'a> WriteCluster<'a, DEFAULT_K> for VectorIndexProvider {
    type WriteContext = IndexTree<'a>;

    fn write_data(&self, ctx: &mut IndexTree<'a>, id: Vec<u8>, centroid: &[f32]) {
        ctx.insert(&id, point_to_bytes(centroid)).expect("vector index write failed");
    }
}

impl<'a> ReadCluster<'a> for VectorIndexProvider {
    type ReadContext = IndexTree<'a>;
    type Iter<'b> = VectorReadIter<'b> where Self: 'b;

    fn read_data<'b>(&'b self, ctx: &'b IndexTree<'a>, prefix: Vec<u8>) -> VectorReadIter<'b> {
        VectorReadIter { inner: ctx.prefix(&prefix).expect("vector index prefix scan failed") }
    }
}

/// Maps the engine's `(key, value_bytes)` index entries to the `(key, Vec<f32>)` shape `marci_vector` wants.
pub struct VectorReadIter<'b> {
    inner: marcidb::IndexIter<'b>,
}

impl<'b> Iterator for VectorReadIter<'b> {
    type Item = (Vec<u8>, Vec<f32>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| {
            let (key, value) = entry.expect("vector index read failed");
            (key, bytes_to_point(&value).to_vec())
        })
    }
}

// ─────────────────────────────────────── config & payload ───────────────────────────────────────

struct VectorConfig {
    distance: CustomDistance<f32>,
    /// Cosine works on unit vectors, so points/queries are L2-normalized at index and query time.
    normalize: bool,
}

impl VectorConfig {
    fn parse(args: &str) -> Result<Self, ProviderError> {
        match args.trim().to_lowercase().as_str() {
            "" | "euclidean" | "l2" => Ok(VectorConfig { distance: CustomDistance::Euclidean, normalize: false }),
            "cosine" => Ok(VectorConfig { distance: CustomDistance::Cosine, normalize: true }),
            other => Err(ProviderError::Invalid(format!(
                "@custom(vector, {}): unknown metric — use 'euclidean' or 'cosine'", other
            ))),
        }
    }
}

struct SearchParams {
    vector: Vec<f32>,
    k: usize,
    threshold: f32,
}

impl SearchParams {
    fn parse(payload: &Value) -> Result<Self, ProviderError> {
        let obj = payload.as_object()
            .ok_or_else(|| ProviderError::Invalid("$near payload must be an object { vector, k?, threshold? }".into()))?;

        let vector = obj.get("vector").and_then(|v| v.as_array())
            .ok_or_else(|| ProviderError::Invalid("$near requires 'vector': [numbers]".into()))?
            .iter().map(|n| n.as_f64().map(|f| f as f32))
            .collect::<Option<Vec<f32>>>()
            .ok_or_else(|| ProviderError::Invalid("$near 'vector' must be an array of numbers".into()))?;

        let k = obj.get("k").and_then(|v| v.as_u64()).unwrap_or(10).max(1) as usize;
        let threshold = obj.get("threshold").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
        Ok(SearchParams { vector, k, threshold })
    }
}

/// `Float[N]` → N. Vector indexes require a fixed-size float list.
fn vector_dim(field: &Field) -> Result<usize, ProviderError> {
    match &field.ty {
        FieldType::PrimitiveList(PrimitiveFieldType::Float, Some(n)) => Ok(*n),
        _ => Err(ProviderError::Invalid(format!(
            "@custom(vector) requires a Float[N] fixed-list field, but '{}' is not one", field.full_name
        ))),
    }
}

/// Decode MarciDB's big-endian `Float[N]` column bytes into `f32`s.
fn decode_be_f32(bytes: &[u8]) -> Vec<f32> {
    bytes.chunks_exact(4).map(|c| f32::from_be_bytes(c.try_into().unwrap())).collect()
}

fn l2_normalize(v: &mut [f32]) {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() { *x /= norm; }
    }
}
