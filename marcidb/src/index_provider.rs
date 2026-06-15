//! Module index SPI: the contract a search/index module (vector, full-text, …) implements so the engine
//! can build, maintain, and query a `@custom(...)` index without depending on the module. The engine holds
//! a registry of `dyn IndexProvider` keyed by provider name; the provider name comes from the schema's
//! [`FieldIndex::Custom`]. Dispatch is coarse (build/search run once per migration/query), and inside each
//! call the provider gets *concrete* handles ([`IndexTree`], [`RowScan`]) — so a module's hot KV loops stay
//! monomorphized and no `canopydb` type leaks across the trait boundary.

use std::collections::HashMap;

use canopydb::{Bytes, Tree};
use serde_json::Value;

use crate::{Entity, Field, Schema, StorageError, utils::get_data};

/// One scored search result: a primary-key id and the provider's distance/relevance score.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: Vec<u8>,
    pub score: f32,
}

/// Error surface a provider may return. `Invalid` → a 4xx (bad args/payload/field type); `Storage` → 5xx.
#[derive(Debug)]
pub enum ProviderError {
    /// Bad `@custom` args, an unsupported field type, or a malformed search payload.
    Invalid(String),
    Storage(StorageError),
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderError::Invalid(s) => write!(f, "{}", s),
            ProviderError::Storage(e) => write!(f, "{:?}", e),
        }
    }
}
impl std::error::Error for ProviderError {}

impl From<canopydb::Error> for ProviderError { fn from(e: canopydb::Error) -> Self { ProviderError::Storage(StorageError::Backend(e)) } }
impl From<StorageError> for ProviderError { fn from(e: StorageError) -> Self { ProviderError::Storage(e) } }

/// The contract implemented by an index module. Registered into a [`ProviderRegistry`] under [`Self::name`].
///
/// `args` is the raw remainder of `@custom(name, args)`, parsed by the provider itself. `field` is the
/// indexed schema field. `store` is the provider's own KV tree (one per `@custom` index). v1 drives
/// population through [`Self::rebuild`] (batch `$reindex`); the `on_*` hooks are reserved for incremental
/// maintenance and default to no-op, so a batch-only module ignores them.
pub trait IndexProvider: Send + Sync {
    /// Provider name, matched against `@custom(<name>, …)`. e.g. "vector", "fulltext".
    fn name(&self) -> &str;

    /// Validate the field type + parse `args`. Called before a (re)build so a bad config fails loudly.
    fn validate(&self, field: &Field, args: &str) -> Result<(), ProviderError>;

    /// Batch (re)build the index from a full scan of the model's rows. `store` has already been cleared.
    fn rebuild(&self, field: &Field, args: &str, scan: RowScan<'_>, store: &mut IndexTree<'_>) -> Result<(), ProviderError>;

    /// Resolve a search operator payload (e.g. `$near` / `$match` JSON) to a ranked candidate id list.
    fn search(&self, field: &Field, args: &str, payload: &Value, store: &IndexTree<'_>) -> Result<Vec<SearchHit>, ProviderError>;

    // --- reserved for incremental maintenance (FTS is the first consumer); default no-op so v1/batch
    // modules ignore them. `row` is the document context (id + body + sibling-field accessor); these run
    // inside the same write transaction as the row mutation, so index and data commit/roll back together. ---
    fn on_insert(&self, _field: &Field, _args: &str, _row: RowRef<'_>, _new: Option<&[u8]>, _store: &mut IndexTree<'_>) -> Result<(), ProviderError> { Ok(()) }
    fn on_update(&self, _field: &Field, _args: &str, _row: RowRef<'_>, _old: Option<&[u8]>, _new: Option<&[u8]>, _store: &mut IndexTree<'_>) -> Result<(), ProviderError> { Ok(()) }
    fn on_delete(&self, _field: &Field, _args: &str, _row: RowRef<'_>, _old: Option<&[u8]>, _store: &mut IndexTree<'_>) -> Result<(), ProviderError> { Ok(()) }
}

/// A set of providers, keyed by [`IndexProvider::name`]. Built once (per process) and shared across DBs.
#[derive(Default)]
pub struct ProviderRegistry {
    providers: HashMap<String, Box<dyn IndexProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self { Self { providers: HashMap::new() } }

    /// Register a provider. A later registration under the same name replaces the earlier one.
    pub fn register(&mut self, provider: Box<dyn IndexProvider>) {
        self.providers.insert(provider.name().to_string(), provider);
    }

    pub fn get(&self, name: &str) -> Option<&dyn IndexProvider> {
        self.providers.get(name).map(|b| b.as_ref())
    }

    pub fn is_empty(&self) -> bool { self.providers.is_empty() }
}

/// A single row's context, handed to the incremental hooks. Single-field providers use only `id` and the
/// passed field value; `field` lets a provider read sibling fields of the same row (e.g. a weight column).
pub struct RowRef<'a> {
    pub id: &'a [u8],
    pub body: &'a [u8],
    pub entity: &'a Entity,
    pub schema: &'a Schema,
}

impl<'a> RowRef<'a> {
    /// Decoded bytes of another field of this row, or `None` if absent/null.
    pub fn field(&self, field: &Field) -> Option<&'a [u8]> {
        get_data(self.entity, field, self.id, self.body, self.schema)
    }
}

/// A writable/readable handle to one `@custom` index's KV tree. Thin newtype over the storage backend —
/// providers see only byte keys/values, never the engine's `canopydb` types.
pub struct IndexTree<'tx> {
    tree: Tree<'tx>,
}

impl<'tx> IndexTree<'tx> {
    pub(crate) fn new(tree: Tree<'tx>) -> Self { Self { tree } }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ProviderError> {
        self.tree.insert(key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ProviderError> {
        Ok(self.tree.get(key)?.map(|b| b.to_vec()))
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<bool, ProviderError> {
        Ok(self.tree.delete(key)?)
    }

    /// Removes every entry — used at the start of a rebuild.
    pub fn clear(&mut self) -> Result<(), ProviderError> {
        self.tree.clear()?;
        Ok(())
    }

    /// Lazily iterate all entries whose key starts with `prefix` (empty prefix → the whole tree).
    pub fn prefix(&self, prefix: &[u8]) -> Result<IndexIter<'_>, ProviderError> {
        Ok(IndexIter { inner: Box::new(self.tree.prefix(&prefix)?) })
    }

    /// Lazily iterate all entries.
    pub fn iter(&self) -> Result<IndexIter<'_>, ProviderError> {
        Ok(IndexIter { inner: Box::new(self.tree.iter()?) })
    }
}

/// Lazy iterator over `(key, value)` byte pairs of an [`IndexTree`]. Boxes the backend iterator so the
/// public item type stays free of `canopydb`; the box is per-scan (not per-item) and the per-item virtual
/// call is dwarfed by whatever the provider does with each entry.
pub struct IndexIter<'a> {
    inner: Box<dyn Iterator<Item = Result<(Bytes, Bytes), canopydb::Error>> + 'a>,
}

impl<'a> Iterator for IndexIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>), ProviderError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| {
            r.map(|(k, v)| (k.to_vec(), v.to_vec())).map_err(ProviderError::from)
        })
    }
}

/// Iterator over a model's rows projecting one field: yields `(id, Option<field_value>)`. `None` value =
/// the field is absent/null on that row. Built on the model tree scan + the engine's `get_data` extractor;
/// handed to [`IndexProvider::rebuild`].
pub struct RowScan<'a> {
    inner: Box<dyn Iterator<Item = Result<(Bytes, Bytes), canopydb::Error>> + 'a>,
    entity: &'a Entity,
    field: &'a Field,
    schema: &'a Schema,
}

impl<'a> RowScan<'a> {
    pub(crate) fn new(
        inner: Box<dyn Iterator<Item = Result<(Bytes, Bytes), canopydb::Error>> + 'a>,
        entity: &'a Entity,
        field: &'a Field,
        schema: &'a Schema,
    ) -> Self {
        Self { inner, entity, field, schema }
    }
}

impl<'a> Iterator for RowScan<'a> {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>), ProviderError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let (id, body) = row.map_err(ProviderError::from)?;
            let value = get_data(self.entity, self.field, &id, &body, self.schema).map(|v| v.to_vec());
            Ok((id.to_vec(), value))
        })
    }
}
