//! Module index SPI: the contract a search/index module (vector, full-text, …) implements so the engine
//! can build, maintain, and query a `@custom(...)` index without depending on the module. The engine holds
//! a registry of `dyn IndexProvider` keyed by provider name; the provider name comes from the schema's
//! [`FieldIndex::Custom`]. Dispatch is coarse (build/search run once per migration/query), and inside each
//! call the provider gets *concrete* handles ([`IndexTree`], [`RowScan`]) — so a module's hot KV loops stay
//! monomorphized and no `canopydb` type leaks across the trait boundary.

use std::collections::HashMap;

use canopydb::{Bytes, Tree, WriteTransaction};
use serde_json::Value;

use crate::{Entity, Field, Schema, StorageError, schema::FieldIndex, utils::get_data};

/// One scored search result: a primary-key id and the provider's distance/relevance score.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: Vec<u8>,
    pub score: f32,
}

/// Error surface a provider may return. `Invalid` → a 4xx (bad args/payload/field type); `Storage` → 5xx.
#[derive(Debug, PartialEq)]
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
/// indexed schema field. `store` is the provider's own KV tree (one per `@custom` index). A provider chooses
/// its maintenance model via [`Self::maintains_incrementally`]: opt in (full-text) and the engine drives the
/// `on_*` hooks on every write; keep the default (vector) and the index is built only by [`Self::rebuild`]
/// (batch `$reindex`), the hooks staying no-ops the engine never calls.
pub trait IndexProvider: Send + Sync {
    /// Provider name, matched against `@custom(<name>, …)`. e.g. "vector", "fulltext".
    fn name(&self) -> &str;

    /// Validate the field type + parse `args`. Called before a (re)build so a bad config fails loudly.
    fn validate(&self, field: &Field, args: &str) -> Result<(), ProviderError>;

    /// Batch (re)build the index from a full scan of the model's rows. `store` has already been cleared.
    fn rebuild(&self, field: &Field, args: &str, scan: RowScan<'_>, store: &mut IndexTree<'_>) -> Result<(), ProviderError>;

    /// Resolve a search operator payload (e.g. `$near` / `$match` JSON) to a ranked candidate id list.
    fn search(&self, field: &Field, args: &str, payload: &Value, store: &IndexTree<'_>) -> Result<Vec<SearchHit>, ProviderError>;

    /// Whether this provider maintains its index incrementally via the `on_*` hooks. Default `false` → the
    /// engine skips the hooks entirely on the write path (the index is built only by `$reindex`), so a
    /// batch-only module stays zero-cost there. Full-text returns `true`; vector keeps the default.
    fn maintains_incrementally(&self) -> bool { false }

    // --- incremental maintenance (driven only for providers that opt in via `maintains_incrementally`;
    // FTS is the first consumer). Default no-op so a batch-only module ignores them. `row` is the document
    // context (id + body + sibling-field accessor); these run inside the same write transaction as the row
    // mutation, so index and data commit/roll back together. ---
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

// ───────────────────────── incremental maintenance dispatch (write path) ─────────────────────────
// The engine calls these from the three write paths (insert / update / delete). They resolve a field's
// `@custom` indexes to their providers and, only for providers that opt in via `maintains_incrementally`,
// open the index tree (from the row's own write tx) and invoke the matching `on_*` hook. An unregistered or
// batch-only provider is silently skipped, so a DB whose module isn't installed still serves plain CRUD and
// vector-style batch indexes keep their zero write-path cost.

/// `true` if `field` carries at least one `@custom` index — the cheap gate before extracting field values.
pub(crate) fn has_custom_index(field: &Field) -> bool {
    field.indexes.iter().any(|i| matches!(i, FieldIndex::Custom { .. }))
}

/// For every live `@custom` index on `field`, open its tree and call `hook(provider, args, store)`.
fn for_each_live_index<F>(providers: &ProviderRegistry, tx: &WriteTransaction, field: &Field, mut hook: F) -> Result<(), ProviderError>
    where F: FnMut(&dyn IndexProvider, &str, &mut IndexTree<'_>) -> Result<(), ProviderError> {
    for index in field.indexes.iter() {
        let FieldIndex::Custom { tree_name, name, args } = index else { continue };
        let Some(provider) = providers.get(name) else { continue };
        if !provider.maintains_incrementally() { continue; }
        let mut store = IndexTree::new(tx.get_or_create_tree(tree_name.as_bytes())?);
        hook(provider, args, &mut store)?;
    }
    Ok(())
}

/// Dispatch `on_insert` for every live `@custom` index of a freshly-written `row`. Each indexed field's new
/// value is read back from the row (absent/null → `None`). Called per row (recursion through nested relations
/// re-enters this for each child).
pub(crate) fn on_row_insert(providers: &ProviderRegistry, tx: &WriteTransaction, row: RowRef<'_>) -> Result<(), ProviderError> {
    for field in row.entity.fields.iter() {
        if !has_custom_index(field) { continue; }
        let new = row.field(field);
        for_each_live_index(providers, tx, field, |provider, args, store| {
            provider.on_insert(field, args, row, new, store)
        })?;
    }
    Ok(())
}

/// Dispatch `on_update` for every live `@custom` index of a single changed `field`, carrying both the old and
/// the new value (a null↔value transition arrives as `old = None` / `new = None`). Called from the update
/// path's per-field change hook, which fires only for fields whose value actually changed.
pub(crate) fn on_field_update(providers: &ProviderRegistry, tx: &WriteTransaction, field: &Field, row: RowRef<'_>, old: Option<&[u8]>, new: Option<&[u8]>) -> Result<(), ProviderError> {
    for_each_live_index(providers, tx, field, |provider, args, store| {
        provider.on_update(field, args, row, old, new, store)
    })
}

/// Dispatch `on_delete` for every live `@custom` index of a single `field` of a `row` being removed. The old
/// value is read from the row (absent/null → `None`). Called per indexed field from the delete path, which
/// reads the old body first (forced by the `DeleteIndex::Custom` entry).
pub(crate) fn on_field_delete(providers: &ProviderRegistry, tx: &WriteTransaction, field: &Field, row: RowRef<'_>) -> Result<(), ProviderError> {
    let old = row.field(field);
    for_each_live_index(providers, tx, field, |provider, args, store| {
        provider.on_delete(field, args, row, old, store)
    })
}

/// A single row's context, handed to the incremental hooks. Single-field providers use only `id` and the
/// passed field value; `field` lets a provider read sibling fields of the same row (e.g. a weight column).
/// `Copy` (all fields are shared references), so the dispatch helpers pass it by value into each hook call.
#[derive(Clone, Copy)]
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
