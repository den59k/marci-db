//! Migration RUNTIME: the operation set (`MigrateOp`) and the physical `apply` of those ops against the DB
//! (create/drop entity trees, build/drop index trees). This is all the engine needs — given a target
//! `Schema` and a list of ops, make the storage match. Everything "smart" (computing the ops via `diff`,
//! and the `.march` file/snapshot text format via `serialize_migration`/`evolve`/`migration_ops`) lives in
//! the `marcidb-schema` crate (the authoring layer).

use canopydb::WriteTransaction;

use crate::index_utils::{encode_full_index, encode_index};
use crate::schema::{Entity, Field, FieldLocation, FieldType, RefBinding, Schema, SchemaError};
use crate::utils::get_data;
use crate::StorageError;

/// A migration operation over a flat schema. Entities/fields are addressed by name;
/// resolved fields are taken from the `new`/`old` schema at apply time.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrateOp {
  CreateEntity { name: String },
  DropEntity { name: String },
  AddField { entity: String, field: String },
  DropField { entity: String, field: String },
  /// Field metadata: nullable / default / format / added enum variants
  AlterField { entity: String, field: String },
  AddIndex { entity: String, field: String, unique: bool },
  DropIndex { entity: String, field: String, unique: bool },
}

#[derive(Debug, PartialEq)]
pub enum MigrateError {
  /// Changing a field's type requires data transformation — not supported
  UnsupportedTypeChange { entity: String, field: String, from: String, to: String },
  /// Changing the key (@id) requires a re-key — not supported
  UnsupportedKeyChange { entity: String, field: String },
  /// An existing field's slot moved — old rows would break (slots must be reconciled before diff)
  UnsupportedLayoutChange { entity: String, field: String },
  /// Destructive enum change (variant removed or id reassigned) — requires data migration
  UnsupportedEnumChange { entity: String, field: String, detail: String },
  /// A relation's storage binding changed (current_id / field_value / index_tree) — e.g. adding @bind to a
  /// composite-key relation flips index_tree → current_id. That moves where the relation lives, so it can't
  /// be applied as an in-place migration.
  UnsupportedBindingChange { entity: String, field: String, from: String, to: String },
}

impl std::fmt::Display for MigrateError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      MigrateError::UnsupportedTypeChange { entity, field, from, to } =>
        write!(f, "unsupported type change on {}.{}: {} -> {} (migrate the data manually)", entity, field, from, to),
      MigrateError::UnsupportedKeyChange { entity, field } =>
        write!(f, "unsupported key change on {}.{}", entity, field),
      MigrateError::UnsupportedLayoutChange { entity, field } =>
        write!(f, "field slot moved on {}.{} — existing rows would break (slots must be carried from the snapshot)", entity, field),
      MigrateError::UnsupportedEnumChange { entity, field, detail } =>
        write!(f, "unsupported enum change on {}.{}: {}", entity, field, detail),
      MigrateError::UnsupportedBindingChange { entity, field, from, to } =>
        write!(f, "relation binding changed on {}.{}: {} -> {} (a relation's storage layout can't be migrated in place — recreate the relation, or apply the schema to a fresh database via $sync)", entity, field, from, to),
    }
  }
}

// ─────────────────────────────── apply (execution against the DB) ───────────────────────────────

/// Service tree holding migration state: keys `schema` (materialized snapshot) and `version` (u64 BE,
/// bumped per applied migration). No applied-id ledger — coordination is the client's job (see docs).
pub const META_TREE: &[u8] = b"__marci_meta__";

#[derive(Debug)]
pub enum MigrateApplyError {
  /// Operation not supported (yet): drop field (needs a slot tombstone), etc.
  Unsupported(&'static str),
  /// `add unique` found duplicates in existing data
  UniqueViolation { field: String },
  /// Error computing the diff (carried from the authoring layer)
  Diff(MigrateError),
  /// Invalid snapshot/schema
  Schema(SchemaError),
  Storage(StorageError),
}

impl std::fmt::Display for MigrateApplyError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      MigrateApplyError::Unsupported(s) => write!(f, "{}", s),
      MigrateApplyError::UniqueViolation { field } => write!(f, "unique violation on {} in existing data", field),
      MigrateApplyError::Diff(e) => write!(f, "{}", e),
      MigrateApplyError::Schema(e) => write!(f, "{}", e),
      MigrateApplyError::Storage(e) => write!(f, "{:?}", e),
    }
  }
}

impl From<canopydb::Error> for MigrateApplyError { fn from(e: canopydb::Error) -> Self { MigrateApplyError::Storage(StorageError(e)) } }
impl From<StorageError> for MigrateApplyError { fn from(e: StorageError) -> Self { MigrateApplyError::Storage(e) } }
impl From<SchemaError> for MigrateApplyError { fn from(e: SchemaError) -> Self { MigrateApplyError::Schema(e) } }
impl From<MigrateError> for MigrateApplyError { fn from(e: MigrateError) -> Self { MigrateApplyError::Diff(e) } }

/// Executes the physical migration operations in an open write transaction.
/// add/alter field — metadata only (v2 format, no row rewriting); add index — builds
/// the tree from existing rows; drop index — deletes the tree; create/drop entity — the entity's trees.
/// A slot shift is already rejected in `diff` (`UnsupportedLayoutChange`), so we don't check for it here.
pub fn apply(tx: &WriteTransaction, old: &Schema, new: &Schema, ops: &[MigrateOp]) -> Result<(), MigrateApplyError> {
  for op in ops {
    match op {
      // Metadata: the new slot is already in `new`, old rows are read by the forward-compatible reader
      MigrateOp::AddField { .. } | MigrateOp::AlterField { .. } => {}
      MigrateOp::AddIndex { entity, field, .. } => build_index(tx, new, entity, field)?,
      MigrateOp::DropIndex { entity, field, .. } => drop_index(tx, old, entity, field)?,
      MigrateOp::CreateEntity { name } => create_entity_trees(tx, find_entity(new, name))?,
      MigrateOp::DropEntity { name } => drop_entity_trees(tx, find_entity(old, name))?,
      MigrateOp::DropField { entity, field } => {
        // The field is leaving `new`, so look it up in `old`. A Body scalar/enum/list needs NO physical work:
        // old rows keep dead bytes at the now-retired slot, new rows skip it, and a value index (if any) was
        // dropped by a preceding DropIndex op. Relation/key fields aren't supported yet.
        let f = find_field(find_entity(old, entity), field);
        match (&f.ty, &f.location) {
          (FieldType::Ref(_) | FieldType::RefList(_), _) =>
            return Err(MigrateApplyError::Unsupported("dropping a relation field is not supported yet — remove the related model or its @bind instead")),
          (_, FieldLocation::Key { .. }) =>
            return Err(MigrateApplyError::Unsupported("cannot drop a primary-key field")),
          _ => {}
        }
      }
    }
  }
  Ok(())
}

/// Creates the entity's trees: main + index + relation-index. Used both during `MarciDB` initialization
/// and during apply `CreateEntity` (the entity is empty, no backfill needed)
pub fn create_entity_trees(tx: &WriteTransaction, entity: &Entity) -> Result<(), canopydb::Error> {
  tx.get_or_create_tree(entity.name.as_bytes())?;
  for field in entity.fields.iter() {
    if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &field.ty {
      if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
        tx.get_or_create_tree(tree_name.as_bytes())?;
      }
    }
    for index in field.indexes.iter() {
      tx.get_or_create_tree(index.tree_name())?;
    }
  }
  Ok(())
}

fn drop_entity_trees(tx: &WriteTransaction, entity: &Entity) -> Result<(), canopydb::Error> {
  for field in entity.fields.iter() {
    if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &field.ty {
      if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
        tx.delete_tree(tree_name.as_bytes())?;
      }
    }
    for index in field.indexes.iter() {
      tx.delete_tree(index.tree_name())?;
    }
  }
  tx.delete_tree(entity.name.as_bytes())?;
  Ok(())
}

fn find_entity<'a>(schema: &'a Schema, name: &str) -> &'a Entity {
  schema.models.iter().find(|m| m.name == name).unwrap_or_else(|| panic!("entity {} not found", name))
}

fn find_field<'a>(entity: &'a Entity, name: &str) -> &'a Field {
  entity.fields.iter().find(|f| f.name == name).unwrap_or_else(|| panic!("field {}.{} not found", entity.name, name))
}

/// Builds a field's index tree from all existing rows of the entity (backfill).
/// For a just-added field, old rows yield `None` (the field is absent) — which is correct
fn build_index(tx: &WriteTransaction, schema: &Schema, entity: &str, field_name: &str) -> Result<(), MigrateApplyError> {
  let entity = find_entity(schema, entity);
  let field = find_field(entity, field_name);
  if field.indexes.is_empty() {
    return Err(MigrateApplyError::Unsupported("indexing a field of this type is not supported yet"));
  }

  let model_tree = tx.get_tree(entity.name.as_bytes())?.unwrap();
  for index in field.indexes.iter() {
    let mut index_tree = tx.get_or_create_tree(index.tree_name())?;
    for row in model_tree.iter()? {
      let (id, body) = row?;
      let Some(value) = get_data(entity, field, &id, &body, schema) else { continue };
      if index.is_unique() {
        let encoded = encode_index(field, index, value);
        if index_tree.prefix_keys(&encoded)?.next().transpose()?.is_some() {
          return Err(MigrateApplyError::UniqueViolation { field: field.full_name.clone() });
        }
      }
      index_tree.insert(&encode_full_index(field, index, &id, value), &[])?;
    }
  }
  Ok(())
}

fn drop_index(tx: &WriteTransaction, old: &Schema, entity: &str, field_name: &str) -> Result<(), MigrateApplyError> {
  let field = find_field(find_entity(old, entity), field_name);
  for index in field.indexes.iter() {
    tx.delete_tree(index.tree_name())?;
  }
  Ok(())
}
