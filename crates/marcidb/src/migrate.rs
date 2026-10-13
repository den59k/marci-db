//! Migration RUNTIME: the operation set (`MigrateOp`) and the physical `apply` of those ops against the DB
//! (create/drop entity trees, build/drop index trees). This is all the engine needs — given a target
//! `Schema` and a list of ops, make the storage match. Everything "smart" (computing the ops via `diff`,
//! and the `.march` file/snapshot text format via `serialize_migration`/`evolve`/`migration_ops`) lives in
//! the `marcidb-schema` crate (the authoring layer).

use canopydb::WriteTransaction;

use crate::index_utils::{encode_full_index, encode_index, encode_unique_group};
use crate::json_parsers::default_bytes;
use crate::schema::{Entity, Field, FieldDefault, FieldIndex, FieldLocation, FieldType, RefBinding, Schema, SchemaError, MigrateError, MigrateOp};
use crate::update_op::process_update::with_body_value;
use crate::utils::{check_exists_condition, get_data};
use crate::StorageError;
use crate::error::RequireTree;

// `MigrateOp` (the op set) and `MigrateError` (authoring-side diff errors) now live in the `marcidb-schema`
// foundation crate — the engine only consumes them when applying.

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
  /// A required field (no `?`, no `@default`) added to a model that has rows: they'd have no value.
  RequiredField { field: String },
  /// A `@custom` (module) index names a provider that isn't registered — module not compiled in, or a typo.
  /// Caught at migration time so a broken schema fails fast rather than at the first reindex/query.
  NoProvider { provider: String, field: String },
  /// A provider rejected the indexed field/args at migration time (wrong field type, bad args).
  InvalidIndex { field: String, detail: String },
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
      MigrateApplyError::RequiredField { field } => write!(f, "{} can't be added as required: existing rows would have no value — give it a @default or make it optional (?)", field),
      MigrateApplyError::NoProvider { provider, field } => write!(f, "no index provider registered for '@{}' (on field '{}') — the module is not compiled in, or the attribute is a typo", provider, field),
      MigrateApplyError::InvalidIndex { field, detail } => write!(f, "invalid index on '{}': {}", field, detail),
      MigrateApplyError::Diff(e) => write!(f, "{}", e),
      MigrateApplyError::Schema(e) => write!(f, "{}", e),
      MigrateApplyError::Storage(e) => write!(f, "{:?}", e),
    }
  }
}

impl From<canopydb::Error> for MigrateApplyError { fn from(e: canopydb::Error) -> Self { MigrateApplyError::Storage(StorageError::Backend(e)) } }
impl From<StorageError> for MigrateApplyError { fn from(e: StorageError) -> Self { MigrateApplyError::Storage(e) } }
impl From<SchemaError> for MigrateApplyError { fn from(e: SchemaError) -> Self { MigrateApplyError::Schema(e) } }
impl From<MigrateError> for MigrateApplyError { fn from(e: MigrateError) -> Self { MigrateApplyError::Diff(e) } }

/// Executes the physical migration operations in an open write transaction.
/// add field — writes its `@default` into existing rows, refuses a required field when there are rows
/// (otherwise metadata only); alter field —
/// metadata only; add index — builds the tree from existing rows; drop index — deletes the tree;
/// create/drop entity — the entity's trees.
/// A slot shift is already rejected in `diff` (`UnsupportedLayoutChange`), so we don't check for it here.
pub fn apply(tx: &WriteTransaction, old: &Schema, new: &Schema, ops: &[MigrateOp]) -> Result<(), MigrateApplyError> {
  for op in ops {
    match op {
      // The new slot is already in `new`; old rows are read by the forward-compatible reader, and a
      // `@default` is written into them (a required field is refused instead). A `@list` relation field additionally needs its reverse tree to
      // exist before the first write (existing rows simply have an empty array).
      MigrateOp::AddField { entity, field } => {
        let e = find_entity(new, entity);
        let f = find_field(e, field);
        fill_added_field(tx, new, e, f)?;
        if let FieldType::RefList(ref_info) = &f.ty && let RefBinding::IdList { rev_tree } = &ref_info.binding {
          tx.get_or_create_tree(rev_tree.as_bytes())?;
        }
        // A list bound to the other side's reference (`files File[] @bind(File.owner)`) keeps its pairs in
        // a binding tree of its own: without it the first connect — or the first delete of a row of this
        // model — fails on a missing tree. Rows that already point here are entered now.
        if let FieldType::RefList(ref_info) = &f.ty && let RefBinding::IndexTree(tree_name) = &ref_info.binding {
          let mut tree = tx.get_or_create_tree(tree_name.as_bytes())?;
          if let Some(rev_idx) = ref_info.rev_field_idx {
            let child = &new.models[ref_info.model_index];
            let rev_field = &child.fields[rev_idx];
            if let FieldType::Ref(rev_info) = &rev_field.ty && matches!(rev_info.binding, RefBinding::FieldValue)
              && let Some(child_tree) = tx.get_tree(child.name.as_bytes())? {
              for row in child_tree.iter()? {
                let (child_id, body) = row?;
                if let Some(parent_id) = get_data(child, rev_field, &child_id, &body, new) {
                  tree.insert(&[parent_id, child_id.as_ref()].concat(), &[])?;
                }
              }
            }
          }
        }
      }
      MigrateOp::AlterField { .. } => {}
      MigrateOp::AddIndex { entity, field, .. } => build_index(tx, new, entity, field)?,
      MigrateOp::DropIndex { entity, field, .. } => drop_index(tx, old, entity, field)?,
      MigrateOp::AddUniqueGroup { entity, name, .. } => build_unique_group(tx, new, entity, name)?,
      MigrateOp::DropUniqueGroup { entity, name } => {
        if let Some(group) = find_entity(old, entity).unique_groups.iter().find(|g| &g.name == name) {
          tx.delete_tree(group.tree_name.as_bytes())?;
        }
      }
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
      match &ref_info.binding {
        RefBinding::IndexTree(tree_name) => { tx.get_or_create_tree(tree_name.as_bytes())?; }
        // Idempotent with the declared back-reference's IndexTree (same name), and the only
        // creation point for a hidden reverse tree
        RefBinding::IdList { rev_tree } => { tx.get_or_create_tree(rev_tree.as_bytes())?; }
        _ => {}
      }
    }
    for index in field.indexes.iter() {
      tx.get_or_create_tree(index.tree_name())?;
    }
  }
  for group in entity.unique_groups.iter() {
    tx.get_or_create_tree(group.tree_name.as_bytes())?;
  }
  Ok(())
}

fn drop_entity_trees(tx: &WriteTransaction, entity: &Entity) -> Result<(), canopydb::Error> {
  for field in entity.fields.iter() {
    if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &field.ty {
      match &ref_info.binding {
        RefBinding::IndexTree(tree_name) => { tx.delete_tree(tree_name.as_bytes())?; }
        // A declared back-reference owns the same tree and drops it with ITS entity — only the
        // hidden reverse tree is this entity's to drop
        RefBinding::IdList { rev_tree } if ref_info.rev_field_idx.is_none() => {
          tx.delete_tree(rev_tree.as_bytes())?;
        }
        _ => {}
      }
    }
    for index in field.indexes.iter() {
      tx.delete_tree(index.tree_name())?;
    }
  }
  for group in entity.unique_groups.iter() {
    tx.delete_tree(group.tree_name.as_bytes())?;
  }
  tx.delete_tree(entity.name.as_bytes())?;
  Ok(())
}

/// Builds a composite unique index from the existing rows; two rows with the same parts refuse the migration.
fn build_unique_group(tx: &WriteTransaction, schema: &Schema, entity: &str, name: &str) -> Result<(), MigrateApplyError> {
  let entity = find_entity(schema, entity);
  let group = entity.unique_groups.iter().find(|g| g.name == name)
    .unwrap_or_else(|| panic!("unique group {}.{} not found", entity.name, name));
  let mut tree = tx.get_or_create_tree(group.tree_name.as_bytes())?;
  let model_tree = tx.require_tree(entity.name.as_bytes())?;
  for row in model_tree.iter()? {
    let (id, body) = row?;
    let Some(key) = encode_unique_group(entity, group, &id, &body, schema) else { continue };
    if tree.prefix_keys(&key.as_slice())?.next().transpose()?.is_some() {
      return Err(MigrateApplyError::UniqueViolation { field: group.fields_text(entity) });
    }
    tree.insert(&[ key.as_slice(), id.as_ref() ].concat(), &[])?;
  }
  Ok(())
}

fn find_entity<'a>(schema: &'a Schema, name: &str) -> &'a Entity {
  schema.models.iter().find(|m| m.name == name).unwrap_or_else(|| panic!("entity {} not found", name))
}

fn find_field<'a>(entity: &'a Entity, name: &str) -> &'a Field {
  entity.fields.iter().find(|f| f.name == name).unwrap_or_else(|| panic!("field {}.{} not found", entity.name, name))
}

/// Builds a field's index tree from all existing rows of the entity (backfill). A just-added field
/// already holds its default in old rows (AddField precedes AddIndex); without one, they're skipped.
fn build_index(tx: &WriteTransaction, schema: &Schema, entity: &str, field_name: &str) -> Result<(), MigrateApplyError> {
  let entity = find_entity(schema, entity);
  let field = find_field(entity, field_name);
  if field.indexes.is_empty() {
    return Err(MigrateApplyError::Unsupported("indexing a field of this type is not supported yet"));
  }

  // Module (`@custom`) indexes: just create the (empty) tree. Pre-existing rows are backfilled by `$reindex`
  // (a vector index needs global clustering; full-text is then maintained live on subsequent writes), never
  // by inline backfill here. Value/number indexes are backfilled below.
  let mut has_backfill = false;
  for index in field.indexes.iter() {
    if let FieldIndex::Custom { .. } = index {
      tx.get_or_create_tree(index.tree_name())?;
    } else {
      has_backfill = true;
    }
  }
  if !has_backfill {
    return Ok(());
  }

  let model_tree = tx.require_tree(entity.name.as_bytes())?;
  for index in field.indexes.iter() {
    if matches!(index, FieldIndex::Custom { .. }) { continue; }
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

/// Existing rows and an added field, as in SQL's ADD COLUMN. A `@default` is written into every row, as
/// an insert would have: from then on it's a stored value like any other — for reads, filters and the
/// indexes built after this; a later change of `@default` leaves these rows alone, and `now()` stamps
/// them with the migration time. `autoincrement()` numbers the rows 1, 2, 3… in key order, as Postgres
/// does for an added identity column; the counter rebuilt after the commit continues from the last row.
/// A required field (no `?`, no default) is refused while there are rows: they would read `null` for a
/// type that promises a value.
fn fill_added_field(tx: &WriteTransaction, schema: &Schema, entity: &Entity, field: &Field) -> Result<(), MigrateApplyError> {
  let FieldLocation::Body { offset_pos } = field.location else { return Ok(()) };
  let fill = match &field.default_value {
    Some(FieldDefault::Counter(_)) => Some(Fill::Counter(1)),
    Some(default) => default_bytes(default).map(Fill::Value),
    None => None,
  };
  // An absent list reads as `[]`, so only a scalar, enum or reference needs a value.
  let required = !field.nullable && field.default_value.is_none() && !matches!(field.ty, FieldType::PrimitiveList(..));
  if fill.is_none() && !required {
    return Ok(());
  }
  // A new entity gets its tree from CreateEntity — and has no rows yet.
  let Some(mut tree) = tx.get_tree(entity.name.as_bytes())? else { return Ok(()) };

  let mut ids = vec![];
  for row in tree.iter()? {
    let (id, body) = row?;
    // An enum-variant field exists only on rows of its variant.
    if !check_exists_condition(entity, &field.condition, &id, &body, schema) {
      continue;
    }
    if required {
      return Err(MigrateApplyError::RequiredField { field: field.full_name.clone() });
    }
    ids.push(id.to_vec());
  }
  let Some(mut fill) = fill else { return Ok(()) };
  for id in ids {
    let value = fill.next();
    // The read borrows the tree's page; it must be gone before the write.
    let filled = match tree.get(&id)? {
      Some(body) => with_body_value(&body, entity, offset_pos, &value),
      None => None,
    };
    if let Some(body) = filled {
      tree.insert(&id, &body)?;
    }
  }
  Ok(())
}

/// What an add-field migration writes into each existing row.
enum Fill {
  /// The same stored bytes for every row (a literal, or `now()` taken once)
  Value(Vec<u8>),
  /// `autoincrement()`: the next number, in the counter's stored encoding (u64 big-endian)
  Counter(u64),
}

impl Fill {
  fn next(&mut self) -> Vec<u8> {
    match self {
      Fill::Value(v) => v.clone(),
      Fill::Counter(n) => { let v = n.to_be_bytes().to_vec(); *n += 1; v },
    }
  }
}

fn drop_index(tx: &WriteTransaction, old: &Schema, entity: &str, field_name: &str) -> Result<(), MigrateApplyError> {
  let field = find_field(find_entity(old, entity), field_name);
  for index in field.indexes.iter() {
    tx.delete_tree(index.tree_name())?;
  }
  Ok(())
}
