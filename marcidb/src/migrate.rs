//! Migration RUNTIME on top of the materialized snapshot (see [`crate::snapshot`]): the operation set
//! (`MigrateOp`), the physical `apply` against the DB, and the file/snapshot text manipulation (`evolve`,
//! `migration_ops`) used by the dumb `$migrate` path. The "smart" side — computing a diff between two
//! schemas and rendering migration files — lives in the `marcidb-schema` crate (the authoring layer).

use canopydb::WriteTransaction;

use crate::index_utils::{encode_full_index, encode_index};
use crate::schema::{Entity, Field, FieldType, RefBinding, Schema, SchemaError};
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
  /// Error computing the diff
  Diff(MigrateError),
  /// The supplied migration history diverges from the applied one (ledger): the applied part must be a prefix
  HistoryDiverged { position: usize, applied: String, incoming: String },
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
      MigrateApplyError::HistoryDiverged { position, applied, incoming } =>
        write!(f, "migration history diverged at #{}: server applied \"{}\", got \"{}\"", position, applied, incoming),
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
      MigrateOp::DropField { .. } => return Err(MigrateApplyError::Unsupported("drop field (slot tombstone) is not supported yet")),
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

// ─────────────────────── migration file (self-contained actions) ───────────────────────
//
// A migration file is a list of SELF-CONTAINED actions, ONE PER LINE: each carries its definition (a
// snapshot line), so the whole snapshot isn't put into the file. `create entity X` is a bare line — the
// entity's fields follow as `add field X.f` actions, so a field has a single definition surface everywhere.
// The developer sees the CHANGES (this is what gets reviewed) and looks at the full schema in schema.marci.
// The server is dumb: `evolve` applies the actions to its current snapshot → new snapshot → parse_snapshot;
// the physics is determined by the actions themselves (create entity/add field/add index/...).
// Migration FILES are rendered by `serialize_migration` in the `marcidb-schema` crate (the authoring layer).

/// An action from a migration file. Field actions carry the definition text (snapshot line) for `evolve`
#[derive(Debug, Clone)]
enum FileOp {
  CreateEntity { name: String },
  DropEntity { name: String },
  AddField { entity: String, field: String, line: String },
  AlterField { entity: String, field: String, line: String },
  DropField { entity: String, field: String },
  AddIndex { entity: String, field: String, unique: bool },
  DropIndex { entity: String, field: String, unique: bool },
}

impl FileOp {
  /// The physical operation (without definitions) for `apply`
  fn to_migrate_op(&self) -> MigrateOp {
    match self {
      FileOp::CreateEntity { name } => MigrateOp::CreateEntity { name: name.clone() },
      FileOp::DropEntity { name } => MigrateOp::DropEntity { name: name.clone() },
      FileOp::AddField { entity, field, .. } => MigrateOp::AddField { entity: entity.clone(), field: field.clone() },
      FileOp::AlterField { entity, field, .. } => MigrateOp::AlterField { entity: entity.clone(), field: field.clone() },
      FileOp::DropField { entity, field } => MigrateOp::DropField { entity: entity.clone(), field: field.clone() },
      FileOp::AddIndex { entity, field, unique } => MigrateOp::AddIndex { entity: entity.clone(), field: field.clone(), unique: *unique },
      FileOp::DropIndex { entity, field, unique } => MigrateOp::DropIndex { entity: entity.clone(), field: field.clone(), unique: *unique },
    }
  }
}

/// The physical operations from migration-file text (for `apply`)
pub fn migration_ops(text: &str) -> Result<Vec<MigrateOp>, SchemaError> {
  Ok(parse_migration(text)?.iter().map(FileOp::to_migrate_op).collect())
}

/// Applies migration actions to a snapshot → new snapshot text. Used both by the server (apply)
/// and by the client `marci-migrate` (computing prev / planning). Actions are self-contained, so
/// slots/definitions are taken from them; the result is a snapshot that `parse_snapshot` parses next.
pub fn evolve(old_snapshot_text: &str, migration_text: &str) -> Result<String, SchemaError> {
  let mut blocks = snapshot_blocks(old_snapshot_text);
  for op in parse_migration(migration_text)? {
    apply_file_op(&mut blocks, op)?;
  }
  Ok(emit_blocks(&blocks))
}

fn apply_file_op(blocks: &mut Vec<(String, Vec<String>)>, op: FileOp) -> Result<(), SchemaError> {
  let find = |blocks: &mut Vec<(String, Vec<String>)>, name: &str| -> Option<usize> {
    blocks.iter().position(|(n, _)| n == name)
  };
  match op {
    FileOp::CreateEntity { name } => {
      if find(blocks, &name).is_some() {
        return Err(SchemaError(format!("evolve: entity {} already exists", name)));
      }
      blocks.push((name, vec![]));   // empty entity; fields arrive as subsequent AddField actions
    }
    FileOp::DropEntity { name } => { blocks.retain(|(n, _)| n != &name); }
    FileOp::AddField { entity, field, line } => {
      let i = find(blocks, &entity).ok_or_else(|| SchemaError(format!("evolve: unknown entity {}", entity)))?;
      if blocks[i].1.iter().any(|l| field_name_of(l) == field) {
        return Err(SchemaError(format!("evolve: field {}.{} already exists", entity, field)));
      }
      blocks[i].1.push(line);
    }
    FileOp::AlterField { entity, field, line } => {
      let i = find(blocks, &entity).ok_or_else(|| SchemaError(format!("evolve: unknown entity {}", entity)))?;
      let f = blocks[i].1.iter_mut().find(|l| field_name_of(l) == field)
        .ok_or_else(|| SchemaError(format!("evolve: unknown field {}.{}", entity, field)))?;
      *f = line;
    }
    FileOp::DropField { entity, field } => {
      let i = find(blocks, &entity).ok_or_else(|| SchemaError(format!("evolve: unknown entity {}", entity)))?;
      blocks[i].1.retain(|l| field_name_of(l) != field);
    }
    FileOp::AddIndex { entity, field, unique } => set_field_index(blocks, &entity, &field, Some(unique))?,
    FileOp::DropIndex { entity, field, .. } => set_field_index(blocks, &entity, &field, None)?,
  }
  Ok(())
}

/// Changes the index attribute in a field line (None — remove; Some(true) — `@unique`; Some(false) — `@index`)
fn set_field_index(blocks: &mut [(String, Vec<String>)], entity: &str, field: &str, idx: Option<bool>) -> Result<(), SchemaError> {
  let block = blocks.iter_mut().find(|(n, _)| n == entity)
    .ok_or_else(|| SchemaError(format!("evolve: unknown entity {}", entity)))?;
  let line = block.1.iter_mut().find(|l| field_name_of(l) == field)
    .ok_or_else(|| SchemaError(format!("evolve: unknown field {}.{}", entity, field)))?;
  let mut toks: Vec<&str> = line.split_whitespace().filter(|t| *t != "@index" && *t != "@unique").collect();
  match idx {
    Some(true) => toks.push("@unique"),
    Some(false) => toks.push("@index"),
    None => {}
  }
  *line = toks.join(" ");
  Ok(())
}

fn field_name_of(line: &str) -> &str {
  line.split_whitespace().next().unwrap_or("")
}

/// Splits snapshot text into blocks `(entity name, field lines)`, preserving order.
/// The snapshot is flat (fields are single-line), so the parser is simple, with no brace balancing.
fn snapshot_blocks(text: &str) -> Vec<(String, Vec<String>)> {
  let mut blocks = vec![];
  let mut lines = text.lines();
  while let Some(line) = lines.next() {
    let t = line.trim();
    let Some(rest) = t.strip_prefix("Entity ") else { continue };
    let name = rest.trim_end_matches('{').trim().to_string();
    let mut fields = vec![];
    for l in lines.by_ref() {
      let lt = l.trim();
      if lt == "}" { break; }
      if lt.is_empty() { continue; }
      fields.push(lt.to_string());
    }
    blocks.push((name, fields));
  }
  blocks
}

fn emit_blocks(blocks: &[(String, Vec<String>)]) -> String {
  let mut out = String::new();
  for (i, (name, fields)) in blocks.iter().enumerate() {
    if i > 0 { out.push('\n'); }
    out.push_str(&format!("Entity {} {{\n", name));
    for f in fields { out.push_str(&format!("  {}\n", f)); }
    out.push_str("}\n");
  }
  out
}

/// Parses migration-file text into actions — strictly one action per line.
fn parse_migration(text: &str) -> Result<Vec<FileOp>, SchemaError> {
  let mut ops = vec![];
  for raw in text.lines() {
    let line = raw.trim();
    if line.is_empty() || line.starts_with('#') || line.starts_with("//") { continue; }
    ops.push(parse_line_op(line)?);
  }
  Ok(ops)
}

fn parse_line_op(line: &str) -> Result<FileOp, SchemaError> {
  if let Some(rest) = line.strip_prefix("create entity ") {
    return Ok(FileOp::CreateEntity { name: rest.trim().to_string() });
  }
  if let Some(rest) = line.strip_prefix("drop entity ") {
    return Ok(FileOp::DropEntity { name: rest.trim().to_string() });
  }
  if let Some(rest) = line.strip_prefix("add field ") {
    let (entity, field, fline) = parse_field_action(rest)?;
    return Ok(FileOp::AddField { entity, field, line: fline });
  }
  if let Some(rest) = line.strip_prefix("alter field ") {
    let (entity, field, fline) = parse_field_action(rest)?;
    return Ok(FileOp::AlterField { entity, field, line: fline });
  }
  if let Some(rest) = line.strip_prefix("drop field ") {
    let (entity, field) = split_path(rest.trim())?;
    return Ok(FileOp::DropField { entity, field });
  }
  for (kw, unique) in [("add unique ", true), ("add index ", false)] {
    if let Some(rest) = line.strip_prefix(kw) {
      let (entity, field) = split_path(rest.trim())?;
      return Ok(FileOp::AddIndex { entity, field, unique });
    }
  }
  for (kw, unique) in [("drop unique ", true), ("drop index ", false)] {
    if let Some(rest) = line.strip_prefix(kw) {
      let (entity, field) = split_path(rest.trim())?;
      return Ok(FileOp::DropIndex { entity, field, unique });
    }
  }
  Err(SchemaError(format!("unknown migration action: {}", line)))
}

/// `Entity.field String @slot(8) @unique` → (entity, field, "field String @slot(8) @unique")
fn parse_field_action(rest: &str) -> Result<(String, String, String), SchemaError> {
  let rest = rest.trim();
  let (path, spec) = rest.split_once(char::is_whitespace)
    .ok_or_else(|| SchemaError(format!("expected Entity.field <spec> in: \"{}\"", rest)))?;
  let (entity, field) = split_path(path)?;
  let line = format!("{} {}", field, spec.trim());
  Ok((entity, field, line))
}

/// `Entity.field` → ("Entity", "field"). The entity name may contain dots (`User.info`) — split on the last one
fn split_path(path: &str) -> Result<(String, String), SchemaError> {
  let (entity, field) = path.rsplit_once('.')
    .ok_or_else(|| SchemaError(format!("expected Entity.field in: \"{}\"", path)))?;
  Ok((entity.to_string(), field.to_string()))
}
