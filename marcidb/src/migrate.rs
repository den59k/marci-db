//! New migration engine on top of the materialized snapshot (see [`crate::snapshot`]).
//! Works on FLAT resolved `Schema` — structs are already models, enums are already inlined into fields,
//! refs/slots/bindings are pinned. So the diff is trivial: comparing entities and fields by name,
//! without expanding sugar. This fully replaced the old text-based migration engine.

use std::collections::HashMap;

use canopydb::WriteTransaction;

use crate::index_utils::{encode_full_index, encode_index};
use crate::schema::{Attribute, Entity, EnumInfo, Field, FieldDefault, FieldExistsCondition, FieldLocation, FieldType, RefBinding, Schema, SchemaError};
use crate::snapshot::serialize_type;
use crate::utils::get_data;
use crate::StorageError;

/// Size of the row "pre-header" (the first Body slot starts at this byte offset)
const PRE_HEADER: usize = 4;

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

/// Diff of two flat schemas: `old` (old snapshot) → `new` (materialized new schema).
/// Both must be in snapshot form (slots/variants reconciled); slot reconciliation is a separate step before diff.
pub fn diff(old: &Schema, new: &Schema) -> Result<Vec<MigrateOp>, MigrateError> {
  let old_by: HashMap<&str, &_> = old.models.iter().map(|e| (e.name.as_str(), e)).collect();
  let new_by: HashMap<&str, &_> = new.models.iter().map(|e| (e.name.as_str(), e)).collect();

  let mut ops = vec![];

  // Created and existing entities. A new entity is just "diff against nothing": CreateEntity, then
  // every field flows through diff_fields as AddField/AddIndex — one field-definition surface, no
  // special-casing of new entities.
  for e in new.models.iter() {
    let old_fields: &[Field] = match old_by.get(e.name.as_str()) {
      Some(old_e) => &old_e.fields,
      None => {
        ops.push(MigrateOp::CreateEntity { name: e.name.clone() });
        &[]
      }
    };
    diff_fields(old, new, &e.name, old_fields, &e.fields, &mut ops)?;
  }

  // Removed entities
  for e in old.models.iter() {
    if !new_by.contains_key(e.name.as_str()) {
      ops.push(MigrateOp::DropEntity { name: e.name.clone() });
    }
  }

  Ok(ops)
}

fn diff_fields(
  old_schema: &Schema, new_schema: &Schema,
  entity: &str, old_fields: &[Field], new_fields: &[Field],
  ops: &mut Vec<MigrateOp>,
) -> Result<(), MigrateError> {
  let old_by: HashMap<&str, &Field> = old_fields.iter().map(|f| (f.name.as_str(), f)).collect();
  let new_by: HashMap<&str, &Field> = new_fields.iter().map(|f| (f.name.as_str(), f)).collect();

  // Added fields (+ index as a separate operation)
  for f in new_fields.iter() {
    if !old_by.contains_key(f.name.as_str()) {
      ops.push(MigrateOp::AddField { entity: entity.to_string(), field: f.name.clone() });
      if let Some(unique) = index_kind(f) {
        ops.push(MigrateOp::AddIndex { entity: entity.to_string(), field: f.name.clone(), unique });
      }
    }
  }

  // Changed fields
  for f in new_fields.iter() {
    let Some(old_f) = old_by.get(f.name.as_str()) else { continue };

    if is_key(old_f) != is_key(f) {
      return Err(MigrateError::UnsupportedKeyChange { entity: entity.to_string(), field: f.name.clone() });
    }
    if let (FieldLocation::Body { offset_pos: a }, FieldLocation::Body { offset_pos: b }) = (&old_f.location, &f.location) {
      if a != b {
        return Err(MigrateError::UnsupportedLayoutChange { entity: entity.to_string(), field: f.name.clone() });
      }
    }

    // Type comparison: enum — separately (adding variants is allowed), everything else — by name
    let mut needs_alter = false;
    match type_cmp(old_schema, old_f, new_schema, f) {
      TypeCmp::Same => {}
      TypeCmp::AdditiveEnum => needs_alter = true,
      TypeCmp::EnumIncompatible(detail) =>
        return Err(MigrateError::UnsupportedEnumChange { entity: entity.to_string(), field: f.name.clone(), detail }),
      TypeCmp::Incompatible =>
        return Err(MigrateError::UnsupportedTypeChange {
          entity: entity.to_string(), field: f.name.clone(),
          from: serialize_type(old_schema, old_f), to: serialize_type(new_schema, f),
        }),
    }

    // A relation's storage binding changing (e.g. adding @bind flips index_tree → current_id) moves the
    // relation's physical home — not an in-place migration. Catch it so it never becomes a silent no-op.
    if let (Some(a), Some(b)) = (ref_binding(old_f), ref_binding(f)) && a != b {
      return Err(MigrateError::UnsupportedBindingChange {
        entity: entity.to_string(), field: f.name.clone(),
        from: binding_kind(a).to_string(), to: binding_kind(b).to_string(),
      });
    }

    // Metadata: nullable / default / format
    if old_f.nullable != f.nullable || default_attr(old_f) != default_attr(f) || format_attr(old_f) != format_attr(f) {
      needs_alter = true;
    }
    if needs_alter {
      ops.push(MigrateOp::AlterField { entity: entity.to_string(), field: f.name.clone() });
    }

    // Index: drop the old one / add the new one
    let (old_idx, new_idx) = (index_kind(old_f), index_kind(f));
    if old_idx != new_idx {
      if let Some(unique) = old_idx {
        ops.push(MigrateOp::DropIndex { entity: entity.to_string(), field: f.name.clone(), unique });
      }
      if let Some(unique) = new_idx {
        ops.push(MigrateOp::AddIndex { entity: entity.to_string(), field: f.name.clone(), unique });
      }
    }
  }

  // Removed fields
  for f in old_fields.iter() {
    if !new_by.contains_key(f.name.as_str()) {
      if let Some(unique) = index_kind(f) {
        ops.push(MigrateOp::DropIndex { entity: entity.to_string(), field: f.name.clone(), unique });
      }
      ops.push(MigrateOp::DropField { entity: entity.to_string(), field: f.name.clone() });
    }
  }

  Ok(())
}

// ─────────────────────────────── slot reconciliation ───────────────────────────────

/// Carries Body-field slots from the old snapshot into the new materialized schema.
/// Fields that match by name keep the slot from `old`; new ones get the next free slot (append-only, above
/// the high-water mark of the old entity). This way "slot ownership" moves from the parser (declaration order,
/// unstable when a field is inserted in the middle) to the migration layer (history). Run BEFORE [`diff`].
///
/// Carries everything order-dependent from the old snapshot: Body-field slots AND enum variant ids
/// (data is stored by them). Then canonicalizes field order to satisfy the row-format invariant.
pub fn reconcile(new: &mut Schema, old: &Schema) {
  carry_slots(new, old);
  carry_enum_ids(new, old);
  // The field order in the array may have diverged from the slot order — canonicalize it
  canonicalize_field_order(new);
}

/// Body fields matching by name keep the slot from old; new ones get the next free slot (append-only)
fn carry_slots(new: &mut Schema, old: &Schema) {
  let old_by: HashMap<&str, &Entity> = old.models.iter().map(|e| (e.name.as_str(), e)).collect();

  for entity in new.models.iter_mut() {
    // For a new entity (not in old) there's nothing to reassign — the parser's slots are fine
    let Some(old_entity) = old_by.get(entity.name.as_str()) else { continue };

    // Old slots by name + the high-water mark (max byte offset)
    let mut old_slots: HashMap<&str, usize> = HashMap::new();
    let mut max_offset = 0usize;
    for f in old_entity.fields.iter() {
      if let FieldLocation::Body { offset_pos } = f.location {
        old_slots.insert(f.name.as_str(), offset_pos);
        max_offset = max_offset.max(offset_pos);
      }
    }
    // Next free slot: above the old high-water mark (or from the very start if there was no Body).
    // This way new fields never collide with carried ones (and later — with retired slots).
    let mut next = if max_offset == 0 { PRE_HEADER } else { max_offset + 4 };

    for f in entity.fields.iter_mut() {
      if let FieldLocation::Body { offset_pos } = &mut f.location {
        match old_slots.get(f.name.as_str()) {
          Some(&old_off) => *offset_pos = old_off,   // matched by name — take the old slot
          None => { *offset_pos = next; next += 4; } // new field — next free slot
        }
      }
    }

    entity.payload_offset = next; // header size = past the last slot
  }
}

/// Carries u16 enum variant ids from the old snapshot (for those matching by name), giving new ones the next
/// free id. Without this, reordering variants in schema.marci would change ids (the parser assigns
/// them in declaration order) → stored discriminants would mean something different. Remaps
/// `EnumInfo`, payload-field conditions, and the enum `@default`.
fn carry_enum_ids(new: &mut Schema, old: &Schema) {
  let old_by: HashMap<&str, &Entity> = old.models.iter().map(|e| (e.name.as_str(), e)).collect();

  for entity in new.models.iter_mut() {
    let Some(old_entity) = old_by.get(entity.name.as_str()) else { continue };
    let old_fields: HashMap<&str, &Field> = old_entity.fields.iter().map(|f| (f.name.as_str(), f)).collect();

    // 1) for each discriminant, compute the remap parser_id → reconciled_id (only if something changes)
    let mut remaps: Vec<(usize, HashMap<u16, u16>)> = vec![];
    for (fi, field) in entity.fields.iter().enumerate() {
      let FieldType::Enum(new_ei) = &field.ty else { continue };
      let Some(old_field) = old_fields.get(field.name.as_str()) else { continue };
      let FieldType::Enum(old_ei) = &old_field.ty else { continue };

      let mut next = old_ei.variants_map.values().copied().max().map(|m| m + 1).unwrap_or(0);
      let mut remap = HashMap::new();
      let mut changed = false;
      // variants in id order for determinism
      let mut variants: Vec<(&String, u16)> = new_ei.variants_map.iter().map(|(n, i)| (n, *i)).collect();
      variants.sort_by_key(|(_, i)| *i);
      for (name, parser_id) in variants {
        let reconciled = match old_ei.variants_map.get(name) {
          Some(&old_id) => old_id,
          None => { let id = next; next += 1; id }
        };
        if reconciled != parser_id { changed = true; }
        remap.insert(parser_id, reconciled);
      }
      if changed { remaps.push((fi, remap)); }
    }

    // 2) apply the remap to the discriminant, its @default, and payload-field conditions
    for (fi, remap) in remaps {
      if let FieldType::Enum(ei) = &mut entity.fields[fi].ty {
        ei.variants_map = std::mem::take(&mut ei.variants_map).into_iter().map(|(n, id)| (n, remap[&id])).collect();
        ei.variants = std::mem::take(&mut ei.variants).into_iter().map(|(id, v)| (remap[&id], v)).collect();
        ei.variants_names_map = ei.variants_map.iter().map(|(n, id)| (*id, n.clone())).collect();
      }
      // the enum @default is stored as a 2-byte id — remap it
      if let Some(FieldDefault::Value(bytes)) = &mut entity.fields[fi].default_value {
        if bytes.len() == 2 {
          let parser_id = u16::from_be_bytes([bytes[0], bytes[1]]);
          if let Some(&reconciled) = remap.get(&parser_id) {
            *bytes = reconciled.to_be_bytes().to_vec();
          }
        }
      }
      // payload-field conditions of this discriminant
      for field in entity.fields.iter_mut() {
        if let FieldExistsCondition::EnumValue { field_index, variants } = &mut field.condition {
          if *field_index == fi {
            for v in variants.iter_mut() { *v = remap[v]; }
          }
        }
      }
    }
  }
}

/// Brings each entity's field order to canonical: keys (by key-index), then Body
/// (by offset_pos), then Virtual. This is the row-format invariant: the writer writes Body fields in
/// ARRAY order, while `get_end` reads them in SLOT order — the orders must match.
/// Index references (condition.field_index, enum variants, rev_field_idx) are remapped.
fn canonicalize_field_order(schema: &mut Schema) {
  // 1) for each entity — the order order[new] = old and the inverse permutation perm[old] = new
  let mut perms: Vec<Vec<usize>> = Vec::with_capacity(schema.models.len());
  for entity in schema.models.iter_mut() {
    let n = entity.fields.len();
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| field_sort_key(&entity.fields[i], i));

    let mut perm = vec![0usize; n];
    for (new_idx, &old_idx) in order.iter().enumerate() { perm[old_idx] = new_idx; }

    let mut reordered: Vec<Field> = Vec::with_capacity(n);
    for &old_idx in order.iter() { reordered.push(entity.fields[old_idx].clone()); }
    entity.fields = reordered;
    perms.push(perm);
  }

  // 2) remap index references (values in fields still point to the OLD indices)
  for (ei, entity) in schema.models.iter_mut().enumerate() {
    let self_perm = &perms[ei];
    for field in entity.fields.iter_mut() {
      if let FieldExistsCondition::EnumValue { field_index, .. } = &mut field.condition {
        *field_index = self_perm[*field_index];
      }
      match &mut field.ty {
        FieldType::Enum(enum_info) => {
          for indices in enum_info.variants.values_mut() {
            for idx in indices.iter_mut() { *idx = self_perm[*idx]; }
          }
        }
        FieldType::Ref(ref_info) | FieldType::RefList(ref_info) => {
          if let Some(rev) = ref_info.rev_field_idx {
            ref_info.rev_field_idx = Some(perms[ref_info.model_index][rev]);
          }
        }
        _ => {}
      }
    }
  }
}

/// Field sort key for the canonical order: (category, sub-order)
fn field_sort_key(field: &Field, original_index: usize) -> (u8, usize) {
  match field.location {
    FieldLocation::Key { index } => (0, index),
    FieldLocation::Body { offset_pos } => (1, offset_pos),
    FieldLocation::Virtual => (2, original_index),
  }
}

// ─────────────────────────────── type comparison ───────────────────────────────

enum TypeCmp {
  Same,
  AdditiveEnum,
  EnumIncompatible(String),
  Incompatible,
}

fn type_cmp(old_schema: &Schema, old_f: &Field, new_schema: &Schema, new_f: &Field) -> TypeCmp {
  match (&old_f.ty, &new_f.ty) {
    (FieldType::Enum(a), FieldType::Enum(b)) => enum_cmp(a, b),
    _ => {
      if serialize_type(old_schema, old_f) == serialize_type(new_schema, new_f) {
        TypeCmp::Same
      } else {
        TypeCmp::Incompatible
      }
    }
  }
}

/// Only ADDING variants is allowed: every old `name→id` must be preserved with the same id.
/// Removing/reassigning an id is destructive (stored u16 discriminants would mean something different).
fn enum_cmp(old: &EnumInfo, new: &EnumInfo) -> TypeCmp {
  for (name, id) in old.variants_map.iter() {
    match new.variants_map.get(name) {
      Some(new_id) if new_id == id => {}
      Some(new_id) => return TypeCmp::EnumIncompatible(format!("variant '{}' changed id {} -> {}", name, id, new_id)),
      None => return TypeCmp::EnumIncompatible(format!("variant '{}' removed", name)),
    }
  }
  if new.variants_map.len() == old.variants_map.len() { TypeCmp::Same } else { TypeCmp::AdditiveEnum }
}

// ─────────────────────────────── field attributes ───────────────────────────────

fn is_key(field: &Field) -> bool {
  matches!(field.location, FieldLocation::Key { .. })
}

/// The storage binding of a relation field (None for non-relations)
fn ref_binding(field: &Field) -> Option<&RefBinding> {
  match &field.ty {
    FieldType::Ref(ri) | FieldType::RefList(ri) => Some(&ri.binding),
    _ => None,
  }
}

fn binding_kind(binding: &RefBinding) -> &'static str {
  match binding {
    RefBinding::CurrentId => "current_id",
    RefBinding::FieldValue => "field_value",
    RefBinding::IndexTree(_) => "index_tree",
  }
}

/// None — no index, Some(false) — `@index`, Some(true) — `@unique`
fn index_kind(field: &Field) -> Option<bool> {
  let mut kind = None;
  for attr in field.attributes.iter() {
    match attr {
      Attribute::Unique => kind = Some(true),
      Attribute::Index if kind.is_none() => kind = Some(false),
      _ => {}
    }
  }
  kind
}

fn default_attr(field: &Field) -> Option<&str> {
  field.attributes.iter().find_map(|a| if let Attribute::Default(s) = a { Some(s.as_str()) } else { None })
}

fn format_attr(field: &Field) -> Option<String> {
  field.attributes.iter().find_map(|a| if let Attribute::Format(fmt) = a { Some(format!("{:?}", fmt)) } else { None })
}

// ─────────────────────────────── apply (execution against the DB) ───────────────────────────────

/// Service tree holding migration state: keys `schema` (materialized snapshot), `version` (u64 BE),
/// and `applied` (ledger of applied migration ids separated by `\n`)
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
/// A slot shift is already rejected in [`diff`] (`UnsupportedLayoutChange`), so we don't check for it here.
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

/// Serializes the operations into migration-file text — one action per line. `create entity` is a bare
/// line (its fields follow as `add field` actions); `add/alter field` carry the field line; index/drop
/// ops reference by name. Definitions are taken from `schema` (the target).
pub fn serialize_migration(ops: &[MigrateOp], schema: &Schema) -> String {
  ops.iter().map(|op| serialize_op(op, schema)).collect::<Vec<_>>().join("\n")
}

fn serialize_op(op: &MigrateOp, schema: &Schema) -> String {
  match op {
    MigrateOp::CreateEntity { name } => format!("create entity {}", name),
    MigrateOp::DropEntity { name } => format!("drop entity {}", name),
    MigrateOp::AddField { entity, field } => format!("add field {}.{}", entity, field_spec(schema, entity, field)),
    MigrateOp::AlterField { entity, field } => format!("alter field {}.{}", entity, field_spec(schema, entity, field)),
    MigrateOp::DropField { entity, field } => format!("drop field {}.{}", entity, field),
    MigrateOp::AddIndex { entity, field, unique } => format!("add {} {}.{}", index_kw(*unique), entity, field),
    MigrateOp::DropIndex { entity, field, unique } => format!("drop {} {}.{}", index_kw(*unique), entity, field),
  }
}

fn index_kw(unique: bool) -> &'static str { if unique { "unique" } else { "index" } }

/// Field spec = the snapshot line without the leading name (`String @slot(8) @unique`) — the name is already in `E.field`
fn field_spec(schema: &Schema, entity: &str, field: &str) -> String {
  let e = find_entity(schema, entity);
  let f = find_field(e, field);
  let line = crate::snapshot::serialize_field(schema, e, f);
  let prefix = format!("{} ", field);
  format!("{} {}", field, line.strip_prefix(&prefix).unwrap_or(&line))
}

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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::parse_schema;

  fn diff_text(old: &str, new: &str) -> Result<Vec<MigrateOp>, MigrateError> {
    diff(&parse_schema(old), &parse_schema(new))
  }

  #[test]
  fn diff_create_and_drop_entity() {
    let old = "model User {\n  name String\n}";
    let new = "model User {\n  name String\n}\nmodel Post {\n  title String\n}";
    // a new entity = CreateEntity + an AddField per field (id is inserted first); drop is a single op
    assert_eq!(diff_text(old, new).unwrap(), vec![
      MigrateOp::CreateEntity { name: "Post".into() },
      MigrateOp::AddField { entity: "Post".into(), field: "id".into() },
      MigrateOp::AddField { entity: "Post".into(), field: "title".into() },
    ]);
    assert_eq!(diff_text(new, old).unwrap(), vec![MigrateOp::DropEntity { name: "Post".into() }]);
  }

  #[test]
  fn diff_add_field_with_index() {
    let old = "model User {\n  name String\n}";
    let new = "model User {\n  name String\n  email String @unique\n}";
    assert_eq!(diff_text(old, new).unwrap(), vec![
      MigrateOp::AddField { entity: "User".into(), field: "email".into() },
      MigrateOp::AddIndex { entity: "User".into(), field: "email".into(), unique: true },
    ]);
  }

  #[test]
  fn diff_alter_nullable_and_index_swap() {
    let old = "model User {\n  email String @index\n}";
    let new = "model User {\n  email String?\n}";
    // email: @index → no index (drop), and became nullable (alter)
    assert_eq!(diff_text(old, new).unwrap(), vec![
      MigrateOp::AlterField { entity: "User".into(), field: "email".into() },
      MigrateOp::DropIndex { entity: "User".into(), field: "email".into(), unique: false },
    ]);
  }

  #[test]
  fn diff_drop_field() {
    let old = "model User {\n  name String\n  age UInt\n}";
    let new = "model User {\n  name String\n}";
    assert_eq!(diff_text(old, new).unwrap(), vec![MigrateOp::DropField { entity: "User".into(), field: "age".into() }]);
  }

  #[test]
  fn diff_type_change_rejected() {
    let old = "model User {\n  age UInt\n}";
    let new = "model User {\n  age String\n}";
    assert!(matches!(diff_text(old, new), Err(MigrateError::UnsupportedTypeChange { .. })));
  }

  #[test]
  fn diff_struct_is_just_entities() {
    // a struct expands into a Parent.field model — adding a struct field = new entity + alter? no:
    // info becomes a new model User.info; in User a virtual field info appears
    let old = "model User {\n  name String\n}";
    let new = "model User {\n  name String\n  info Info?\n}\nstruct Info {\n  bio String\n}";
    let ops = diff_text(old, new).unwrap();
    assert!(ops.contains(&MigrateOp::CreateEntity { name: "User.info".into() }));
    assert!(ops.contains(&MigrateOp::AddField { entity: "User".into(), field: "info".into() }));
  }

  #[test]
  fn diff_enum_add_variant_is_additive() {
    // Adding an enum variant at the end: discriminant → AlterField, new payload → AddField
    let old = "model A {\n  t E\n}\nenum E {\n  a\n  b {\n    x Int\n  }\n}";
    let new = "model A {\n  t E\n}\nenum E {\n  a\n  b {\n    x Int\n  }\n  c {\n    y Int\n  }\n}";
    let ops = diff_text(old, new).unwrap();
    assert!(ops.contains(&MigrateOp::AlterField { entity: "A".into(), field: "t".into() }), "ops: {:?}", ops);
    assert!(ops.contains(&MigrateOp::AddField { entity: "A".into(), field: "y".into() }), "ops: {:?}", ops);
  }

  #[test]
  fn diff_enum_remove_variant_rejected() {
    let old = "model A {\n  t E\n}\nenum E {\n  a\n  b\n}";
    let new = "model A {\n  t E\n}\nenum E {\n  a\n}";
    assert!(matches!(diff_text(old, new), Err(MigrateError::UnsupportedEnumChange { .. })));
  }

  #[test]
  fn diff_unchanged_is_empty() {
    let s = "model User {\n  name String\n  email String @unique\n}";
    assert!(diff_text(s, s).unwrap().is_empty());
  }

  // ─────────── reconcile_slots ───────────

  fn body_offset(schema: &Schema, entity: &str, field: &str) -> usize {
    let e = schema.models.iter().find(|m| m.name == entity).unwrap();
    let f = e.fields.iter().find(|f| f.name == field).unwrap();
    match f.location { FieldLocation::Body { offset_pos } => offset_pos, _ => panic!("{}.{} is not Body", entity, field) }
  }

  /// Inserting a field in the middle shifts slots in parse_schema → without reconciliation, diff rejects it as a layout change
  #[test]
  fn insert_middle_shifts_slots_without_reconcile() {
    let old = parse_schema("model M {\n  a String\n  b String\n}");
    let new = parse_schema("model M {\n  a String\n  c String\n  b String\n}");
    // b shifted 8 → 12
    assert_eq!(body_offset(&old, "M", "b"), 8);
    assert_eq!(body_offset(&new, "M", "b"), 12);
    assert!(matches!(diff(&old, &new), Err(MigrateError::UnsupportedLayoutChange { .. })));
  }

  /// With reconciliation: matched fields keep their old slots, the new one gets the next free slot
  #[test]
  fn reconcile_carries_slots_then_diff_is_clean() {
    let old = parse_schema("model M {\n  a String\n  b String\n}");
    let mut new = parse_schema("model M {\n  a String\n  c String\n  b String\n}");

    reconcile(&mut new, &old);

    // a and b kept their slots from old; c — the next free one (after max=8 → 12)
    assert_eq!(body_offset(&new, "M", "a"), 4);
    assert_eq!(body_offset(&new, "M", "b"), 8);
    assert_eq!(body_offset(&new, "M", "c"), 12);

    // now the diff is clean: only the addition of c
    assert_eq!(diff(&old, &new).unwrap(), vec![MigrateOp::AddField { entity: "M".into(), field: "c".into() }]);
  }

  /// Reconciliation leaves a new entity (not in old) untouched — the parser's slots are fine
  #[test]
  fn reconcile_leaves_new_entity_untouched() {
    let old = parse_schema("model M {\n  a String\n}");
    let mut new = parse_schema("model M {\n  a String\n}\nmodel N {\n  x String\n  y String\n}");
    reconcile(&mut new, &old);
    assert_eq!(body_offset(&new, "N", "x"), 4);
    assert_eq!(body_offset(&new, "N", "y"), 8);
  }

  /// Reordering enum variants: ids are carried from the old snapshot → diff is clean (not rejected)
  #[test]
  fn reconcile_carries_enum_variant_ids() {
    let old = parse_schema("model A {\n  t E\n}\nenum E {\n  a\n  b\n}");
    // in the new schema the variants are swapped
    let mut new = parse_schema("model A {\n  t E\n}\nenum E {\n  b\n  a\n}");

    // without reconciliation the parser would give b=0,a=1 → enum_cmp would see an id change → reject
    assert!(matches!(diff(&old, &new), Err(MigrateError::UnsupportedEnumChange { .. })));

    reconcile(&mut new, &old);
    // ids carried over: a=0, b=1 as in old → diff is empty
    assert!(diff(&old, &new).unwrap().is_empty());
  }

  /// Normalizes snapshot text to canonical form (for comparison)
  fn canon(snapshot_text: &str) -> String {
    crate::snapshot::serialize_snapshot(&crate::snapshot::parse_snapshot(snapshot_text).unwrap())
  }

  /// The migration file is self-contained (carries definitions) and does NOT contain the whole snapshot.
  /// A new entity = a bare `create entity` line + one `add field` per field — no block syntax.
  #[test]
  fn migration_file_is_self_contained() {
    let new = parse_schema("model User {\n  name String\n  email String @unique\n}");
    let file = serialize_migration(&diff(&parse_schema(""), &new).unwrap(), &new);

    assert!(file.contains("create entity User"), "file:\n{}", file);
    assert!(file.contains("add field User.email String @slot(8) @unique"), "file:\n{}", file);
    assert!(!file.contains("--- snapshot ---"));
    assert!(!file.contains('{'), "block syntax is gone:\n{}", file);
    // physical operations are extracted from the file for apply
    let ops = migration_ops(&file).unwrap();
    assert!(ops.contains(&MigrateOp::CreateEntity { name: "User".into() }), "ops: {:?}", ops);
    assert!(ops.contains(&MigrateOp::AddField { entity: "User".into(), field: "email".into() }), "ops: {:?}", ops);
  }

  /// evolve(empty, from-scratch migration) yields the snapshot of the target schema
  #[test]
  fn evolve_from_empty_equals_target() {
    let new = parse_schema("model User {\n  name String\n  email String @unique\n}\nmodel Post {\n  title String\n  author User?\n}");
    let file = serialize_migration(&diff(&parse_schema(""), &new).unwrap(), &new);
    let evolved = evolve("", &file).unwrap();
    assert_eq!(canon(&evolved), crate::snapshot::serialize_snapshot(&new));
  }

  /// Incremental evolve: apply v0, then the diff v0→v1 — get the v1 snapshot
  #[test]
  fn evolve_incremental() {
    let v0 = parse_schema("model User {\n  name String\n}");
    let m0 = serialize_migration(&diff(&parse_schema(""), &v0).unwrap(), &v0);
    let snap0 = evolve("", &m0).unwrap();

    let mut v1 = parse_schema("model User {\n  name String\n  email String @unique\n  age UInt\n}");
    reconcile(&mut v1, &crate::snapshot::parse_snapshot(&snap0).unwrap());
    let prev = crate::snapshot::parse_snapshot(&snap0).unwrap();
    let m1 = serialize_migration(&diff(&prev, &v1).unwrap(), &v1);

    // the actions are visible in the file
    assert!(m1.contains("add field User.email"), "m1:\n{}", m1);
    assert!(m1.contains("add index User.email") || m1.contains("add unique User.email"), "m1:\n{}", m1);

    let snap1 = evolve(&snap0, &m1).unwrap();
    assert_eq!(canon(&snap1), crate::snapshot::serialize_snapshot(&v1));
  }
}
