//! Materialized snapshot of the schema: a textual projection of the FLAT `Schema.models`
//! array — exactly the representation that marcidb keeps in memory. structs are already
//! expanded into models (`Parent.field` with `@parent_id`), enum is inlined into fields
//! (discriminant + payload with `@variant`), refs carry the fully resolved `RefInfo`.
//! The snapshot is the source of truth for diff/apply of migrations.
//!
//! What is PINNED (physics/history, which the parser would emit in declaration order — unstable):
//!   Body field slots, enum variant ids, location, ref bindings.
//! What is DERIVED on load (positional name resolution, no decisions made):
//!   model_index, rev_field_idx, parent_index, index trees, default bytes, payload_offset.

use crate::schema::{
  Attribute, DeleteConstraint, EnumInfo, FieldCustomFormat, FieldLocation, FieldType,
  PrimitiveFieldType, RefBinding, Schema,
};

// ─────────────────────────────── serialization ───────────────────────────────

/// Canonical snapshot text from a resolved `Schema`. Deterministic order:
/// entities as in the array, fields as in the entity, enum variants by id.
pub fn serialize_snapshot(schema: &Schema) -> String {
  let mut out = String::new();
  for (i, entity) in schema.models.iter().enumerate() {
    if i > 0 {
      out.push('\n');
    }
    out.push_str(&format!("Entity {} {{\n", entity.name));
    for field in entity.fields.iter() {
      out.push_str("  ");
      out.push_str(&serialize_field(schema, entity, field));
      out.push('\n');
    }
    // Retired (dropped-field) slots — reserved so they are never reused. Sorted for a deterministic snapshot.
    let mut retired = entity.retired_slots.clone();
    retired.sort_unstable();
    for slot in retired {
      out.push_str(&format!("  @retired({})\n", slot));
    }
    out.push_str("}\n");
  }
  out
}

pub fn serialize_field(schema: &Schema, entity: &crate::schema::Entity, field: &crate::schema::Field) -> String {
  let mut tokens: Vec<String> = vec![field.name.clone(), serialize_type(schema, field)];

  if field.nullable {
    tokens.push("@nullable".to_string());
  }

  // location
  match &field.location {
    FieldLocation::Key { .. } => tokens.push("@id".to_string()),
    FieldLocation::Body { offset_pos } => tokens.push(format!("@slot({})", offset_pos)),
    FieldLocation::Virtual => tokens.push("@virtual".to_string()),
  }

  // ref binding (pinned)
  if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &field.ty {
    tokens.push(serialize_binding(&ref_info.binding));
    if let Some(rev_idx) = ref_info.rev_field_idx {
      let rev_name = &schema.models[ref_info.model_index].fields[rev_idx].name;
      tokens.push(format!("@rev({})", rev_name));
    }
    if let Some(parent_idx) = ref_info.parent_index {
      tokens.push(format!("@parent({})", schema.models[parent_idx].name));
    }
  }

  // @variant(disc: a|b) — payload field of an enum variant
  if let crate::schema::FieldExistsCondition::EnumValue { field_index, variants } = &field.condition {
    let disc = &entity.fields[*field_index];
    let FieldType::Enum(enum_info) = &disc.ty else { panic!("EnumValue condition points to non-enum field") };
    let names: Vec<&str> = variants.iter()
      .map(|v| enum_info.variants_names_map.get(v).map(String::as_str).unwrap_or("?"))
      .collect();
    tokens.push(format!("@variant({}:{})", disc.name, names.join("|")));
  }

  // structural attributes (id → location; bind → pinned in the binding; both are skipped)
  for attr in field.attributes.iter() {
    if let Some(rendered) = serialize_attr(attr) {
      tokens.push(rendered);
    }
  }

  tokens.join(" ")
}

pub fn serialize_type(schema: &Schema, field: &crate::schema::Field) -> String {
  match &field.ty {
    FieldType::Primitive(p) => primitive_name(p).to_string(),
    FieldType::PrimitiveList(p, None) => format!("{}[]", primitive_name(p)),
    FieldType::PrimitiveList(p, Some(n)) => format!("{}[{}]", primitive_name(p), n),
    FieldType::Ref(ref_info) => schema.models[ref_info.model_index].name.clone(),
    FieldType::RefList(ref_info) => format!("{}[]", schema.models[ref_info.model_index].name),
    FieldType::Enum(enum_info) => serialize_enum_type(enum_info),
    FieldType::RefUnresolved(name) => name.clone(),
    FieldType::RefListUnresolved(name) => format!("{}[]", name),
  }
}

/// `Enum(creator=0, admin=1)` — names with explicit ids, ordered by ascending id (canonical order)
fn serialize_enum_type(enum_info: &EnumInfo) -> String {
  let mut pairs: Vec<(u16, &String)> = enum_info.variants_names_map.iter().map(|(id, name)| (*id, name)).collect();
  pairs.sort_by_key(|(id, _)| *id);
  let body: Vec<String> = pairs.iter().map(|(id, name)| format!("{}={}", name, id)).collect();
  format!("Enum({})", body.join(","))
}

fn serialize_binding(binding: &RefBinding) -> String {
  match binding {
    RefBinding::CurrentId => "@current_id".to_string(),
    RefBinding::FieldValue => "@field_value".to_string(),
    RefBinding::IndexTree(name) => format!("@index_tree({})", name),
  }
}

/// Structural attributes needed to rebuild caches on load.
/// `Id` → provided by location; `BindUnresolved` → replaced by the pinned binding; both → None.
fn serialize_attr(attr: &Attribute) -> Option<String> {
  Some(match attr {
    Attribute::Id | Attribute::BindUnresolved(_) => return None,
    Attribute::Unique => "@unique".to_string(),
    Attribute::Index => "@index".to_string(),
    Attribute::Default(s) => format!("@default({})", s),
    Attribute::Format(FieldCustomFormat::Uuid) => "@format(uuid)".to_string(),
    Attribute::Format(FieldCustomFormat::Hex) => "@format(hex)".to_string(),
    Attribute::OnDelete(c) => format!("@onDelete({})", delete_constraint_name(c)),
    // Module index — serialized as `@<provider>(args)` (the canonical form), round-tripped so the custom
    // index survives open()/migrations and reads the same way the user wrote it (`@vector(cosine)`).
    Attribute::Custom { name, args } =>
      if args.is_empty() { format!("@{}", name) } else { format!("@{}({})", name, args) },
    Attribute::InjectUnresolved(_) => return None,
  })
}

fn primitive_name(ty: &PrimitiveFieldType) -> &'static str {
  match ty {
    PrimitiveFieldType::String => "String",
    PrimitiveFieldType::Int64 => "Int",
    PrimitiveFieldType::UInt64 => "UInt",
    PrimitiveFieldType::Float => "Float",
    PrimitiveFieldType::Double => "Double",
    PrimitiveFieldType::Bool => "Bool",
    PrimitiveFieldType::Byte => "Byte",
    PrimitiveFieldType::DateTime => "DateTime",
  }
}

fn delete_constraint_name(c: &DeleteConstraint) -> &'static str {
  match c {
    DeleteConstraint::SetNull => "SetNull",
    DeleteConstraint::Restrict => "Restrict",
    DeleteConstraint::Cascade => "Cascade",
    DeleteConstraint::RemoveItem => "RemoveItem",
  }
}

// ─────────────────────────────── parsing ───────────────────────────────

use crate::schema::{Entity, Field, FieldExistsCondition, RefInfo, SchemaError};
use std::collections::HashMap;

/// Intermediate field type — refs/enum hold names, indices are resolved in the 2nd phase
enum SnapType {
  Primitive(PrimitiveFieldType),
  PrimitiveList(PrimitiveFieldType, Option<usize>),
  Ref(String),
  RefList(String),
  Enum(Vec<(String, u16)>),
}

/// A snapshot field in intermediate form (names instead of indices)
struct SnapField {
  name: String,
  ty: SnapType,
  nullable: bool,
  location: FieldLocation,
  attributes: Vec<Attribute>,
  binding: Option<RefBinding>,
  rev: Option<String>,      // name of the rev field in the target entity
  parent: Option<String>,   // name of the parent entity (for struct refs)
  variant_of: Option<(String, Vec<String>)>, // (discriminant name, variant names)
}

struct SnapEntity {
  name: String,
  fields: Vec<SnapField>,
  retired: Vec<usize>,
}

/// Parses snapshot text back into a flat `Schema`. Without expanding sugar and without reassigning
/// slots/bindings — all of that is already pinned in the text; on load we only resolve names into indices.
pub fn parse_snapshot(text: &str) -> Result<Schema, SchemaError> {
  let snap_entities = parse_blocks(text)?;
  let name_to_idx: HashMap<&str, usize> = snap_entities.iter().enumerate().map(|(i, e)| (e.name.as_str(), i)).collect();

  let mut models: Vec<Entity> = Vec::with_capacity(snap_entities.len());

  for se in snap_entities.iter() {
    let mut key_index = 0usize;
    let mut fields: Vec<Field> = Vec::with_capacity(se.fields.len());

    for sf in se.fields.iter() {
      // Key{index} is assigned by the order of key fields within the entity
      let location = match &sf.location {
        FieldLocation::Key { .. } => { let i = key_index; key_index += 1; FieldLocation::Key { index: i } }
        other => other.clone(),
      };

      let ty = build_field_type(sf, &se.name, &name_to_idx)?;
      let format = sf.attributes.iter().find_map(|a| if let Attribute::Format(f) = a { Some(f.clone()) } else { None });

      fields.push(Field {
        full_name: format!("{}.{}", se.name, sf.name),
        name: sf.name.clone(),
        ty,
        location,
        nullable: sf.nullable,
        attributes: sf.attributes.clone(),
        default_value: None,           // cache — restored by rebuild_caches below
        indexes: vec![],               // cache — restored by rebuild_caches below
        condition: FieldExistsCondition::None, // enum conditions filled by resolve_variants (needs field indices)
        format,
      });
    }

    let mut payload_offset = 4;
    for f in fields.iter() {
      if let FieldLocation::Body { offset_pos } = f.location {
        payload_offset = payload_offset.max(offset_pos + 4);
      }
    }
    // Retired slots keep the header high-water mark up even after the top field was dropped
    for slot in se.retired.iter() {
      payload_offset = payload_offset.max(slot + 4);
    }

    let mut entity = Entity::new(se.name.clone(), fields);
    entity.payload_offset = payload_offset;
    entity.retired_slots = se.retired.clone();
    models.push(entity);
  }

  // 2nd phase: enum conditions (needs field indices and the discriminant's variant map)
  resolve_variants(&mut models, &snap_entities)?;

  // Rebuild the computed caches (default bytes, indexes, counter_idx, rev_dependencies)
  crate::schema::rebuild_caches(&mut models)?;

  Ok(Schema { models })
}

fn build_field_type(sf: &SnapField, entity_name: &str, name_to_idx: &HashMap<&str, usize>) -> Result<FieldType, SchemaError> {
  let make_ref = |target: &str| -> Result<RefInfo, SchemaError> {
    let model_index = *name_to_idx.get(target)
      .ok_or_else(|| SchemaError(format!("snapshot: unknown ref target '{}' in {}.{}", target, entity_name, sf.name)))?;
    let mut ref_info = RefInfo::new(model_index);
    ref_info.binding = sf.binding.clone().unwrap_or(RefBinding::FieldValue);
    ref_info.is_unique = sf.attributes.iter().any(|a| matches!(a, Attribute::Unique));
    if let Some(parent) = &sf.parent {
      ref_info.parent_index = Some(*name_to_idx.get(parent.as_str())
        .ok_or_else(|| SchemaError(format!("snapshot: unknown parent '{}' in {}.{}", parent, entity_name, sf.name)))?);
    }
    Ok(ref_info)
  };

  Ok(match &sf.ty {
    SnapType::Primitive(p) => FieldType::Primitive(*p),
    SnapType::PrimitiveList(p, n) => FieldType::PrimitiveList(*p, *n),
    SnapType::Ref(target) => FieldType::Ref(make_ref(target)?),
    SnapType::RefList(target) => FieldType::RefList(make_ref(target)?),
    SnapType::Enum(pairs) => {
      let variants_map: HashMap<String, u16> = pairs.iter().cloned().collect();
      let variants_names_map: HashMap<u16, String> = pairs.iter().map(|(n, id)| (*id, n.clone())).collect();
      let variants: HashMap<u16, Vec<usize>> = pairs.iter().map(|(_, id)| (*id, vec![])).collect();
      FieldType::Enum(EnumInfo { variants, variants_map, variants_names_map })
    }
  })
}

/// Sets `@rev` (target field indices) and enum conditions + populates the discriminant's `variants`.
/// Reads and writes are split into two passes — otherwise a borrow conflict on `models`.
fn resolve_variants(models: &mut [Entity], snap: &[SnapEntity]) -> Result<(), SchemaError> {
  // rev_field_idx: name of the rev field in the target entity → index (models[ei] == snap[ei], order preserved)
  let mut rev_updates: Vec<(usize, usize, usize)> = vec![];
  for (ei, se) in snap.iter().enumerate() {
    for (fi, sf) in se.fields.iter().enumerate() {
      let Some(rev_name) = &sf.rev else { continue };
      let target_mi = match &models[ei].fields[fi].ty {
        FieldType::Ref(ri) | FieldType::RefList(ri) => ri.model_index,
        _ => continue,
      };
      let rev_idx = models[target_mi].fields.iter().position(|f| &f.name == rev_name)
        .ok_or_else(|| SchemaError(format!("snapshot: unknown @rev field '{}' for {}.{}", rev_name, se.name, sf.name)))?;
      rev_updates.push((ei, fi, rev_idx));
    }
  }
  for (ei, fi, rev_idx) in rev_updates {
    if let FieldType::Ref(ri) | FieldType::RefList(ri) = &mut models[ei].fields[fi].ty {
      ri.rev_field_idx = Some(rev_idx);
    }
  }

  // enum conditions: @variant(disc:a|b) → EnumValue + populating the discriminant's variants
  let mut variant_updates: Vec<(usize, usize, usize, Vec<u16>)> = vec![];
  for (ei, se) in snap.iter().enumerate() {
    for (fi, sf) in se.fields.iter().enumerate() {
      let Some((disc_name, variant_names)) = &sf.variant_of else { continue };
      let disc_idx = models[ei].fields.iter().position(|f| &f.name == disc_name)
        .ok_or_else(|| SchemaError(format!("snapshot: @variant points to unknown field '{}' in {}", disc_name, se.name)))?;
      let FieldType::Enum(enum_info) = &models[ei].fields[disc_idx].ty else {
        return Err(SchemaError(format!("snapshot: @variant '{}' is not an enum in {}", disc_name, se.name)));
      };
      let ids: Vec<u16> = variant_names.iter()
        .map(|n| enum_info.variants_map.get(n).copied()
          .ok_or_else(|| SchemaError(format!("snapshot: unknown variant '{}' of enum {} in {}", n, disc_name, se.name))))
        .collect::<Result<_, _>>()?;
      variant_updates.push((ei, fi, disc_idx, ids));
    }
  }
  for (ei, fi, disc_idx, ids) in variant_updates {
    models[ei].fields[fi].condition = FieldExistsCondition::EnumValue { field_index: disc_idx, variants: ids.clone() };
    if let FieldType::Enum(enum_info) = &mut models[ei].fields[disc_idx].ty {
      for id in ids {
        enum_info.variants.entry(id).or_default().push(fi);
      }
    }
  }

  Ok(())
}

// ─────────── parsing text into intermediate form ───────────

fn parse_blocks(text: &str) -> Result<Vec<SnapEntity>, SchemaError> {
  let mut entities = vec![];
  let mut lines = text.lines();
  while let Some(raw) = lines.next() {
    let line = raw.trim();
    if line.is_empty() || line.starts_with("//") { continue; }
    let Some(rest) = line.strip_prefix("Entity ") else {
      return Err(SchemaError(format!("snapshot: expected 'Entity', got: \"{}\"", line)));
    };
    let name = rest.trim_end_matches('{').trim().to_string();
    let mut fields = vec![];
    let mut retired = vec![];
    for raw in lines.by_ref() {
      let l = raw.trim();
      if l == "}" { break; }
      if l.is_empty() || l.starts_with("//") { continue; }
      if let Some(n) = l.strip_prefix("@retired(").and_then(|x| x.strip_suffix(")")) {
        retired.push(n.trim().parse().map_err(|_| SchemaError(format!("snapshot: bad @retired slot: {}", l)))?);
        continue;
      }
      fields.push(parse_field_line(l)?);
    }
    entities.push(SnapEntity { name, fields, retired });
  }
  Ok(entities)
}

fn parse_field_line(line: &str) -> Result<SnapField, SchemaError> {
  // name and type are the first two tokens without internal spaces; the rest are @-attributes
  let line = line.trim();
  let (name, rest) = line.split_once(char::is_whitespace)
    .ok_or_else(|| SchemaError(format!("snapshot: expected '<name> <type>' in: \"{}\"", line)))?;
  let rest = rest.trim_start();
  let (type_str, attrs_str) = match rest.split_once(char::is_whitespace) {
    Some((t, a)) => (t, a),
    None => (rest, ""),
  };

  let mut sf = SnapField {
    name: name.to_string(),
    ty: parse_snap_type(type_str)?,
    nullable: false,
    location: FieldLocation::Body { offset_pos: 0 },
    attributes: vec![],
    binding: None,
    rev: None,
    parent: None,
    variant_of: None,
  };

  for token in split_attr_tokens(attrs_str) {
    apply_attr_token(&mut sf, &token)?;
  }

  Ok(sf)
}

/// Splits the attribute string into `@` tokens, WITHOUT splitting on `@` inside parentheses
/// (a value may contain `@`, e.g. `@rev(@parent_id)`)
fn split_attr_tokens(s: &str) -> Vec<String> {
  let mut tokens = vec![];
  let mut cur = String::new();
  let mut depth = 0i32;
  let mut started = false;
  for ch in s.chars() {
    match ch {
      '@' if depth == 0 => {
        if started && !cur.trim().is_empty() { tokens.push(cur.trim().to_string()); }
        cur.clear();
        started = true;
      }
      '(' => { depth += 1; cur.push(ch); }
      ')' => { depth -= 1; cur.push(ch); }
      _ => cur.push(ch),
    }
  }
  if started && !cur.trim().is_empty() { tokens.push(cur.trim().to_string()); }
  tokens
}

fn apply_attr_token(sf: &mut SnapField, token: &str) -> Result<(), SchemaError> {
  let inside = |t: &str, kw: &str| -> Option<String> {
    t.strip_prefix(kw).and_then(|x| x.strip_prefix('(')).and_then(|x| x.strip_suffix(')')).map(str::to_string)
  };

  match token {
    "nullable" => sf.nullable = true,
    "id" => sf.location = FieldLocation::Key { index: 0 },
    "virtual" => sf.location = FieldLocation::Virtual,
    "current_id" => sf.binding = Some(RefBinding::CurrentId),
    "field_value" => sf.binding = Some(RefBinding::FieldValue),
    "unique" => sf.attributes.push(Attribute::Unique),
    "index" => sf.attributes.push(Attribute::Index),
    _ => {
      if let Some(n) = inside(token, "slot") {
        sf.location = FieldLocation::Body { offset_pos: n.parse().map_err(|_| SchemaError(format!("snapshot: bad slot: {}", token)))? };
      } else if let Some(t) = inside(token, "index_tree") {
        sf.binding = Some(RefBinding::IndexTree(t));
      } else if let Some(r) = inside(token, "rev") {
        sf.rev = Some(r);
      } else if let Some(p) = inside(token, "parent") {
        sf.parent = Some(p);
      } else if let Some(v) = inside(token, "variant") {
        let (disc, names) = v.split_once(':')
          .ok_or_else(|| SchemaError(format!("snapshot: bad @variant: {}", token)))?;
        sf.variant_of = Some((disc.trim().to_string(), names.split('|').map(|s| s.trim().to_string()).collect()));
      } else if let Some(s) = inside(token, "default") {
        sf.attributes.push(Attribute::Default(s));
      } else if let Some(f) = inside(token, "format") {
        sf.attributes.push(Attribute::Format(match f.as_str() {
          "uuid" => FieldCustomFormat::Uuid,
          "hex" => FieldCustomFormat::Hex,
          _ => return Err(SchemaError(format!("snapshot: unknown format: {}", f))),
        }));
      } else if let Some(c) = inside(token, "onDelete") {
        sf.attributes.push(Attribute::OnDelete(match c.as_str() {
          "SetNull" => DeleteConstraint::SetNull,
          "Restrict" => DeleteConstraint::Restrict,
          "Cascade" => DeleteConstraint::Cascade,
          "RemoveItem" => DeleteConstraint::RemoveItem,
          _ => return Err(SchemaError(format!("snapshot: unknown onDelete: {}", c))),
        }));
      } else if let Some(c) = inside(token, "custom") {
        // Explicit `@custom(provider, args)` (mirror parse_attribute): first token = provider name.
        let (name, args) = match c.split_once(',') {
          Some((n, a)) => (n.trim().to_string(), a.trim().to_string()),
          None => (c.trim().to_string(), String::new()),
        };
        if name.is_empty() { return Err(SchemaError(format!("snapshot: @custom requires a provider name: {}", token))); }
        sf.attributes.push(Attribute::Custom { name, args });
      } else {
        // Catch-all module attribute: `@vector(cosine)` / `@fulltext(english)` / `@<provider>(args)`.
        let (name, args) = match token.strip_suffix(')').and_then(|x| x.split_once('(')) {
          Some((n, a)) => (n.trim().to_string(), a.trim().to_string()),
          None => (token.trim().to_string(), String::new()),
        };
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
          return Err(SchemaError(format!("snapshot: unknown attribute: @{}", token)));
        }
        sf.attributes.push(Attribute::Custom { name, args });
      }
    }
  }
  Ok(())
}

fn parse_snap_type(s: &str) -> Result<SnapType, SchemaError> {
  if let Some(body) = s.strip_prefix("Enum(").and_then(|x| x.strip_suffix(')')) {
    let pairs = body.split(',').map(|pair| {
      let (name, id) = pair.split_once('=')
        .ok_or_else(|| SchemaError(format!("snapshot: bad enum variant: {}", pair)))?;
      Ok((name.trim().to_string(), id.trim().parse().map_err(|_| SchemaError(format!("snapshot: bad variant id: {}", id)))?))
    }).collect::<Result<Vec<_>, SchemaError>>()?;
    return Ok(SnapType::Enum(pairs));
  }

  if let Some((base, bracket)) = s.strip_suffix(']').and_then(|x| x.split_once('[')) {
    let bracket = bracket.trim();
    if let Some(p) = snap_primitive(base) {
      let size = if bracket.is_empty() { None } else { Some(bracket.parse().map_err(|_| SchemaError(format!("snapshot: bad list size: {}", s)))?) };
      return Ok(SnapType::PrimitiveList(p, size));
    }
    return Ok(SnapType::RefList(base.to_string()));
  }

  if let Some(p) = snap_primitive(s) {
    return Ok(SnapType::Primitive(p));
  }
  Ok(SnapType::Ref(s.to_string()))
}

fn snap_primitive(s: &str) -> Option<PrimitiveFieldType> {
  Some(match s {
    "String" => PrimitiveFieldType::String,
    "Int" => PrimitiveFieldType::Int64,
    "UInt" => PrimitiveFieldType::UInt64,
    "Float" => PrimitiveFieldType::Float,
    "Double" => PrimitiveFieldType::Double,
    "Bool" => PrimitiveFieldType::Bool,
    "Byte" => PrimitiveFieldType::Byte,
    "DateTime" => PrimitiveFieldType::DateTime,
    _ => return None,
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::parse_schema;

  const ROOT_SCHEMA: &str = "
model User {
    id          Byte[16]      @id   @format(uuid)
    name        String
    info        UserInfo?
    posts       Post[]        @bind(Post.author)
}

struct UserInfo {
    bio         String
    age         Int
}

model Post {
    title       String
    author      User?
}

model Project {
    name        String
    users       UserRole[]
}

enum Role {
    creator
    admin {
        level     Int
    }
    admin | creator {
        sign      String
    }
}

struct UserRole {
    user        User          @id
    role        Role
}
";

  /// Round-trip: snapshot of a materialized schema → parse → re-serialize yields the same text.
  /// Idempotence proves that parse_snapshot restored everything that serialize reads.
  #[test]
  fn snapshot_round_trip_root() {
    let schema = parse_schema(ROOT_SCHEMA);
    let s1 = serialize_snapshot(&schema);
    eprintln!("\n===== SNAPSHOT =====\n{}", s1);

    let reparsed = parse_snapshot(&s1).unwrap();
    let s2 = serialize_snapshot(&reparsed);
    assert_eq!(s1, s2, "round-trip diverged:\n--- s1 ---\n{}\n--- s2 ---\n{}", s1, s2);
  }

  /// A `@custom` module index must survive the snapshot (it was silently dropped before): the attribute
  /// round-trips textually AND the FieldIndex::Custom is rebuilt from it on load.
  #[test]
  fn snapshot_round_trips_custom_index() {
    use crate::FieldIndex;

    let schema = parse_schema("model Doc {\n  embedding Float[4] @vector(cosine)\n  body String @fulltext\n}");
    let s1 = serialize_snapshot(&schema);
    assert!(s1.contains("@vector(cosine)"), "snapshot lost custom args:\n{}", s1);
    assert!(s1.contains("@fulltext"), "snapshot lost argless custom:\n{}", s1);

    let reparsed = parse_snapshot(&s1).unwrap();
    assert_eq!(s1, serialize_snapshot(&reparsed), "custom round-trip diverged:\n{}", s1);

    let emb = reparsed.models[0].fields.iter().find(|f| f.name == "embedding").unwrap();
    assert!(emb.indexes.iter().any(|i|
      matches!(i, FieldIndex::Custom { name, args, .. } if name == "vector" && args == "cosine")),
      "custom index not rebuilt from snapshot: {:?}", emb.indexes);
  }

  /// Targeted check that parse_snapshot restored the resolved state (not just the text)
  #[test]
  fn snapshot_parse_rebuilds_resolved_state() {
    let schema = parse_snapshot(&serialize_snapshot(&parse_schema(ROOT_SCHEMA))).unwrap();

    let by_name: std::collections::HashMap<&str, usize> =
      schema.models.iter().enumerate().map(|(i, m)| (m.name.as_str(), i)).collect();

    // struct expanded into synthetic models
    assert!(by_name.contains_key("User.info"));
    assert!(by_name.contains_key("Project.users"));

    // enum inlined into Project.users: discriminant role + payload fields level/sign with conditions
    let pu = &schema.models[by_name["Project.users"]];
    let role = pu.fields.iter().find(|f| f.name == "role").unwrap();
    let FieldType::Enum(ei) = &role.ty else { panic!("role is not an enum") };
    assert_eq!(ei.variants_map.len(), 2);
    let sign = pu.fields.iter().find(|f| f.name == "sign").unwrap();
    assert!(matches!(sign.condition, FieldExistsCondition::EnumValue { .. }));

    // ref binding is pinned: User.posts — IndexTree, Post.author — FieldValue
    let user = &schema.models[by_name["User"]];
    let posts = user.fields.iter().find(|f| f.name == "posts").unwrap();
    assert!(matches!(&posts.ty, FieldType::RefList(ri) if matches!(ri.binding, RefBinding::IndexTree(_))));
  }
}
