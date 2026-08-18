use std::{env, fs, path::Path};

use marcidb::{Entity, EnumInfo, Field, FieldExistsCondition, FieldIndex, FieldLocation, FieldType, PrimitiveFieldType, RefBinding, RefInfo, Schema, parse_schema};

/// Whether this relation field is a `@list` id array (the owning side)
fn is_id_list(field: &Field) -> bool {
  matches!(&field.ty, FieldType::Ref(ri) | FieldType::RefList(ri) if matches!(ri.binding, RefBinding::IdList { .. }))
}

/// Whether the relation's partner field is a `@list` id array. Such a back-reference is read-only:
/// membership is changed through the `@list` side, so Insert/Update expose it as `never`.
fn partner_is_id_list(schema: &Schema, ref_info: &RefInfo) -> bool {
  ref_info.rev_field_idx.is_some_and(|i| is_id_list(&schema.models[ref_info.model_index].fields[i]))
}

fn get_model_id_name(model: &Entity) -> String {
  format!("{}ModelId", model.name.replace('.', "_"))
}

fn get_model_name(model: &Entity) -> String {
  format!("{}Model", model.name.replace('.', "_"))
}

fn get_model_select_name(model: &Entity) -> String {
  format!("{}ModelSelect", model.name.replace('.', "_"))
}

fn get_model_insert_name(model: &Entity) -> String {
  format!("{}ModelInsert", model.name.replace('.', "_"))
}

fn get_model_update_name(model: &Entity) -> String {
  format!("{}ModelUpdate", model.name.replace('.', "_"))
}

fn get_model_where_name(model: &Entity) -> String {
  format!("{}Model$Where", model.name.replace('.', "_"))
}

/// The bare field conditions. `$Where` itself is this wrapped in [`WhereValue`], which adds `$and`/`$or`/
/// `$not` and makes them mutually exclusive with the fields.
fn get_model_where_fields_name(model: &Entity) -> String {
  format!("{}Model$WhereFields", model.name.replace('.', "_"))
}

fn get_model_order_name(model: &Entity) -> String {
  format!("{}Model$Order", model.name.replace('.', "_"))
}

fn get_model_aggregate_name(model: &Entity) -> String {
  format!("{}ModelAggregateQuery", model.name.replace('.', "_"))
}

/// Names of fields available to aggregations (mirror of parse_aggregate_field)
fn get_aggregate_fields_union(model: &Entity, numeric: bool) -> String {
  let names: Vec<String> = model.fields.iter()
    .filter(|field| {
      if field.name.starts_with("@") { return false; }
      if !matches!(field.condition, FieldExistsCondition::None) { return false; }
      if matches!(field.location, FieldLocation::Virtual) { return false; }
      match &field.ty {
        FieldType::Primitive(ty) => {
          !numeric || matches!(ty, PrimitiveFieldType::Int64 | PrimitiveFieldType::UInt64 | PrimitiveFieldType::Float | PrimitiveFieldType::Double)
        },
        _ => false
      }
    })
    .map(|field| format!("\"{}\"", field.name))
    .collect();

  if names.is_empty() { "never".to_string() } else { names.join(" | ") }
}

fn get_model_query_name(model: &Entity) -> String {
  format!("{}ModelQuery", model.name.replace('.', "_"))
}

fn get_model_small_name(model: &Entity) -> String {
  [ &model.name[0..1].to_lowercase(), &model.name[1..] ].concat()
}

fn get_primitive_str(ty: &PrimitiveFieldType) -> &str {
  match ty {
    PrimitiveFieldType::String => "string",
    PrimitiveFieldType::Int64 => "number",
    PrimitiveFieldType::UInt64 => "number",
    PrimitiveFieldType::Float => "number",
    PrimitiveFieldType::Double => "number",
    PrimitiveFieldType::Byte => "number",
    PrimitiveFieldType::Bool => "boolean",
    PrimitiveFieldType::DateTime => "Date | number",
    PrimitiveFieldType::Json => "JsonValue",
  }
}

/// The type a field accepts when *written* (insert / `$where` / update). A byte list travels as either a
/// `@format`-encoded string or a raw array of bytes, and the engine accepts both in every write position
/// regardless of whether the field declares a `@format` — so every write shape has to say so. Read shapes
/// (`Model`, `ModelId`) stay narrow: the decoder emits one form, a string when the field is formatted.
fn get_field_write_ty(field: &Field, schema: &Schema) -> String {
  match &field.ty {
    FieldType::PrimitiveList(PrimitiveFieldType::Byte, _) =>
      format!("{} | string", get_field_ty(&field.ty, schema)),
    _ => get_field_ty(&field.ty, schema)
  }
}

fn get_field_ty(ty: &FieldType, schema: &Schema) -> String {
  match ty {
    FieldType::Primitive(ty) => get_primitive_str(ty).to_string(),
    FieldType::Ref(ref_info) => get_model_name(&schema.models[ref_info.model_index]),
    FieldType::RefList(ref_info) => format!("{}[]", get_model_name(&schema.models[ref_info.model_index])),
    FieldType::PrimitiveList(ty, fixed_size) => {
      let ty = get_primitive_str(ty);
      if let Some(fixed_size) = fixed_size && *fixed_size < 5 {
        let s =  (0..*fixed_size).map(|_| ty).fold(String::new(), |mut acc, item| {
          if !acc.is_empty() { acc.push_str(", ") }
          acc.push_str(item);
          acc
        });
        format!("[{}]", s)
      } else {
        format!("{}[]", ty)
      }
    },
    FieldType::Enum(enum_info) => {
      let mut variants: Vec<(&String, &u16)> = enum_info.variants_map.iter().collect();
      variants.sort_by_key(|(_, index)| **index);
      variants.iter()
        .map(|(name, _)| format!("\"{}\"", name))
        .collect::<Vec<_>>()
        .join(" | ")
    },
    _ => panic!("Unsupported field type")
  }
}

/// An enum where at least one variant contains fields — in the types it becomes a discriminated union
fn is_payload_enum(field: &Field) -> bool {
  if !matches!(field.location, FieldLocation::Body { .. }) { return false; }
  match &field.ty {
    FieldType::Enum(enum_info) => enum_info.variants.values().any(|fields| !fields.is_empty()),
    _ => false
  }
}

/// A field injected into the model from an enum variant
fn is_variant_field(field: &Field) -> bool {
  matches!(field.condition, FieldExistsCondition::EnumValue { .. })
}

/// Enum variants in declaration order together with their injected fields
fn enum_variants_sorted<'a>(entity: &'a Entity, enum_info: &'a EnumInfo) -> Vec<(&'a String, Vec<&'a Field>)> {
  let mut keys: Vec<u16> = enum_info.variants.keys().copied().collect();
  keys.sort();
  keys.into_iter().map(|variant| {
    let name = enum_info.variants_names_map.get(&variant).unwrap();
    let fields = enum_info.variants.get(&variant).unwrap().iter()
      .map(|field_index| &entity.fields[*field_index])
      .collect();
    (name, fields)
  }).collect()
}

// fn get_field_nullable_ty(field: &Field, schema: &Schema) -> String {
//   if field.nullable && !matches!(field.ty, FieldType::RefList(_)) {
//     format!("{} | null", get_field_ty(&field.ty, schema))
//   } else {
//     format!("{}", get_field_ty(&field.ty, schema))
//   }
// }

fn get_field_nullable(field: &Field) -> &str {
  if field.nullable && !matches!(field.ty, FieldType::RefList(_)) { " | null" } else { "" }
}

fn get_field_str(field: &Field, schema: &Schema) -> String {
  // format!("  {}: {}", field.name, get_field_nullable_ty(&field, schema))
  let field_nullable = get_field_nullable(field);

  if field.format.is_some() {
    return format!("  {}: string{}", field.name, field_nullable)
  }

  format!("  {}: {}{}", field.name, get_field_ty(&field.ty, schema), field_nullable)
}

// In the Id type, refs are narrowed to ModelId: for update/delete params and the insert result
// only the key is needed. The full ref type is additionally written out in the Model itself
fn get_field_id_str(field: &Field, schema: &Schema) -> String {
  if field.format.is_none() {
    if let FieldType::Ref(ref_info) = &field.ty {
      return format!("  {}: {}", field.name, get_model_id_name(&schema.models[ref_info.model_index]));
    }
  }
  get_field_str(field, schema)
}

/// The contents of the union branches for the Model type (without braces): `role: "admin", sign: string`
fn get_model_enum_branches(field: &Field, entity: &Entity, schema: &Schema) -> Vec<String> {
  let FieldType::Enum(enum_info) = &field.ty else { panic!("Expected enum field") };
  let mut branches = vec![];
  for (variant_name, variant_fields) in enum_variants_sorted(entity, enum_info) {
    let mut parts = vec![format!("{}: \"{}\"", field.name, variant_name)];
    for variant_field in variant_fields {
      parts.push(get_field_str(variant_field, schema).trim_start().to_string());
    }
    branches.push(parts.join(", "));
  }
  if field.nullable {
    branches.push(format!("{}: null", field.name));
  }
  branches
}

/// Union branches for the Insert type: fields of the chosen variant are required, fields of other variants are forbidden
fn get_insert_enum_branches(field: &Field, entity: &Entity, schema: &Schema) -> Vec<String> {
  let FieldType::Enum(enum_info) = &field.ty else { panic!("Expected enum field") };
  let variants = enum_variants_sorted(entity, enum_info);
  let mut branches = vec![];
  for (variant_name, variant_fields) in variants.iter() {
    let mut parts = vec![format!("{}: \"{}\"", field.name, variant_name)];
    for variant_field in variant_fields {
      parts.push(get_field_insert_str(variant_field, schema).trim_start().to_string());
    }
    push_never_fields(&mut parts, &variants, Some(variant_name));
    branches.push(format!("{{ {} }}", parts.join(", ")));
  }
  // If the enum can be omitted (nullable or has a default) — a branch without variant fields
  if field.nullable || field.default_value.is_some() {
    let enum_part = if field.nullable {
      format!("{}?: null", field.name)
    } else {
      format!("{}?: never", field.name)
    };
    let mut parts = vec![enum_part];
    push_never_fields(&mut parts, &variants, None);
    branches.push(format!("{{ {} }}", parts.join(", ")));
  }
  branches
}

/// Union branches for the Update type: either we leave the enum alone (the current variant's fields can be changed partially),
/// or we change it together with the full set of fields of the new variant
fn get_update_enum_branches(field: &Field, entity: &Entity, schema: &Schema) -> Vec<String> {
  let FieldType::Enum(enum_info) = &field.ty else { panic!("Expected enum field") };
  let variants = enum_variants_sorted(entity, enum_info);
  let mut branches = vec![];

  let mut parts = vec![format!("{}?: never", field.name)];
  let mut seen: Vec<&str> = vec![];
  for (_, variant_fields) in variants.iter() {
    for variant_field in variant_fields {
      // A field shared by several variants is added once
      if seen.contains(&variant_field.name.as_str()) { continue; }
      seen.push(variant_field.name.as_str());
      parts.push(get_field_update_str(variant_field, schema).trim_start().to_string());
    }
  }
  branches.push(format!("{{ {} }}", parts.join(", ")));

  for (variant_name, variant_fields) in variants.iter() {
    let mut parts = vec![format!("{}: \"{}\"", field.name, variant_name)];
    for variant_field in variant_fields {
      parts.push(get_field_insert_str(variant_field, schema).trim_start().to_string());
    }
    push_never_fields(&mut parts, &variants, Some(variant_name));
    branches.push(format!("{{ {} }}", parts.join(", ")));
  }

  if field.nullable {
    let mut parts = vec![format!("{}: null", field.name)];
    push_never_fields(&mut parts, &variants, None);
    branches.push(format!("{{ {} }}", parts.join(", ")));
  }
  branches
}

/// Adds `field?: never` for the fields of all variants except the current one —
/// closes the union branch off from other variants' fields.
/// A field shared by several variants is not forbidden if the current variant is one of them
fn push_never_fields(parts: &mut Vec<String>, variants: &[(&String, Vec<&Field>)], current_variant: Option<&String>) {
  let current_fields: Vec<&str> = variants.iter()
    .filter(|(name, _)| Some(*name) == current_variant)
    .flat_map(|(_, fields)| fields.iter().map(|f| f.name.as_str()))
    .collect();

  let mut seen: Vec<&str> = vec![];
  for (variant_name, variant_fields) in variants.iter() {
    if Some(*variant_name) == current_variant { continue; }
    for variant_field in variant_fields {
      let name = variant_field.name.as_str();
      if current_fields.contains(&name) || seen.contains(&name) { continue; }
      seen.push(name);
      parts.push(format!("{}?: never", name));
    }
  }
}

/// Appends union blocks of the form `} & (\n  {...}\n  | {...}\n)` to the output file for Insert/Update
fn push_union_blocks(lines: &mut Vec<String>, payload_enums: &[&Field], branches_list: Vec<Vec<String>>) {
  if payload_enums.is_empty() {
    lines.push("}".to_string());
    return;
  }
  for (i, branches) in branches_list.iter().enumerate() {
    lines.push(if i == 0 { "} & (".to_string() } else { ") & (".to_string() });
    for (j, branch) in branches.iter().enumerate() {
      lines.push(if j == 0 { format!("  {}", branch) } else { format!("  | {}", branch) });
    }
  }
  lines.push(")".to_string());
}

/// A relation to a top-level model also accepts a sub-query built from that model's collection
/// (`posts: db.post.limit(5)`, `db.post.count()`) — branded `Sub<"Post">` (see prefix.ts). Struct targets don't.
fn get_sub_query_str(ref_model: &Entity) -> String {
  if ref_model.name.contains('.') { String::new() } else { format!(" | Sub<\"{}\">", ref_model.name) }
}

fn get_field_select_str(field: &Field, schema: &Schema) -> String {
  match &field.ty {
    FieldType::Ref(ref_info) => {
      let ref_model = &schema.models[ref_info.model_index];
      format!("  {}?: {} | boolean{}", field.name, get_model_select_name(ref_model), get_sub_query_str(ref_model))
    },
    FieldType::RefList(ref_info) => {
      let ref_model = &schema.models[ref_info.model_index];
      format!("  {}?: {} | {} | boolean{}", field.name, get_model_query_name(ref_model), get_model_aggregate_name(ref_model), get_sub_query_str(ref_model))
    },
    _ => format!("  {}?: boolean", field.name)
  }
}

fn get_model_scalars_name(model: &Entity) -> String {
  format!("{}ModelScalars", model.name.replace('.', "_"))
}

fn get_model_types_name(model: &Entity) -> String {
  format!("{}Types", model.name.replace('.', "_"))
}

/// Whether any field of the model carries a `@custom` (module) index — i.e. the model has a `reindex()`.
fn model_has_custom_index(model: &Entity) -> bool {
  model.fields.iter().any(|f| f.indexes.iter().any(|i| matches!(i, FieldIndex::Custom { .. })))
}

/// A `@custom`-indexed field also accepts `$near`/`$search` in `$Where`. The payload type is the provider's:
/// vectors get a typed `VectorSearch`, other providers a generic `CustomSearch` (extend this match per module).
fn get_custom_search_str(field: &Field) -> Option<String> {
  let name = field.indexes.iter().find_map(|i| match i {
    FieldIndex::Custom { name, .. } => Some(name.as_str()),
    _ => None,
  })?;
  let payload = match name {
    "vector" => "VectorSearch",
    "fulltext" => "FullTextSearch",
    _ => "CustomSearch",
  };
  Some(format!(" | CustomSearchValue<{}>", payload))
}

fn get_field_where_str(field: &Field, schema: &Schema) -> String {
  let field_nullable = if field.nullable { " | null" } else { "" };
  let line = match &field.ty {
    FieldType::Ref(ref_info) => {
      format!("  {}?: CompareRefValue<{}{}>", field.name, get_model_where_name(&schema.models[ref_info.model_index]), field_nullable)
    },
    FieldType::RefList(ref_info) => {
      format!("  {}?: CompareRefListValue<{}>", field.name, get_model_where_name(&schema.models[ref_info.model_index]))
    },
    FieldType::Primitive(ty) => {
      if ty.get_num_type().is_some() {
        format!("  {}?: CompareValue<{}{}> | CompareNumValue<{}>", field.name, get_field_ty(&field.ty, schema), field_nullable, get_field_ty(&field.ty, schema))
      } else if matches!(ty, PrimitiveFieldType::String) {
        format!("  {}?: CompareValue<{}{}> | CompareStrValue<{}>", field.name, get_field_ty(&field.ty, schema), field_nullable, get_field_ty(&field.ty, schema))
      } else if matches!(ty, PrimitiveFieldType::Json) {
        // Whole-value compare, or a map of JSON paths → per-leaf conditions.
        format!("  {}?: CompareValue<{}{}> | JsonPathWhere", field.name, get_field_ty(&field.ty, schema), field_nullable)
      } else {
        format!("  {}?: CompareValue<{}{}>", field.name, get_field_ty(&field.ty, schema), field_nullable)
      }
    },
    _ => format!("  {}?: CompareValue<{}{}>", field.name, get_field_write_ty(field, schema), field_nullable)
  };
  match get_custom_search_str(field) {
    Some(suffix) => format!("{}{}", line, suffix),
    None => line,
  }
}

fn get_field_insert_str(field: &Field, schema: &Schema) -> String {
  let field_optional = if field.default_value.is_some() || field.nullable { "?" } else { "" };
  let field_nullable = if field.nullable { " | null" } else { "" };
  match &field.ty {
    FieldType::RefList(ref_info) => {
      // The back-reference of a `@list` relation stores nothing — writes go through the @list side
      if partner_is_id_list(schema, ref_info) {
        return format!("  {}?: never", field.name);
      }
      let to_insert = if schema.models[ref_info.model_index].autoinsert {
        get_model_insert_name(&schema.models[ref_info.model_index])
      } else {
        get_model_id_name(&schema.models[ref_info.model_index])
      };
      format!("  {}?: {}[]", field.name, to_insert)
    },
    FieldType::Ref(ref_info) => {
      let to_insert = if schema.models[ref_info.model_index].autoinsert { 
        get_model_insert_name(&schema.models[ref_info.model_index])
      } else {
        get_model_id_name(&schema.models[ref_info.model_index])
      };
      format!("  {}{}: {}{}", field.name, field_optional, to_insert, field_nullable)
    },
    _ => format!("  {}{}: {}{}", field.name, field_optional, get_field_write_ty(field, schema), field_nullable)
  }
}

fn get_field_update_str(field: &Field, schema: &Schema) -> String {
  let field_nullable = if field.nullable { " | null" } else { "" };

  match &field.ty {
    FieldType::RefList(ref_info) => {
      // The back-reference of a `@list` relation is read-only — writes go through the @list side
      if partner_is_id_list(schema, ref_info) {
        return format!("  {}?: never", field.name);
      }
      let ref_model = &schema.models[ref_info.model_index];
      let to_update = if ref_model.autoinsert {
        format!("RefListUpdateStruct<{},{},{}>", get_model_insert_name(ref_model), get_model_update_name(ref_model), get_model_id_name(ref_model))
      } else if is_id_list(field) {
        // `@list`: $set replaces (and reorders) the whole array; $connect appends; $remove splices out
        format!("RefListUpdateOrdered<{}>", get_model_id_name(ref_model))
      } else {
        format!("RefListUpdate<{}>", get_model_id_name(ref_model))
      };
      // One object, not a list of them: the engine reads this field as a single object and applies every
      // operator key in it (`{ $connect: …, $remove: … }`). An array is rejected outright.
      format!("  {}?: {}", field.name, to_update)
    },
    FieldType::Ref(ref_info) => {
      let ref_model = &schema.models[ref_info.model_index];
      let to_update = if ref_model.autoinsert { 
        format!("RefUpdateStruct<{},{}>", get_model_insert_name(ref_model), get_model_update_name(ref_model))
      } else {
        format!("RefUpdate<{}>", get_model_id_name(ref_model))
      };
      format!("  {}?: {}{}", field.name, to_update, field_nullable)
    },
    // Numeric fields also take an atomic in-place `$increment` (see `UpdateNumValue`).
    FieldType::Primitive(ty) if ty.get_num_type().is_some() => {
      format!("  {}?: {} | UpdateNumValue{}", field.name, get_field_ty(&field.ty, schema), field_nullable)
    },
    // Variable-length lists also take in-place operators; fixed-size lists ([Byte;16]) cannot
    // change length and fall through to whole-value assignment only
    FieldType::PrimitiveList(ty, None) => {
      format!("  {}?: {} | PrimitiveListUpdate<{}>{}", field.name, get_field_write_ty(field, schema), get_primitive_str(ty), field_nullable)
    },
    _ => format!("  {}?: {}{}", field.name, get_field_write_ty(field, schema), field_nullable)
  }
}

/// Binary-transport type code for a primitive. `Float` is decoded as f64 (the engine widens it on the wire
/// for exact parity) and `DateTime` as i64 (an epoch number, matching the JSON path). `Json` ships as its
/// decoded JSON text (string framing) under the `json` code, which the client `JSON.parse`s.
fn binary_type_code(ty: &PrimitiveFieldType) -> &'static str {
  match ty {
    PrimitiveFieldType::String => "str",
    PrimitiveFieldType::Int64 => "i64",
    PrimitiveFieldType::UInt64 => "u64",
    PrimitiveFieldType::Float => "f64",
    PrimitiveFieldType::Double => "f64",
    PrimitiveFieldType::Bool => "bool",
    PrimitiveFieldType::Byte => "u8",
    PrimitiveFieldType::DateTime => "i64",
    PrimitiveFieldType::Json => "json",
  }
}

/// One field descriptor for the binary decoder-compiler: `{ n: name, k: kind, t: typecode, m: relModel }`.
/// `k` is `key` | `body` | `one` | `many`; `t` is the scalar type code (or `null` for a scalar the binary
/// path can't decode yet → selecting it makes the whole query fall back to JSON); `m` is the target model
/// for relations. The order mirrors `entity.fields`, which is the slot order the engine encodes in.
fn binary_field_descriptor(field: &Field, schema: &Schema) -> Option<String> {
  // `@`-prefixed internal fields are never selectable, so they never reach the wire — omit them (the
  // relative order of the remaining, selectable fields is preserved, which is all the decoder needs).
  if field.name.starts_with('@') {
    return None;
  }

  let descriptor = match &field.ty {
    FieldType::Ref(ref_info) => {
      format!("{{ n: \"{}\", k: \"one\", m: \"{}\" }}", field.name, schema.models[ref_info.model_index].name)
    }
    FieldType::RefList(ref_info) => {
      format!("{{ n: \"{}\", k: \"many\", m: \"{}\" }}", field.name, schema.models[ref_info.model_index].name)
    }
    _ => {
      let kind = if matches!(field.location, FieldLocation::Key { .. }) { "key" } else { "body" };
      // A scalar is binary-decodable only if it is an unformatted primitive on the matching enum value
      // (no @format, no enum-variant condition). Everything else is `t: null` → JSON fallback.
      let code = match &field.ty {
        FieldType::Primitive(ty)
          if field.format.is_none() && matches!(field.condition, FieldExistsCondition::None) =>
        {
          format!("\"{}\"", binary_type_code(ty))
        }
        _ => "null".to_string(),
      };
      format!("{{ n: \"{}\", k: \"{}\", t: {} }}", field.name, kind, code)
    }
  };
  Some(descriptor)
}

/// The `MODELS` metadata object literal the decoder-compiler consumes: every model (including struct models,
/// which relations target) → its ordered field descriptors. Emitted once, statically, into the client.
fn get_models_metadata(schema: &Schema) -> String {
  let mut entries = vec![];
  for model in schema.models.iter() {
    let fields: Vec<String> = model.fields.iter()
      .filter_map(|field| binary_field_descriptor(field, schema))
      .collect();
    entries.push(format!("  \"{}\": [ {} ]", model.name, fields.join(", ")));
  }
  format!("{{\n{}\n}}", entries.join(",\n"))
}

fn main() {
  let args: Vec<String> = env::args().collect();
  match args.get(1).map(String::as_str) {
    Some("types") => generate_types(
      args.get(2).map(String::as_str).unwrap_or("schema.marci"),
      args.get(3).map(String::as_str).unwrap_or("."),
    ),
    // Backward compatibility: `marcidb-ts <schema> <output>` = type generation.
    // Migrations moved to a separate binary `marci-migrate`.
    _ => generate_types(
      args.get(1).map(String::as_str).unwrap_or("schema.marci"),
      args.get(2).map(String::as_str).unwrap_or("."),
    ),
  }
}

fn generate_types(input: &str, output_dir: &str) {
  let schema_str = &fs::read_to_string(input)
      .unwrap_or_else(|_| panic!("File not found: {}", input));

  let schema = parse_schema(schema_str);

  let mut lines = vec![];
  for model in schema.models.iter() {

    lines.push(format!("// {}", model.name));

    // Enums with fields in variants are turned into a discriminated union
    let payload_enums: Vec<&Field> = model.fields.iter()
      .filter(|field| !field.name.starts_with("@") && is_payload_enum(field))
      .collect();

    // Fill in the fields from the ID
    lines.push(format!("type {} = {{", get_model_id_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      if matches!(field.location, FieldLocation::Key { .. }) {
        lines.push(get_field_id_str(field, &schema));
      }
    }
    lines.push("}".to_string());

    // Fill in the fields from the body
    // When there's a payload enum we generate XModelBase + union, so that GetResult distributively
    // infers a variant's fields only for the corresponding enum value
    let model_base_name = if payload_enums.is_empty() {
      get_model_name(model)
    } else {
      format!("{}Base", get_model_name(model))
    };
    lines.push(format!("type {} = {} & {{", model_base_name, get_model_id_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      if matches!(field.location, FieldLocation::Key { .. }) {
        // Ref keys in the Id type are narrowed to ModelId — here we give them back their full type
        if matches!(field.ty, FieldType::Ref(_)) {
          lines.push(get_field_str(field, &schema));
        }
        continue;
      }
      if is_variant_field(field) || is_payload_enum(field) { continue; }
      lines.push(get_field_str(field, &schema));
    }
    lines.push("}".to_string());

    if !payload_enums.is_empty() {
      // Cartesian product of the branches of all the model's payload enums
      let mut combos: Vec<String> = vec![String::new()];
      for field in payload_enums.iter() {
        let branches = get_model_enum_branches(field, model, &schema);
        combos = combos.iter().flat_map(|combo| {
          branches.iter().map(move |branch| {
            if combo.is_empty() { branch.clone() } else { format!("{}, {}", combo, branch) }
          })
        }).collect();
      }
      lines.push(format!("type {} =", get_model_name(model)));
      for combo in combos {
        lines.push(format!("  | ({} & {{ {} }})", model_base_name, combo));
      }
    }

    // Fill in the fields for Insert
    lines.push(format!("type {} = {{", get_model_insert_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      if is_variant_field(field) || is_payload_enum(field) { continue; }
      lines.push(get_field_insert_str(field, &schema));
    }
    push_union_blocks(&mut lines, &payload_enums,
      payload_enums.iter().map(|field| get_insert_enum_branches(field, model, &schema)).collect());

    // Fill in the fields for Update. Key fields are omitted: the engine refuses to edit them
    // (`OnlyBodyKeyAvailableToEdit`), so offering them in the type only invites a runtime error.
    lines.push(format!("type {} = {{", get_model_update_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      if is_variant_field(field) || is_payload_enum(field) { continue; }
      if matches!(field.location, FieldLocation::Key { .. }) { continue; }
      lines.push(get_field_update_str(field, &schema));
    }
    push_union_blocks(&mut lines, &payload_enums,
      payload_enums.iter().map(|field| get_update_enum_branches(field, model, &schema)).collect());

    
    // Fill in the fields for select
    lines.push(format!("type {} = {{", get_model_select_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      lines.push(get_field_select_str(field, &schema));
    }
    lines.push("}".to_string());

    // Fill in the fields for Where. The boolean combinators come from `WhereValue`, which also forbids
    // mixing them with sibling field conditions (the engine ignores such siblings).
    lines.push(format!("type {} = {{", get_model_where_fields_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      lines.push(get_field_where_str(field, &schema));
    }
    lines.push("}".to_string());
    lines.push(format!("type {} = WhereValue<{}>", get_model_where_name(model), get_model_where_fields_name(model)));

    // Fill in the fields for Order (mirror of parse_order: key fields + primitives/enums from body)
    lines.push(format!("type {} = {{", get_model_order_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      let sortable = match field.location {
        FieldLocation::Key { .. } => true,
        FieldLocation::Body { .. } => matches!(field.ty, FieldType::Primitive(_) | FieldType::Enum(_)),
        FieldLocation::Virtual => false
      };
      if sortable {
        lines.push(format!("  {}?: \"asc\" | \"desc\"", field.name));
      }
    }
    lines.push("}".to_string());

    // Fill in the fields for Query
    lines.push(format!("type {} = {} & {{", get_model_query_name(model), get_model_select_name(model)));
    lines.push(format!("  $where?: {}", get_model_where_name(model)));
    lines.push(format!("  $order?: {}", get_model_order_name(model)));
    lines.push("  $limit?: number".to_string());
    lines.push("  $skip?: number".to_string());
    lines.push(format!("  $cursor?: {}", get_model_id_name(model)));
    lines.push("}".to_string());

    // Fill in the fields for Aggregate
    lines.push(format!("type {} = {{", get_model_aggregate_name(model)));
    lines.push(format!("  $where?: {}", get_model_where_name(model)));
    lines.push("  $count?: true".to_string());
    lines.push(format!("  $sum?: {}", get_aggregate_fields_union(model, true)));
    lines.push(format!("  $avg?: {}", get_aggregate_fields_union(model, true)));
    lines.push(format!("  $min?: {}", get_aggregate_fields_union(model, false)));
    lines.push(format!("  $max?: {}", get_aggregate_fields_union(model, false)));
    lines.push("}".to_string());

    // What an empty select returns (mirror of `QueryOp::all` / the client's `scalarSelect`): every non-relation
    // field, as the `{ field: true }` shape the result type is inferred from.
    lines.push(format!("type {} = {{", get_model_scalars_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      if matches!(field.ty, FieldType::Ref(_) | FieldType::RefList(_)) { continue; }
      lines.push(format!("  {}: true", field.name));
    }
    lines.push("}".to_string());

    // The type bag `Query<T>` (prefix.ts) is parametrised by — top-level models only (structs have no collection).
    if !model.name.contains('.') {
      lines.push(format!("type {} = {{", get_model_types_name(model)));
      lines.push(format!("  name: \"{}\"", model.name));
      lines.push(format!("  model: {}", get_model_name(model)));
      lines.push(format!("  id: {}", get_model_id_name(model)));
      lines.push(format!("  scalars: {}", get_model_scalars_name(model)));
      lines.push(format!("  select: {}", get_model_select_name(model)));
      lines.push(format!("  query: {}", get_model_query_name(model)));
      lines.push(format!("  where: {}", get_model_where_name(model)));
      lines.push(format!("  order: {}", get_model_order_name(model)));
      lines.push(format!("  insert: {}", get_model_insert_name(model)));
      lines.push(format!("  update: {}", get_model_update_name(model)));
      lines.push(format!("  aggregate: {}", get_model_aggregate_name(model)));
      lines.push(format!("  reindex: {}", model_has_custom_index(model)));
      lines.push("}".to_string());
    }

    lines.push("".to_string());
  }
  
  // `db.<model>` is a `Query` root (prefix.ts): the chain (where/order/limit/select/…) and the object form
  // (findMany/findFirst/…) over the model's type bag; `reindex()` only for models with a `@custom` index.
  lines.push(format!("export interface MarciDB {{"));
  for model in schema.models.iter() {
    if model.name.contains(".") { continue; };
    lines.push(format!("  {}: Collection<{}>", get_model_small_name(model), get_model_types_name(model)));
  }
  // Atomic batch transaction: operations are applied all or none; the results are
  // a tuple sized by the number of operations. Use ref("0.id") to reference previous results
  lines.push("  $transaction<P extends readonly Op<any>[]>(ops: [...P]): Promise<{ [K in keyof P]: P[K] extends Op<infer T> ? T : never }>".to_string());
  lines.push("}".to_string());

  let prefix = include_str!("prefix.ts").to_string();
  lines.insert(0, prefix);
  let types_out = lines.join("\n");
  
  // Create the index.js file
  let mut lines: Vec<String> = vec![];
  for model in schema.models.iter() {
    if model.name.contains(".") { continue; };
    lines.push(format!("{}: collection(\"{}\"),", get_model_small_name(model), model.name));
  }

  let index_out = include_str!("prefix.js")
    .replace("/* generated_models */", &get_models_metadata(&schema))
    .replace("/* generated_schema_hash */", &format!("\"{}\"", marcidb::schema_fingerprint(&schema)))
    .replace("/* generated_data */", &lines.join("\n    "));

  let out_path = Path::new(output_dir);
  fs::create_dir_all(out_path).unwrap();

  let dts_path = out_path.join("index.d.ts");
  fs::write(&dts_path, types_out).unwrap();

  let js_path = out_path.join("index.js");
  fs::write(&js_path, index_out).unwrap();

  // One concise line: show the dir relative to the cwd when possible (the default output lives deep in
  // node_modules), and normalize separators so the path reads the same on every OS.
  let shown = std::env::current_dir().ok()
    .and_then(|cwd| out_path.strip_prefix(&cwd).ok().map(Path::to_path_buf))
    .unwrap_or_else(|| out_path.to_path_buf());
  let dir = shown.display().to_string().replace('\\', "/");
  println!("Generated client \u{2192} {}/ (index.js, index.d.ts)", dir);
}

#[cfg(test)]
mod binary_descriptor_tests {
  use super::*;

  /// Per-field descriptors the binary decoder-compiler consumes — one assertion per field *kind*, since the
  /// `k`/`t`/`m` split is what the TS gate keys off of. The order within a model must mirror `entity.fields`
  /// (the slot order the engine encodes), so we also assert the relative order of a couple of fields.
  #[test]
  fn descriptors_cover_every_field_kind() {
    // Block keywords must be column-0 (the parser doesn't dedent), so keep this literal flush-left. The
    // relation is exercised both ways: User.posts (to-many) and Post.author (to-one).
    let schema = parse_schema(
"model User {
  name   String
  age    Int
  rating Float?
  when   DateTime?
  ok     Bool
  uid    Byte[16]  @format(uuid)
  tags   String[]
  role   Role
  posts  Post[]    @bind(Post.author)
}
model Post {
  title  String
  author User?
}
enum Role {
  basic
  pro { seats Int }
}
",
    );

    let meta = get_models_metadata(&schema);

    // Auto `id` (UInt64) is the key field, emitted first, with no presence tag on the wire.
    assert!(meta.contains(r#"{ n: "id", k: "key", t: "u64" }"#), "id key descriptor\n{meta}");

    // Plain primitives → their type code. Float decodes as f64, DateTime as i64 (epoch number).
    assert!(meta.contains(r#"{ n: "name", k: "body", t: "str" }"#), "{meta}");
    assert!(meta.contains(r#"{ n: "age", k: "body", t: "i64" }"#), "{meta}");
    assert!(meta.contains(r#"{ n: "rating", k: "body", t: "f64" }"#), "{meta}");
    assert!(meta.contains(r#"{ n: "when", k: "body", t: "i64" }"#), "{meta}");
    assert!(meta.contains(r#"{ n: "ok", k: "body", t: "bool" }"#), "{meta}");

    // Types the binary path doesn't cover yet → `t: null` (selecting them falls back to JSON).
    assert!(meta.contains(r#"{ n: "uid", k: "body", t: null }"#), "formatted → null\n{meta}");
    assert!(meta.contains(r#"{ n: "tags", k: "body", t: null }"#), "list → null\n{meta}");
    assert!(meta.contains(r#"{ n: "role", k: "body", t: null }"#), "enum → null\n{meta}");
    // The enum field is `t: null`, so any shape selecting it falls back to JSON. Injected variant fields
    // (`seats`) aren't materialized as separate fields at codegen time, only in the post-sync runtime schema
    // — that's fine: if one is ever selected, the engine's `shape_supported` sees the condition and reports
    // "unsupported" (status 2) → JSON fallback. The two sides don't need identical field sets, only an
    // agreeing answer on what's binary-decodable.
    assert!(!meta.contains(r#""seats""#), "variant field not materialized at codegen time\n{meta}");

    // Relations carry the target model, no type code — to-many (User.posts) and to-one (Post.author).
    assert!(meta.contains(r#"{ n: "posts", k: "many", m: "Post" }"#), "to-many\n{meta}");
    assert!(meta.contains(r#"{ n: "author", k: "one", m: "User" }"#), "to-one\n{meta}");

    // Models that relations point at must also be present in the table, in slot order.
    assert!(meta.contains(r#""Post": [ { n: "id", k: "key", t: "u64" }, { n: "title", k: "body", t: "str" }"#), "Post model emitted\n{meta}");

    // Field order within a model mirrors entity.fields: name precedes age precedes rating.
    let user = meta.find(r#""User""#).unwrap();
    let pos = |needle: &str| meta[user..].find(needle).unwrap();
    assert!(pos("\"name\"") < pos("\"age\"") && pos("\"age\"") < pos("\"rating\""), "slot order preserved\n{meta}");
  }

  /// Internal (`@`-prefixed) fields are never selectable, so they must not appear in the descriptor list —
  /// the decoder only needs the relative order of the *selectable* fields preserved.
  #[test]
  fn descriptors_skip_internal_fields() {
    // A reverse-relation `@bind` does not create an `@`-field here, but variant/enum bookkeeping can; assert
    // no descriptor name starts with '@' for a schema that exercises enums + relations.
    let schema = parse_schema(
"enum Kind { x  y { n Int } }
model B { label String }
model A {
  kind Kind
  b    B?
}
",
    );
    let meta = get_models_metadata(&schema);
    assert!(!meta.contains(r#"n: "@"#), "no @-prefixed field descriptors\n{meta}");
  }
}