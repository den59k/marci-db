use std::{env, fs, path::Path};

use marcidb::{Entity, EnumInfo, Field, FieldExistsCondition, FieldLocation, FieldType, PrimitiveFieldType, Schema, parse_schema};

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

fn get_field_select_str(field: &Field, schema: &Schema) -> String {
  match &field.ty {
    FieldType::Ref(ref_info) => {
      format!("  {}?: {} | boolean", field.name, get_model_select_name(&schema.models[ref_info.model_index]))
    },
    FieldType::RefList(ref_info) => {
      let ref_model = &schema.models[ref_info.model_index];
      format!("  {}?: {} | {} | boolean", field.name, get_model_query_name(ref_model), get_model_aggregate_name(ref_model))
    },
    _ => format!("  {}?: boolean", field.name)
  }
}

fn get_field_where_str(field: &Field, schema: &Schema) -> String {
  let field_nullable = if field.nullable { " | null" } else { "" };
  match &field.ty {
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
        format!("  {}?: CompareValue<{}{}> | CompareStrValue", field.name, get_field_ty(&field.ty, schema), field_nullable)
      } else {
        format!("  {}?: CompareValue<{}{}>", field.name, get_field_ty(&field.ty, schema), field_nullable)
      }
    },
    _ => format!("  {}?: CompareValue<{}{}>", field.name, get_field_ty(&field.ty, schema), field_nullable)
  }
}

fn get_field_insert_str(field: &Field, schema: &Schema) -> String {
  let field_optional = if field.default_value.is_some() || field.nullable { "?" } else { "" };
  let field_nullable = if field.nullable { " | null" } else { "" };
  match &field.ty {
    FieldType::RefList(ref_info) => {
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
    FieldType::PrimitiveList(PrimitiveFieldType::Byte, _) => {
      format!("  {}{}: {} | string{}", field.name, field_optional, get_field_ty(&field.ty, schema), field_nullable)
    },
    _ => format!("  {}{}: {}{}", field.name, field_optional, get_field_ty(&field.ty, schema), field_nullable)
  }
}

fn get_field_update_str(field: &Field, schema: &Schema) -> String {
  let field_nullable = if field.nullable { " | null" } else { "" };

  match &field.ty {
    FieldType::RefList(ref_info) => {
      let ref_model = &schema.models[ref_info.model_index];
      let to_update = if ref_model.autoinsert { 
        format!("RefListUpdateStruct<{},{}>", get_model_insert_name(ref_model), get_model_update_name(ref_model))
      } else {
        format!("RefListUpdate<{}>", get_model_id_name(ref_model))
      };
      format!("  {}?: {}[]", field.name, to_update)
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
    _ => format!("  {}?: {}{}", field.name, get_field_ty(&field.ty, schema), field_nullable)
  }
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

    // Fill in the fields for Update
    lines.push(format!("type {} = {{", get_model_update_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      if is_variant_field(field) || is_payload_enum(field) { continue; }
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

    // Fill in the fields for Where
    lines.push(format!("type {} = {{", get_model_where_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      lines.push(get_field_where_str(field, &schema));
    }
    lines.push("}".to_string());

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

    lines.push("".to_string());
  }
  
  lines.push(format!("export interface MarciDB {{"));
  for model in schema.models.iter() {
    if model.name.contains(".") { continue; };
    lines.push(format!("  {}: {{", get_model_small_name(model)));
    lines.push(format!("    findMany<T extends {}>(select: T): Op<GetResult<{}, T>[]>", get_model_query_name(model), get_model_name(model)));
    lines.push(format!("    findFirst<T extends {}>(select: T): Op<GetResult<{}, T> | null>", get_model_query_name(model), get_model_name(model)));
    lines.push(format!("    insert(data: {}): Op<{}>", get_model_insert_name(model), get_model_id_name(model)));
    lines.push(format!("    update(id: {}, data: {}): Op<void>", get_model_id_name(model), get_model_update_name(model)));
    lines.push(format!("    delete(id: {}): Op<void>", get_model_id_name(model)));
    lines.push(format!("    count(query?: {{ $where?: {} }}): Op<number>", get_model_where_name(model)));
    lines.push(format!("    aggregate<T extends {}>(query: T): Op<AggregateResult<{}, T>>", get_model_aggregate_name(model), get_model_name(model)));
    lines.push("  }".to_string());
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
    lines.push(format!("{}: {{", get_model_small_name(model)));
    lines.push(format!("  findMany: (select) => op({{ model: \"{0}\", action: \"findMany\", query: select }}, () => findMany(\"{0}\", select)),", model.name));
    lines.push(format!("  findFirst: (select) => op({{ model: \"{0}\", action: \"findFirst\", query: select }}, () => findFirst(\"{0}\", select)),", model.name));
    lines.push(format!("  insert: (data) => op({{ model: \"{0}\", action: \"insert\", data }}, () => insert(\"{0}\", data)),", model.name));
    lines.push(format!("  update: (id, data) => op({{ model: \"{0}\", action: \"update\", id, data }}, () => update(\"{0}\", id, data)),", model.name));
    lines.push(format!("  delete: (id) => op({{ model: \"{0}\", action: \"delete\", id }}, () => runDelete(\"{0}\", id)),", model.name));
    lines.push(format!("  count: (query) => op({{ model: \"{0}\", action: \"count\", query: query ?? {{}} }}, () => count(\"{0}\", query)),", model.name));
    lines.push(format!("  aggregate: (query) => op({{ model: \"{0}\", action: \"aggregate\", query }}, () => aggregate(\"{0}\", query))", model.name));
    lines.push("},".to_string());
  }

  let index_out = include_str!("prefix.js").replace("/* generated_data */", &lines.join("\n    "));

  let out_path = Path::new(output_dir);
  fs::create_dir_all(out_path).unwrap();

  let dts_path = out_path.join("index.d.ts");
  fs::write(&dts_path, types_out).unwrap();
  println!("File {} created", dts_path.display());

  let js_path = out_path.join("index.js");
  fs::write(&js_path, index_out).unwrap();
  println!("File {} created", js_path.display());
}