use std::{env, fs, path::Path};

use marcidb::{Entity, Field, FieldLocation, FieldType, PrimitiveFieldType, Schema, parse_schema};

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
      enum_info.variants_map.keys().fold(String::new(), |mut acc, item| {
        if !acc.is_empty() { acc.push_str(" | ") }
        acc.push_str(format!("\"{}\"", item).as_str());
        acc
      })
    },
    _ => panic!("Unsupported field type")
  }
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

fn get_field_select_str(field: &Field, schema: &Schema) -> String {
  match &field.ty {
    FieldType::Ref(ref_info) => {
      format!("  {}?: {} | boolean", field.name, get_model_select_name(&schema.models[ref_info.model_index]))
    },
    FieldType::RefList(ref_info) => {
      format!("  {}?: {} | boolean", field.name, get_model_query_name(&schema.models[ref_info.model_index]))
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

  let input = args.get(1).map(String::as_str).unwrap_or("schema.marci");
  let output_dir = args.get(2).map(String::as_str).unwrap_or(".");

  let schema_str = &fs::read_to_string(input)
      .unwrap_or_else(|_| panic!("File not found: {}", input));

  let schema = parse_schema(schema_str);

  let mut lines = vec![];
  for model in schema.models.iter() {

    lines.push(format!("// {}", model.name));

    // Заполняем поля от ID
    lines.push(format!("type {} = {{", get_model_id_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      if matches!(field.location, FieldLocation::Key { .. }) {
        lines.push(get_field_str(field, &schema));
      }
    }
    lines.push("}".to_string());

    // Заполняем поля от body
    lines.push(format!("type {} = {} & {{", get_model_name(model), get_model_id_name(model)));
    for field in model.fields.iter() {
      if !matches!(field.location, FieldLocation::Key { .. }) {
        lines.push(get_field_str(field, &schema));
      }
    }
    lines.push("}".to_string());

    // Заполняем поля для Insert
    lines.push(format!("type {} = {{", get_model_insert_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      lines.push(get_field_insert_str(field, &schema));
    }
    lines.push("}".to_string());

    // Заполняем поля для Update
    lines.push(format!("type {} = {{", get_model_update_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      lines.push(get_field_update_str(field, &schema));
    }
    lines.push("}".to_string());

    
    // Заполняем поля для select
    lines.push(format!("type {} = {{", get_model_select_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      lines.push(get_field_select_str(field, &schema));
    }
    lines.push("}".to_string());

    // Заполняем поля для Where
    lines.push(format!("type {} = {{", get_model_where_name(model)));
    for field in model.fields.iter() {
      if field.name.starts_with("@") { continue; }
      lines.push(get_field_where_str(field, &schema));
    }
    lines.push("}".to_string());

    // Заполняем поля для Query
    lines.push(format!("type {} = {} & {{", get_model_query_name(model), get_model_select_name(model)));
    lines.push(format!("  $where?: {}", get_model_where_name(model)));
    lines.push("}".to_string());

    lines.push("".to_string());
  }
  
  lines.push(format!("export interface MarciDB {{"));
  for model in schema.models.iter() {
    if model.name.contains(".") { continue; };
    lines.push(format!("  {}: {{", get_model_small_name(model)));
    lines.push(format!("    findMany<T extends {}>(select: T): Promise<GetResult<{}, T>[]>", get_model_query_name(model), get_model_name(model)));
    lines.push(format!("    findFirst<T extends {}>(select: T): Promise<GetResult<{}, T> | null>", get_model_query_name(model), get_model_name(model)));
    lines.push(format!("    insert(data: {}): Promise<{}>", get_model_insert_name(model), get_model_id_name(model)));
    lines.push(format!("    update(id: {}, data: {}): Promise<void>", get_model_id_name(model), get_model_update_name(model)));
    lines.push(format!("    delete(id: {}): Promise<void>", get_model_id_name(model)));
    lines.push("  }".to_string());
  }
  lines.push("}".to_string());

  let prefix = include_str!("prefix.ts").to_string();
  lines.insert(0, prefix);
  let types_out = lines.join("\n");
  
  // Создаем index.js файл
  let mut lines: Vec<String> = vec![];
  for model in schema.models.iter() { 
    if model.name.contains(".") { continue; };
    lines.push(format!("{}: {{", get_model_small_name(model)));
    lines.push(format!("  findMany: (select) => findMany(\"{}\", select),", model.name));
    lines.push(format!("  findFirst: (select) => findFirst(\"{}\", select),", model.name));
    lines.push(format!("  insert: (data) => insert(\"{}\", data),", model.name));
    lines.push(format!("  update: (id,data) => update(\"{}\", id, data),", model.name));
    lines.push(format!("  delete: (id) => runDelete(\"{}\", id)", model.name));
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