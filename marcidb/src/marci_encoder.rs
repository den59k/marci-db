
use serde_json::{Map, Value};
use bitvec::prelude::*;

use crate::{marci_db::InsertStruct, schema::{Entity, Field, FieldType, PrimitiveFieldType, Schema}};

#[derive(Debug)]
pub enum EncodeError {
    NotAnObject,
    MissingIdField(String),
    TypeMismatch { field: String, expected: &'static str },
    TypeMismatchEnum { field: String, expected: String },
    TryWriteToVirtualField,
    UnavailableKeyField,
    EmptyObject
}

/// Кодируем JSON-документ для заданной модели в бинарный формат. Возвращает данные и changed_mask
/// Не все данные записываются в document, используйте также функцию encode_id для кодирования полей в ID
pub fn encode_document<'a>(schema: &'a Schema, model: &'a Entity, json: &Value, structs: &mut Vec<InsertStruct<'a>>) -> Result<(Vec<u8>,BitVec), EncodeError> {
    let obj = json
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    const VERSION: u8 = 1;

    // [version: u8] + [field_count: u16] + [offsets: N * u32]
    let mut buf = Vec::with_capacity(model.payload_offset + 128);

    // version
    buf.push(VERSION);
    // field_count
    buf.extend_from_slice(&(model.payload_offset as u16).to_be_bytes());
    // offsets (плейсхолдеры)
    buf.resize(model.payload_offset, 0);

    // let initial_size = buf.len();

    let changed_mask = write_fields(obj, &mut buf, &model, &schema, structs)?;

    // if buf.len() == initial_size && structs.len() == 0 {
    //     return Err(EncodeError::EmptyObject);
    // }

    Ok((buf, changed_mask))
}

/// Кодирует массив значений и дописывает в конец `dst`
// fn encode_list<T>(
//     dst: &mut Vec<u8>,
//     ty: &PrimitiveFieldType,
//     field_name: &str,
//     v: &[T],
// )  -> Result<(), EncodeError> where T: Borrow<Value> {
//     dst.extend_from_slice(&(v.len() as u32).to_be_bytes());
//     for (index, val) in v.iter().enumerate() {
//         // TODO: remove format! from this
//         encode_value(dst, ty, &format!("{}[{}]", field_name, index), val.borrow())?;
//     }
//     Ok(())
// }

fn write_fields<'a>(
    obj: &Map<String, Value>, 
    buf: &mut Vec<u8>, 
    entity: &'a Entity,
    schema: &'a Schema, 
    structs: &mut Vec<InsertStruct<'a>>
) -> Result<BitVec, EncodeError> {
    let mut changed_mask = bitvec![0; entity.fields.len()];

    // Тело
    for (field_index, field) in entity.fields.iter().enumerate() {
        let Some(value) = obj.get(&field.name) else {
            // TODO: set default value here. Now it setting null (offset = 0)
            continue;
        };

        changed_mask.set(field_index, true);

        if field.id_idx.is_some() {
            continue;
        }

        if value.is_null() {
            match field.ty {
                FieldType::Struct(ref st) => {
                    structs.push(InsertStruct::None { st: &st });
                },
                FieldType::StructList(_) => {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array" })
                },
                FieldType::ModelRefList(_) => {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array<{ id: u64 }>" })
                },
                _ => { }
            }
            continue;
        }

        match &field.ty {
            FieldType::Primitive(primitive_type) => {
                if field.offset_pos == 0 {
                    println!("Warn: try to write to field {} has not offset_pos", field.full_name);
                    continue;
                }
                write_header(buf, field)?;
                encode_value(buf, field, primitive_type,  value)?;
            }
            FieldType::PrimitiveList(primitive_type) => {
                let Some(arr) = value.as_array() else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array" })
                };
                let byte_start = buf.len();
                write_header(buf, field)?;
                buf.extend_from_slice(&(arr.len() as u32).to_be_bytes());

                let mut offset_index = buf.len();
                let is_dynamic_len = primitive_type.is_dynamic_size();
                if is_dynamic_len {
                    buf.resize(offset_index + arr.len()*4 + 4, 0);

                    // Write offset for first element
                    let el_offset = (buf.len() - byte_start) as u32;
                    buf[offset_index..offset_index + 4].copy_from_slice(&el_offset.to_be_bytes());
                    offset_index += 4;
                }

                for arr_item in arr.iter() {
                    encode_value(buf, field, primitive_type,  arr_item)?;

                    // Write offset for next element. (Also write sentinel offset)
                    if is_dynamic_len {
                        let el_offset = (buf.len() - byte_start) as u32;
                        buf[offset_index..offset_index + 4].copy_from_slice(&el_offset.to_be_bytes());
                        offset_index += 4;
                    }
                }
            }
            FieldType::ModelRef(_) => {
                if !value.is_object() {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "object" })
                }

                let Some(item_id) = value.get("id") else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "{ id: u64 }" })
                };

                if field.offset_pos != 0 {
                    write_header(buf, field)?;
                    // TODO: write all key from model, not only id
                    encode_value(buf, field, &PrimitiveFieldType::UInt64,  item_id)?;
                }
            }
            FieldType::ModelRefList(model_index) => {
                let Some(value) = value.as_array() else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array<{ id: u64 }>" })
                };

                let ref_model = &schema.models[*model_index];

                let mut ids = Vec::with_capacity(value.len());
                for obj in value.iter() {
                    let id = encode_id(ref_model, obj, false)?;
                    ids.push(id);
                }
                structs.push(InsertStruct::Connect { field, ref_model: *model_index, ids });
            }
            FieldType::Struct(st) => {
                let (data, changed_values) = encode_document(schema, st, value, structs)?;
                structs.push(InsertStruct::One { st, changed_mask: changed_values, data });
            }
            FieldType::StructList(st) => {
                let Some(value) = value.as_array() else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array" })
                };
                if value.len() == 0 {
                    structs.push(InsertStruct::Empty { st });
                } else {
                    let mut vec_many = Vec::with_capacity(value.len());
                    for item in value {
                        let (data, _) = encode_document(schema, st, item, structs)?;
                        // TODO: get base_key_size from model struct
                        // let base_key = [0u8; 8];

                        // TODO: skip counter only in insert
                        let key = encode_id(st, item, true)?;
                        vec_many.push((key, data));
                    }
                    structs.push(InsertStruct::Many { st, data: vec_many });
                }
            }
            FieldType::Enum(en) => {
                if field.offset_pos == 0 {
                    println!("Warn: try to write to enum {} has not offset_pos", field.full_name);
                    continue;
                }
                let Some(variant_index) = value.as_str().and_then(|f| en.variants_map.get(f)) else {
                    return Err(EncodeError::TypeMismatchEnum { 
                        field: field.name.clone(), 
                        expected: format!("One of: [{}]", en.variants_str())
                    })
                };
                write_header(buf, field)?;

                let current_variant = &en.variants[*variant_index as usize];
                if !current_variant.fields.is_empty() {
                    let mut new_buf = vec![0u8; current_variant.payload_offset];
                    new_buf[0..2].copy_from_slice(&variant_index.to_be_bytes());
                    new_buf[2..4].copy_from_slice(&(current_variant.payload_offset as u16).to_be_bytes());

                    let mut enum_mask = write_fields(obj, &mut new_buf, &current_variant, schema, structs)?;

                    buf.append(&mut new_buf);

                    changed_mask.append(&mut enum_mask);
                } else {
                    buf.extend_from_slice(&variant_index.to_be_bytes());
                }
            }
            _ => {

            }
        }
    }

    // Sentnel offset нам здесь не нужен, так как у нас фиксированное количество полей
    // let len = buf.len() as u32;
    // buf[entity.payload_offset-4..entity.payload_offset].copy_from_slice(&len.to_be_bytes());

    return Ok(changed_mask)
}

/// Записывает в offset текущий курсор на buf
fn write_header(dst: &mut [u8], field: &Field) -> Result<(), EncodeError> {
    if field.offset_pos == 0 {
        return Err(EncodeError::TryWriteToVirtualField);
    }
    
    let start = dst.len() as u32;
    dst[field.offset_pos..field.offset_pos + 4].copy_from_slice(&start.to_be_bytes());
    Ok(())
}

/// Кодирует одно значение и дописывает в конец `dst`
fn encode_value(
    dst: &mut Vec<u8>,
    field: &Field,
    ty: &PrimitiveFieldType,
    v: &Value,
) -> Result<(), EncodeError> {
    match ty {
        PrimitiveFieldType::String => {
            let s = v
                .as_str()
                .ok_or_else(|| EncodeError::TypeMismatch {
                    field: field.name.clone(),
                    expected: "string",
                })?;
            let bytes = s.as_bytes();
            dst.extend_from_slice(bytes);
        }
        PrimitiveFieldType::DateTime => {
          let epoch: i64 = match v {
              // Путь 1: число — уже epoch
              Value::Number(num) => num
                  .as_i64()
                  .ok_or_else(|| EncodeError::TypeMismatch {
                      field: field.name.clone(),
                      expected: "int64 (epoch) or string (ISO-8601)",
                  })?,

              // Путь 2: ISO-строка → парсим
              Value::String(s) => {
                  use chrono::{DateTime, Utc};

                  let dt: DateTime<Utc> = s
                      .parse()
                      .map_err(|_| EncodeError::TypeMismatch {
                          field: field.name.clone(),
                          expected: "valid ISO-8601 datetime string",
                      })?;

                  dt.timestamp_millis()
              }

              _ => {
                  return Err(EncodeError::TypeMismatch {
                      field: field.name.clone(),
                      expected: "int64 (epoch) or ISO-8601 string",
                  });
              }
          };

          // Записываем epoch как i64 (8 байт)
          dst.extend_from_slice(&epoch.to_be_bytes());
        }
        PrimitiveFieldType::Int64 => {
            let n = match v {
                Value::Number(num) => num
                    .as_i64()
                    .ok_or_else(|| EncodeError::TypeMismatch {
                        field: field.name.clone(),
                        expected: "int64",
                    })?,
                _ => {
                    return Err(EncodeError::TypeMismatch {
                        field: field.name.clone(),
                        expected: "int64",
                    })
                }
            };
            dst.extend_from_slice(&n.to_be_bytes());
        }
        PrimitiveFieldType::UInt64 => {
            let n = match v {
                Value::Number(num) => num
                    .as_u64()
                    .ok_or_else(|| EncodeError::TypeMismatch {
                        field: field.name.clone(),
                        expected: "uint64",
                    })?,
                _ => {
                    return Err(EncodeError::TypeMismatch {
                        field: field.name.clone(),
                        expected: "uint64",
                    })
                }
            };
            dst.extend_from_slice(&n.to_be_bytes());
        }
        PrimitiveFieldType::Float => {
            let n = match v {
                Value::Number(num) => num
                    .as_f64()
                    .map(|f| f as f32)
                    .ok_or_else(|| EncodeError::TypeMismatch {
                        field: field.name.clone(),
                        expected: "float",
                    })?,
                _ => {
                    return Err(EncodeError::TypeMismatch {
                        field: field.name.clone(),
                        expected: "float",
                    })
                }
            };
            dst.extend_from_slice(&n.to_be_bytes());
        }
        PrimitiveFieldType::Double => {
            let n = match v {
                Value::Number(num) => num
                    .as_f64()
                    .ok_or_else(|| EncodeError::TypeMismatch {
                        field: field.name.clone(),
                        expected: "double",
                    })?,
                _ => {
                    return Err(EncodeError::TypeMismatch {
                        field: field.name.clone(),
                        expected: "double",
                    })
                }
            };
            dst.extend_from_slice(&n.to_be_bytes());
        }
        PrimitiveFieldType::Bool => {
            let b = v
                .as_bool()
                .ok_or_else(|| EncodeError::TypeMismatch {
                    field: field.name.clone(),
                    expected: "bool",
                })?;
            dst.push(if b { 1 } else { 0 });
        }
    }

    Ok(())
}

pub fn encode_id(model: &Entity, obj: &Value, skip_counters: bool) -> Result<Vec<u8>, EncodeError> {
    let mut key_buf = Vec::with_capacity(model.key_min_size());

    encode_id_internal(&mut key_buf, model, obj, skip_counters)?;

    return Ok(key_buf);
}

// pub fn encode_id_with_prefix(model: &Entity, obj: &Value, prefix: &[u8], skip_counters: bool) -> Result<Vec<u8>, EncodeError> {
//     let mut key_buf = Vec::with_capacity(prefix.len() + model.key_min_size());

//     key_buf.extend_from_slice(prefix);
//     encode_id_internal(&mut key_buf, model, obj, skip_counters)?;

//     return Ok(key_buf);
// }

fn encode_id_internal(key_buf: &mut Vec<u8>, model: &Entity, obj: &Value, skip_counters: bool) -> Result<(), EncodeError> {

    for field in model.fields.iter() {
        if field.id_idx.is_none() { continue; }
        if field.counter_idx.is_some() && skip_counters {
            key_buf.extend_from_slice(&[ 0u8;8 ]);
            continue;
        }
        let Some(value): Option<&Value> = obj.get(&field.name) else {
            if field.name.starts_with("@") {
                key_buf.extend_from_slice(&[ 0u8;8 ]);
                continue;
            }
            return Err(EncodeError::MissingIdField(field.name.clone()))
        };
        match field.ty {
            FieldType::Primitive(primitive_type) => {
                encode_value(key_buf, field, &primitive_type, value)?;
            }
            FieldType::ModelRef(_) => {
                if !value.is_object() {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "object" })
                }
                let Some(item_id) = value.get("id") else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "{ id: u64 }" })
                };
                // TODO: write all key from model, not only id
                encode_value(key_buf, field, &PrimitiveFieldType::UInt64, item_id)?;
            }
            _ => {
                return Err(EncodeError::UnavailableKeyField)
            }
        }
    }

    return Ok(());
}

#[cfg(test)]
mod tests {
    use crate::{marci_db::{InsertStruct, get_end, get_offsets}, marci_encoder::{encode_document, encode_id}, schema::{FieldType, parse_schema}};
    use serde_json::json;

    #[test]
    fn test_encode_simple_document() {

        let schema_str = "
            model User {
                name        String
                age         UInt
                profile     Profile
            }
            model Profile {
                
            }
        ";
        let schema=  parse_schema(schema_str);
        let model = &schema.models[0];

        let input = json!({
            "name": "Alice",
            "age": 30,
            "profile": { "id": 1 }
        });

        let mut structs = vec![];
        let (encoded, _) = encode_document(&schema, model, &input, &mut structs).unwrap();
        
        // Проверяем версию
        assert_eq!(encoded[0], 1);

        // Читаем field_count
        let field_count = u16::from_be_bytes(encoded[1..3].try_into().unwrap());
        assert_eq!(field_count, model.payload_offset as u16);

        // Читаем смещения
        let offset_name = u32::from_be_bytes(encoded[3..7].try_into().unwrap()) as usize;
        let offset_age  = u32::from_be_bytes(encoded[7..11].try_into().unwrap()) as usize;
        let _offset_profile  = u32::from_be_bytes(encoded[11..15].try_into().unwrap()) as usize;

        assert_eq!(offset_name, model.payload_offset);

        // Проверяем, что смещения действительно указывают на данные
        // name: [len=5][bytes]
        let name_end = get_end(&encoded, 3, model.payload_offset);

        let name_value = &encoded[offset_name .. name_end];
        assert_eq!(name_value, b"Alice");
        
        // age: i64
        let age_bytes = &encoded[offset_age .. offset_age + 8];
        let age_value = i64::from_be_bytes(age_bytes.try_into().unwrap());
        assert_eq!(age_value, 30);
        
        let key_field = encode_id(model, &input, true).unwrap();
        assert_eq!(key_field, vec![0u8;8])
    }

    #[test]
    fn test_encode_struct_data() {
        let schema_str = "
            model User {
                name        String
            }

            model Project {
                name        String
                users       UserRole[]
            }

            struct UserRole {
                user        User          @id
                role        String
            }
        ";

        let schema=  parse_schema(schema_str);
        let model = &schema.models[1];

        let input = json!({
            "name": "First project",
            "users": [{ 
                "user": { "id": 1 },
                "role": "creator"
            }]
        });

        let mut structs = vec![];
        let (encoded, _) = encode_document(&schema, model, &input, &mut structs).unwrap();
        
        // Проверяем версию
        assert_eq!(encoded[0], 1);

        assert_eq!(get_offsets(&encoded, model), vec![ model.payload_offset ]); // 3 bytes + 4 byte offset
        assert_eq!(structs.len(), 1);

        let InsertStruct::Many { st, data } = &structs[0] else {
            panic!("Expected InsertStruct::Many, found {:?}", structs[0]);
        };

        assert_eq!(data.len(), 1);
        // В InsertStruct::Many в ключе первые 8 байт - ID родителя, остальное - ID структуры
        assert_eq!(&data[0].0, &[0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(get_offsets(&data[0].1, *st), vec![ model.payload_offset ]);

        // assert_eq!()

    }   

    #[test]
    fn test_encode_enum_data() {
        let schema_str = "
            model User {
                name        String
            }

            model Project {
                name        String
                users       UserRole[]
            }

            enum RoleKind {
                viewer
                admin {
                    features String[]
                }
            }

            struct UserRole {
                user        User          @id
                role        RoleKind
            }
        ";

        let schema = parse_schema(schema_str);
        let project_model = &schema.models[1];
        let FieldType::StructList(user_role_st) = &project_model.fields[2].ty else {
            panic!("Project field type is not a struct list");
        };

        let FieldType::Enum(role_field) = &user_role_st.fields[2].ty else {
            panic!("ProjectRole field type is not a enum");
        };
        assert_eq!(role_field.variants.len(), 2);
        
        let input = json!({
            "name": "First project",
            "users": [{ 
                "user": { "id": 1 },
                "role": "admin",
                "features": [ "root", "tester" ]
            }]
        });

        let mut structs = vec![];
        let _ = encode_document(&schema, project_model, &input, &mut structs).unwrap();

        assert_eq!(structs.len(), 1);
        let InsertStruct::Many { st, data: inserted_roles } = &structs[0] else {
            panic!("Inserted data is not a InsertStruct::Many");
        };
        
        assert_eq!(inserted_roles.len(), 1);

        let body = &inserted_roles[0].1[st.payload_offset..];
        let enum_variant = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
        assert_eq!(enum_variant, 1);

        let enum_variant_body = &body[role_field.variants[enum_variant].payload_offset..];
        let arr_size = u32::from_be_bytes(enum_variant_body[0..4].try_into().unwrap()) as usize;
        assert_eq!(arr_size, 2);

        let first_item_offset = u32::from_be_bytes(enum_variant_body[4..8].try_into().unwrap()) as usize;
        let second_item_offset = u32::from_be_bytes(enum_variant_body[8..12].try_into().unwrap()) as usize;

        let first_item = str::from_utf8(&enum_variant_body[first_item_offset..second_item_offset]).unwrap();
        assert_eq!(first_item, "root");

        let second_item = str::from_utf8(&enum_variant_body[second_item_offset..]).unwrap();
        assert_eq!(second_item, "tester");            
    }


}

