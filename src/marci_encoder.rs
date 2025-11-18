
use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::InsertStruct, schema::{Field, FieldType, PrimitiveFieldType, Schema, WithFields}};

#[derive(Debug)]
pub enum EncodeError {
    NotAnObject,
    MissingIdField(String),
    TypeMismatch { field: String, expected: &'static str },
    TryWriteToVirtualField,
    UnavailableKeyField,
    EmptyObject
}

/// Кодируем JSON-документ для заданной модели в бинарный формат. Возвращает данные и changed_mask
/// Не все данные записываются в document, используйте также функцию encode_id для кодирования полей в ID
pub fn encode_document<'a, T>(schema: &'a Schema, model: &'a T, json: &Value, structs: &mut Vec<InsertStruct<'a>>) -> Result<(Vec<u8>,BitVec), EncodeError> where T: WithFields {
    let obj = json
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    const VERSION: u8 = 1;

    // [version: u8] + [field_count: u16] + [offsets: N * u32]
    let mut buf = Vec::with_capacity(model.payload_offset() + 128);

    // version
    buf.push(VERSION);
    // field_count
    buf.extend_from_slice(&(model.payload_offset() as u16).to_be_bytes());
    // offsets (плейсхолдеры)
    buf.resize(model.payload_offset(), 0);

    let initial_size = buf.len();

    let mut changed_mask = bitvec![0; model.fields().len()];

    // Тело
    for (field_index, field) in model.fields().iter().enumerate() {
        let Some(value) = obj.get(&field.name) else {
            // TODO: set default value here. Now it setting null (offset = 0)
            continue;
        };

        changed_mask.set(field_index, true);

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

        match field.ty {
            FieldType::Primitive(primitive_type) => {
                if field.offset_pos != 0 {
                    write_header(&mut buf, field)?;
                    encode_value(&mut buf, field, &primitive_type,  value)?;
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
                    write_header(&mut buf, field)?;
                    // TODO: write all key from model, not only id
                    encode_value(&mut buf, field, &PrimitiveFieldType::UInt64,  item_id)?;
                }
            }
            FieldType::ModelRefList(model_index) => {
                let Some(value) = value.as_array() else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array<{ id: u64 }>" })
                };

                let ref_model = &schema.models[model_index];

                let mut ids = Vec::with_capacity(value.len());
                for obj in value.iter() {
                    let id = encode_id(ref_model, obj, false)?;
                    ids.push(id);
                }
                structs.push(InsertStruct::Connect { field, ref_model: model_index, ids });
            }
            FieldType::Struct(ref st) => {
                let (data, changed_values) = encode_document(schema, st, value, structs)?;
                structs.push(InsertStruct::One { st, changed_mask: changed_values, data });
            }
            FieldType::StructList(ref st) => {
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
            _ => {

            }
        }
    }

    if buf.len() == initial_size && structs.len() == 0 {
        return Err(EncodeError::EmptyObject);
    }

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

/// Записывает в offset текущий курсор на buf
fn write_header(dst: &mut Vec<u8>, field: &Field) -> Result<(), EncodeError> {
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
            // let len = bytes.len();
            // if len > u32::MAX as usize {
            //     // на практике вряд ли, но проверка не помешает
            //     return Err(EncodeError::OffsetOverflow);
            // }
            // dst.extend_from_slice(&(len as u32).to_be_bytes());
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

pub fn encode_id<T>(model: &T, obj: &Value, skip_counters: bool) -> Result<Vec<u8>, EncodeError> where T : WithFields {
    let mut key_buf = Vec::with_capacity(model.key_min_size());

    encode_id_internal(&mut key_buf, model, obj, skip_counters)?;

    return Ok(key_buf);
}

pub fn encode_id_with_prefix<T>(model: &T, obj: &Value, prefix: &[u8], skip_counters: bool) -> Result<Vec<u8>, EncodeError> where T : WithFields {
    let mut key_buf = Vec::with_capacity(prefix.len() + model.key_min_size());

    key_buf.extend_from_slice(prefix);
    encode_id_internal(&mut key_buf, model, obj, skip_counters)?;

    return Ok(key_buf);
}

fn encode_id_internal<T>(key_buf: &mut Vec<u8>, model: &T, obj: &Value, skip_counters: bool) -> Result<(), EncodeError> where T : WithFields {

    for field in model.fields() {
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
    use crate::{marci_db::{InsertStruct, get_end, get_offsets}, marci_encoder::{encode_document, encode_id}, schema::parse_schema};
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

        assert_eq!(offset_name, 15);

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

        assert_eq!(get_offsets(&encoded, model), vec![7]); // 3 bytes + 4 byte offset
        assert_eq!(structs.len(), 1);

        let InsertStruct::Many { st, data } = &structs[0] else {
            panic!("Expected InsertStruct::Many, found {:?}", structs[0]);
        };

        assert_eq!(data.len(), 1);
        // В InsertStruct::Many в ключе первые 8 байт - ID родителя, остальное - ID структуры
        assert_eq!(&data[0].0, &[0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(get_offsets(&data[0].1, *st), vec![7]);

        // assert_eq!()

    }   

}

