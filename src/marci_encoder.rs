use std::borrow::Borrow;

use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::InsertStruct, schema::{FieldType, InsertedIndex, Model, PrimitiveFieldType, WithFields}};

#[derive(Debug)]
pub enum EncodeError {
    NotAnObject,
    MissingField(String),
    TypeMismatch { field: String, expected: &'static str },
    OffsetOverflow,
    EmptyObject
}

static EMPTY_ARRAY: Value = Value::Array(vec![]);

/// Кодируем JSON-документ для заданной модели в бинарный формат
pub fn encode_document<'a, T>(model: &'a T, json: &Value, structs: &mut Vec<InsertStruct<'a>>) -> Result<(Vec<u8>, BitVec), EncodeError> where T: WithFields {
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

    let max_offset_index = model.fields().iter().map(|a| a.offset_index).max().unwrap();
    let mut changed_mask = bitvec![0; max_offset_index+1];

    // Тело
    for field in model.fields() {
        let value_opt: Option<&Value> = obj.get(&field.name);
        let Some(value) = value_opt else {
            // TODO: set default value here. Now it setting null (offset = 0)
            continue;
        };

        if value.is_null() {
            match field.ty {
                FieldType::Struct(ref st) => {
                    structs.push(InsertStruct::None { st: &st });
                },
                FieldType::StructList(_, _) => {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array" })
                },
                FieldType::ModelRefList(_) => {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array<{ id: u64 }>" })
                },
                _ => {
                    changed_mask.set(field.offset_index, true);
                }
            }
            continue;
        }

        match field.ty {
            FieldType::Primitive(primitive_type) => {
                changed_mask.set(field.offset_index, true);

                // Смещение начала данных этого поля
                let start = buf.len() as u32;
                buf[field.offset_pos..field.offset_pos + 4].copy_from_slice(&start.to_be_bytes());

                // Кодируем само значение
                encode_value(&mut buf, &primitive_type, &field.name, value)?;
            }
            FieldType::ModelRef(_) => {
                changed_mask.set(field.offset_index, true);

                if !value.is_object() {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "object" })
                }

                let Some(item_id) = value.get("id") else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "{ id: u64 }" })
                };

                let start = buf.len() as u32;
                buf[field.offset_pos..field.offset_pos + 4].copy_from_slice(&start.to_be_bytes());

                encode_value(&mut buf, &PrimitiveFieldType::UInt64, &field.name, item_id)?;
            }
            FieldType::ModelRefList(model_index) => {
                let Some(value) = value.as_array() else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array<{ id: u64 }>" })
                };

                let ids: Vec<u64> = value
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        item.get("id").and_then(|i| i.as_u64()).ok_or_else(|| EncodeError::TypeMismatch {
                            field: format!("{}[{}]", field.name, index),
                            expected: "{ id: u64 }"
                        })
                    })
                    .collect::<Result<_, _>>()?; // <---- вот здесь вся магия

                structs.push(InsertStruct::Connect { field, ref_model: model_index, ids: ids.clone() });
            }
            FieldType::Struct(ref st) => {
                let (data, changed_values) = encode_document(st, value, structs)?;
                structs.push(InsertStruct::One { st, changed_mask: changed_values, data });
            }
            FieldType::StructList(ref st, counter_idx) => {
                let Some(value) = value.as_array() else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array" })
                };
                if value.len() == 0 {
                    structs.push(InsertStruct::Empty { st });
                } else {
                    let mut vec_many = Vec::with_capacity(value.len());
                    for item in value {
                        if let Some(id) = item.get("id").and_then(|a|a.as_u64()) {
                            let (data, _) = encode_document(st, item, structs)?;
                            vec_many.push((Some(id), data));
                        } else {
                            let (data, _) = encode_document(st, item, structs)?;
                            vec_many.push((None, data));
                        }
                    }
                    structs.push(InsertStruct::Many { st, data: vec_many, counter_idx });
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
fn encode_list<T>(
    dst: &mut Vec<u8>,
    ty: &PrimitiveFieldType,
    field_name: &str,
    v: &[T],
)  -> Result<(), EncodeError> where T: Borrow<Value> {
    dst.extend_from_slice(&(v.len() as u32).to_be_bytes());
    for (index, val) in v.iter().enumerate() {
        // TODO: remove format! from this
        encode_value(dst, ty, &format!("{}[{}]", field_name, index), val.borrow())?;
    }
    Ok(())
}

/// Кодирует одно значение и дописывает в конец `dst`
fn encode_value(
    dst: &mut Vec<u8>,
    ty: &PrimitiveFieldType,
    field_name: &str,
    v: &Value,
) -> Result<(), EncodeError> {

    match ty {
        PrimitiveFieldType::String => {
            let s = v
                .as_str()
                .ok_or_else(|| EncodeError::TypeMismatch {
                    field: field_name.to_string(),
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
                      field: field_name.to_string(),
                      expected: "int64 (epoch) or string (ISO-8601)",
                  })?,

              // Путь 2: ISO-строка → парсим
              Value::String(s) => {
                  use chrono::{DateTime, Utc};

                  let dt: DateTime<Utc> = s
                      .parse()
                      .map_err(|_| EncodeError::TypeMismatch {
                          field: field_name.to_string(),
                          expected: "valid ISO-8601 datetime string",
                      })?;

                  dt.timestamp_millis()
              }

              _ => {
                  return Err(EncodeError::TypeMismatch {
                      field: field_name.to_string(),
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
                        field: field_name.to_string(),
                        expected: "int64",
                    })?,
                _ => {
                    return Err(EncodeError::TypeMismatch {
                        field: field_name.to_string(),
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
                        field: field_name.to_string(),
                        expected: "uint64",
                    })?,
                _ => {
                    return Err(EncodeError::TypeMismatch {
                        field: field_name.to_string(),
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
                        field: field_name.to_string(),
                        expected: "float",
                    })?,
                _ => {
                    return Err(EncodeError::TypeMismatch {
                        field: field_name.to_string(),
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
                        field: field_name.to_string(),
                        expected: "double",
                    })?,
                _ => {
                    return Err(EncodeError::TypeMismatch {
                        field: field_name.to_string(),
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
                    field: field_name.to_string(),
                    expected: "bool",
                })?;
            dst.push(if b { 1 } else { 0 });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{marci_db::get_end, marci_encoder::encode_document, schema::{FieldType, Model, PrimitiveFieldType}};
    use serde_json::json;

    #[test]
    fn test_encode_simple_document() {
        // Модель: два поля: name: String, age: Int64
        let model = Model {
            name: "User".to_string(),
            counter_idx: 0,
            fields: vec![
                crate::schema::Field {
                    name: "name".to_string(),
                    ty: FieldType::Primitive(PrimitiveFieldType::String),
                    offset_index: 0,
                    offset_pos: 3,
                    derived_from: None,
                    is_nullable: false,
                    inserted_indexes: vec![], select_index: None,
                    attributes: vec![]
                },
                crate::schema::Field {
                    name: "age".to_string(),
                    ty: FieldType::Primitive(PrimitiveFieldType::Int64),
                    offset_index: 1,
                    offset_pos: 3 + 1 * 4,
                    derived_from: None,
                    is_nullable: false,
                    inserted_indexes: vec![], select_index: None,
                    attributes: vec![]
                },
                crate::schema::Field {
                    name: "profile".to_string(),
                    ty: FieldType::ModelRef(1),
                    offset_index: 2,
                    offset_pos: 3 + 2 * 4,
                    derived_from: None,
                    is_nullable: false,
                    inserted_indexes: vec![], select_index: None,
                    attributes: vec![]
                },
            ],
            payload_offset: 3 + 3 * 4
        };

        let input = json!({
            "name": "Alice",
            "age": 30,
            "profile": { "id": 1 }
        });

        let mut structs = vec![];
        let (encoded, _) = encode_document(&model, &input, &mut structs).expect("encode ok");

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
        println!("{} {}", name_end, model.payload_offset);

        let name_value = &encoded[offset_name .. name_end];
        assert_eq!(name_value, b"Alice");

        // age: i64
        let age_bytes = &encoded[offset_age .. offset_age + 8];
        let age_value = i64::from_be_bytes(age_bytes.try_into().unwrap());
        assert_eq!(age_value, 30);
    }
}

