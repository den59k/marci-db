use std::borrow::Borrow;

use serde_json::Value;
use bitvec::prelude::*;

use crate::schema::{FieldType, Model, PrimitiveFieldType};

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
pub fn encode_document(model: &Model, json: &Value) -> Result<(Vec<u8>, BitVec), EncodeError> {
    let obj = json
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    const VERSION: u8 = 1;

    // [version: u8] + [field_count: u16] + [offsets: N * u32]
    let mut buf = Vec::with_capacity(model.payload_offset + 128);

    // version
    buf.push(VERSION);
    // field_count
    buf.extend_from_slice(&model.fields_size.to_be_bytes());
    // offsets (плейсхолдеры)
    buf.resize(model.payload_offset, 0);

    let initial_size = buf.len();

    let mut changed_mask = bitvec![0; 200];

    // Тело
    for field in &model.fields {

        if field.derived_from.is_some() {
            continue;
        }

        let value_opt: Option<&Value> = obj.get(&field.name);
        let Some(value) = value_opt.or_else(|| {
            matches!(field.ty, FieldType::ModelRefList(_)).then(|| &EMPTY_ARRAY)
        }) else {
            // TODO: set default value here. Now it setting null (offset = 0)
            continue;
        };

        if value.is_null() {
            changed_mask.set(field.offset_index, true);
            continue;
        }

        match field.ty {
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
            FieldType::Primitive(primitive_type) => {
                changed_mask.set(field.offset_index, true);

                // Смещение начала данных этого поля
                let start = buf.len() as u32;
                buf[field.offset_pos..field.offset_pos + 4].copy_from_slice(&start.to_be_bytes());

                // Кодируем само значение
                encode_value(&mut buf, &primitive_type, &field.name, value)?;
            }
            FieldType::ModelRefList(_) => {
                let start = buf.len() as u32;
                buf[field.offset_pos..field.offset_pos + 4].copy_from_slice(&start.to_be_bytes());

                let Some(value) = value.as_array() else {
                    return Err(EncodeError::TypeMismatch { field: field.name.clone(), expected: "Array" })
                };

                let ids: Vec<&Value> = value
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        item.get("id").ok_or_else(|| EncodeError::TypeMismatch {
                            field: format!("{}[{}]", field.name, index),
                            expected: "{ id: u64 }"
                        })
                    })
                    .collect::<Result<_, _>>()?; // <---- вот здесь вся магия

                encode_list(&mut buf, &PrimitiveFieldType::UInt64, &field.name, &ids)?;
            }
            _ => {

            }
        }
    }

    if buf.len() == initial_size {
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
            let len = bytes.len();
            if len > u32::MAX as usize {
                // на практике вряд ли, но проверка не помешает
                return Err(EncodeError::OffsetOverflow);
            }
            dst.extend_from_slice(&(len as u32).to_be_bytes());
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

mod tests {
  #[cfg(test)]
mod tests {
    use crate::{marci_encoder::encode_document, schema::{FieldType, Model, PrimitiveFieldType}};
    use serde_json::json;

    #[test]
    fn test_encode_simple_document() {
        // Модель: два поля: name: String, age: Int64
        let model = Model {
            name: "User".to_string(),
            fields: vec![
                crate::schema::Field {
                    name: "name".to_string(),
                    ty: FieldType::Primitive(PrimitiveFieldType::String),
                    offset_index: 0,
                    offset_pos: 3,
                    derived_from: None,
                    is_nullable: false,
                    index_name: None,
                    attributes: vec![],
                    ext_indexes: vec![]
                },
                crate::schema::Field {
                    name: "age".to_string(),
                    ty: FieldType::Primitive(PrimitiveFieldType::Int64),
                    offset_index: 1,
                    offset_pos: 3 + 1 * 4,
                    derived_from: None,
                    is_nullable: false,
                    index_name: None,
                    attributes: vec![],
                    ext_indexes: vec![]
                },
                crate::schema::Field {
                    name: "profile".to_string(),
                    ty: FieldType::ModelRef(1),
                    offset_index: 2,
                    offset_pos: 3 + 2 * 4,
                    derived_from: None,
                    is_nullable: false,
                    index_name: None,
                    attributes: vec![],
                    ext_indexes: vec![]
                },
            ],
            payload_offset: 3 + 3 * 4,
            fields_size: 3,
        };

        let input = json!({
            "name": "Alice",
            "age": 30,
            "profile": { "id": 1 }
        });

        let (encoded, _) = encode_document(&model, &input).expect("encode ok");

        // Проверяем версию
        assert_eq!(encoded[0], 1);

        // Читаем field_count
        let field_count = u16::from_be_bytes(encoded[1..3].try_into().unwrap());
        assert_eq!(field_count, 3);

        // Читаем смещения
        let offset_name = u32::from_be_bytes(encoded[3..7].try_into().unwrap()) as usize;
        let offset_age  = u32::from_be_bytes(encoded[7..11].try_into().unwrap()) as usize;
        let _offset_profile  = u32::from_be_bytes(encoded[11..15].try_into().unwrap()) as usize;

        assert_eq!(offset_name, 15);

        // Проверяем, что смещения действительно указывают на данные
        // name: [len=5][bytes]
        let name_len = u32::from_be_bytes([
            encoded[offset_name],
            encoded[offset_name + 1],
            encoded[offset_name + 2],
            encoded[offset_name + 3],
        ]) as usize;
        assert_eq!(name_len, 5);

        let name_value = &encoded[offset_name + 4 .. offset_name + 4 + name_len];
        assert_eq!(name_value, b"Alice");

        // age: i64
        let age_bytes = &encoded[offset_age .. offset_age + 8];
        let age_value = i64::from_be_bytes(age_bytes.try_into().unwrap());
        assert_eq!(age_value, 30);
    }
}

}