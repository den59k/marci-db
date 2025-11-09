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

/// Кодируем JSON-документ для заданной модели в бинарный формат
pub fn encode_document(model: &Model, json: &Value) -> Result<(Vec<u8>, BitVec), EncodeError> {
    let obj = json
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    const VERSION: u8 = 1;

    // [version: u8] + [field_count: u16] + [offsets: N * u32]
    let header_size = 1 + 2 + (model.fields_size as usize) * 4;
    let mut buf = Vec::with_capacity(header_size + 128);

    // version
    buf.push(VERSION);
    // field_count
    buf.extend_from_slice(&model.fields_size.to_be_bytes());
    // offsets (плейсхолдеры)
    buf.resize(buf.len() + (model.fields_size as usize * 4), 0);

    let initial_size = buf.len();

    let mut changed_mask = bitvec![0; 200];

    // Тело
    for field in &model.fields {
      let FieldType::Primitive(primitive_type) = field.ty else {
        continue;
      };
      
      let value_opt = obj.get(&field.name);

      if let Some(value) = value_opt {
        changed_mask.set(field.offset_index, true);

        // Keep offset 0 
        if value.is_null() {
            continue;
        }

          // Смещение начала данных этого поля
          let start = buf.len();
          if start > u32::MAX as usize {
              return Err(EncodeError::OffsetOverflow);
          }
          let start_u32 = start as u32;

          let offset_pos = 3 + field.offset_index * 4; // позиция u32 в заголовке
          buf[offset_pos..offset_pos + 4].copy_from_slice(&start_u32.to_be_bytes());

          // Кодируем само значение
          encode_value(&mut buf, &primitive_type, &field.name, value)?;
      } else {
        // TODO: set default value here. Now it is setting null
      }
    }

    if buf.len() == initial_size {
        return Err(EncodeError::EmptyObject);
    }

    Ok((buf, changed_mask))
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

                  dt.timestamp()
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
                    attributes: vec![]
                },
                crate::schema::Field {
                    name: "age".to_string(),
                    ty: FieldType::Primitive(PrimitiveFieldType::Int64),
                    offset_index: 1,
                    attributes: vec![]
                },
            ],
            payload_offset: 3,
            fields_size: 2,
        };

        let input = json!({
            "name": "Alice",
            "age": 30
        });

        let (encoded, _) = encode_document(&model, &input).expect("encode ok");

        // Проверяем версию
        assert_eq!(encoded[0], 1);

        // Читаем field_count
        let field_count = u16::from_be_bytes([encoded[1], encoded[2]]);
        assert_eq!(field_count, 2);

        // Читаем смещения
        let offset_name = u32::from_be_bytes([encoded[3], encoded[4], encoded[5], encoded[6]]) as usize;
        let offset_age  = u32::from_be_bytes([encoded[7], encoded[8], encoded[9], encoded[10]]) as usize;

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