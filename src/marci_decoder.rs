use serde_json::{Map, Value};

use crate::{marci_db::{DecodeCtx, IncludeResult, get_end, get_offset}, schema::{FieldType, PrimitiveFieldType}};

#[derive(Debug)]
pub enum DecodeError {
    WrongVersion,
    BufferTooSmall,
    Utf8Error,
    TypeMismatch(String),
    OffsetOutOfRange,
}

pub fn decode_document(ctx: DecodeCtx<Value>) -> Result<Value, DecodeError>  {
    let DecodeCtx { data, fields, payload_offset, id, select, includes } = ctx;

    if data.len() < 3 {
        return Err(DecodeError::BufferTooSmall);
    }

    let version = data[0];
    if version != 1 {
        return Err(DecodeError::WrongVersion);
    }

    if u16::from_be_bytes([data[1], data[2]]) != payload_offset as u16 {
        let offset = u16::from_be_bytes([data[1], data[2]]);
        return Err(DecodeError::TypeMismatch(format!("payload offset mismatch; Expected: {}, Get {}", payload_offset, offset)));
    }

    if data.len() < payload_offset {
        return Err(DecodeError::BufferTooSmall);
    }

    let mut obj = Map::new();
    if select[0] {
        obj.insert("id".to_string(), Value::Number(id.into()));
    }

    for (field_index, field) in fields.iter().enumerate() {
        if !select[field_index+1] {
            continue;
        }

        let FieldType::Primitive(ref primitive) = field.ty else {
            // пропускаем derived / relation
            continue;
        };

        // читаем offset
        let offset = get_offset(data, field.offset_pos);

        // Поле = null
        if offset == 0 {
          obj.insert(field.name.clone(), Value::Null);
          continue;
        }

        let offset = offset as usize;
        if offset >= data.len() {
            return Err(DecodeError::OffsetOutOfRange);
        }

        // Декодируем
        let value = decode_value(primitive, &data, field.offset_pos, offset, payload_offset)?;
        obj.insert(field.name.clone(), value);
    }

    for include in includes {
        match include {
            IncludeResult::None(field_index) => {
                obj.insert(fields[field_index].name.clone(), Value::Null);
            },
            IncludeResult::One(field_index, val) => {
                obj.insert(fields[field_index].name.clone(), val);
            },
            IncludeResult::Many(field_index, val) => {
                let vec = Value::Array(val);
                obj.insert(fields[field_index].name.clone(), vec);
            }
        }
    }

    return Ok(Value::Object(obj));
}

#[inline(always)]
fn decode_value(ty: &PrimitiveFieldType, data: &[u8], offset_pos: usize, offset: usize, payload_offset: usize) -> Result<Value, DecodeError> {
    match ty {
        PrimitiveFieldType::String => {
            if data.len() < 4 {
                return Err(DecodeError::BufferTooSmall);
            }
            let end = get_end(data, offset_pos, payload_offset);
            let s = std::str::from_utf8(&data[offset..end]).map_err(|_| DecodeError::Utf8Error)?;
            Ok(Value::String(s.to_string()))
        }
        PrimitiveFieldType::DateTime => {
            if data.len() < 8 {
                return Err(DecodeError::BufferTooSmall);
            }
            let epoch = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            // Возвращаем как число (или можно форматировать обратно в ISO)
            Ok(Value::Number(epoch.into()))
        }
        PrimitiveFieldType::Int64 => {
            if data.len() < 8 {
                return Err(DecodeError::BufferTooSmall);
            }
            let n = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(Value::Number(n.into()))
        }
        PrimitiveFieldType::UInt64 => {
            if data.len() < 8 {
                return Err(DecodeError::BufferTooSmall);
            }
            let n = u64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(Value::Number(n.into()))
        }
        PrimitiveFieldType::Float => {
            if data.len() < 4 {
                return Err(DecodeError::BufferTooSmall);
            }
            let n = f32::from_be_bytes(data[offset..offset+4].try_into().unwrap());
            Ok(Value::Number(serde_json::Number::from_f64(n as f64).unwrap()))
        }
        PrimitiveFieldType::Double => {
            if data.len() < 8 {
                return Err(DecodeError::BufferTooSmall);
            }
            let n = f64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(Value::Number(serde_json::Number::from_f64(n).unwrap()))
        }
        PrimitiveFieldType::Bool => {
            if data.is_empty() {
                return Err(DecodeError::BufferTooSmall);
            }
            Ok(Value::Bool(data[offset] != 0))
        }
    }
}
