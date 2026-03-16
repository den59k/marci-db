use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{Field, schema::{EnumInfo, PrimitiveFieldType}};

/// Кодирует одно значение и дописывает в конец `dst`
pub fn encode_primitive_value(dst: &mut Vec<u8>, field: &Field, ty: &PrimitiveFieldType, v: &Value) -> Result<(), EncodeError> {
    match ty {
        PrimitiveFieldType::String => {
            let s = v.as_str().ok_or_else(|| EncodeError::type_mismatch(field, "string"))?;
            dst.extend_from_slice(s.as_bytes());
        }

        PrimitiveFieldType::DateTime => {
            dst.extend_from_slice(&as_datetime(v, field)?.to_be_bytes());
        }

        PrimitiveFieldType::Int64 => {
            dst.extend_from_slice(&as_i64(v, field)?.to_be_bytes());
        }

        PrimitiveFieldType::UInt64 => {
            dst.extend_from_slice(&as_u64(v, field)?.to_be_bytes());
        }

        PrimitiveFieldType::Float => {
            dst.extend_from_slice(&as_f32(v, field)?.to_be_bytes());
        }

        PrimitiveFieldType::Double => {
            dst.extend_from_slice(&as_f64(v, field)?.to_be_bytes());
        }

        PrimitiveFieldType::Bool => {
            let b = v.as_bool().ok_or_else(|| EncodeError::type_mismatch(field, "bool"))?;
            dst.push(if b { 1 } else { 0 });
        }
    }
    Ok(())
}

// Кодирует enum в binary
pub fn encode_enum(dst: &mut Vec<u8>, field: &Field, enum_def: &EnumInfo, v: &Value) -> Result<(), EncodeError> {
    let Some(s) = v.as_str() else {
        return Err(EncodeError::type_mismatch(field, "string"));
    };
    let Some(val) = enum_def.variants_map.get(s) else {
        return Err(EncodeError::type_mismatch(field, enum_def.keys_to_string() ));
    };

    dst.extend_from_slice(&val.to_be_bytes());
    Ok(())
}

// Кодирует PrimitiveList и PrimitiveFixedList в binary
pub fn encode_list(buf: &mut Vec<u8>, value: &Value, field: &Field, primitive_type: &PrimitiveFieldType, fixed_size: Option<usize>) -> Result<(), EncodeError> {
   let Some(arr) = value.as_array() else {
      return Err(EncodeError::type_mismatch(field, "Array"))
   };

   let byte_start = buf.len();
   if let Some(fixed_size) = fixed_size {
      if arr.len() != fixed_size {
         return Err(EncodeError::type_mismatch(field, format!("{}[{}]", primitive_type.to_string(), fixed_size)))
      }
   } else {
      buf.extend_from_slice(&(arr.len() as u32).to_be_bytes());
   }
   
   match primitive_type.get_size() {
      None => encode_list_dynamic(buf, arr, field, primitive_type, byte_start)?,
      Some(_) => encode_list_static(buf, arr, field, primitive_type)?
   };

   Ok(())
}

// Записывает в массив значения с переменной длиной (т.е. строки)
// [item_0_end,item_1_end..item_n_end][item_0,item_1..item_n]
pub fn encode_list_dynamic(buf: &mut Vec<u8>, arr: &[Value], field: &Field, primitive_type: &PrimitiveFieldType, byte_start: usize) -> Result<(), EncodeError> {

    let mut offset_index = buf.len();

    buf.resize(offset_index + arr.len()*4 + 4, 0);

    let el_offset = (buf.len() - byte_start) as u32;
    buf[offset_index..offset_index + 4].copy_from_slice(&el_offset.to_be_bytes());
    offset_index += 4;

    for arr_item in arr.iter() {
        encode_primitive_value(buf, field, primitive_type,  arr_item)?;

        let el_offset = (buf.len() - byte_start) as u32;
        buf[offset_index..offset_index + 4].copy_from_slice(&el_offset.to_be_bytes());
        offset_index += 4;
    }

    return Ok(());
}

// Записывает в массив значения с фиксированной длиной
// [item_0,item_1..item_n]
pub fn encode_list_static(buf: &mut Vec<u8>, arr: &[Value], field: &Field, primitive_type: &PrimitiveFieldType) -> Result<(), EncodeError> {
    for arr_item in arr.iter() {
        encode_primitive_value(buf, field, primitive_type,  arr_item)?;
    }
    return Ok(());
}

pub fn as_i64(v: &Value, field: &Field) -> Result<i64, EncodeError> {
    v.as_i64().ok_or_else(|| EncodeError::type_mismatch(field, "i64"))
}

pub fn as_u64(v: &Value, field: &Field) -> Result<u64, EncodeError> {
    v.as_u64().ok_or_else(|| EncodeError::type_mismatch(field, "u64"))
}

pub fn as_f32(v: &Value, field: &Field) -> Result<f32, EncodeError> {
    v.as_f64()
        .map(|f| f as f32)
        .ok_or_else(|| EncodeError::type_mismatch(field, "float"))
}

pub fn as_f64(v: &Value, field: &Field) -> Result<f64, EncodeError> {
    v.as_f64().ok_or_else(|| EncodeError::type_mismatch(field, "double"))
}

pub fn as_datetime(v: &Value, field: &Field) -> Result<i64, EncodeError> {
    let epoch = match v {
        Value::Number(_) => as_i64(v, field)?,
        Value::String(s) => {
            let dt: DateTime<Utc> = s.parse().map_err(|_| {
                EncodeError::type_mismatch(field, "ISO-8601 datetime string or int64 epoch")
            })?;
            dt.timestamp_millis()
        }
        _ => return Err(EncodeError::type_mismatch(field, "ISO-8601 string or int64 epoch")),
    };
    Ok(epoch)
}

#[derive(Debug)]
pub enum EncodeError {
    NotAnObject,
    MissingIdField(String),
    IdFieldIsNull(String),
    TypeMismatch { field: String, expected: String },
    TryWriteToVirtualField,
    WrongEnumValue(String, String),
    UnavailableKeyField(String),
    UnavailableKeyFieldId(String),
    EmptyObject,
    DerivedFieldNotWritable(String)
}


impl EncodeError {
    pub fn type_mismatch(field: &Field, expected: impl Into<String>) -> Self {
        EncodeError::TypeMismatch {
            field: field.name.clone(),
            expected: expected.into(),
        }
    }
}
