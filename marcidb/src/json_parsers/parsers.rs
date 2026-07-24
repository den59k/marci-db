use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{Field, num_utils::{NumberDelta, NumberValue}, schema::{Entity, EnumInfo, FieldLocation, FieldType, PrimitiveFieldType, Schema}};

/// Encodes a single value and appends it to the end of `dst`
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
        
        PrimitiveFieldType::Byte => {
            dst.extend_from_slice(&as_u8(v, field)?.to_be_bytes());
        }

        PrimitiveFieldType::Bool => {
            let b = v.as_bool().ok_or_else(|| EncodeError::type_mismatch(field, "bool"))?;
            dst.push(if b { 1 } else { 0 });
        }

        // Any JSON value encodes; a top-level `null` is handled upstream as field-absence (offset 0), not
        // stored as a JSON null — consistent with every other type.
        PrimitiveFieldType::Json => {
            crate::json_parsers::jsonb::encode_into(dst, v);
        }
    }
    Ok(())
}

// Encodes an enum into binary
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

fn insert_list_counter(current_len: usize, dst: &mut Vec<u8>, fixed_size: Option<usize>, field: &Field, ty: &PrimitiveFieldType) -> Result<(), EncodeError>  {
    if let Some(fixed_size) = fixed_size {
        if current_len != fixed_size {
            return Err(EncodeError::type_mismatch(field, format!("{}[{}]", ty.to_string(), fixed_size)))
        }
    } else {
        dst.extend_from_slice(&(current_len as u32).to_be_bytes());
    }
    Ok(())
}

// Encodes PrimitiveList and PrimitiveFixedList into binary
pub fn encode_list(dst: &mut Vec<u8>, value: &Value, field: &Field, primitive_type: &PrimitiveFieldType, fixed_size: Option<usize>) -> Result<(), EncodeError> {
    let Some(arr) = value.as_array() else {
        if let Some(str) = value.as_str() {
            return encode_list_str(dst, str, field, primitive_type, fixed_size)
        }
        return Err(EncodeError::type_mismatch(field, "Array"))
    };

    let byte_start = dst.len();
    insert_list_counter(arr.len(), dst, fixed_size, field, &primitive_type)?;

    match primitive_type.get_size() {
        None => encode_list_dynamic(dst, arr, field, primitive_type, byte_start)?,
        Some(_) => encode_list_static(dst, arr, field, primitive_type)?
    };

    Ok(())
}

fn encode_list_str(dst: &mut Vec<u8>, str: &str, field: &Field, primitive_type: &PrimitiveFieldType, fixed_size: Option<usize>) -> Result<(), EncodeError> {
    if matches!(primitive_type, PrimitiveFieldType::Byte) {
        let mut hex = parse_hex(str);
        // if let Some(fixed_size) = fixed_size && hex.len() < fixed_size {
        //     hex.resize(fixed_size, 0u8);
        // }
        insert_list_counter(hex.len(), dst, fixed_size, field, &primitive_type)?;
        dst.append(&mut hex);
        Ok(())
    } else {
        Err(EncodeError::type_mismatch(field, "Array"))
    }
}

// Writes variable-length values into the array (i.e. strings)
// [item_0_start,item_1_start,item_2_start..item_n_end][item_0,item_1..item_n]
pub fn encode_list_dynamic(buf: &mut Vec<u8>, arr: &[Value], field: &Field, primitive_type: &PrimitiveFieldType, byte_start: usize) -> Result<(), EncodeError> {

    let mut offset_index = buf.len();

    buf.resize(offset_index + arr.len()*4 + 4, 0);

    for arr_item in arr.iter() {
        let el_offset = (buf.len() - byte_start) as u32;
        buf[offset_index..offset_index + 4].copy_from_slice(&el_offset.to_be_bytes());

        encode_primitive_value(buf, field, primitive_type,  arr_item)?;
        offset_index += 4;
    }
    
    // Write sentinel offset
    let el_offset = (buf.len() - byte_start) as u32;
    buf[offset_index..offset_index + 4].copy_from_slice(&el_offset.to_be_bytes());

    return Ok(());
}

// Writes fixed-length values into the array
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

pub fn as_u8(v: &Value, field: &Field) -> Result<u8, EncodeError> {
    v.as_u64().map(|i| i as u8).ok_or_else(|| EncodeError::type_mismatch(field, "u8"))
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


pub fn encode_id_value(dst: &mut Vec<u8>, field: &Field, schema: &Schema, value: &Value) -> Result<(), EncodeError> {
    match &field.ty {
        FieldType::Primitive(primitive_type) => {
            encode_primitive_value(dst, field, &primitive_type, value)?;
        }
        FieldType::Ref (ref_info) => {
            let connect_id = parse_id(schema, &schema.models[ref_info.model_index], value)?;
            dst.extend(connect_id);
        }
        FieldType::PrimitiveList(ty, Some(fixed_size)) => {
            encode_list(dst, value, field, ty, Some(*fixed_size))?;
        },
        _ => {
            return Err(EncodeError::UnavailableKeyFieldId(field.full_name.clone()))
        }
    }
    Ok(())
}

// A method that encodes only the ID (and complains if any field is missing)
pub fn parse_id<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<Vec<u8>, EncodeError> {

    let Some(obj) = json_val.as_object() else {
        return Err(EncodeError::NotAnObject);
    };
    
    let mut id = vec![];
    for field in entity.fields.iter() {
        let FieldLocation::Key { index: _ } = field.location else {
            continue;
        };
        let Some(value) = obj.get(&field.name) else {
            return Err(EncodeError::MissingIdField(field.full_name.clone()));
        };
        if value.is_null() {
            return Err(EncodeError::IdFieldIsNull(field.full_name.clone()));
        }
        
        encode_id_value(&mut id, field, schema, value)?;
        if matches!(field.ty, FieldType::Primitive(ty) if ty.get_size().is_none()) {
            id.push(b'\0');
        }
    }

   Ok(id)
}

fn parse_hex(input: &str) -> Vec<u8> {
    let hex_chars: Vec<u8> = input
        .chars()
        .filter_map(|c| c.to_digit(16).map(|d| d as u8))
        .collect();

    hex_chars
        .chunks(2)
        .filter(|chunk| chunk.len() == 2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect()
}

pub fn parse_field_value_num<'a>(field: &'a Field, v: &Value) -> Result<NumberValue,EncodeError> {
  match &field.ty {
    FieldType::Primitive(primitive_field_type) => {
      match primitive_field_type {
        PrimitiveFieldType::DateTime => as_datetime(v, field)
          .map(|val| NumberValue::DateTime(val)),
        PrimitiveFieldType::Int64 => as_i64(v, field)
          .map(|val| NumberValue::Int64(val)),
        PrimitiveFieldType::UInt64 => as_u64(v, field)
          .map(|val| NumberValue::UInt64(val)),
        PrimitiveFieldType::Float => as_f32(v, field)
          .map(|val| NumberValue::Float(val)),
        PrimitiveFieldType::Double => as_f64(v, field)
          .map(|val| NumberValue::Double(val)),
        _ => Err(EncodeError::NotNumber(field.full_name.clone()))
      }
    },
    _ => Err(EncodeError::NotNumber(field.full_name.clone()))
  }
}

/// Parses the delta of an `$increment`. Unlike [`parse_field_value_num`] the delta is signed even on an
/// unsigned field, so a `UInt` column can be decremented; the result is range-checked when it is applied.
/// A `DateTime` delta is a plain number of milliseconds, not a date.
pub fn parse_field_value_delta<'a>(field: &'a Field, v: &Value) -> Result<NumberDelta,EncodeError> {
  match &field.ty {
    FieldType::Primitive(primitive_field_type) => {
      match primitive_field_type {
        PrimitiveFieldType::DateTime => as_i64(v, field).map(NumberDelta::DateTime),
        PrimitiveFieldType::Int64 => as_i64(v, field).map(NumberDelta::Int64),
        PrimitiveFieldType::UInt64 => as_i64(v, field).map(NumberDelta::UInt64),
        PrimitiveFieldType::Float => as_f32(v, field).map(NumberDelta::Float),
        PrimitiveFieldType::Double => as_f64(v, field).map(NumberDelta::Double),
        _ => Err(EncodeError::NotNumber(field.full_name.clone()))
      }
    },
    _ => Err(EncodeError::NotNumber(field.full_name.clone()))
  }
}

#[derive(Debug,PartialEq)]
pub enum EncodeError {
    NotAnObject,
    MissingIdField(String),
    IdFieldIsNull(String),
    TypeMismatch { field: String, expected: String },
    TryWriteToVirtualField,
    WrongEnumValue(String, String),
    UnavailableKeyField(String),
    FieldHasDynamicSize(String),
    UnavailableKeyFieldId(String),
    EmptyObject,
    VirtualFieldNotWritable(String),
    NotNumber(String),
    OnlyOneKeyExpected(String,String),
    UnsupportedOperation(String),
    NotAnArray,
    OnlyBodyKeyAvailableToEdit(String),
    RevFieldRequired(String),
    /// `$set`/`$ensure` (nested write) applied to a ref field stored in body (FK):
    /// nested record creation is supported only for owned/struct relations — use `$connect`
    NestedWriteNotSupported { field: String, op: String },
    /// A required field (non-nullable, without `@default`) is missing in the insert
    MissingRequiredField(String),
    /// `null` was passed to the field, but it is non-nullable
    NullNotAllowed(String),
    /// The relation is owned by a `@list` id array on the other side — membership (connect/remove/set)
    /// can only be changed through the `@list` field itself
    MutateViaListSide(String, String),
}


/// If the relation's partner field is a `@list` id array, returns its full name — membership of such a
/// relation can only be changed through the `@list` side (the tree-backed side stores nothing itself)
pub fn rev_id_list_field(schema: &Schema, ref_info: &crate::schema::RefInfo) -> Option<String> {
    let rev_idx = ref_info.rev_field_idx?;
    let rev_field = &schema.models[ref_info.model_index].fields[rev_idx];
    match &rev_field.ty {
        FieldType::Ref(ri) | FieldType::RefList(ri) if matches!(ri.binding, crate::schema::RefBinding::IdList { .. }) => {
            Some(rev_field.full_name.clone())
        }
        _ => None,
    }
}

impl EncodeError {
    pub fn type_mismatch(field: &Field, expected: impl Into<String>) -> Self {
        EncodeError::TypeMismatch {
            field: field.name.clone(),
            expected: expected.into(),
        }
    }
}
