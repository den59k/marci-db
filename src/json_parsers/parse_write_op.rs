use bitvec::{bitvec};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use crate::{Field, schema::{Entity, FieldDefault, FieldLocation, FieldType, PrimitiveFieldType, Schema}, write_op::{WriteDefault, WriteOp, WriteRelation}};

const VERSION: u8 = 1;

pub fn parse_insert<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<WriteOp<'a>, EncodeError> {
    let obj = json_val
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    return from_json_internal(schema, entity, obj, true, None);
}    

pub fn parse_update<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<WriteOp<'a>, EncodeError> {
    let obj = json_val
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    return from_json_internal(schema, entity, obj, false, None);
}    

fn from_json_internal<'a>(
    schema: &'a Schema, 
    entity: &'a Entity, 
    obj: &Map<String, Value>,
    is_create: bool,
    parent: Option<&Entity>
) -> Result<WriteOp<'a>, EncodeError> {
    let mut mask = bitvec![0; entity.fields.len()];
    let mut refs: Vec<WriteRelation<'_>> = vec![];
    let mut defaults: Vec<WriteDefault> = vec![];

    let mut data = Vec::with_capacity(entity.payload_offset + 128);
    let mut id = vec![];
    // version
    data.push(VERSION);
    // payload_offset
    data.extend_from_slice(&(entity.payload_offset as u16).to_be_bytes());
    // offsets (плейсхолдеры)
    data.resize(entity.payload_offset, 0);

    for (field_index, field) in entity.fields.iter().enumerate() {
        if is_create {
            if let Some(default_value) = &field.default_value {
                match field.location {
                    FieldLocation::Key { index: _ } => {
                        if let Some(offset) = parse_default_value(&mut id, field, default_value)? {
                            defaults.push(WriteDefault::Key(offset, default_value));
                        }
                    }
                    FieldLocation::Body { offset } => {
                        write_header(&mut data, offset);
                        if let Some(offset) = parse_default_value(&mut data, field, default_value)? {
                            defaults.push(WriteDefault::Body(offset, default_value));
                        }
                    }
                    _ => {}
                }
                continue;
            }
        }

        if let Some(parent) = parent && schema.is_parent_key(field, parent) {
            defaults.push(WriteDefault::ParentId(id.len()));
            continue;
        }

        let Some(value) = obj.get(&field.name) else {
            if matches!(field.location, FieldLocation::Key { .. }) {
                return Err(EncodeError::MissingIdField(field.full_name.clone()));
            }
            // TODO: set also default value here. Now it setting null (offset = 0)
            continue;
        };

        mask.set(field_index, true);

        if value.is_null() {
            // TODO: add check not-null here
            continue;
        }

        match field.location {
            FieldLocation::Key { index: _ } => {
                encode_id_value(&mut id, field, schema, value)?;
            },
            FieldLocation::Body { offset } => {
                match &field.ty {
                    FieldType::Primitive(primitive_type) => {
                        write_header(&mut data, offset);
                        encode_primitive_value(&mut data, field, &primitive_type, value)?;
                    }
                    FieldType::PrimitiveList(primitive_type) => {
                        write_header(&mut data, offset);
                        encode_list(&mut data, value, field, &primitive_type, None)?;
                    },
                    FieldType::PrimitiveFixedList(primitive_type, fixed_size) => {
                        write_header(&mut data, offset);
                        encode_list(&mut data, value, field, &primitive_type, Some(*fixed_size))?;
                    },
                    FieldType::Ref (ref_info) => {
                        let connect_id = encode_id(schema, &schema.models[ref_info.model_index], value)?;
                        write_header(&mut data, offset);
                        data.extend(connect_id);
                    },
                    _ => { 
                        return Err(EncodeError::UnavailableKeyField(field.full_name.clone()));
                    }
                }
            }
            FieldLocation::Virtual => {
                match &field.ty {
                FieldType::Ref (ref_info) => {
                    // Здесь может быть как структура, так и модель. Если структура, используем insert
                    let ref_entity = &schema.models[ref_info.model_index];
                    if ref_entity.autoinsert {
                        let Some(value) = value.as_object() else {
                            return Err(EncodeError::type_mismatch(field, "{ }"))
                        };
                        let op = from_json_internal(schema, ref_entity, value, true, Some(entity))?;
                        refs.push(WriteRelation::Create { field, op });
                    } else {
                        let id = encode_id(schema, ref_entity, value)?;
                        refs.push(WriteRelation::Connect { field, st: ref_entity, ids: vec![id] });
                    }
                },
                FieldType::RefList (ref_info) => {
                    let ref_entity = &schema.models[ref_info.model_index];
                    let Some(value) = value.as_array() else {
                        return Err(EncodeError::type_mismatch(field, "Array"))
                    };
                    if value.len() == 0 {
                        refs.push(WriteRelation::Empty { field, st: ref_entity });
                        continue;
                    }

                    if ref_entity.autoinsert {
                        let mut ops = Vec::with_capacity(value.len());
                        for item in value {
                            let Some(item) = item.as_object() else {
                            return Err(EncodeError::type_mismatch(field, "{ }"))
                            };
                            let op = from_json_internal(schema, ref_entity, item, true, Some(entity))?;
                            ops.push(op);
                        }
                        refs.push(WriteRelation::CreateMany { field, ops });
                    } else {
                        let mut ids = Vec::with_capacity(value.len());
                        for obj in value.iter() {
                            let id = encode_id(schema, ref_entity, obj)?;
                            ids.push(id);
                        }
                        refs.push(WriteRelation::Connect { field, st: ref_entity, ids });
                    }
                },
                _ => { 
                    return Err(EncodeError::DerivedFieldNotWritable(field.full_name.clone()));
                }
                }
            }
        }
   }

   Ok(WriteOp { id, data, refs, mask, entity, defaults })
}

// Метод, который кодирует только ID (и ругается, если пропущено какое-либо поле)
pub fn encode_id<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<Vec<u8>, EncodeError> {
   let obj = json_val
      .as_object()
      .ok_or(EncodeError::NotAnObject)?;

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
   }

   Ok(id)
}

// Записывает значение по умолчанию. Если значение вычисляется во время записи - возвращает offset
fn parse_default_value(dst: &mut Vec<u8>, field: &Field, _default_value: &FieldDefault) -> Result<Option<usize>, EncodeError> {
    let offset = dst.len();
    encode_empty(dst, field)?;
    return Ok(Some(offset));
}

// Записывает пустые байты. Само значение заполняется во время выполнения insert
fn encode_empty(dst: &mut Vec<u8>, field: &Field) -> Result<(), EncodeError> {
    match field.ty {
        FieldType::Primitive(primitive_type) => {
            let Some(size) = primitive_type.get_size() else {
                return Err(EncodeError::UnavailableKeyField(field.full_name.clone()))
            };
            dst.resize(dst.len() + size, 0);
        }
        _ => {
            return Err(EncodeError::UnavailableKeyField(field.full_name.clone()))
        }
    }
    Ok(())
}

fn encode_id_value(dst: &mut Vec<u8>, field: &Field, schema: &Schema, value: &Value) -> Result<(), EncodeError> {
    match &field.ty {
        FieldType::Primitive(primitive_type) => {
            encode_primitive_value( dst, field, &primitive_type, value)?;
        }
        FieldType::Ref (ref_info) => {
            let connect_id = encode_id(schema, &schema.models[ref_info.model_index], value)?;
            dst.extend(connect_id);
        }
        _ => {
            return Err(EncodeError::UnavailableKeyFieldId(field.full_name.clone()))
        }
    }
    Ok(())
}

/// Записывает в offset текущий курсор на buf
fn write_header(dst: &mut [u8], offset_pos: usize) {
   let start = dst.len() as u32;
   dst[offset_pos..offset_pos + 4].copy_from_slice(&start.to_be_bytes());
}

/// Кодирует одно значение и дописывает в конец `dst`
fn encode_primitive_value(dst: &mut Vec<u8>, field: &Field, ty: &PrimitiveFieldType, v: &Value) -> Result<(), EncodeError> {
    match ty {
        PrimitiveFieldType::String => {
            let s = v.as_str().ok_or_else(|| EncodeError::type_mismatch(field, "string"))?;
            dst.extend_from_slice(s.as_bytes());
        }

        PrimitiveFieldType::DateTime => {
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

            dst.extend_from_slice(&epoch.to_be_bytes());
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

fn as_i64(v: &Value, field: &Field) -> Result<i64, EncodeError> {
    v.as_i64().ok_or_else(|| EncodeError::type_mismatch(field, "i64"))
}

fn as_u64(v: &Value, field: &Field) -> Result<u64, EncodeError> {
    v.as_u64().ok_or_else(|| EncodeError::type_mismatch(field, "u64"))
}

fn as_f32(v: &Value, field: &Field) -> Result<f32, EncodeError> {
    v.as_f64()
        .map(|f| f as f32)
        .ok_or_else(|| EncodeError::type_mismatch(field, "float"))
}

fn as_f64(v: &Value, field: &Field) -> Result<f64, EncodeError> {
    v.as_f64().ok_or_else(|| EncodeError::type_mismatch(field, "double"))
}

fn encode_list(buf: &mut Vec<u8>, value: &Value, field: &Field, primitive_type: &PrimitiveFieldType, fixed_size: Option<usize>) -> Result<(), EncodeError> {
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
fn encode_list_dynamic(buf: &mut Vec<u8>, arr: &[Value], field: &Field, primitive_type: &PrimitiveFieldType, byte_start: usize) -> Result<(), EncodeError> {

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
fn encode_list_static(buf: &mut Vec<u8>, arr: &[Value], field: &Field, primitive_type: &PrimitiveFieldType) -> Result<(), EncodeError> {
    for arr_item in arr.iter() {
        encode_primitive_value(buf, field, primitive_type,  arr_item)?;
    }
    return Ok(());
}


#[derive(Debug)]
pub enum EncodeError {
    NotAnObject,
    MissingIdField(String),
    IdFieldIsNull(String),
    TypeMismatch { field: String, expected: String },
    TryWriteToVirtualField,
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use crate::{json_parsers::parse_insert, parse_schema, utils::{get_end, get_offsets}, write_op::{WriteDefault, WriteRelation}};

    #[test]
    fn encode_test() {
        let schema = parse_schema("
            model User {
                name        String
                age         UInt
                profile     Profile
            }
            model Profile {
                
            }
        ");  

        let model = &schema.models[0];

        let input = json!({
            "name": "Alice",
            "age": 30,
            "profile": { "id": 1 }
        });

        let encoded = parse_insert(&schema, model, &input).unwrap();

        // Проверяем версию
        assert_eq!(encoded.data[0], 1);
        match encoded.defaults[0] {
            WriteDefault::Key(offset, _) => assert_eq!(offset, 0),
            _ => panic!("Wrong default value")
        }

        // Читаем field_count
        let field_count = u16::from_be_bytes(encoded.data[1..3].try_into().unwrap());
        assert_eq!(field_count, model.payload_offset as u16);

        // Читаем смещения
        let offset_name = u32::from_be_bytes(encoded.data[3..7].try_into().unwrap()) as usize;
        let offset_age  = u32::from_be_bytes(encoded.data[7..11].try_into().unwrap()) as usize;
        let _offset_profile  = u32::from_be_bytes(encoded.data[11..15].try_into().unwrap()) as usize;

        assert_eq!(offset_name, model.payload_offset);

        // Проверяем, что смещения действительно указывают на данные
        // name: [len=5][bytes]
        let name_end = get_end(&encoded.data, 3, model.payload_offset);

        let name_value = &encoded.data[offset_name .. name_end];
        assert_eq!(name_value, b"Alice");
        
        // age: i64
        let age_bytes = &encoded.data[offset_age .. offset_age + 8];
        let age_value = i64::from_be_bytes(age_bytes.try_into().unwrap());
        assert_eq!(age_value, 30);
        
        assert_eq!(encoded.id, vec![0u8;8]);

        assert_eq!(encoded.defaults.len(), 1);
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

        let encoded = parse_insert(&schema, model, &input).unwrap();
        
        // Проверяем версию
        assert_eq!(encoded.data[0], 1);

        assert_eq!(get_offsets(&encoded.data, model), vec![ model.payload_offset ]); // 3 bytes + 4 byte offset
        assert_eq!(encoded.refs.len(), 1);

        let WriteRelation::CreateMany { ops, .. } = &encoded.refs[0] else {
            panic!("Expected WriteRelation::CreateMany, found {:?}", encoded.refs[0]);
        };

        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0].defaults[0], WriteDefault::ParentId(0)));
        assert_eq!(&ops[0].id, &[ 0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(get_offsets(&ops[0].data, ops[0].entity), vec![ model.payload_offset ]);
        
        // assert_eq!()

    }   

}