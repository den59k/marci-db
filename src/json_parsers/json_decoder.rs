use std::sync::Arc;

use crate::{query_op::{DecodeCtx, IncludeResult}, schema::{Entity, Field, FieldLocation, FieldType, PrimitiveFieldType, Schema}, utils::{check_exists_condition, get_end, get_id_field_size, get_offset}};

#[derive(Debug)]
pub enum DecodeError {
    WrongVersion,
    BufferTooSmall,
    OffsetOverflow,
    EmptyPayload,
    Utf8Error,
    TypeMismatch(String),
    OffsetOutOfRange,
    WrongEnumValue(u16)
}

pub trait FieldName {
    fn field_name(&self) -> &str;
}

impl FieldName for &Field {
    fn field_name(&self) -> &str {
        &self.name
    }
}

impl FieldName for &String {
    fn field_name(&self) -> &str {
        self
    }
}

pub trait JsonStr {
    fn as_json_str(&self) -> &str;
}

impl JsonStr for String {
    fn as_json_str(&self) -> &str {
        self
    }
}

impl JsonStr for Arc<String> {
    fn as_json_str(&self) -> &str {
        self
    }
}

impl JsonStr for &str {
    fn as_json_str(&self) -> &str {
        self
    }
}


#[inline(always)]
pub fn insert_null<F: FieldName>(str: &mut String, field: F) {
    if str.len() > 1 {
        str.push(',');
    }
    str.push('"');
    str.push_str(&field.field_name());
    str.push_str("\":null");
}

#[inline(always)]
pub fn insert_value<F: FieldName>(str: &mut String, field: F, val: &String) {
    if str.len() > 1 {
        str.push(',');
    }
    str.push('"');
    str.push_str(&field.field_name());
    str.push_str("\":");
    str.push_str(&val);
}

#[inline(always)]
pub fn insert_array_arc<F: FieldName, T: JsonStr>(str: &mut String, field: F, arr: &[T]) {
    if str.len() > 1 {
        str.push(',');
    }
    str.push('"');
    str.push_str(&field.field_name());
    str.push_str("\":[");
    let mut is_first = true;
    for item in arr {
        if !is_first {
            str.push(',');
        }
        is_first = false;
        str.push_str(item.as_json_str());
    }
    str.push(']');
}

#[inline(always)]
/// Insert value as string to JSON, e.g., insert_string(..,role,admin) -> "role":"admin"
/// NOTE: this method does not encode string
pub fn insert_string<F: FieldName>(str: &mut String, field: F, val: &String) {
    if str.len() > 1 {
        str.push(',');
    }
    str.push('"');
    str.push_str(&field.field_name());
    str.push_str("\":\"");
    str.push_str(&val);
    str.push('"');
}

#[inline(always)]
pub fn array_to_json(arr: &[String]) -> String {
    if arr.is_empty() {
        return "[]".to_owned();
    }

    let mut total_len = 2; // '[' и ']'
    total_len += arr.len().saturating_sub(1); // запятые

    for s in arr {
        total_len += s.len();
    }

    let mut out = String::with_capacity(total_len);

    out.push('[');

    let mut first = true;
    for s in arr {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(s);
    }

    out.push(']');

    out
}

pub fn decode_document(ctx: DecodeCtx<String>) -> Result<String, DecodeError>  {
    let DecodeCtx { data, entity, id, mask, includes, schema } = ctx;
    if !data.is_empty() {
        if data.len() < 3 {
            return Err(DecodeError::BufferTooSmall);
        }
    
        let version = data[0];
        if version != 1 {
            return Err(DecodeError::WrongVersion);
        }
    
        if u16::from_be_bytes([data[1], data[2]]) != entity.payload_offset as u16 {
            let offset = u16::from_be_bytes([data[1], data[2]]);
            return Err(DecodeError::TypeMismatch(format!("payload offset mismatch; Expected: {}, Get {}", entity.payload_offset, offset)));
        }
    
        if data.len() < entity.payload_offset {
            return Err(DecodeError::BufferTooSmall);
        }
    }

    // let mut obj = Map::new();
    let mut str = String::with_capacity(256);
    str.push('{');

    // Декодируем все поля
    let mut id_offset = 0;
    for (field_index, field) in entity.fields.iter().enumerate() {
        match field.location {
            FieldLocation::Key { .. } => {
                let size = get_id_field_size(id, field, id_offset, schema);
                if !mask[field_index] { 
                    id_offset += size;
                    continue;
                }
                decode_id_field(&id[id_offset..id_offset+size], field, schema, &mut str)?;
                id_offset += size;
            },
            FieldLocation::Body { offset } => {
                if !mask[field_index] { continue; }
                if !check_exists_condition(entity, &field.condition, &id, &data, schema) {
                    continue;
                }
                decode_body_field(data, offset, field, &mut str, entity.payload_offset)?
            },
            FieldLocation::Virtual => {}
        }
    }
    
    // if let Some(inject_str) = inject {
    //     if inject_str.len() > 2 {
    //         if str.len() > 1 {
    //             str.push(',');
    //         }
    //         str.push_str(&inject_str[1..inject_str.len()-1]);
    //         // obj.append(&mut map);
    //     } else {
    //         println!("WARN: you pass empty inject");
    //     }
    // }

    for include in includes {
        match include {
            IncludeResult::None(field) => {
                insert_null(&mut str, field);
            },
            IncludeResult::One(field, val) => {
                insert_value(&mut str, field, &val);
            },
            IncludeResult::Many(field, val) => {
                insert_array_arc(&mut str, field, &val);
            }
        }
    }

    str.push('}');

    return Ok(str);
    // return Ok(Value::Object(obj));
}

pub fn decode_id(id: &[u8], entity: &Entity, schema: &Schema) -> String {
    let mut str = String::new();
    str.push('{');
    let mut id_offset = 0;
    for field in entity.fields.iter() {
        match field.location {
            FieldLocation::Key { .. } => {
                let size = get_id_field_size(id, field, id_offset, schema);
                decode_id_field(&id[id_offset..id_offset+size], field, schema, &mut str).unwrap();
                id_offset += size;
            },
            _ => {}
        }
    }
    str.push('}');
    return str;
}

// Декодирует в JSON значение из ID
fn decode_id_field<'a>(data: &'a [u8], field: &Field, schema: &Schema, obj: &mut String) -> Result<(), DecodeError> {
  let field_name = field.name.clone();
  match &field.ty {
    FieldType::Primitive(primitive) => {
        let value = decode_value(&primitive, &data)?;
        insert_value(obj, &field_name, &value);
        Ok(())
    },
    FieldType::Ref (ref_info) => {
        let ref_entity = &schema.models[ref_info.model_index];
        let value = &decode_id(data, ref_entity, schema);
        insert_value(obj, &field_name, value);
        Ok(())
    },
    _ => { panic!("Non primitive types in key is not supported") }
  }
}

// Декодирует в JSON значение из Body
fn decode_body_field<'a>(data: &'a [u8], offset_pos: usize, field: &Field, obj: &mut String, payload_offset: usize) -> Result<(), DecodeError> {
  let offset = get_offset_checked(data, offset_pos)?;
  if offset == 0 {
    insert_null(obj, field);
    return Ok(())
  }

  let field_name = field.name.clone();

  // let field_name = aliases
  //     .and_then(|a| a.get(&field.name))
  //     .map(|i|i.to_string())
  //     .unwrap_or_else(|| field.name.clone());
  
  match &field.ty {
    FieldType::Primitive(primitive) => {
      let offset_end = primitive.get_size()
        .map(|size| offset_pos + size)
        .unwrap_or_else(|| get_end(&data, offset_pos, payload_offset));

      // Декодируем
      let value = decode_value(primitive, &data[offset..offset_end])?;
      insert_value(obj, &field_name, &value);
      // obj.insert(field_name, value);
    }
    FieldType::PrimitiveList(primitive) => {
        let size = u32::from_be_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
        let vec = match primitive.get_size() {
            None => decode_array_dynamic(data, primitive, size, offset, offset + 4)?,
            Some(el_size) => decode_array_static(data, primitive, size, offset + 4, el_size)?
        };
        
        insert_array_arc(obj, &field_name, &vec);
    },
    FieldType::PrimitiveFixedList(primitive, fixed_size) => {
        let vec = match primitive.get_size() {
            None => decode_array_dynamic(data, primitive, *fixed_size, offset, offset)?,
            Some(el_size) => decode_array_static(data, primitive, *fixed_size, offset, el_size)?
        };
        insert_array_arc(obj, field, &vec);
    },
    FieldType::Enum(en) => {
        let variant_index = u16::from_be_bytes(data[offset..offset+2].try_into().unwrap());
        let Some(variant_value) = &en.variants_names_map.get(&variant_index) else {
            return Err(DecodeError::WrongEnumValue(variant_index))
        };
        insert_string(obj, &field_name, variant_value);
    }
    _ => {}
  };

  Ok(())
}

fn decode_array_dynamic(
    data: &[u8], 
    primitive: &PrimitiveFieldType, 
    size: usize, 
    field_offset: usize, 
    field_header_offset: usize
) -> Result<Vec<String>, DecodeError> {
    let mut vec = Vec::with_capacity(size);
    for i in 0..size {
        let offset_pos = field_header_offset + i * 4;

        let item_offset = field_offset + u32::from_be_bytes(data[offset_pos .. offset_pos + 4].try_into().unwrap()) as usize;
        let offset_end = field_offset + u32::from_be_bytes(data[offset_pos + 4 .. offset_pos + 8].try_into().unwrap()) as usize;

        vec.push(decode_value(primitive, &data[item_offset..offset_end])?);
    }
    
    return Ok(vec);
}

fn decode_array_static(data: &[u8], primitive: &PrimitiveFieldType, size: usize, payload_offset: usize, static_size: usize) -> Result<Vec<String>, DecodeError> {
    let mut vec = Vec::with_capacity(size);
    for i in 0..size {
        let item_offset = payload_offset + static_size * i;
        let item_end = item_offset+primitive.get_size().expect("Expect static primitive type");
        vec.push(decode_value(primitive, &data[item_offset..item_end])?);
    }
    return Ok(vec);
}

#[inline(always)]
fn decode_value(ty: &PrimitiveFieldType, data: &[u8]) -> Result<String, DecodeError> {
    match ty {
        PrimitiveFieldType::String => {
            let s = std::str::from_utf8(&data).map_err(|_| DecodeError::Utf8Error)?;

            let json = serde_json::to_string(s).unwrap();
            Ok(json)
            // Ok(Value::String(s.to_string()))
        }
        PrimitiveFieldType::DateTime => {
            let epoch = i64::from_be_bytes(data.try_into().unwrap());
            // Возвращаем как число (или можно форматировать обратно в ISO)
            Ok(epoch.to_string())
            // Ok(Value::Number(epoch.into()))
        }
        PrimitiveFieldType::Int64 => {
            let n = i64::from_be_bytes(data.try_into().unwrap());
            Ok(n.to_string())
            // Ok(Value::Number(n.into()))
        }
        PrimitiveFieldType::UInt64 => {
            let n = u64::from_be_bytes(data.try_into().unwrap());
            Ok(n.to_string())
            // Ok(Value::Number(n.into()))
        }
        PrimitiveFieldType::Float => {
            let n = f32::from_be_bytes(data.try_into().unwrap());
            Ok(n.to_string())
            // Ok(Value::Number(serde_json::Number::from_f64(n as f64).unwrap()))
        }
        PrimitiveFieldType::Double => {
            let n = f64::from_be_bytes(data.try_into().unwrap());
            Ok(n.to_string())
            // Ok(Value::Number(serde_json::Number::from_f64(n).unwrap()))
        }
        PrimitiveFieldType::Bool => {
            Ok((data[0] != 0).to_string())
            // Ok(Value::Bool(data[offset] != 0))
        }
    }
}

#[inline(always)]
pub fn get_offset_checked<'a>(data: &'a [u8], offset_pos: usize) -> Result<usize,DecodeError> {
  let offset = get_offset(data, offset_pos);
  if offset >= data.len() {
    return Err(DecodeError::OffsetOutOfRange);
  }
  return Ok(offset);
}

#[cfg(test)]
mod tests {
    use bitvec::bitvec;
    use serde_json::{Value, json};

    use crate::{WriteRelation, json_parsers::decode_document, parse_insert, parse_schema, query_op::{DecodeCtx, IncludeResult}};

    #[test]
    fn test_decode_list() {
        let schema = parse_schema("
            model Project {
                features     String[]
                counters     Int[]
            }
        ");

        let input = json!({
            "features": []
        });

        let encoded = parse_insert(&schema, &schema.models[0], &input).unwrap();

        // First 4 bytes - array size, second 4 bytes - last item offset
        // Field "counters" is null
        assert_eq!(&encoded.data[schema.models[0].payload_offset..], &[0, 0, 0, 0,  0, 0, 0, 8]);


        let input = json!({
            "features": ["tester", "tests"],
            "counters": [ 4, 5 ]
        });

        let encoded = parse_insert(&schema, &schema.models[0], &input).unwrap();

        let resp = decode_document(DecodeCtx { 
            id: &encoded.id, 
            data: &encoded.data, 
            entity: &schema.models[0], 
            mask: &bitvec!(1;  &schema.models[0].fields.len() + 1), 
            includes: vec![],
            schema: &schema
        }).unwrap();

        assert_eq!(serde_json::from_str::<Value>(&resp).unwrap(), json!({
            "id": 0,
            "features": ["tester", "tests"],
            "counters": [ 4, 5 ]
        }))
    }

    #[test]
    fn test_decode_nested() {
        let schema = parse_schema("
            model User {
                name        String
                info        UserInfo?
            }
            struct UserInfo {
                bio         String
            }
            model Project {
                name        String
                users       UserRole[]
            }
            struct UserRole {
                user        User          @id
                role        String
            }
        ");

        let input = json!({
            "name": "Project A",
            "users": [{ "user": { "id": 1 }, "role": "creator" }]
        });

        let encoded = parse_insert(&schema, &schema.models[1], &input).unwrap();

        let resp = decode_document(DecodeCtx { 
            id: &encoded.id, 
            data: &encoded.data, 
            entity: &schema.models[0], 
            mask: &bitvec!(1;  schema.models[1].fields.len()), 
            includes: encoded.refs.iter().map(|f| {
                let WriteRelation::CreateMany { field, ops, st } = f else {
                    panic!("Wrong WriteRelation type")
                };
                assert_eq!(ops[0].defaults.len(), 1);
                let resp = ops.iter().map(|op| { 
                    let id: Vec<u8> = [ &vec![0;8], op.id.as_slice() ].concat();
                    let mut mask = bitvec!(1; st.fields.len());
                    mask.set(0, false);

                    decode_document(DecodeCtx { 
                        id: &id, 
                        data: &op.data, 
                        entity: &st, 
                        mask: &mask,
                        includes: vec![], 
                        schema: &schema
                    }).unwrap() 
                }).collect();
                IncludeResult::Many(field, resp)
            }).collect(),
            schema: &schema
        }).unwrap();

        assert_eq!(serde_json::from_str::<Value>(&resp).unwrap(), json!({
            "id": 0,
            "name": "Project A",
            "users": [{ "user": { "id": 1 }, "role": "creator" }]
        }))
    }

    #[test]
    fn test_decode_enum() {
        let schema = parse_schema("
            model Account {
                name        String
                type        AccountType
            }

            enum AccountType {
                basic
                pro {
                    features String[]
                }
            } 
        ");

        let account_model = &schema.models[0];

        {
            let input = json!({
                "name": "Alice",
                "type": "basic"
            });
    
            let encoded = parse_insert(&schema, account_model, &input).unwrap();
                        
            let resp: String = decode_document(DecodeCtx {
                id: &encoded.id,
                data: &encoded.data,
                entity: account_model,
                mask: &bitvec!(1;  schema.models[0].fields.len()),
                includes: vec![],
                schema: &schema,
            }).unwrap();
    
            assert_eq!(serde_json::from_str::<Value>(&resp).unwrap(), json!({
                "id": 0,
                "name": "Alice",
                "type": "basic"
            }))
        }

        {
            let input = json!({
                "name": "Bob",
                "type": "pro",
                "features": [ "pro-user" ]
            });
    
            let encoded = parse_insert(&schema, account_model, &input).unwrap();

            let resp: String = decode_document(DecodeCtx {
                id: &encoded.id,
                data: &encoded.data,
                entity: account_model,
                mask: &bitvec!(1; schema.models[0].fields.len()),
                includes: vec![],
                schema: &schema,
            }).unwrap();
      
            assert_eq!(serde_json::from_str::<Value>(&resp).unwrap(), json!({
                "id": 0,
                "name": "Bob",
                "type": "pro",
                "features": [ "pro-user" ]
            }))
        }

    }
}