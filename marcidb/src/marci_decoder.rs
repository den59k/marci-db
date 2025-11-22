use std::collections::HashMap;

use bitvec::vec::BitVec;
use serde_json::{Map, Value};

use crate::{marci_db::{DecodeCtx, IncludeResult, get_end, get_offset}, schema::{Aliases, Entity, Field, FieldType, PrimitiveFieldType}};

#[derive(Debug)]
pub enum DecodeError {
    WrongVersion,
    BufferTooSmall,
    EmptyPayload,
    Utf8Error,
    TypeMismatch(String),
    OffsetOutOfRange,
}

pub fn decode_document(ctx: DecodeCtx<Value>) -> Result<Value, DecodeError>  {
    let DecodeCtx { data, entity, id, select, includes, inject, aliases } = ctx;
    if !data.is_empty() {
        if data.len() < 3 {
            return Err(DecodeError::BufferTooSmall);
        }
    
        // let version = data[0];
        // if version != 1 {
        //     return Err(DecodeError::WrongVersion);
        // }
    
        // if u16::from_be_bytes([data[1], data[2]]) != entity.payload_offset as u16 {
        //     let offset = u16::from_be_bytes([data[1], data[2]]);
        //     return Err(DecodeError::TypeMismatch(format!("payload offset mismatch; Expected: {}, Get {}", entity.payload_offset, offset)));
        // }
    
        if data.len() < entity.payload_offset {
            return Err(DecodeError::BufferTooSmall);
        }
    }

    let mut obj = Map::new();

    decode_fields(id, data, &entity.fields, &mut obj, select, aliases, entity.payload_offset)?;
    
    if let Some(Value::Object(mut map)) = inject {
        obj.append(&mut map);
    }

    for include in includes {
        match include {
            IncludeResult::None(field) => {
                obj.insert(field.name.clone(), Value::Null);
            },
            IncludeResult::One(field, val) => {
                obj.insert(field.name.clone(), val);
            },
            IncludeResult::Many(field, val) => {
                let vec = Value::Array(val);
                obj.insert(field.name.clone(), vec);
            }
        }
    }

    return Ok(Value::Object(obj));
}

pub fn decode_fields<'a>(
    id: &'a [u8],
    data: &'a [u8], 
    fields: &[Field], 
    obj: &mut Map<String, Value>, 
    select: &BitVec, 
    aliases: Option<&Aliases>,
    payload_offset: usize
) -> Result<(), DecodeError> {
    for (field_index, field) in fields.iter().enumerate() {
        if !select[field_index] { continue;  }

        if let Some(id_idx) = field.id_idx {
            match &field.ty {
                FieldType::Primitive(primitive) => {
                    // TODO: correct calc offset
                    let value = decode_value(primitive, id, id_idx*8, None)?;
                    obj.insert(field.name.clone(), value);
                }
                _ => {}
            }
        }

        if field.offset_pos == 0 { continue; }
        
        if data.is_empty() {
            return Err(DecodeError::EmptyPayload);
        }

        let field_name = aliases
            .and_then(|a| a.get(&field.name))
            .map(|i|i.to_string())
            .unwrap_or_else(|| field.name.clone());

        match &field.ty {
            FieldType::Primitive(primitive) => {
                let offset = get_offset_checked(data, field.offset_pos)?;
                // Поле = null
                if offset == 0 {
                    obj.insert(field.name.clone(), Value::Null);
                    continue;
                }

                let offset_end = primitive.is_dynamic_size().then(|| {
                    return get_end(&data, field.offset_pos, payload_offset)
                });

                // Декодируем
                let value = decode_value(primitive, &data, offset, offset_end)?;

                obj.insert(field_name, value);
            }
            FieldType::PrimitiveList(primitive) => {
                let offset = get_offset_checked(data, field.offset_pos)?;
                if offset == 0 {
                    // NOTE: If field is not nullable, it must write empty list on value
                    obj.insert(field.name.clone(), Value::Null);
                    continue;
                }

                let size = u32::from_be_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
                let mut vec = Vec::with_capacity(size);

                let is_dynamic_size = primitive.is_dynamic_size();
                let static_size = primitive.get_size();

                for i in 0..size {
                    let offset_pos = offset+4 + i * 4;

                    let item_offset = if is_dynamic_size {
                        offset + u32::from_be_bytes(data[offset_pos .. offset_pos + 4].try_into().unwrap()) as usize
                    } else {
                        offset + 4 + static_size * i
                    };
                    let offset_end = is_dynamic_size.then(|| {
                        offset + u32::from_be_bytes(data[offset_pos + 4 .. offset_pos + 8].try_into().unwrap()) as usize
                    });
                    vec.push(decode_value(primitive, &data, item_offset, offset_end)?);
                }
                
                obj.insert(field_name, Value::Array(vec));
            },
            FieldType::Enum(en) => {
                let offset = get_offset_checked(data, field.offset_pos)?;
                // Поле = null
                if offset == 0 {
                    obj.insert(field_name, Value::Null);
                    continue;
                }

                let variant_index = u16::from_be_bytes(data[offset..offset+2].try_into().unwrap()) as usize;
                obj.insert(field_name, Value::String(en.variants[variant_index].name.clone()));
                // let variant_fields = &en.variants[variant_index].fields;
                // if !variant_fields.is_empty() {
                //     let bit_vec = bitvec!(1; variant_fields.len());

                //     println!("{:?} {:?}", &data[offset..], variant_fields);

                //     decode_fields(&[], &data[offset..], variant_fields, obj, &bit_vec, None, en.variants[variant_index].payload_offset)?;
                // }
            }
            _ => {}
        }
    }

    return Ok(());
}

#[inline(always)]
fn decode_value(ty: &PrimitiveFieldType, data: &[u8], offset: usize, offset_end: Option<usize>) -> Result<Value, DecodeError> {
    if !ty.is_dynamic_size() && data.len() < offset + ty.get_size() {
        return Err(DecodeError::BufferTooSmall);
    }
    match ty {
        // TODO: Create string decoder supports 2 mode: read len from header and read len by terminate character
        PrimitiveFieldType::String => {
            let offset_end = offset_end.expect("offset_end must be defined for decode string");
            let s = std::str::from_utf8(&data[offset..offset_end]).map_err(|_| DecodeError::Utf8Error)?;
            Ok(Value::String(s.to_string()))
        }
        PrimitiveFieldType::DateTime => {
            let epoch = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            // Возвращаем как число (или можно форматировать обратно в ISO)
            Ok(Value::Number(epoch.into()))
        }
        PrimitiveFieldType::Int64 => {
            let n = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(Value::Number(n.into()))
        }
        PrimitiveFieldType::UInt64 => {
            let n = u64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(Value::Number(n.into()))
        }
        PrimitiveFieldType::Float => {
            let n = f32::from_be_bytes(data[offset..offset+4].try_into().unwrap());
            Ok(Value::Number(serde_json::Number::from_f64(n as f64).unwrap()))
        }
        PrimitiveFieldType::Double => {
            let n = f64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(Value::Number(serde_json::Number::from_f64(n).unwrap()))
        }
        PrimitiveFieldType::Bool => {
            Ok(Value::Bool(data[offset] != 0))
        }
    }
}

pub fn decode_id(id: &[u8], model: &Entity) -> Result<Value, DecodeError> {
    let mut obj = Map::new();

    for field in model.fields.iter() {
        let Some(id_idx) = field.id_idx else {
            continue;
        };
        match &field.ty {
            FieldType::Primitive(primitive) => {
                let value = decode_value(primitive, id, id_idx * 8, None)?;
                obj.insert(field.name.clone(), value);
            }
            _ => {}
        }
    }

    Ok(Value::Object(obj))
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
    use serde_json::json;

    use crate::{marci_db::{DecodeCtx, InsertStruct}, marci_decoder::decode_document, marci_encoder::{encode_document, encode_id}, marci_select::parse_select, schema::{FieldType, parse_schema}};

    #[test]
    fn test_decode_list() {

        let schema_str = "
            model Project {
                features     String[]
                counters     Int[]
            }
        ";
        let schema = parse_schema(schema_str);

        let input = json!({
            "features": []
        });

        let mut structs = vec![];
        let (data, _) = encode_document(&schema, &schema.models[0], &input, &mut structs).unwrap();

        // First 4 bytes - array size, second 4 bytes - last item offset
        // Field "counters" is null
        assert_eq!(&data[schema.models[0].payload_offset..], &[0, 0, 0, 0,  0, 0, 0, 8]);


        let input = json!({
            "features": ["tester", "tests"],
            "counters": [ 4, 5 ]
        });

        let mut structs = vec![];
        let (data, _) = encode_document(&schema, &schema.models[0], &input, &mut structs).unwrap();
        let id = encode_id(&schema.models[0], &input, true).unwrap();

        let resp = decode_document(DecodeCtx { 
            id: &id, 
            data: &data, 
            entity: &schema.models[0], 
            select: &bitvec!(1;  &schema.models[0].fields.len() + 1), 
            includes: vec![], 
            inject: None,
            aliases: None
        }).unwrap();

        assert_eq!(resp, json!({
            "id": 0,
            "features": ["tester", "tests"],
            "counters": [ 4, 5 ]
        }))
    }

    #[test]
    fn test_decode_enum() {

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
        let InsertStruct::Many { st, data } = &structs[0] else {
            panic!("Inserted data is not a InsertStruct::Many");
        };

        let input_select = json!({
            "role": true,
            "features": true
        });
        let select = parse_select(&st.fields, &input_select, &schema, None).unwrap();

        let resp = decode_document(DecodeCtx { 
            id: &data[0].0, 
            data: &data[0].1, 
            entity: *st, 
            select: &select.mask, 
            includes: vec![], 
            inject: None,
            aliases: None
        }).unwrap();

        assert_eq!(resp, json!({ "role": "admin" }));
    }

}