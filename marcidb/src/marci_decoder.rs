use std::sync::Arc;

use bitvec::vec::BitVec;

use crate::{marci_db::{get_end, get_offset}, schema::{Aliases, Entity, Field, FieldType, PrimitiveFieldType}, select::{DecodeCtx, IncludeResult}};

#[derive(Debug)]
pub enum DecodeError {
    WrongVersion,
    BufferTooSmall,
    OffsetOverflow,
    EmptyPayload,
    Utf8Error,
    TypeMismatch(String),
    OffsetOutOfRange,
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

    // let mut obj = Map::new();
    let mut str = String::with_capacity(256);
    str.push('{');

    decode_fields(id, data, &entity.fields, &mut str, select, aliases, entity.payload_offset)?;
    
    if let Some(inject_str) = inject {
        if inject_str.len() > 2 {
            if str.len() > 1 {
                str.push(',');
            }
            str.push_str(&inject_str[1..inject_str.len()-1]);
            // obj.append(&mut map);
        } else {
            println!("WARN: you pass empty inject");
        }
    }

    for include in includes {
        match include {
            IncludeResult::None(field) => {
                insert_null(&mut str, field);
                // obj.insert(field.name.clone(), Value::Null);
            },
            IncludeResult::One(field, val) => {
                insert_value(&mut str, field, &val);
                // obj.insert(field.name.clone(), val);
            },
            IncludeResult::Many(field, val) => {
                insert_array_arc(&mut str, field, &val);
                // let vec = Value::Array(val);
                // obj.insert(field.name.clone(), vec);
            }
        }
    }

    str.push('}');

    return Ok(str);
    // return Ok(Value::Object(obj));
}

pub fn decode_fields<'a>(
    id: &'a [u8],
    data: &'a [u8], 
    fields: &[Field], 
    obj: &mut String, 
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
                    insert_value(obj, field, &value);
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
                    insert_null(obj, field);
                    // obj.insert(field.name.clone(), Value::Null);
                    continue;
                }

                let offset_end = primitive.get_size().is_none().then(|| {
                    return get_end(&data, field.offset_pos, payload_offset)
                });

                // Декодируем
                let value = decode_value(primitive, &data, offset, offset_end)?;
                
                insert_value(obj, &field_name, &value);
                // obj.insert(field_name, value);
            }
            FieldType::PrimitiveList(primitive) => {
                let offset = get_offset_checked(data, field.offset_pos)?;
                if offset == 0 {
                    insert_null(obj, field);
                    // obj.insert(field.name.clone(), Value::Null);
                    continue;
                }

                let size = u32::from_be_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
                let vec = match primitive.get_size() {
                    None => parse_array_dynamic(data, primitive, size, offset, offset + 4)?,
                    Some(el_size) => parse_array_static(data, primitive, size, offset + 4, el_size)?
                };
                
                insert_array_arc(obj, &field_name, &vec);
                // obj.insert(field_name, Value::Array(vec));
            },
            FieldType::PrimitiveFixedList(primitive, fixed_size) => {
                let offset = get_offset_checked(data, field.offset_pos)?;
                if offset == 0 {
                    insert_null(obj, &field_name);
                    // obj.insert(field.name.clone(), Value::Null);
                    continue;
                }
                let vec = match primitive.get_size() {
                    None => parse_array_dynamic(data, primitive, *fixed_size, offset, offset)?,
                    Some(el_size) => parse_array_static(data, primitive, *fixed_size, offset, el_size)?
                };
                insert_array_arc(obj, field, &vec);
                // obj.insert(field_name, Value::Array(vec));
            },
            FieldType::Enum(en) => {
                let offset = get_offset_checked(data, field.offset_pos)?;
                // Поле = null
                if offset == 0 {
                    insert_null(obj, &field_name);
                    // obj.insert(field_name, Value::Null);
                    continue;
                }

                let variant_index = u16::from_be_bytes(data[offset..offset+2].try_into().unwrap()) as usize;
                insert_string(obj, &field_name, &en.variants[variant_index].name);
                // obj.insert(field_name, Value::String(en.variants[variant_index].name.clone()));

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

fn parse_array_dynamic(
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

        vec.push(decode_value(primitive, &data, item_offset, Some(offset_end))?);
    }
    
    return Ok(vec);
}

fn parse_array_static(data: &[u8], primitive: &PrimitiveFieldType, size: usize, payload_offset: usize, static_size: usize) -> Result<Vec<String>, DecodeError> {
    let mut vec = Vec::with_capacity(size);
    for i in 0..size {
        let item_offset = payload_offset + static_size * i;
        vec.push(decode_value(primitive, &data, item_offset, None)?);
    }
    return Ok(vec);
}

#[inline(always)]
fn decode_value(ty: &PrimitiveFieldType, data: &[u8], offset: usize, offset_end: Option<usize>) -> Result<String, DecodeError> {
    if let Some(el_size) = ty.get_size() && data.len() < offset + el_size {
        return Err(DecodeError::OffsetOverflow);
    }
    match ty {
        // TODO: Create string decoder supports 2 mode: read len from header and read len by terminate character
        PrimitiveFieldType::String => {
            let offset_end = offset_end.expect("offset_end must be defined for decode string");
            let s = std::str::from_utf8(&data[offset..offset_end]).map_err(|_| DecodeError::Utf8Error)?;

            let json = serde_json::to_string(s).unwrap();
            Ok(json)
            // Ok(Value::String(s.to_string()))
        }
        PrimitiveFieldType::DateTime => {
            let epoch = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            // Возвращаем как число (или можно форматировать обратно в ISO)
            Ok(epoch.to_string())
            // Ok(Value::Number(epoch.into()))
        }
        PrimitiveFieldType::Int64 => {
            let n = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(n.to_string())
            // Ok(Value::Number(n.into()))
        }
        PrimitiveFieldType::UInt64 => {
            let n = u64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(n.to_string())
            // Ok(Value::Number(n.into()))
        }
        PrimitiveFieldType::Float => {
            let n = f32::from_be_bytes(data[offset..offset+4].try_into().unwrap());
            Ok(n.to_string())
            // Ok(Value::Number(serde_json::Number::from_f64(n as f64).unwrap()))
        }
        PrimitiveFieldType::Double => {
            let n = f64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
            Ok(n.to_string())
            // Ok(Value::Number(serde_json::Number::from_f64(n).unwrap()))
        }
        PrimitiveFieldType::Bool => {
            Ok((data[offset] != 0).to_string())
            // Ok(Value::Bool(data[offset] != 0))
        }
    }
}

pub fn decode_id(id: &[u8], model: &Entity) -> Result<String, DecodeError> {
    // let mut obj = Map::new();

    let mut str = String::with_capacity(256);
    str.push('{');
    let mut is_first = true;

    for field in model.fields.iter() {
        let Some(id_idx) = field.id_idx else {
            continue;
        };
        match &field.ty {
            FieldType::Primitive(primitive) => {
                if !is_first {
                    str.push(',');
                }
                is_first = false;
                let value = decode_value(primitive, id, id_idx * 8, None)?;
                str.push('"');
                str.push_str(&field.name);
                str.push_str("\":");
                str.push_str(&value);
                // obj.insert(field.name.clone(), value);
            }
            _ => {}
        }
    }

    str.push('}');

    Ok(str)
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

    use crate::{marci_db::InsertStruct, marci_decoder::decode_document, marci_encoder::{encode_document, encode_id}, marci_select::parse_select, schema::{FieldType, parse_schema}, select::DecodeCtx};

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

        assert_eq!(serde_json::from_str::<Value>(&resp).unwrap(), json!({
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

        assert_eq!(serde_json::from_str::<Value>(&resp).unwrap(), json!({ "role": "admin" }));
    }

}