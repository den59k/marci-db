use serde_json::{Map, Value};

use crate::{Field, json_parsers::{EncodeError, parsers::{encode_enum, encode_list, encode_primitive_value, parse_field_value_num}}, schema::{Entity, FieldLocation, FieldType, Schema}, update_op::{UpdateField, UpdateOp, UpdateValue}};

pub fn parse_update<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<UpdateOp<'a>, EncodeError> {
    let obj = json_val
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    return parse_update_op(schema, entity, obj);
}    

fn parse_update_op<'a>(
    schema: &'a Schema, 
    entity: &'a Entity, 
    obj: &Map<String, Value>
) -> Result<UpdateOp<'a>, EncodeError> {

    let mut update_fields = vec![];
    let mut update_refs = vec![];

    for field in entity.fields.iter() {
        let Some(value) = obj.get(&field.name) else {
            continue;
        };

        match &field.ty {
            FieldType::Ref(ref_info) => {
                if value.is_null() {
                    
                }
                let Some(obj) = value.as_object() else {
                    return Err(EncodeError::type_mismatch(field, "object"));
                };
                for (key, value) in obj {
                    match key.as_str() {
                        "$update" => todo!(),
                        "$create" => todo!(),
                        "$replace" => todo!(),
                        "$connect" => todo!(),
                        _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
                    }
                }
                continue;
            },
            FieldType::RefList(ref_info) => {
                let Some(obj) = value.as_object() else {
                    return Err(EncodeError::type_mismatch(field, "object"));
                };
                for (key, value) in obj {
                    match key.as_str() {
                        "$push" => todo!(),
                        "$remove" => todo!(),
                        "$update" => todo!(),
                        "$updateAll" => todo!(),
                        "$replaceAll" => todo!(),
                        _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
                    }
                }
                continue;
            },
            _ => {}
        }

        let FieldLocation::Body { offset_pos } = field.location else {
            return Err(EncodeError::OnlyBodyKeyAvailableToEdit(field.full_name.clone()));
        };

        if value.is_null() {
            update_fields.push(UpdateField { field, value: UpdateValue::Null, offset_pos });
            continue;
        }

        update_fields.push(UpdateField { field, value: parse_field(field, value)?, offset_pos });
    }

    Ok(UpdateOp {
        fields: update_fields,
        refs: update_refs
    })
}

fn parse_field<'a>(field: &'a Field, value: &Value) -> Result<UpdateValue, EncodeError> {
    return match &field.ty {
        FieldType::Primitive(primitive_field_type) => {
            if let Some(obj) = value.as_object() {
                if obj.len() != 1 {
                    return Err(EncodeError::OnlyOneKeyExpected(field.full_name.clone(), value.to_string()))
                }
                let (key, value) = obj.iter().next().unwrap();
                return match key.as_str() {
                    "$increment" => {
                        Ok(UpdateValue::Increment(parse_field_value_num(field, value)?))
                    }
                    _ => Err(EncodeError::UnsupportedOperation(key.clone()))
                }
            }

            let mut dst = vec![];
            encode_primitive_value(&mut dst, field, primitive_field_type, value)?;
            Ok(UpdateValue::Value(dst))
        },
        FieldType::PrimitiveList(primitive_field_type, fixed_size) => {
            let mut dst = vec![];
            encode_list(&mut dst, value, field, primitive_field_type, *fixed_size)?;
            Ok(UpdateValue::Value(dst))
        },
        FieldType::Enum(enum_info) => {
            let mut dst = vec![];
            encode_enum(&mut dst, field, enum_info, value)?;
            Ok(UpdateValue::Value(dst))
        },
        _ => Err(EncodeError::UnavailableKeyField(field.full_name.clone()))
    }
}