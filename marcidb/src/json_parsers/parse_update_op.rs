use serde_json::{Map, Value};

use crate::{Field, delete_op::prepare_delete, json_parsers::{EncodeError, parse_write_op::parse_insert_nested, parsers::{encode_enum, encode_list, encode_primitive_value, parse_field_value_delta, rev_id_list_field}}, parse_id, schema::{Entity, FieldExistsCondition, FieldLocation, FieldType, PrimitiveFieldType, RefBinding, Schema}, update_op::{UpdateField, UpdateOp, UpdateRelation, UpdateRelationOp, UpdateValue}};

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

    for (field_index, field) in entity.fields.iter().enumerate() {
        let Some(value) = obj.get(&field.name) else {
            continue;
        };

        match &field.ty {
            FieldType::Ref(ref_info) => {
                let ref_entity = &schema.models[ref_info.model_index];
                let rev_field = ref_info.rev_field_idx.map(|i| &ref_entity.fields[i]);
                let rev_ref_info = rev_field.and_then(|f| {
                    match &f.ty {
                        FieldType::Ref(ref_info) | FieldType::RefList(ref_info) => Some(ref_info),
                        _ => None
                    }
                });

                if value.is_null() {
                    match field.location {
                        FieldLocation::Key { .. } => return Err(EncodeError::OnlyBodyKeyAvailableToEdit(field.full_name.clone())),
                        FieldLocation::Body { offset_pos } => {
                            update_fields.push(UpdateField { field, value: UpdateValue::Null, offset_pos });
                        }
                        FieldLocation::Virtual => {}
                    }
                    if ref_entity.autoinsert {
                        let delete_op: crate::delete_op::DeleteOp<'_> = prepare_delete(schema, entity, None, rev_field);
                        update_refs.push(UpdateRelation { field, st: ref_entity, op: UpdateRelationOp::Remove(delete_op), ref_info, rev_ref_info });
                    } else {
                        update_refs.push(UpdateRelation { field, st: ref_entity, op: UpdateRelationOp::DisconnectAll, ref_info, rev_ref_info });
                    }
                    continue;
                }
                let Some(obj) = value.as_object() else {
                    return Err(EncodeError::type_mismatch(field, "object"));
                };
                for (key, value) in obj {
                    let op = match key.as_str() {
                        "$update" => {
                            UpdateRelationOp::Update(parse_update(schema, ref_entity, value)?)
                        }
                        "$ensure" => {
                            if matches!(field.location, FieldLocation::Body { .. } | FieldLocation::Key { .. }) {
                                return Err(EncodeError::NestedWriteNotSupported { field: field.full_name.clone(), op: key.clone() });
                            }
                            UpdateRelationOp::Create(parse_insert_nested(schema, ref_info, value)?)
                        },
                        "$set" => {
                            if matches!(field.location, FieldLocation::Body { .. } | FieldLocation::Key { .. }) {
                                return Err(EncodeError::NestedWriteNotSupported { field: field.full_name.clone(), op: key.clone() });
                            }
                            let delete_op = prepare_delete(schema, entity, None, rev_field);
                            update_refs.push(UpdateRelation { field, st: ref_entity, op: UpdateRelationOp::Remove(delete_op), ref_info, rev_ref_info });
                            UpdateRelationOp::Create(parse_insert_nested(schema, ref_info, value)?)
                        },
                        "$connect" => {
                            let item_id = parse_id(schema, ref_entity, value)?;
                            match field.location {
                                FieldLocation::Key { .. } => return Err(EncodeError::OnlyBodyKeyAvailableToEdit(field.full_name.clone())),
                                FieldLocation::Body { offset_pos } => {
                                    update_fields.push(UpdateField { field, value: UpdateValue::Value(item_id.clone()), offset_pos });
                                }
                                FieldLocation::Virtual => {}
                            }

                            update_refs.push(UpdateRelation { field, st: ref_entity, op: UpdateRelationOp::DisconnectAll, ref_info, rev_ref_info });
                            UpdateRelationOp::Connect(vec![item_id])
                        }
                        _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
                    };

                    update_refs.push(UpdateRelation { field, st: ref_entity, op, ref_info, rev_ref_info });
                }
                continue;
            },
            FieldType::RefList(ref_info) => {
                let Some(obj) = value.as_object() else {
                    return Err(EncodeError::type_mismatch(field, "object"));
                };
                let ref_entity = &schema.models[ref_info.model_index];
                let rev_field = ref_info.rev_field_idx.map(|i| &ref_entity.fields[i]);
                let rev_ref_info = rev_field.and_then(|f| {
                    match &f.ty {
                        FieldType::Ref(ref_info) | FieldType::RefList(ref_info) => Some(ref_info),
                        _ => None
                    }
                });

                // `@list` relation: membership ops work on the inline id array (order-preserving)
                if matches!(ref_info.binding, RefBinding::IdList { .. }) {
                    for (key, value) in obj {
                        let op = match key.as_str() {
                            // The array is a sequence — duplicates are allowed, order is kept as given
                            "$set" => UpdateRelationOp::SetList(parse_many(field, value, |v| parse_id(schema, ref_entity, v))?),
                            "$connect" => UpdateRelationOp::ConnectList(unfold_array(value, |v| parse_id(schema, ref_entity, v))?),
                            "$remove" => UpdateRelationOp::DisconnectList(unfold_array(value, |v| parse_id(schema, ref_entity, v))?),
                            _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
                        };
                        update_refs.push(UpdateRelation { field, st: ref_entity, op, ref_info, rev_ref_info });
                    }
                    continue;
                }

                // The partner owns the relation as an inline @list id array — this (tree-backed) side
                // stores nothing, so membership can only be changed through the @list field
                if let Some(list_field) = rev_id_list_field(schema, ref_info) {
                    return Err(EncodeError::MutateViaListSide(field.full_name.clone(), list_field));
                }

                for (key, value) in obj {
                    let op = match key.as_str() {
                        "$push" => {
                            UpdateRelationOp::Push(unfold_array(value, |v| parse_insert_nested(schema, ref_info, v))?)
                        },
                        "$remove" => {
                            if ref_entity.autoinsert {
                                let delete_op = prepare_delete(schema, entity, None, rev_field);
                                UpdateRelationOp::RemoveItems(unfold_array(value, |v| parse_id(schema, ref_entity, v))?, delete_op)
                            } else {
                                UpdateRelationOp::Disconnect(unfold_array(value, |v| parse_id(schema, ref_entity, v))?)
                            }
                        },
                        // "$updateAll" => {
                        //     UpdateRelationOp::UpdateAll(parse_update(schema, ref_entity, value)?)
                        // },
                        "$set" => {
                            let write_op = parse_many(field, value, |v| parse_insert_nested(schema, ref_info, v))?;
                            
                            let delete_op = prepare_delete(schema, ref_entity, None, rev_field);
                            if write_op.is_empty() {
                                UpdateRelationOp::RemoveAll(delete_op)
                            } else {
                                update_refs.push(UpdateRelation { field, st: ref_entity, op: UpdateRelationOp::Remove(delete_op), ref_info, rev_ref_info });
                                UpdateRelationOp::Push(write_op)
                            }
                        },
                        "$connect" => {
                            UpdateRelationOp::Connect(unfold_array(value, |v| parse_id(schema, ref_entity, v))?)
                        },
                        // "$disconnect" => {
                        //     UpdateRelationOp::Disconnect(parse_one_or_many(value, |v| parse_id(schema, ref_entity, v))?)
                        // },
                        _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
                    };
                    update_refs.push(UpdateRelation { field, st: ref_entity, op, ref_info, rev_ref_info });
                }
                continue;
            },
            _ => {}
        }

        let FieldLocation::Body { offset_pos } = field.location else {
            return Err(EncodeError::OnlyBodyKeyAvailableToEdit(field.full_name.clone()));
        };

        // When changing an enum value, we clear the fields of other variants, otherwise their data
        // would stay in the body and "resurrect" when switching the variant back.
        // These Null operations are added BEFORE writing the enum itself: their exists_condition
        // is checked against the not-yet-changed enum value
        if let FieldType::Enum(enum_info) = &field.ty {
            let new_variant = value.as_str().and_then(|s| enum_info.variants_map.get(s)).copied();
            for variant_field in entity.fields.iter() {
                let FieldExistsCondition::EnumValue { field_index: cond_field_index, variants } = &variant_field.condition else {
                    continue;
                };
                // A shared field that also belongs to the new variant is not cleared
                if *cond_field_index != field_index || new_variant.is_some_and(|v| variants.contains(&v)) {
                    continue;
                }
                let FieldLocation::Body { offset_pos } = variant_field.location else {
                    continue;
                };
                update_fields.push(UpdateField { field: variant_field, value: UpdateValue::Null, offset_pos });
            }
        }

        if value.is_null() {
            update_fields.push(UpdateField { field, value: UpdateValue::Null, offset_pos });
            continue;
        }

        update_fields.push(UpdateField { field, value: parse_field(field, value)?, offset_pos });
    }

    Ok(UpdateOp {
        update_fields,
        update_refs
    })
}

fn parse_field<'a>(field: &'a Field, value: &Value) -> Result<UpdateValue, EncodeError> {
    return match &field.ty {
        FieldType::Primitive(primitive_field_type) => {
            // A Json field's value is itself a JSON document (object/array/scalar) — encode it directly.
            // It must not be interpreted as an update operator (the `{ "$..." }` object form below).
            if matches!(primitive_field_type, PrimitiveFieldType::Json) {
                let mut dst = vec![];
                encode_primitive_value(&mut dst, field, primitive_field_type, value)?;
                return Ok(UpdateValue::Value(dst));
            }
            if let Some(obj) = value.as_object() {
                if obj.len() != 1 {
                    return Err(EncodeError::OnlyOneKeyExpected(field.full_name.clone(), value.to_string()))
                }
                let (key, value) = obj.iter().next().unwrap();
                return match key.as_str() {
                    "$increment" => {
                        Ok(UpdateValue::Increment(parse_field_value_delta(field, value)?))
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

fn unfold_array<T, F>(value: &Value, f: F,) -> Result<Vec<T>, EncodeError>
where F: Fn(&Value) -> Result<T, EncodeError> {
    if let Some(arr) = value.as_array() {
        arr.iter().map(|v| f(v)).collect()
    } else {
        Ok(vec![f(value)?])
    }
}

fn parse_many<T, F>(field: &Field, value: &Value, f: F,) -> Result<Vec<T>, EncodeError>
where F: Fn(&Value) -> Result<T, EncodeError> {
    if let Some(arr) = value.as_array() {
        arr.iter().map(|v| f(v)).collect()
    } else {
        Err(EncodeError::type_mismatch(field, "array"))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use crate::{json_parsers::EncodeError, parse_schema, parse_update};

    /// `$set`/`$ensure` on a ref field in body (FK) — a typed error, not a panic (`todo!()`)
    #[test]
    fn update_nested_write_on_body_ref_errors() {
        let schema = parse_schema("model User {\n  name String\n}\nmodel Post {\n  title String\n  author User?\n}");
        let post = &schema.models[1];

        let set = parse_update(&schema, post, &json!({ "author": { "$set": { "name": "x" } } }));
        assert!(matches!(set, Err(EncodeError::NestedWriteNotSupported { .. })), "got {:?}", set);

        let ensure = parse_update(&schema, post, &json!({ "author": { "$ensure": { "name": "x" } } }));
        assert!(matches!(ensure, Err(EncodeError::NestedWriteNotSupported { .. })), "got {:?}", ensure);
    }
}