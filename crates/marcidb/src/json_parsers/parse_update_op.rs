use serde_json::{Map, Value};

use crate::{Field, delete_op::prepare_delete, json_parsers::{EncodeError, parse_write_op::parse_insert_nested, parsers::{encode_enum, encode_id_value, encode_list, encode_primitive_value, parse_field_value_delta, rev_id_list_field}}, parse_id, schema::{Entity, FieldExistsCondition, FieldLocation, FieldType, PrimitiveFieldType, RefBinding, RefInfo, Schema}, update_op::{ListOp, UpdateField, UpdateOp, UpdateRelation, UpdateRelationOp, UpdateValue}};

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
                parse_ref_update(schema, field, ref_info, value, &mut update_fields, &mut update_refs)?;
                continue;
            },
            FieldType::RefList(ref_info) => {
                parse_ref_list_update(schema, field, ref_info, value, &mut update_refs)?;
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

/// Resolves the back-reference field and its RefInfo for a relation's target model
fn rev_ref_of<'a>(ref_entity: &'a Entity, ref_info: &RefInfo) -> (Option<&'a Field>, Option<&'a RefInfo>) {
    let rev_field = ref_info.rev_field_idx.map(|i| &ref_entity.fields[i]);
    let rev_ref_info = rev_field.and_then(|f| match &f.ty {
        FieldType::Ref(ref_info) | FieldType::RefList(ref_info) => Some(ref_info),
        _ => None
    });
    (rev_field, rev_ref_info)
}

/// Update operators for a to-one relation. The two operator families never mix (see the
/// relation-op matrix in docs/API.md): an owned (struct) child takes content ops — `$update`,
/// `$ensure`, `$set`, `null` = delete the child; an independent model takes link ops —
/// `$connect`, `null` = disconnect.
fn parse_ref_update<'a>(
    schema: &'a Schema,
    field: &'a Field,
    ref_info: &'a RefInfo,
    value: &Value,
    update_fields: &mut Vec<UpdateField<'a>>,
    update_refs: &mut Vec<UpdateRelation<'a>>,
) -> Result<(), EncodeError> {
    let ref_entity = &schema.models[ref_info.model_index];
    let (rev_field, rev_ref_info) = rev_ref_of(ref_entity, ref_info);

    if value.is_null() {
        match field.location {
            FieldLocation::Key { .. } => return Err(EncodeError::OnlyBodyKeyAvailableToEdit(field.full_name.clone())),
            FieldLocation::Body { offset_pos } => {
                update_fields.push(UpdateField { field, value: UpdateValue::Null, offset_pos });
            }
            FieldLocation::Virtual => {}
        }
        // An owned child cannot exist without its parent — nulling the field deletes it;
        // an independent model is merely disconnected
        let op = if ref_entity.autoinsert {
            UpdateRelationOp::Remove(prepare_delete(schema, ref_entity, None, rev_field))
        } else {
            UpdateRelationOp::DisconnectAll
        };
        update_refs.push(UpdateRelation { field, st: ref_entity, op, ref_info, rev_ref_info });
        return Ok(());
    }

    let Some(obj) = value.as_object() else {
        return Err(EncodeError::type_mismatch(field, "object"));
    };
    for (key, value) in obj {
        let op = match (key.as_str(), ref_entity.autoinsert) {
            ("$update", true) => {
                UpdateRelationOp::Update(parse_update(schema, ref_entity, value)?)
            },
            ("$ensure", true) => {
                if matches!(field.location, FieldLocation::Body { .. } | FieldLocation::Key { .. }) {
                    return Err(EncodeError::NestedWriteNotSupported { field: field.full_name.clone(), op: key.clone() });
                }
                UpdateRelationOp::Create(parse_insert_nested(schema, ref_info, value)?)
            },
            ("$set", true) => {
                if matches!(field.location, FieldLocation::Body { .. } | FieldLocation::Key { .. }) {
                    return Err(EncodeError::NestedWriteNotSupported { field: field.full_name.clone(), op: key.clone() });
                }
                let delete_op = prepare_delete(schema, ref_entity, None, rev_field);
                update_refs.push(UpdateRelation { field, st: ref_entity, op: UpdateRelationOp::Remove(delete_op), ref_info, rev_ref_info });
                UpdateRelationOp::Create(parse_insert_nested(schema, ref_info, value)?)
            },
            ("$connect", false) => {
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
            },
            ("$update" | "$ensure" | "$set", false) => {
                return Err(EncodeError::OwnedRelationOnly { field: field.full_name.clone(), op: key.clone() });
            },
            ("$connect", true) => {
                return Err(EncodeError::LinkedRelationOnly { field: field.full_name.clone(), op: key.clone() });
            },
            _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
        };
        update_refs.push(UpdateRelation { field, st: ref_entity, op, ref_info, rev_ref_info });
    }
    Ok(())
}

/// Update operators for a to-many relation, dispatched by the relation kind (see the
/// relation-op matrix in docs/API.md): a `@list` field edits its ordered id array; an owned
/// (struct) list creates/edits/deletes its children; a tree-backed model list only links and
/// unlinks existing rows.
fn parse_ref_list_update<'a>(
    schema: &'a Schema,
    field: &'a Field,
    ref_info: &'a RefInfo,
    value: &Value,
    update_refs: &mut Vec<UpdateRelation<'a>>,
) -> Result<(), EncodeError> {
    let Some(obj) = value.as_object() else {
        return Err(EncodeError::type_mismatch(field, "object"));
    };
    let ref_entity = &schema.models[ref_info.model_index];
    let (rev_field, rev_ref_info) = rev_ref_of(ref_entity, ref_info);

    // `@list` relation: membership ops work on the inline id array (order-preserving).
    // The array is a sequence — duplicates are allowed, order is kept as given
    if matches!(ref_info.binding, RefBinding::IdList { .. }) {
        for (key, value) in obj {
            let op = match key.as_str() {
                "$set" => UpdateRelationOp::SetList(parse_many(field, value, |v| parse_id(schema, ref_entity, v))?),
                "$connect" => UpdateRelationOp::ConnectList(unfold_array(value, |v| parse_id(schema, ref_entity, v))?),
                "$connectUnique" => UpdateRelationOp::ConnectUniqueList(unfold_array(value, |v| parse_id(schema, ref_entity, v))?),
                "$remove" => UpdateRelationOp::DisconnectList(unfold_array(value, |v| parse_id(schema, ref_entity, v))?),
                _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
            };
            update_refs.push(UpdateRelation { field, st: ref_entity, op, ref_info, rev_ref_info });
        }
        return Ok(());
    }

    // The partner owns the relation as an inline @list id array — this (tree-backed) side
    // stores nothing, so membership can only be changed through the @list field
    if let Some(list_field) = rev_id_list_field(schema, ref_info) {
        return Err(EncodeError::MutateViaListSide(field.full_name.clone(), list_field));
    }

    for (key, value) in obj {
        let op = match (key.as_str(), ref_entity.autoinsert) {
            // Owned (struct) children — content ops: the elements themselves are created,
            // edited and deleted through the parent
            ("$push", true) => {
                UpdateRelationOp::Push(unfold_array(value, |v| parse_insert_nested(schema, ref_info, v))?)
            },
            ("$update", true) => {
                UpdateRelationOp::UpdateItems(parse_update_items(schema, ref_entity, field, value)?)
            },
            ("$remove", true) => {
                let delete_op = prepare_delete(schema, ref_entity, None, rev_field);
                UpdateRelationOp::RemoveItems(unfold_array(value, |v| parse_child_id(schema, ref_entity, v))?, delete_op)
            },
            ("$set", true) => {
                let write_op = parse_many(field, value, |v| parse_insert_nested(schema, ref_info, v))?;
                let delete_op = prepare_delete(schema, ref_entity, None, rev_field);
                if write_op.is_empty() {
                    UpdateRelationOp::RemoveAll(delete_op)
                } else {
                    // Replace = delete every existing child, then create the new ones
                    update_refs.push(UpdateRelation { field, st: ref_entity, op: UpdateRelationOp::RemoveAll(delete_op), ref_info, rev_ref_info });
                    UpdateRelationOp::Push(write_op)
                }
            },
            // Independent models — link ops: only the relation changes, never the rows
            ("$connect", false) => {
                UpdateRelationOp::Connect(unfold_array(value, |v| parse_id(schema, ref_entity, v))?)
            },
            ("$remove", false) => {
                UpdateRelationOp::Disconnect(unfold_array(value, |v| parse_id(schema, ref_entity, v))?)
            },
            ("$set", false) => {
                UpdateRelationOp::SetLinks(parse_many(field, value, |v| parse_id(schema, ref_entity, v))?)
            },
            ("$push" | "$update", false) => {
                return Err(EncodeError::OwnedRelationOnly { field: field.full_name.clone(), op: key.clone() });
            },
            ("$connect", true) => {
                return Err(EncodeError::LinkedRelationOnly { field: field.full_name.clone(), op: key.clone() });
            },
            _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
        };
        update_refs.push(UpdateRelation { field, st: ref_entity, op, ref_info, rev_ref_info });
    }
    Ok(())
}

/// `$update: { ...childId, data } | [{ ...childId, data }]` — partial update of single owned
/// children. Each item carries the child's key fields inline (the same shape query results
/// return them in, e.g. `{ id: 3, data: {...} }`) plus the changes under `data`
fn parse_update_items<'a>(schema: &'a Schema, ref_entity: &'a Entity, field: &Field, value: &Value) -> Result<Vec<(Vec<u8>, UpdateOp<'a>)>, EncodeError> {
    unfold_array(value, |v| {
        let obj = v.as_object().ok_or(EncodeError::NotAnObject)?;
        let data = obj.get("data").ok_or_else(|| EncodeError::MissingRequiredField(format!("{}.$update.data", field.full_name)))?;
        Ok((parse_child_id(schema, ref_entity, v)?, parse_update(schema, ref_entity, data)?))
    })
}

/// Parses an owned child's id the way the client sees it: the child's own key fields, without
/// the hidden `@parent_id` prefix. The parent id is prepended at process time (the child's
/// storage key is `parent_id ++ own_id`), so another parent's children are unreachable by
/// construction.
fn parse_child_id(schema: &Schema, ref_entity: &Entity, json_val: &Value) -> Result<Vec<u8>, EncodeError> {
    let Some(obj) = json_val.as_object() else {
        return Err(EncodeError::NotAnObject);
    };

    let mut id = vec![];
    for field in ref_entity.fields.iter() {
        let FieldLocation::Key { .. } = field.location else { continue };
        // Hidden system key fields (`@parent_id`) are supplied by the update context, not the client
        if field.name.starts_with('@') { continue }
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
            // `{ $... }` is an in-place list operator; a bare array (or hex string for Byte
            // lists) replaces the whole value
            if value.is_object() {
                return parse_list_op(field, value, primitive_field_type, *fixed_size);
            }
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

/// Primitive-list operators, mirroring the `@list` relation semantics: `$set` replaces the
/// array (also the positional-edit path — send the full new array), `$push` appends
/// (an already-present value gains another occurrence), `$pushUnique` appends only absent
/// values, `$remove` removes every occurrence.
fn parse_list_op(field: &Field, value: &Value, primitive_type: &PrimitiveFieldType, fixed_size: Option<usize>) -> Result<UpdateValue, EncodeError> {
    // A fixed-size list cannot change length, so in-place operators don't apply
    if fixed_size.is_some() {
        return Err(EncodeError::FixedSizeList(field.full_name.clone()));
    }
    let obj = value.as_object().expect("parse_list_op takes an operator object");
    if obj.len() != 1 {
        return Err(EncodeError::OnlyOneKeyExpected(field.full_name.clone(), value.to_string()));
    }
    let (key, value) = obj.iter().next().unwrap();
    let op = match key.as_str() {
        "$set" => {
            let mut dst = vec![];
            encode_list(&mut dst, value, field, primitive_type, None)?;
            return Ok(UpdateValue::Value(dst));
        }
        "$push" => ListOp::Push,
        "$pushUnique" => ListOp::PushUnique,
        "$remove" => ListOp::Remove,
        _ => return Err(EncodeError::UnsupportedOperation(key.clone()))
    };
    let items = unfold_array(value, |v| {
        let mut dst = vec![];
        encode_primitive_value(&mut dst, field, primitive_type, v)?;
        Ok(dst)
    })?;
    Ok(UpdateValue::ListOp { op, items, elem_size: primitive_type.get_size() })
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

    /// `$set`/`$ensure` create owned children — on a relation to an independent model they are
    /// rejected with a typed error (link with `$connect`, update through the model's collection)
    #[test]
    fn update_nested_write_on_model_ref_errors() {
        let schema = parse_schema("model User {\n  name String\n}\nmodel Post {\n  title String\n  author User?\n}");
        let post = &schema.models[1];

        let set = parse_update(&schema, post, &json!({ "author": { "$set": { "name": "x" } } }));
        assert!(matches!(set, Err(EncodeError::OwnedRelationOnly { .. })), "got {:?}", set);

        let ensure = parse_update(&schema, post, &json!({ "author": { "$ensure": { "name": "x" } } }));
        assert!(matches!(ensure, Err(EncodeError::OwnedRelationOnly { .. })), "got {:?}", ensure);
    }
}