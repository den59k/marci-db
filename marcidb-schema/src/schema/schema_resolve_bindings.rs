use std::collections::HashMap;

use crate::{Field, FieldRef, schema::{Entity, FieldLocation, FieldType, RefBinding, SchemaError, schema_attributes::Attribute}};


/// Links @bind fields to each other
pub fn resolve_bind_refs(models: &mut [Entity]) -> Result<(), SchemaError> {

    let mut field_bindings = vec![];
    let mut st_refs = HashMap::new();

    for (model_index, model) in models.iter().enumerate() {
        for (field_index, field) in model.fields.iter().enumerate() {
            let (FieldType::Ref(ref_info) | FieldType::RefList(ref_info)) = &field.ty else {
                continue;
            };
            let Some(bind_field_name) = field.attributes
                .iter()
                .find_map(|a| match a {
                    Attribute::BindUnresolved(name) => Some(name),
                    _ => None
                }) else { continue; };

            let (table_name,bind_field_name) = split_by_last_dot(bind_field_name);
            let mut ref_model_index = ref_info.model_index;
            if let Some(table_name) = table_name && table_name.contains(".") {
                ref_model_index = models.iter().position(|m| m.name == table_name)
                    .ok_or_else(|| SchemaError(format!("Cannot find bind field {}.{} ({})", table_name, bind_field_name, field.full_name)))?;

                st_refs.insert(FieldRef::new(model_index, field_index), ref_model_index);
            }

            let ref_model = &models[ref_model_index];
            let ref_field_index = ref_model.fields.iter().position(|f| &f.name == bind_field_name)
                .ok_or_else(|| SchemaError(format!("Cannot find nested bind field {}.{} ({})", ref_model.name, bind_field_name, field.full_name)))?;

            field_bindings.push((FieldRef::new(model_index, field_index), FieldRef::new(ref_model_index, ref_field_index)));
        }
    }

    // TODO: @inject fields can be added here
    for (field_a_ref, field_b_ref) in field_bindings.iter() {
        check_one_to_one(models, field_a_ref, field_b_ref)?;

        let field_a = &mut models[field_a_ref.model_index].fields[field_a_ref.field_index];
        update_rev_index(field_a, field_a_ref, field_b_ref, &st_refs);

        let field_b = &mut models[field_b_ref.model_index].fields[field_b_ref.field_index];
        update_rev_index(field_b, field_b_ref, field_a_ref, &st_refs);
    }
    Ok(())
}

// For OneToOne one of the fields must be @unique (or @id). Also one of the fields needs to be set to FieldLocation::Virtual
fn check_one_to_one(models: &mut [Entity], field_a_ref: &FieldRef, field_b_ref: &FieldRef) -> Result<(), SchemaError> {
    if !is_one_to_one_binding(
        &models[field_a_ref.model_index].fields[field_a_ref.field_index],
        &models[field_b_ref.model_index].fields[field_b_ref.field_index]
    ) {
        return Ok(());
    }

    let ref_a_unique = is_unique_ref(&models[field_a_ref.model_index].fields[field_a_ref.field_index]);
    let ref_b_unique = is_unique_ref(&models[field_b_ref.model_index].fields[field_b_ref.field_index]);
    match (ref_a_unique, ref_b_unique) {
        (true, true) => {
            let field_a = &models[field_a_ref.model_index].fields[field_a_ref.field_index];
            let field_b = &models[field_a_ref.model_index].fields[field_a_ref.field_index];
            return Err(SchemaError(format!("Remove @unique from field {} or {}", field_a.full_name, field_b.full_name)));
        },
        (true, false) => {
            models[field_b_ref.model_index].fields[field_b_ref.field_index].location = FieldLocation::Virtual;
        },
        (false, true) => {
            models[field_a_ref.model_index].fields[field_a_ref.field_index].location = FieldLocation::Virtual;
        },
        (false, false) => {
            return Err(one_to_one_error(
                &models[field_a_ref.model_index].fields[field_a_ref.field_index],
                &models[field_b_ref.model_index].fields[field_b_ref.field_index]
            ));
        }
    }
    Ok(())
}

fn is_unique_ref(a: &Field) -> bool {
    return matches!(a.location, FieldLocation::Key { .. }) || matches!(&a.ty, FieldType::Ref(ref_info_a) if ref_info_a.is_unique)
}

fn is_one_to_one_binding(a: &Field, b: &Field) -> bool { 
    return matches!(&a.ty, FieldType::Ref(_)) && matches!(&b.ty, FieldType::Ref(_))
}

fn one_to_one_error(a: &Field, b: &Field) -> SchemaError {
    if a.attributes.iter().any(|f| matches!(f, Attribute::BindUnresolved(_))) {
        SchemaError(format!("OneToOne binding failed: Field {} must be unique", b.full_name))
    } else if b.attributes.iter().any(|f| matches!(f, Attribute::BindUnresolved(_))) {
        SchemaError(format!("OneToOne binding failed: Field {} must be unique", a.full_name))
    } else {
        SchemaError(format!("OneToOne binding failed: One of the fields must be unique  {} {}", a.full_name, b.full_name))
    }
}

/// Sets rev_field_idx for ref_info. Also adjusts model_index if the ref pointed to a struct
fn update_rev_index(field: &mut Field, field_ref: &FieldRef, rev_field_ref: &FieldRef, st_refs: &HashMap<FieldRef, usize>) {
    let (FieldType::Ref(ref_info) | FieldType::RefList(ref_info)) = &mut field.ty else {
        panic!("Wrong field ty. Expected: Ref or RefList {}", field.full_name);
    };
    ref_info.rev_field_idx = Some(rev_field_ref.field_index);
    if let Some(st_ref) = st_refs.get(field_ref) {
        ref_info.parent_index = Some(ref_info.model_index);
        ref_info.model_index = *st_ref;
    }
}

/// Cross-model field facts, snapshotted before the mutable binding pass below
struct FieldMeta {
    full_name: String,
    is_key: bool,
    virtual_reflist: bool,
}

/// Sets RefBinding for Ref and RefList
pub fn resolve_ref_bindings(models: &mut [Entity]) -> Result<(), SchemaError> {
    let model_names: Vec<String> = models.iter().map(|f| f.name.clone()).collect();
    let metas: Vec<Vec<FieldMeta>> = models.iter().map(|m| m.fields.iter().map(|f| FieldMeta {
        full_name: f.full_name.clone(),
        is_key: matches!(f.location, FieldLocation::Key { .. }),
        virtual_reflist: matches!(f.ty, FieldType::RefList(_)) && matches!(f.location, FieldLocation::Virtual),
    }).collect()).collect();
    let id_sizes: Vec<Option<usize>> = models.iter().map(|m| super::fixed_id_size(models, m)).collect();

    // (expected reverse tree, target model, rev field index, owner field name) — checked in the post-pass
    let mut id_list_revs: Vec<(String, usize, usize, String)> = vec![];

    for (model_index, model) in models.iter_mut().enumerate() {
        let model_name = model_names[model_index].clone();
        let key_fields_count = model.fields.iter().filter(|f| matches!(f.location, FieldLocation::Key { .. })).count();
        for (field_index, field) in model.fields.iter_mut().enumerate() {
            let has_list_attr = field.attributes.iter().any(|a| matches!(a, Attribute::List));
            let is_virtual = matches!(field.location, FieldLocation::Virtual);
            let is_body = matches!(field.location, FieldLocation::Body { .. });

            // `@list`: the relation is stored inline as an ordered id array in the row body,
            // plus a reverse index tree (`related_id ++ owner_id`) for back-references and delete integrity
            if has_list_attr {
                let FieldType::RefList(ref_info) = &mut field.ty else {
                    return Err(SchemaError(format!("@list is only allowed on relation lists (Model[]): {}", field.full_name)));
                };
                // A struct target flips the field to Virtual during desugaring — an owned collection has no id array
                if !is_body {
                    return Err(SchemaError(format!("@list relation {} must target a model, not a struct (owned collections cannot be stored as an id array)", field.full_name)));
                }
                if ref_info.parent_index.is_some() {
                    return Err(SchemaError(format!("@list relation {} cannot bind into a struct field", field.full_name)));
                }
                if id_sizes[ref_info.model_index].is_none() {
                    return Err(SchemaError(format!(
                        "@list relation {}: target {} must have a fixed-size id (variable-length keys are not supported in an inline id array)",
                        field.full_name, model_names[ref_info.model_index]
                    )));
                }
                let rev_tree = match ref_info.rev_field_idx {
                    Some(rev_idx) => {
                        let rev = &metas[ref_info.model_index][rev_idx];
                        if !rev.virtual_reflist {
                            return Err(SchemaError(format!(
                                "@list relation {}: the bound field {} must be a plain relation list (the id array lives on the @list side; the back-reference stores nothing)",
                                field.full_name, rev.full_name
                            )));
                        }
                        // Must match the IndexTree name the reverse side derives for itself below
                        let tree = format!("{}->{}", rev.full_name, model_name);
                        id_list_revs.push((tree.clone(), ref_info.model_index, rev_idx, field.full_name.clone()));
                        tree
                    }
                    // No back-reference declared — a hidden reverse tree, still required for delete integrity
                    None => format!("{}<-{}", field.full_name, model_names[ref_info.model_index]),
                };
                ref_info.binding = RefBinding::IdList { rev_tree };
                continue;
            }

            let (FieldType::Ref(ref_info) | FieldType::RefList(ref_info)) = &mut field.ty else {
                continue;
            };

            // We don't set indexes for the first field, since such an element can simply be found by key
            if field_index == 0 && key_fields_count == 1 {
                ref_info.binding = RefBinding::CurrentId;
                continue;
            }
            // The same applies to the table we reference - if its key field comes first, then that element
            // is located directly in the table. (A non-key partner at index 0 — e.g. a @list id array —
            // does not make the related rows reachable by key prefix.)
            if let Some(ref_field_idx) = ref_info.rev_field_idx && ref_field_idx == 0
                && metas[ref_info.model_index][ref_field_idx].is_key {
                ref_info.binding = RefBinding::CurrentId;
                continue;
            }
            // Also there's no point setting indexes on non-virtual fields if there is no back-reference
            if !is_virtual {
                ref_info.binding = RefBinding::FieldValue;
                continue;
            }

            let tree_name = format!("{}->{}", &field.full_name, &model_names[ref_info.model_index]);
            ref_info.binding = RefBinding::IndexTree(tree_name);
        }
    }

    // Post-pass: the declared reverse of every @list relation must have resolved to the expected IndexTree
    // (a partner caught by the CurrentId shortcuts above would silently break the reverse reads)
    for (expected_tree, target_model, rev_idx, owner_full_name) in id_list_revs {
        let rev_field = &models[target_model].fields[rev_idx];
        let (FieldType::Ref(ri) | FieldType::RefList(ri)) = &rev_field.ty else { continue };
        if !matches!(&ri.binding, RefBinding::IndexTree(tree) if tree == &expected_tree) {
            return Err(SchemaError(format!(
                "@list relation {}: the bound field {} did not resolve to an index-tree back-reference (move it after the model's @id field)",
                owner_full_name, rev_field.full_name
            )));
        }
    }

    Ok(())
}


// Splits the string by the last dot (Project.users.user -> [ Project.users, user ])
pub fn split_by_last_dot(value: &str) -> (Option<&str>,&str) {
    let Some(last_index) = value.rfind('.') else {
        return (None,value)
    };

    return (Some(&value[..last_index]),&value[last_index+1..]);
}
