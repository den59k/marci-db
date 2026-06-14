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

/// Sets RefBinding for Ref and RefList
pub fn resolve_ref_bindings(models: &mut [Entity]) {
    let model_names: Vec<String> = models.iter().map(|f| f.name.clone()).collect();
    for model in models.iter_mut() {
        let key_fields_count = model.fields.iter().filter(|f| matches!(f.location, FieldLocation::Key { .. })).count();
        for (field_index, field) in model.fields.iter_mut().enumerate() {
            let (FieldType::Ref(ref_info) | FieldType::RefList(ref_info)) = &mut field.ty else {
                continue;
            };

            // We don't set indexes for the first field, since such an element can simply be found by key
            if field_index == 0 && key_fields_count == 1 {
                ref_info.binding = RefBinding::CurrentId;
                continue;
            }
            // The same applies to the table we reference - if its field comes first, then that element is located directly in the table
            if let Some(ref_field_idx) = ref_info.rev_field_idx && ref_field_idx == 0 { 
                ref_info.binding = RefBinding::CurrentId;
                continue;
            }
            // Also there's no point setting indexes on non-virtual fields if there is no back-reference
            if !matches!(field.location, FieldLocation::Virtual) {
                ref_info.binding = RefBinding::FieldValue;
                continue;
            }

            let tree_name = format!("{}->{}", &field.full_name, &model_names[ref_info.model_index]);
            ref_info.binding = RefBinding::IndexTree(tree_name);
        }
    }
}


// Splits the string by the last dot (Project.users.user -> [ Project.users, user ])
pub fn split_by_last_dot(value: &str) -> (Option<&str>,&str) {
    let Some(last_index) = value.rfind('.') else {
        return (None,value)
    };

    return (Some(&value[..last_index]),&value[last_index+1..]);
}
