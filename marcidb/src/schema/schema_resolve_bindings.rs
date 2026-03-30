use std::collections::HashMap;

use crate::{Field, FieldRef, schema::{Entity, FieldLocation, FieldType, RefBinding, schema_attributes::Attribute}};


/// Связывает между собой @bind поля
pub fn resolve_bind_refs(models: &mut [Entity]) {

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
                ref_model_index = models.iter().position(|m| m.name == table_name).unwrap_or_else(|| {
                    panic!("Cannot find bind field {}.{} ({})", table_name, bind_field_name, field.full_name);
                });

                st_refs.insert(FieldRef::new(model_index, field_index), ref_model_index);
            }

            let ref_model = &models[ref_model_index];
            let ref_field_index = ref_model.fields.iter().position(|f| &f.name == bind_field_name).unwrap_or_else(|| {
                panic!("Cannot find nested bind field {}.{} ({})", ref_model.name, bind_field_name, field.full_name);
            });
            
            field_bindings.push((FieldRef::new(model_index, field_index), FieldRef::new(ref_model_index, ref_field_index)));
        }
    }    

    // TODO: можно добавить @inject поля здесь
    for (field_a_ref, field_b_ref) in field_bindings.iter() {
        check_one_to_one(models, field_a_ref, field_b_ref);

        let field_a = &mut models[field_a_ref.model_index].fields[field_a_ref.field_index];
        update_rev_index(field_a, field_a_ref, field_b_ref, &st_refs);
        
        let field_b = &mut models[field_b_ref.model_index].fields[field_b_ref.field_index];
        update_rev_index(field_b, field_b_ref, field_a_ref, &st_refs);
    }
}

// Для OneToOne один из полей должен быть @unique (или @id). Также одному из полей нужно проставить FieldLocation::Virtual
fn check_one_to_one(models: &mut [Entity], field_a_ref: &FieldRef, field_b_ref: &FieldRef) {
    if !is_one_to_one_binding(
        &models[field_a_ref.model_index].fields[field_a_ref.field_index], 
        &models[field_b_ref.model_index].fields[field_b_ref.field_index]
    ) {
        return;
    }

    let ref_a_unique = is_unique_ref(&models[field_a_ref.model_index].fields[field_a_ref.field_index]);
    let ref_b_unique = is_unique_ref(&models[field_b_ref.model_index].fields[field_b_ref.field_index]);
    match (ref_a_unique, ref_b_unique) {
        (true, true) => {
            let field_a = &models[field_a_ref.model_index].fields[field_a_ref.field_index];
            let field_b = &models[field_a_ref.model_index].fields[field_a_ref.field_index];
            panic!("Remove @unique from field {} or {}", field_a.full_name, field_b.full_name)
        },
        (true, false) => {
            models[field_b_ref.model_index].fields[field_b_ref.field_index].location = FieldLocation::Virtual;
        },
        (false, true) => {
            models[field_a_ref.model_index].fields[field_a_ref.field_index].location = FieldLocation::Virtual;
        },
        (false, false) => {
            panic_one_to_one(
                &models[field_a_ref.model_index].fields[field_a_ref.field_index], 
                &models[field_b_ref.model_index].fields[field_b_ref.field_index]
            );
        }
    }
}

fn is_unique_ref(a: &Field) -> bool {
    return matches!(a.location, FieldLocation::Key { .. }) || matches!(&a.ty, FieldType::Ref(ref_info_a) if ref_info_a.is_unique)
}

fn is_one_to_one_binding(a: &Field, b: &Field) -> bool { 
    return matches!(&a.ty, FieldType::Ref(_)) && matches!(&b.ty, FieldType::Ref(_))
}

fn panic_one_to_one(a: &Field, b: &Field) {
    if a.attributes.iter().any(|f| matches!(f, Attribute::BindUnresolved(_))) {
        panic!("OneToOne binding failed: Field {} must be unique", b.full_name);
    } else if b.attributes.iter().any(|f| matches!(f, Attribute::BindUnresolved(_))) {
        panic!("OneToOne binding failed: Field {} must be unique", a.full_name);
    } else {
        panic!("OneToOne binding failed: One of the fields must be unique  {} {}", a.full_name, b.full_name);
    }
}

/// Проставляет rev_field_idx для ref_info. Также корректирует model_index, если ссылка была на структуру
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

/// Проставляет RefBinding для Ref и RefList
pub fn resolve_ref_bindings(models: &mut [Entity]) {
    let model_names: Vec<String> = models.iter().map(|f| f.name.clone()).collect();
    for model in models.iter_mut() {
        for (field_index, field) in model.fields.iter_mut().enumerate() {
            let (FieldType::Ref(ref_info) | FieldType::RefList(ref_info)) = &mut field.ty else {
                continue;
            };

            // Мы не ставим индексы для первого поля, поскольку такой элемент можно просто найти по ключу
            if field_index == 0 {
                continue;
            }
            // То же самое касается таблицы на которую ссылаемся - если поле стоит первым элементом, то он находится сразу в таблице
            if let Some(ref_field_idx) = ref_info.rev_field_idx && ref_field_idx == 0 { 
                continue;
            }
            // Также нам незачем ставить индексы на не виртуальные поля, если нет обратной ссылки
            if !matches!(field.location, FieldLocation::Virtual) {
                ref_info.binding = RefBinding::FieldValue;
                continue;
            }

            let tree_name = format!("{}->{}", &field.full_name, &model_names[ref_info.model_index]);
            ref_info.binding = RefBinding::IndexTree(tree_name);
        }
    }
}


// Разделяет строку по последней точке (Project.users.user -> [ Project.users, user ])
pub fn split_by_last_dot(value: &str) -> (Option<&str>,&str) {
    let Some(last_index) = value.rfind('.') else {
        return (None,value)
    };

    return (Some(&value[..last_index]),&value[last_index+1..]);
}
