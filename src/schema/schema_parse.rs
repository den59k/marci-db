use std::{collections::HashMap};

use crate::{FieldRef, schema::{Entity, FieldDefault, Schema, schema_attributes::Attribute, schema_field::{Field, FieldLocation, FieldType, parse_field_raw}}};

pub fn parse_schema(input: &str) -> Schema {
    let mut models = Vec::new();
    let mut structs: HashMap<String, Entity> = HashMap::new();
    // let mut enums: HashMap<String,EnumDef> = HashMap::new();
    let mut lines = input.lines().peekable();

    while let Some(line) = lines.next() {
        let line = line.trim();
        if !line.starts_with("model ") && !line.starts_with("struct ") && !line.starts_with("enum ") {
            continue;
        }
        let (kind, rest) = line.trim().split_once(' ').unwrap(); 
        let name = rest.trim_end_matches('{').trim().to_string();

        match kind.trim() {
            "model" => {
                models.push(parse_model_block(name, &mut lines));
            },
            "struct" => {
                structs.insert(name, parse_struct_block(&mut lines));
            },
            // "enum" => {
            //     enums.insert(name.clone(), parse_enum_block(name, &mut lines));
            // }
            _ => {}
        }
    }

    // Добавляем всем моделям обязательный id, если его нет
    for model in models.iter_mut() {
      resolve_model_id(model);
    }

    let mut model_structs: Vec<Entity> = vec![];
    for model in models.iter_mut() {
        resolve_structs(model, &structs, &mut model_structs);
    }
    models.extend(model_structs);

    // На этом этапе у нас есть конечный список моделей с конечным списком ID. Остается только скорректировать ссылки
    let model_by_name = models.iter().enumerate()
        .map(|(i, m)| (m.name.clone(), i))
        .collect();

    for (model_index, model) in models.iter_mut().enumerate() {
        resolve_refs(model, model_index, &model_by_name);
    }

    for model in models.iter_mut() {
        resolve_field_offsets(model);
    }

    let mut counter_id = 0;
    for model in models.iter_mut() {
        resolve_counter_idx(model, &mut counter_id);
    }
    
    resolve_derived_refs(&mut models);

    // let mut ref_indexes = vec![];
    // for (model_index, model) in models.iter().enumerate() {
    //   for (field_index, field) in model.fields.iter().enumerate() {
    //     let FieldType::RefList(ref_model_index) = field.ty else {
    //       continue
    //     };
    //     let ref_model = &models[ref_model_index];
    //     // Пропускаем те модели, у которых первым значением в ID стоит ссылка на текущую таблицу
    //     if matches!(ref_model.fields[0].ty, FieldType::Ref(idx) if idx == model_index) {
    //       continue;
    //     }
    //     ref_indexes.push((model_index,ref_model_index,field_index));
    //   }
    // }

    // for (model_index,ref_model_index,field_index) in ref_indexes {

    // }

    // resolve_attributes(&mut schema, &model_by_name);

    // resolve_foreign_constraints(&mut schema);

    // let mut counter_id = 0;
    // for model in schema.models.iter_mut() {
    //     for field in model.fields.iter_mut() {
    //         if field.counter_idx.is_some() {
    //             field.counter_idx = Some(counter_id);
    //             counter_id += 1;
    //         }
    //     }
    // }

    Schema { models }
}

pub fn parse_model_block(name: String, lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Entity {

    let fields = parse_fields(lines);
    // update_key_fields(&mut fields);

    return Entity { name, fields, payload_offset: 0, autoinsert: false };
}

pub fn parse_struct_block(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Entity {
    let fields = parse_fields(lines);

    return Entity { name: String::new(), fields: fields, payload_offset: 0, autoinsert: true }
}

pub fn parse_fields(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Vec<Field> {
    let mut fields = Vec::new();

    for line in lines {
        let line = line.trim();
        if line == "}" { break }
        if line.is_empty() { continue; }      
        fields.push(parse_field_raw(line));
    }

    return fields;
}

// Добавляет generated_id для модели, если нет собственного id 
fn resolve_model_id(entity: &mut Entity) {
  if !entity.fields.iter().any(|f| f.is_id()) {
    entity.fields.insert(0, Field::new_id());
  }
}

// Находит структуры и превращает их в модели. Также проставляет всем fields field.full_name
fn resolve_structs(entity: &mut Entity, structs: &HashMap<String, Entity>, model_structs: &mut Vec<Entity>) {

  for field in entity.fields.iter_mut(){
    let field_full_name: String = [ &entity.name, ".", &field.name ].concat();
    field.full_name = field_full_name.clone();
  }

  for field in entity.fields.iter_mut() {
    match &mut field.ty {
      FieldType::RefUnresolved(name) | FieldType::RefListUnresolved(name) => {
        let Some(st) = structs.get(name) else {
          continue;
        };
        
        field.location = FieldLocation::Virtual;
        
        let mut entity_model = st.clone();
        entity_model.name = field.full_name.clone();
        *name = entity_model.name.clone();

        if matches!(&field.ty, FieldType::RefListUnresolved(_)) {
          resolve_model_id(&mut entity_model);
        }

        let mut parent_id = Field::new_id();
        parent_id.name = "@parent_id".to_string();
        parent_id.ty = FieldType::RefUnresolved(entity.name.to_string());
        parent_id.default_value = None;
        parent_id.attributes.push(Attribute::DerivedUnresolved(field.name.clone()));
        entity_model.fields.insert(0, parent_id);

        resolve_structs(&mut entity_model, structs, model_structs);

        model_structs.push(entity_model);
      },
      _ => {}
    }
  }
}

/// Находит нужные модели и структуры для ссылок RefUnresolved и RefListUnresolved
fn resolve_refs(
    entity: &mut Entity,
    _model_index: usize,
    model_by_name: &HashMap<String, usize>
    // structs: &HashMap<String, Entity>,
    // enums: &HashMap<String, EnumDef>,
) {
    for field in entity.fields.iter_mut(){
        match &field.ty {
            FieldType::RefUnresolved(name) => {
              if let Some(model_index) = model_by_name.get(name) {
                field.ty = FieldType::Ref { model_index: *model_index, rev_field_idx: None, st_index: None };
              } else {
                panic!("Unknown type {}", name)
              }
              
                // if let Some(en) = enums.get(name) {
                //     let mut en = en.clone();
                //     for variant in en.variants.iter_mut() {
                //         // Example key for enum fields - User[role=admin].features
                //         let name = [ model_name, "[", &field.name, "=", &variant.name, "]" ].concat();
                //         resolve_fields(&mut variant.fields, model_index, &name, model_by_name);
                //     }
                //     field.ty = FieldType::Enum(en);
                // } else if let Some(st) = structs.get(name) {
                //     let mut st = st.clone();
                //     st.name = field_full_name.clone();
                //     resolve_fields(&mut st.fields, model_index, &st.name, model_by_name);
                //     field.ty = FieldType::Struct(st);
                //     // StructOne идет вообще без ключа, поскольку она полностью наследует ключ родителя

                // } else if let Some(model_index) = model_by_name.get(name) {
                //     field.ty = FieldType::ModelRef(*model_index);
                // } else {
                //     panic!("Unknown type {}", name)
                // }
            }
            FieldType::RefListUnresolved(name) => {
              if let Some(model_index) = model_by_name.get(name) {
                field.ty = FieldType::RefList { model_index: *model_index, rev_field_idx: None, st_index: None };
                // let index_name = format!("{}.{}", model_name, field.name);
                // field.inserted_indexes.direct = Some(InsertedIndex { tree_name: index_name });
              } else {
                panic!("Unknown type {}", name)
              }

                // if let Some(_en) = enums.get(name) {
                //     todo!("Enum list not implemented yet");
                // } else if let Some(en) = enums.get(name) {
                //     let mut en = en.clone();
                //     for variant in en.variants.iter_mut() {
                //         resolve_fields(&mut variant.fields, model_index, model_name, model_by_name, structs, enums);
                //     }
                //     field.ty = FieldType::Enum(en);
                // } else if let Some(st) = structs.get(name) {
                //     let mut st = st.clone();
                //     st.name = field_full_name.clone();
                //     update_key_fields(&mut st.fields);

                //     // Мы увеличиваем ключ, сдвигая его, поскольку у нас в структуре первым идет ID родителя
                //     for field in st.fields.iter_mut() {
                //         if let Some(idx) = &mut field.id_idx {
                //             *idx += 1;
                //         }
                //     }
    
                //     st.fields.insert(0, Field { 
                //         name: "@parent".to_string(), 
                //         full_name: [ &st.name, ".@parent" ].concat(),
                //         ty: FieldType::ModelRef(model_index), 
                //         offset_pos: 0, 
                //         is_nullable: true, 
                //         id_idx: Some(0), 
                //         counter_idx: None, 
                //         inserted_indexes: InsertedIndexSt::new(), 
                //         attributes: vec![Attribute::Id],
                //         is_unique: false,
                //         injected_fields: None
                //     });

                //     resolve_fields(&mut st.fields, model_index, &st.name, model_by_name, structs, enums);
                //     field.ty = FieldType::StructList(st.clone());
                // } else if let Some(model_index) = model_by_name.get(name) {
                //     field.ty = FieldType::ModelRefList(*model_index);
                    
                //     // Связь ManyToOne / ManyToMany хранится в индексе
                //     let index_name = format!("{}.{}", model_name, field.name);
                //     field.inserted_indexes.direct = Some(InsertedIndex { tree_name: index_name });
                // } else {
                //     panic!("Unknown type {}", name)
                // }
            }
            _ => {}
        }
    }
}

// Назначает корректные index для Key полей и offset для Body полей
fn resolve_field_offsets(entity: &mut Entity) {
  let mut offset_index: usize = 0;
  let mut key_index: usize = 0;

  let pre_header_size = 3;

  for field in entity.fields.iter_mut(){
    match &mut field.location {
        FieldLocation::Key { index } => {
          *index = key_index;
          key_index += 1;
        },
        FieldLocation::Body { offset } => {
          *offset = pre_header_size + offset_index * 4;
          offset_index += 1;
        },
        FieldLocation::Virtual => {}
    }
  }

  entity.payload_offset = pre_header_size + offset_index * 4;
}

// Проставляет корректные counter_idx для всех моделей
fn resolve_counter_idx(entity: &mut Entity, counter_id: &mut usize) {
  for field in entity.fields.iter_mut() {
    if let Some(FieldDefault::Counter(counter_idx)) = &mut field.default_value {
      *counter_idx = *counter_id;
      *counter_id += 1;
    }
  }
}

fn split_derived_name(value: &str) -> (Option<&str>,&str) {
    let Some(last_index) = value.rfind('.') else {
        return (None,value)
    };

    return (Some(&value[..last_index]),&value[last_index+1..]);
}

fn resolve_derived_refs(models: &mut [Entity]) {

    let mut field_bindings = vec![];
    let mut st_refs = HashMap::new();

    for (model_index, model) in models.iter().enumerate() {
        for (field_index, field) in model.fields.iter().enumerate() {
            let (
                FieldType::Ref { model_index: ref_model_index, .. } | 
                FieldType::RefList { model_index: ref_model_index, .. }
            ) = field.ty else {
                continue;
            };
            let Some(derived_field_name) = field.attributes
                .iter()
                .find_map(|a| match a {
                    Attribute::DerivedUnresolved(name) => Some(name),
                    _ => None
                }) else { continue; };

            let (table_name,derived_field_name) = split_derived_name(derived_field_name);
            let mut ref_model_index = ref_model_index;
            if let Some(table_name) = table_name && table_name.contains(".") {
                ref_model_index = models.iter().position(|m| m.name == table_name).unwrap_or_else(|| {
                    panic!("Cannot find derived field {}.{} ({})", table_name, derived_field_name, field.full_name);
                });

                st_refs.insert(FieldRef::new(model_index, field_index), ref_model_index);
            }

            let ref_model = &models[ref_model_index];
            let ref_field_index = ref_model.fields.iter().position(|f| &f.name == derived_field_name).unwrap_or_else(|| {
                panic!("Cannot find nested derived field {}.{} ({})", ref_model.name, derived_field_name, field.full_name);
            });
            
            field_bindings.push((FieldRef::new(model_index, field_index), FieldRef::new(ref_model_index, ref_field_index)));
        }
    }    

    for (field_a_ref, field_b_ref) in field_bindings.iter() {
        let field_a = &mut models[field_a_ref.model_index].fields[field_a_ref.field_index];
        if let FieldType::Ref { rev_field_idx, st_index, .. } | FieldType::RefList { rev_field_idx, st_index, .. } = &mut field_a.ty {
            *rev_field_idx = Some(field_b_ref.field_index);
            *st_index = st_refs.get(field_a_ref).copied();
        }
        
        let field_b = &mut models[field_b_ref.model_index].fields[field_b_ref.field_index];
        if let FieldType::Ref { rev_field_idx, st_index, .. } | FieldType::RefList { rev_field_idx, st_index, .. } = &mut field_b.ty {
            *rev_field_idx = Some(field_a_ref.field_index);
            *st_index = st_refs.get(field_b_ref).copied();
        }
    }
}


#[cfg(test)]
mod tests {
  use crate::schema::{FieldType, parse_schema};

  #[test]
  fn test_parse_schema() {
    let schema = parse_schema("
    model User {
        name        String
        projects    Project[]     @derived(Project.users.user)
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

    assert_eq!(schema.models.len(), 3);
    assert_eq!(schema.models[0].fields.len(), 3);

    // Сформирована структура Project.users, у нее 3 поля:
    // @parent_id
    // user
    // role
    assert_eq!(schema.models[2].fields.len(), 3);

    match schema.models[2].fields[1].ty {
        FieldType::Ref { model_index, rev_field_idx, st_index } => {
            assert_eq!(schema.models[model_index].name, "User");
            assert_eq!(st_index, None);
            assert_eq!(schema.models[model_index].fields[rev_field_idx.unwrap()].name, "projects");
        },
        _ => panic!("Wrong schema field {} type", schema.models[0].fields[2].name)
    }

    match schema.models[0].fields[2].ty {
        FieldType::RefList { model_index, rev_field_idx, st_index } => {
            assert_eq!(schema.models[model_index].name, "Project");
            assert_eq!(schema.models[st_index.unwrap()].name, "Project.users");
            assert_eq!(schema.models[st_index.unwrap()].fields[rev_field_idx.unwrap()].name, "user");
        },
        _ => panic!("Wrong schema field {} type", schema.models[0].fields[2].name)
    }

  }
}
