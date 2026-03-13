use std::collections::HashMap;

use crate::schema::{Entity, FieldDefault, Schema, schema_field::{Field, FieldLocation, FieldType, parse_field_raw}};

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

    for model in models.iter_mut() {
      resolve_model_id(model);
    }

    let mut model_structs: Vec<Entity> = vec![];
    for model in models.iter_mut() {
        resolve_structs(model, &structs, &mut model_structs);
    }
    models.extend(model_structs);

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

  for field in entity.fields.iter_mut(){
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
                field.ty = FieldType::Ref(*model_index);
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
                field.ty = FieldType::RefList(*model_index);
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

#[cfg(test)]
mod tests {
  use crate::schema::{parse_schema};

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

  }
}
