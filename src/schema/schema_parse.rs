use std::{collections::HashMap};

use crate::{FieldRef, schema::{Entity, FieldDefault, RefInfo, Schema, schema_attributes::Attribute, schema_enum::{EnumDef, parse_enum_block}, schema_field::{EnumInfo, Field, FieldCondition, FieldLocation, FieldType, RefBinding, parse_field_raw}}};

pub fn parse_schema(input: &str) -> Schema {
    let mut models = Vec::new();
    let mut structs: HashMap<String, Entity> = HashMap::new();
    let mut enums: HashMap<String,EnumDef> = HashMap::new();
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
            "enum" => {
                enums.insert(name.clone(), parse_enum_block(&mut lines));
            }
            _ => {}
        }
    }

    // Добавляем всем моделям обязательный id, если его нет
    for model in models.iter_mut() {
      resolve_model_id(model);
    }

    let mut model_structs: Vec<Entity> = vec![];
    for model in models.iter_mut() {
        resolve_structs_and_enums(model, &structs, &enums, &mut model_structs);
    }
    models.extend(model_structs);

    // На этом этапе у нас есть конечный список моделей с конечным списком Fields. Остается только скорректировать ссылки
    let model_by_name = models.iter().enumerate()
        .map(|(i, m)| (m.name.clone(), i))
        .collect();

    for model in models.iter_mut() {
        resolve_refs(model, &model_by_name);
    }

    for model in models.iter_mut() {
        resolve_field_offsets(model);
    }

    let mut counter_id = 0;
    for model in models.iter_mut() {
        resolve_counter_idx(model, &mut counter_id);
    }
    
    resolve_derived_refs(&mut models);

    resolve_ref_bindings(&mut models);

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
        if line.starts_with("//") {
            continue;
        }
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
fn resolve_structs_and_enums(entity: &mut Entity, structs: &HashMap<String, Entity>, enums: &HashMap<String, EnumDef>, model_structs: &mut Vec<Entity>) {

    for field in entity.fields.iter_mut(){
        let field_full_name: String = format!("{}.{}", &entity.name, &field.name);
        field.full_name = field_full_name.clone();
    }

    let mut i = 0;
    loop {
        if i >= entity.fields.len() {
            break;
        }

        let field = &mut entity.fields[i];

        match &field.ty {
            FieldType::RefUnresolved(name) | FieldType::RefListUnresolved(name) => {
                if let Some(st) = structs.get(name) {
                    let mut entity_model = create_entity_model(field, st, &entity.name); 
                    field.location = FieldLocation::Virtual;
                    if let FieldType::RefUnresolved(name) | FieldType::RefListUnresolved(name) = &mut field.ty {
                        *name = entity_model.name.clone();
                    }
                    resolve_structs_and_enums(&mut entity_model, structs, enums, model_structs);
                    model_structs.push(entity_model);
                    i += 1;
                    continue;
                };

                if let Some(enum_def) = enums.get(name) {
                    if matches!(&field.ty, FieldType::RefListUnresolved(_)) {
                        panic!("{}[]: Enum list is not supported!", field.name)
                    }

                    let field_name = field.name.clone();
                    let enum_info = create_enum_info(enum_def, entity, &field_name, i);

                    entity.fields[i].ty = FieldType::Enum(enum_info);

                    i += 1;
                    continue;
                }
            },
            _ => {}
        }

        i += 1;
    }
}

// Создает новую модель из Struct
fn create_entity_model(field: &Field, st: &Entity, parent_name: &String) -> Entity {
    let mut entity_model = st.clone();
    entity_model.name = field.full_name.clone();

    if matches!(&field.ty, FieldType::RefListUnresolved(_)) {
        resolve_model_id(&mut entity_model);
    }

    let mut parent_id = Field::new_id();
    parent_id.name = "@parent_id".to_string();
    parent_id.ty = FieldType::RefUnresolved(parent_name.to_string());
    parent_id.default_value = None;
    parent_id.attributes.push(Attribute::DerivedUnresolved(field.name.clone()));
    entity_model.fields.insert(0, parent_id);

    entity_model
}

/// Добавляет поля из Enum в текущую Entity
fn create_enum_info(enum_def: &EnumDef, entity: &mut Entity, field_name: &String, field_index: usize) -> EnumInfo {
    let mut variants: HashMap<u16,Vec<usize>> = HashMap::new();
    for (variant_name, variant_fields) in enum_def.variants.iter() {
        let base_name = format!("{}[{}={}]", entity.name, field_name, variant_name);
        let mut variant_field_indexes = vec![];
        let variant = *enum_def.variants_map.get(variant_name).unwrap();

        for field in variant_fields {
            variant_field_indexes.push(entity.fields.len());
            let mut field = field.clone();
            field.full_name = format!("{}.{}", base_name, field.name);
            field.condition = FieldCondition::EnumValue { field_index, variant };

            if let Some(exists_field) = entity.fields.iter().find(|f| f.name == field.name) {
                panic!("Cannot add enum field {} to {}. Field {} already exists", field.name, entity.name, exists_field.full_name);
            }
            entity.fields.push(field);
        }
        variants.insert(variant, variant_field_indexes);
    }

    EnumInfo { 
        variants, 
        variants_map: enum_def.variants_map.clone(), 
        variants_names_map: create_variants_names_map(&enum_def.variants_map) 
    }
}

fn create_variants_names_map(variants_map: &HashMap<String,u16>) -> HashMap<u16,String> {
    let mut inverted_map = HashMap::new();
    for (key, value) in variants_map {
        inverted_map.insert(*value, key.clone());
    }
    inverted_map
}

/// Находит нужные модели и структуры для ссылок RefUnresolved и RefListUnresolved
fn resolve_refs(entity: &mut Entity, model_by_name: &HashMap<String, usize>) {
    for field in entity.fields.iter_mut(){
        match &field.ty {
            FieldType::RefUnresolved(name) => {
              if let Some(model_index) = model_by_name.get(name) {
                field.ty = FieldType::Ref(RefInfo::new(*model_index));
              } else {
                panic!("Unknown type {}", name)
              }
            }
            FieldType::RefListUnresolved(name) => {
              if let Some(model_index) = model_by_name.get(name) {
                field.ty = FieldType::RefList(RefInfo::new(*model_index));
              } else {
                panic!("Unknown type {}", name)
              }
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

// Разделяет строку по последней точке (Project.users.user -> [ Project.users, user ])
fn split_derived_name(value: &str) -> (Option<&str>,&str) {
    let Some(last_index) = value.rfind('.') else {
        return (None,value)
    };

    return (Some(&value[..last_index]),&value[last_index+1..]);
}

// Связывает между собой derived поля
fn resolve_derived_refs(models: &mut [Entity]) {

    let mut field_bindings = vec![];
    let mut st_refs = HashMap::new();

    for (model_index, model) in models.iter().enumerate() {
        for (field_index, field) in model.fields.iter().enumerate() {
            let (FieldType::Ref(ref_info) | FieldType::RefList(ref_info)) = &field.ty else {
                continue;
            };
            let Some(derived_field_name) = field.attributes
                .iter()
                .find_map(|a| match a {
                    Attribute::DerivedUnresolved(name) => Some(name),
                    _ => None
                }) else { continue; };

            let (table_name,derived_field_name) = split_derived_name(derived_field_name);
            let mut ref_model_index = ref_info.model_index;
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
        if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &mut field_a.ty {
            ref_info.rev_field_idx = Some(field_b_ref.field_index);
            if let Some(st_ref) = st_refs.get(field_a_ref) {
                ref_info.parent_index = Some(ref_info.model_index);
                ref_info.model_index = *st_ref;
            }
        }
        
        let field_b = &mut models[field_b_ref.model_index].fields[field_b_ref.field_index];
        if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &mut field_b.ty {
            ref_info.rev_field_idx = Some(field_a_ref.field_index);
            if let Some(st_ref) = st_refs.get(field_b_ref) {
                ref_info.parent_index = Some(ref_info.model_index);
                ref_info.model_index = *st_ref;
            }
        }
    }
}

// Проставляет RefBinding для Ref и RefList
fn resolve_ref_bindings(models: &mut [Entity]) {
    let model_names: Vec<String> = models.iter().map(|f| f.name.clone()).collect();
    for model in models.iter_mut() {
        for (field_index, field) in model.fields.iter_mut().enumerate() {
            if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &mut field.ty {
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

                // indexes.push((FieldRef::new(model_index, field_index), ref_info.model_index, ref_info.rev_field_idx));
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::schema::{FieldType, RefBinding, parse_schema};

    #[test]
    fn test_parse_schema() {
        let schema = parse_schema("
        model User {
            name            String
            projects        Project[]       @derived(Project.users.user)
            projectOwners   Project[]       @derived(Project.author)
        }

        model Project {
            name        String
            users       UserRole[]
            info        ProjectInfo?
            author      User
        }

        struct UserRole {
            user        User          @id
            role        String
        }

        struct ProjectInfo {
            description String
        }

        ");

        assert_eq!(schema.models.len(), 4);
        // assert_eq!(schema.models[0].fields.len(), 3);

        // Сформирована структура Project.users, у нее 3 поля:
        // @parent_id
        // user
        // role
        assert_eq!(schema.models[2].fields.len(), 3);

        // Check Project.users.user field type
        match &schema.models[2].fields[1].ty {
            FieldType::Ref (ref_info) => {
                assert_eq!(schema.models[ref_info.model_index].name, "User");
                assert_eq!(schema.models[ref_info.model_index].fields[ref_info.rev_field_idx.unwrap()].name, "projects");
                assert_eq!(ref_info.parent_index, None);

                assert_eq!(ref_info.binding, RefBinding::FieldValue);
            },
            _ => panic!("Wrong schema field {} type", schema.models[0].fields[2].name)
        }

        // Check Project.author type
        match &schema.models[1].fields[4].ty {
            FieldType::Ref (ref_info) => {
                assert_eq!(schema.models[ref_info.model_index].name, "User");
                assert_eq!(ref_info.binding, RefBinding::FieldValue);
            },
            _ => panic!("Wrong schema field {} type", schema.models[0].fields[2].name)
        }
        
        // Check User.projects type
        match &schema.models[0].fields[2].ty {
            FieldType::RefList(ref_info) => {
                assert_eq!(schema.models[ref_info.model_index].name, "Project.users");
                assert_eq!(schema.models[ref_info.model_index].fields[ref_info.rev_field_idx.unwrap()].name, "user");
                assert_eq!(schema.models[ref_info.parent_index.unwrap()].name, "Project");

                assert_eq!(ref_info.binding, RefBinding::IndexTree("User.projects->Project.users".to_string()));
            },
            _ => panic!("Wrong schema field {} type", schema.models[0].fields[2].name)
        }
        
        // assert_eq!(schema.models[0].fields[2].indexes.direct, Some(InsertedIndex { tree_name: "User.projects->Project.users".to_string() }));
        // assert_eq!(schema.models[0].fields[2].indexes.rev, Some(InsertedIndex { tree_name: "Project.users.user->User".to_string() }));

        // assert_eq!(schema.models[2].fields[1].indexes.direct, Some(InsertedIndex { tree_name: "Project.users.user->User".to_string() }));
        // assert_eq!(schema.models[2].fields[1].indexes.rev, Some(InsertedIndex { tree_name: "User.projects->Project.users".to_string() }));


        // assert!(matches!(schema.models[1].fields[3].location, FieldLocation::Virtual));
        // assert_eq!(schema.models[1].fields[3].indexes.direct, None );
        

        // assert_eq!(schema.models[1].fields[4].indexes.direct, None );
    }

  
    #[test]
    fn test_parse_schema_enum() {
        let schema = parse_schema("
        model Project {
            name        String            
            type        ProjectType
        }

        model User {
            name        String
        }

        enum ProjectType {
            base
            shared {
                users   User[]
            }
        }
        ");

        assert_eq!(schema.models.len(), 2);

        assert_eq!(schema.models[0].fields.len(), 4);
        
        match &schema.models[0].fields[2].ty {
            FieldType::Enum(enum_def) => {
                assert_eq!(enum_def.variants.len(), 2);

                assert_eq!(enum_def.variants.get(&0).unwrap(), &Vec::<usize>::new());
                assert_eq!(enum_def.variants.get(&1).unwrap(), &vec![3]);
                assert_eq!(enum_def.variants_map.len(), 2);
            },
            _ => panic!("Wrong field type {:?}", schema.models[0].fields[3].ty)
        }
    }
}
