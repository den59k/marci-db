use std::collections::{HashMap, HashSet};

use crate::schema::{Aliases, Entity, Field, FieldRef, FieldType, InsertedIndex, InsertedIndexSt, PrimitiveFieldType, Schema, schema_attributes::{parse_attribute, split_once_end}, schema_enum::{EnumDef, parse_enum_block}};

pub use crate::schema::{schema_attributes::DeleteConstraint,schema_attributes::Attribute};

pub fn parse_fields(lines: &mut std::iter::Peekable<std::str::Lines<'_>>, pre_header_size: usize) -> (Vec<Field>, usize) {
    let mut offset_index: usize = 0;
    let mut fields = Vec::new();

    for line in lines {
        let line = line.trim();
        if line == "}" { break }
        if line.is_empty() { continue; }

        let mut field = parse_field_raw(line);

        let is_virtual = matches!(field.ty, FieldType::RefListUnresolved(_));

        if !is_virtual && !field.is_derived() && !field.id_idx.is_some() { 
            field.offset_pos = pre_header_size + offset_index * 4;
            offset_index += 1;
        }
        fields.push(field);
    }

    // Sentinel offset здесь не нужен, поскольку у нас фиксированное кол-во полей
    let payload_offset = pre_header_size + offset_index * 4;

    return (fields, payload_offset);
}

// Проставляем нужные индексы для id_idx. Если ключевых полей нет, добавляем
pub fn update_key_fields(fields: &mut Vec<Field>) -> () {

    // TODO: можно вставлять не idx_counter, а idx_offset. Обозначить, что все @id ключи у нас фиксированные
    let mut idx_counter = 0;
    for field in fields.iter_mut() {
        if let Some(idx) = &mut field.id_idx {
            *idx = idx_counter;
            idx_counter += 1;
        }
    }

    if idx_counter == 0 {
        fields.insert(0, Field { 
            name: "id".to_string(), 
            full_name: String::new(),
            ty: FieldType::Primitive(PrimitiveFieldType::UInt64), 
            offset_pos: 0, 
            is_nullable: false, 
            inserted_indexes: InsertedIndexSt::new(), 
            id_idx: Some(0),
            attributes: vec![Attribute::Id], 
            counter_idx: Some(0),
            is_unique: false,
            injected_fields: None
        });
    }
}

pub fn parse_model_block(name: String, lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Entity {

    let (mut fields, payload_offset) = parse_fields(lines, 3);
    update_key_fields(&mut fields);

    return Entity { name, fields, payload_offset };
}

pub fn parse_struct_block(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Entity {
    let (fields, payload_offset) = parse_fields(lines, 3);

    return Entity { name: String::new(), fields: fields, payload_offset }
}

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
                enums.insert(name.clone(), parse_enum_block(name, &mut lines));
            }
            _ => {}
        }
    }

    let mut schema = Schema { models, foreign_bindings: vec![] };

    let model_by_name = build_model_map(&schema);

    for (model_index, model) in schema.models.iter_mut().enumerate() {
        resolve_fields(&mut model.fields, model_index, &model.name, &model_by_name, &structs, &enums);
    }

    resolve_attributes(&mut schema, &model_by_name);

    resolve_foreign_constraints(&mut schema);

    schema
}

fn parse_field_raw(line: &str) -> Field {
    // имя и тип
    let mut parts = line.split_whitespace();

    let name = parts.next().expect("expected field name").to_string();

    let type_str = parts.next().expect("expected field type");

    let is_nullable = type_str.ends_with('?');
    let ty = parse_type(type_str.strip_suffix("?").unwrap_or(type_str));

    // атрибуты: всё, что осталось в строке, интерпретируем как атрибуты
    // Каждое слово, начинающееся с '@', — отдельный атрибут
    let attributes: Vec<Attribute> = line
        .split('@')
        .skip(1)         // игнорируем часть до первого @
        .map(str::trim)  // убираем пробелы
        .filter(|s| !s.is_empty())
        .map(parse_attribute)
        .collect();
    
    let is_id = attributes.iter().any(|attr| matches!(attr, Attribute::Id));
    let is_unique = attributes.iter().any(|attr| matches!(attr, Attribute::Unique));

    Field {
        name,
        full_name: String::new(),
        ty,
        offset_pos: 0,
        attributes,
        is_nullable,
        inserted_indexes: InsertedIndexSt::new(),
        counter_idx: None,
        id_idx: is_id.then_some(0),
        is_unique,
        injected_fields: None
    }
}

fn parse_type(s: &str) -> FieldType {
    if let Some((ty, bracket)) = s.strip_suffix(']').and_then(|s| s.split_once('[')) {
        let bracket = bracket.trim();
        if bracket.is_empty() {
            if let Some(prim) = get_primitive_type(ty) {
                return FieldType::PrimitiveList(prim);
            }
            return FieldType::RefListUnresolved(ty.to_string());
        }

        if let Ok(len) = bracket.parse::<usize>() {
            if let Some(prim) = get_primitive_type(ty) {
                return FieldType::PrimitiveFixedList(prim, len);
            }
            panic!("Fixed list is allowed only for primitive types: {}", s);
        }
        panic!("Invalid array syntax: {}", s);
    }

    if let Some(prim) = get_primitive_type(s) {
        return FieldType::Primitive(prim);
    }

    FieldType::RefUnresolved(s.to_string())
}


fn get_primitive_type(s: &str) -> Option<PrimitiveFieldType> {
    match s {
        "String" => Some(PrimitiveFieldType::String),
        "Bool" => Some(PrimitiveFieldType::Bool),
        "Int" => Some(PrimitiveFieldType::Int64),
        "UInt" => Some(PrimitiveFieldType::UInt64),
        "Float" => Some(PrimitiveFieldType::Float),
        "Double" => Some(PrimitiveFieldType::Double),
        "DateTime" => Some(PrimitiveFieldType::DateTime),
        _ => None
    }
}

/// Находит нужные модели и структуры для ссылок RefUnresolved и RefListUnresolved
fn resolve_fields(
    fields: &mut Vec<Field>,
    model_index: usize,
    model_name: &str, 
    model_by_name: &HashMap<String, usize>, 
    structs: &HashMap<String, Entity>,
    enums: &HashMap<String, EnumDef>,
) {
    for field in fields.iter_mut(){
        let field_full_name: String = [ model_name, ".", &field.name ].concat();
        field.full_name = field_full_name.clone();

        match &field.ty {
            FieldType::RefUnresolved(name) => {
                if let Some(en) = enums.get(name) {
                    let mut en = en.clone();
                    for variant in en.variants.iter_mut() {
                        // Example key for enum fields - User[role=admin].features
                        let name = [ model_name, "[", &field.name, "=", &variant.name, "]" ].concat();
                        resolve_fields(&mut variant.fields, model_index, &name, model_by_name, structs, enums);
                    }
                    field.ty = FieldType::Enum(en);
                } else if let Some(st) = structs.get(name) {
                    let mut st = st.clone();
                    st.name = field_full_name.clone();
                    resolve_fields(&mut st.fields, model_index, &st.name, model_by_name, structs, enums);
                    field.ty = FieldType::Struct(st);
                    // StructOne идет вообще без ключа, поскольку она полностью наследует ключ родителя

                } else if let Some(model_index) = model_by_name.get(name) {
                    field.ty = FieldType::ModelRef(*model_index);
                } else {
                    panic!("Unknown type {}", name)
                }
            }
            FieldType::RefListUnresolved(name) => {
                if let Some(_en) = enums.get(name) {
                    todo!("Enum list not implemented yet");
                } else if let Some(en) = enums.get(name) {
                    let mut en = en.clone();
                    for variant in en.variants.iter_mut() {
                        resolve_fields(&mut variant.fields, model_index, model_name, model_by_name, structs, enums);
                    }
                    field.ty = FieldType::Enum(en);
                } else if let Some(st) = structs.get(name) {
                    let mut st = st.clone();
                    st.name = field_full_name.clone();
                    update_key_fields(&mut st.fields);

                    // Мы увеличиваем ключ, сдвигая его, поскольку у нас в структуре первым идет ID родителя
                    for field in st.fields.iter_mut() {
                        if let Some(idx) = &mut field.id_idx {
                            *idx += 1;
                        }
                    }
    
                    st.fields.insert(0, Field { 
                        name: "@parent".to_string(), 
                        full_name: [ &st.name, ".@parent" ].concat(),
                        ty: FieldType::ModelRef(model_index), 
                        offset_pos: 0, 
                        is_nullable: true, 
                        id_idx: Some(0), 
                        counter_idx: None, 
                        inserted_indexes: InsertedIndexSt::new(), 
                        attributes: vec![Attribute::Id],
                        is_unique: false,
                        injected_fields: None
                    });

                    resolve_fields(&mut st.fields, model_index, &st.name, model_by_name, structs, enums);
                    field.ty = FieldType::StructList(st.clone());
                } else if let Some(model_index) = model_by_name.get(name) {
                    field.ty = FieldType::ModelRefList(*model_index);
                    
                    // Связь ManyToOne / ManyToMany хранится в индексе
                    let index_name = format!("{}.{}", model_name, field.name);
                    field.inserted_indexes.direct = Some(InsertedIndex { tree_name: index_name });
                } else {
                    panic!("Unknown type {}", name)
                }
            }
            _ => {}
        }
    }
}

// Resolve unresolved attributes
fn resolve_attributes(schema: &mut Schema, model_by_name: &HashMap<String, usize>) {
    let mut injects: HashMap<FieldRef, (FieldRef, Aliases)> = HashMap::new();
    let mut bindings: HashSet<(FieldRef,FieldRef)> = HashSet::new();
    let mut field_indexes: HashSet<FieldRef> = HashSet::new();

    schema.walk(|field, field_ref| {
        for attr in field.attributes.iter() {
            match attr {
                Attribute::DerivedUnresolved (inside) => {
                    let derived_ref = get_model_ref(&inside, schema, model_by_name);
                
                    let field_ref = field_ref.clone();
                    // TODO: Create modelRef also from struct
                    let key: (FieldRef,FieldRef) = if derived_ref > field_ref { (field_ref,derived_ref) } else { (derived_ref,field_ref) };
                    bindings.insert(key);
                }
                Attribute::InjectUnresolved(items) => {
                    for (key, alias) in items {
                        let Some((base_name,field_name)) = split_once_end(key, '.') else {
                            panic!("Inject syntax must include path to struct")
                        };
                        let ref_model_ref = &get_model_ref(base_name, &schema, &model_by_name);

                        let field_injects = injects
                            .entry(field_ref.clone())
                            .or_insert_with(|| (ref_model_ref.clone(), HashMap::new()));

                        if &field_injects.0 != ref_model_ref {
                            panic!("You cannot inject from multiple structs");
                        }

                        field_injects.1.insert(field_name.to_string(), alias.to_string());
                    }
                }
                Attribute::Index | Attribute::Unique => {
                    field_indexes.insert(field_ref.clone());
                }
                _ => {}
            }
        }
    });

    // Добавляем inject_fields
    for (field_ref, (st_ref, aliases)) in injects {
        let st_field = schema.get_field(&st_ref);
        let _st = match &st_field.ty {
            FieldType::Struct(st) | FieldType::StructList(st) => st,
            _ => panic!("You cannot inject from non-struct field")
        };
        
        let field = schema.get_field_mut(&field_ref);
        field.injected_fields = Some((st_ref, aliases ));
    }

    // Добавляем inserted_indexes
    for (a, b) in bindings {

        if let Some(index) = schema.get_field(&a).get_direct_index() {
            schema.get_field_mut(&b).inserted_indexes.rev = Some(index.clone());
        }
        if let Some(index) = schema.get_field(&b).get_direct_index() {
            schema.get_field_mut(&a).inserted_indexes.rev = Some(index.clone());
        }
    }

    // Добавляем индексы для полей с @index
    for a in field_indexes {
        let field = schema.get_field_mut(&a);
        field.inserted_indexes.field = Some(InsertedIndex { tree_name: field.full_name.clone() });
    }
}

fn resolve_foreign_constraints(schema: &mut Schema) {
    let mut foreign_bindings: Vec<Vec<(FieldRef,DeleteConstraint)>> = schema.models.iter().map(|_| Vec::new()).collect();

    schema.walk(| field, field_ref | {
        match &field.ty {
            FieldType::ModelRefList(ref_model_idx) => {
                let constraint = field.attributes.iter().find_map(|f| match f {
                    Attribute::OnDelete(c) => Some(c.clone()),
                    _ => None,
                }).unwrap_or(DeleteConstraint::RemoveItem);

                if matches!(constraint, DeleteConstraint::SetNull) {
                    panic!("Use RemoveItem instead SetNull in list field {}", field.full_name);
                }

                foreign_bindings[*ref_model_idx].push((field_ref, constraint));
            },
            FieldType::ModelRef(ref_model_idx) => {
                let constraint = field.attributes.iter().find_map(|f| match f {
                    Attribute::OnDelete(c) => Some(c.clone()),
                    _ => None,
                }).unwrap_or_else(|| {
                    if field.id_idx.is_some() {
                        DeleteConstraint::Restrict
                    } else {
                        DeleteConstraint::SetNull
                    }
                });

                if matches!(constraint, DeleteConstraint::RemoveItem) {
                    panic!("Use SetNull instead RemoveItem in non-list field {}", field.full_name);
                }

                foreign_bindings[*ref_model_idx].push((field_ref, constraint));
            }
            _ => {}
        }
    });

    schema.foreign_bindings = foreign_bindings;
}


fn get_model_ref(s: &str, schema: &Schema, model_by_name: &HashMap<String, usize>) -> FieldRef {
    let mut parts = s.split('.');
    let ref_model_name = parts.next().unwrap();
    let Some(&m) = model_by_name.get(ref_model_name) else {
        panic!("ERROR: Not found model {}", &ref_model_name);
    };
    let model = &schema.models[m];

    let ref_field_name = parts.next().unwrap();
    let Some((f, field)) = model.fields.iter().enumerate().find(|i| i.1.name == ref_field_name) else {
        panic!("ERROR: Not found field {} in model {}", &ref_model_name, model.name);
    };
    let mut model_ref = FieldRef::new(m, f);

    if let Some(ref_struct_field_name) = parts.next() {
        let st = match &field.ty {
            FieldType::Struct(st) => st,
            FieldType::StructList(st) => st,
            _ => { panic!("Trying to get field from not struct {}", ref_struct_field_name); }
        };

        let Some(f) = st.fields.iter().position(|i| i.name == ref_struct_field_name) else {
            panic!("ERROR: Not found field {} in struct {}", &ref_struct_field_name, st.name);
        };
        model_ref.struct_field_index = Some(f);
    }
    return model_ref;
}

fn build_model_map(schema: &Schema) -> HashMap<String, usize> {
    schema.models.iter().enumerate()
        .map(|(i, m)| (m.name.clone(), i))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::schema::{FieldType, InsertedIndex, InsertedIndexSt, parse_schema};

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

        assert_eq!(schema.models.len(), 2);

        assert_eq!(schema.models[0].fields.len(), 3);
        
        let projects_field = &schema.models[0].fields[2];

        let mut st_indexes = InsertedIndexSt::new();
        st_indexes.direct = Some(InsertedIndex { tree_name: "User.projects".to_string() });
        
        assert_eq!(projects_field.inserted_indexes, st_indexes);

        let users_field = &schema.models[1].fields[2];

        assert!(users_field.inserted_indexes.is_empty());

        let FieldType::StructList(st) = &users_field.ty else {
            panic!("Field type is not StructList");
        };

        // В StructList добавляется parentID первым полем
        assert_eq!(st.fields.len(), 3);

        let mut st_indexes = InsertedIndexSt::new();
        st_indexes.rev = Some(InsertedIndex { tree_name: "User.projects".to_string() });

        assert_eq!(st.fields[1].inserted_indexes, st_indexes);
    }
}