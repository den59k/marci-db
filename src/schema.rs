use std::{collections::{HashMap, HashSet}, fmt::Debug};

#[derive(Debug)]
pub struct Schema {
    pub models: Vec<Entity>,
}

impl Schema {
    fn get_field(&self, key: &ModelRef) -> &Field {
        let field = &self.models[key.model_index].fields[key.field_index];
        if let Some(struct_field_index) = &key.struct_field_index {
            let st = match &field.ty {
                FieldType::Struct(st) => st,
                FieldType::StructList(st) => st,
                _ => { panic!("Trying to get index from non-struct {}", *struct_field_index); }
            };
            return &st.fields[*struct_field_index];
        } else {
            return field;
        }
    }
    fn get_field_mut(&mut self, key: &ModelRef) -> &mut Field {
        let field = &mut self.models[key.model_index].fields[key.field_index];
        if let Some(struct_field_index) = &key.struct_field_index {
            let st = match &mut field.ty {
                FieldType::Struct(st) => st,
                FieldType::StructList(st) => st,
                _ => { panic!("Trying to get index from non-struct {}", *struct_field_index); }
            };
            return &mut st.fields[*struct_field_index];
        } else {
            return field;
        }
    }
}

#[derive(Debug,Clone)]
pub struct Entity {
    pub name: String,
    pub fields: Vec<Field>,
    // Count of fields
    pub payload_offset: usize,
    // pub key_fields: Vec<usize>
}

impl Entity {
    pub fn key_min_size(&self) -> usize {
        return self.fields.iter().filter_map(|f| f.id_idx.is_some().then_some(8)).sum();
    }
}

#[derive(Debug,Clone,PartialEq)]
pub enum InsertedIndex {
    /// Вставляем индекс на основе A.id и B.id
    Direct { tree_name: String },
    /// Вставляем индекс на основе B.id и A.id
    Rev { tree_name: String }
}
impl InsertedIndex {
    pub fn tree_name(&self) -> &[u8] {
        match self {
            InsertedIndex::Direct { tree_name } | InsertedIndex::Rev { tree_name } => tree_name.as_bytes(),
        }
    }
}

#[derive(Debug,Clone)]
pub struct Field {
    pub name: String,
    pub ty: FieldType,
    /// Offset in bytes  (3 + offset_index*4)
    pub offset_pos: usize,
    pub is_nullable: bool,
    /// Position in ID key (just index, not bytes)
    pub id_idx: Option<usize>,
    pub counter_idx: Option<usize>,
    pub inserted_indexes: Vec<InsertedIndex>,
    pub select_index: Option<String>,
    pub attributes: Vec<Attribute>
}

impl Field {
    pub fn is_derived(&self) -> bool {
        self.attributes.iter().any(|attr| matches!(attr, Attribute::DerivedUnresolved { .. }))
    }
}

#[derive(Debug,Clone,PartialEq, Eq,Hash,PartialOrd)]
pub struct ModelRef {
    pub model_index: usize,
    pub field_index: usize,
    pub struct_field_index: Option<usize>
}
impl ModelRef {
    pub fn new(model_index: usize, field_index: usize) ->  ModelRef {
        return ModelRef { model_index, field_index, struct_field_index: None };
    }
}

#[derive(Debug,Clone)]
pub struct IndexRef {
    pub model_index: usize,
    pub field_index: usize,
    pub index_name: String
}
impl IndexRef {
    pub fn new(model_index: usize, field_index: usize, index_name: String) -> IndexRef {
        return IndexRef { model_index, field_index, index_name };
    }
}


#[derive(Debug, Clone, Copy)]
pub enum PrimitiveFieldType {
    String,
    Int64,
    UInt64,
    Float,
    Double,
    Bool,
    DateTime,
}

#[derive(Debug, Clone)]
pub enum FieldType {
    Primitive(PrimitiveFieldType),
    // Ссылка на либо model, либо struct
    RefUnresolved(String),
    // Ссылка на список либо model, либо struct
    RefListUnresolved(String),
    ModelRef(usize),
    ModelRefList(usize),
    PrimitiveList(PrimitiveFieldType),
    Struct(Entity),
    StructList(Entity)
}

#[derive(Debug,Clone)]
pub enum Attribute {
    Index,
    DerivedUnresolved (String),
    Id
}

fn parse_fields(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> (Vec<Field>, usize) {
    let mut offset_index: usize = 0;
    let mut fields = Vec::new();

    for line in lines {
        let line = line.trim();
        if line == "}" { break }
        if line.is_empty() { continue; }

        let mut field = parse_field_raw(line);

        let is_virtual = matches!(field.ty, FieldType::RefListUnresolved(_));

        if !is_virtual && !field.is_derived() && !field.id_idx.is_some() { 
            field.offset_pos = 3 + offset_index * 4;
            offset_index += 1;
        }
        fields.push(field);
    }
    return (fields, offset_index);
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
            ty: FieldType::Primitive(PrimitiveFieldType::UInt64), 
            offset_pos: 0, 
            is_nullable: false, 
            inserted_indexes: vec![], 
            select_index: None, 
            id_idx: Some(0),
            attributes: vec![Attribute::Id], 
            counter_idx: Some(0)
        });
    }
}

pub fn parse_model_block(name: String, lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Entity {

    let (mut fields, offset_index) = parse_fields(lines);
    update_key_fields(&mut fields);

    let payload_offset = 3 + offset_index * 4;
    return Entity { name, fields, payload_offset };
}

pub fn parse_struct_block(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Entity {
    let (fields, offset_index) = parse_fields(lines);
    let payload_offset = 3 + offset_index * 4;

    return Entity { name: String::new(), fields: fields, payload_offset }
}

pub fn parse_schema(input: &str) -> Schema {
    let mut models = Vec::new();
    let mut structs: HashMap<String, Entity> = HashMap::new();
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

            }
            _ => {}
        }
    }

    let mut schema = Schema { models };

    // build name maps
    let model_by_name = build_model_map(&schema);

    // let model_names: Vec<String> = schema.models.iter().map(|i| i.name.clone()).collect();
    // let mut indexes: Vec<ModelRef> = vec![];
    for (model_index, model) in schema.models.iter_mut().enumerate() {
        resolve_fields(&mut model.fields, model_index, &model.name, &model_by_name, &structs);
    }
    

    // Set pairs for derived fields
    let mut bindings: HashSet<(ModelRef,ModelRef)> = HashSet::new();
    for (model_index, model) in schema.models.iter().enumerate() {
        resolve_derived(&model.fields, &schema, model_index, &model.name, &model_by_name, &mut bindings);
    }

    for (a, b) in bindings {
        let indexes_b = rev_indexes(schema.get_field(&a));
        let indexes_a = rev_indexes(schema.get_field(&b));

        schema.get_field_mut(&a).inserted_indexes.extend(indexes_a);
        schema.get_field_mut(&b).inserted_indexes.extend(indexes_b);
    }

    schema
}

fn parse_field_raw(line: &str) -> Field {
    // имя и тип
    let mut parts = line.split_whitespace();
    let name = parts.next().unwrap().to_string();

    let type_str = parts.next().unwrap();
    let is_nullable = type_str.ends_with("?");
    let ty = parse_type(if is_nullable { &type_str[0..type_str.len()-1] } else { type_str });

    // атрибуты
    let attributes = line.split_once('@')
        .map(|(_, attr)| parse_attribute(attr.trim()))
        .unwrap_or_else(Vec::new);
    let is_id = attributes.iter().any(|attr| matches!(attr, Attribute::Id));

    Field { 
        name, 
        ty, 
        offset_pos: 0, 
        attributes, 
        is_nullable, 
        inserted_indexes: vec![], 
        select_index: None, 
        counter_idx: None, 
        id_idx: is_id.then_some(0)
    }
}

fn parse_attribute(s: &str) -> Vec<Attribute> {
    if s.starts_with("index") {
        return vec![Attribute::Index];
    }

    if s.starts_with("id") {
        return vec![Attribute::Id];
    }

    if let Some(inside) = s.strip_prefix("derived(").and_then(|x| x.strip_suffix(')')) {
        return vec![Attribute::DerivedUnresolved(inside.to_string())];
    }

    Vec::new()
}

fn parse_type(s: &str) -> FieldType {
    if let Some(inner) = s.strip_suffix("[]") {
        if let Some(primitive_field) = get_primitive_type(inner) {
            FieldType::PrimitiveList(primitive_field)
        } else {
            FieldType::RefListUnresolved(inner.to_string())
        }
    } else if let Some(primitive_field) = get_primitive_type(s) {
        FieldType::Primitive(primitive_field)
    } else {
        FieldType::RefUnresolved(s.to_string())
    }
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
) {
    for field in fields.iter_mut(){
        match &field.ty {
            FieldType::RefUnresolved(name) => {
                if let Some(st) = structs.get(name) {
                    let mut st = st.clone();
                    st.name = format!("{}.{}", model_name, field.name);
                    resolve_fields(&mut st.fields, model_index, &st.name, model_by_name, structs);
                    field.ty = FieldType::Struct(st);
                    // StructOne идет вообще без ключа, поскольку она полностью наследует ключ родителя

                } else {
                    field.ty = FieldType::ModelRef(*model_by_name.get(name).expect(&format!("Not found model {}", name)));
                }
            }
            FieldType::RefListUnresolved(name) => {
                if let Some(st) = structs.get(name) {
                    let mut st = st.clone();
                    st.name = format!("{}.{}", model_name, field.name);
                    update_key_fields(&mut st.fields);

                    // Мы увеличиваем ключ, сдвигая его, поскольку у нас в структуре первым идет ID родителя
                    for field in st.fields.iter_mut() {
                        if let Some(idx) = &mut field.id_idx {
                            *idx += 1;
                        }
                    }
                    // for idx in &mut st.key_fields {
                    //     *idx += 1;
                    // }
                    st.fields.insert(0, Field { 
                        name: "@parent".to_string(), 
                        ty: FieldType::ModelRef(model_index), 
                        offset_pos: 0, 
                        is_nullable: true, 
                        id_idx: Some(0), 
                        counter_idx: None, 
                        inserted_indexes: vec![], 
                        select_index: None, 
                        attributes: vec![] 
                    });
                    // st.key_fields.insert(0, 0);

                    resolve_fields(&mut st.fields, model_index, &st.name, model_by_name, structs);
                    field.ty = FieldType::StructList(st.clone());
                } else if let Some(model_index) = model_by_name.get(name) {
                    field.ty = FieldType::ModelRefList(*model_index);
                    
                    // Связь ManyToOne / ManyToMany хранится в индексе
                    let index_name = format!("{}.{}", model_name, field.name);
                    field.inserted_indexes.push(InsertedIndex::Direct { tree_name: index_name.clone() });
                    field.select_index = Some(index_name)
                } else {
                    panic!("Uknonwn type {}", name)
                }
            }
            _ => {}
        }
    }
}

fn resolve_derived(
    fields: &Vec<Field>,
    schema: &Schema,
    model_index: usize,
    model_name: &str, 
    model_by_name: &HashMap<String, usize>, 
    bindings: &mut HashSet<(ModelRef,ModelRef)>
) {
    for (field_index, field) in fields.iter().enumerate() {
        for attr in field.attributes.iter() {
            if let Attribute::DerivedUnresolved (inside) = attr {
                let mut parts = inside.split('.');

                let ref_model_name = parts.next().unwrap();
                let Some(&m) = model_by_name.get(ref_model_name) else {
                    panic!("ERROR {}.{}: Not found model {}", &model_name, &field.name, &ref_model_name);
                };
                let model = &schema.models[m];

                let ref_field_name = parts.next().unwrap();
                let Some((f, field)) = model.fields.iter().enumerate().find(|i| i.1.name == ref_field_name) else {
                    panic!("ERROR {}.{}: Not found field {} in model {}", &model_name, &field.name, &ref_model_name, model.name);
                };
                let mut derived_ref = ModelRef::new(m, f);

                if let Some(ref_struct_field_name) = parts.next() {
                    let st = match &field.ty {
                        FieldType::Struct(st) => st,
                        FieldType::StructList(st) => st,
                        _ => { panic!("Trying to get field from not struct {}", ref_struct_field_name); }
                    };

                    let Some(f) = st.fields.iter().position(|i| i.name == ref_struct_field_name) else {
                        panic!("ERROR {}.{}: Not found field {} in struct {}", &model_name, &field.name, &ref_struct_field_name, st.name);
                    };
                    derived_ref.struct_field_index = Some(f);
                }
                
                // TODO: Create modelRef also from struct
                let field_ref = ModelRef::new(model_index, field_index);
                let key: (ModelRef,ModelRef) = if derived_ref > field_ref { (field_ref,derived_ref) } else { (field_ref,derived_ref) };
                bindings.insert(key);
            }
        }
    }
}

fn build_model_map(schema: &Schema) -> HashMap<String, usize> {
    schema.models.iter().enumerate()
        .map(|(i, m)| (m.name.clone(), i))
        .collect()
}

fn build_field_map(schema: &Schema) -> Vec<HashMap<String, usize>> {
    schema.models.iter()
        .map(|m| {
            m.fields.iter().enumerate()
                .map(|(i, f)| (f.name.clone(), i))
                .collect()
        })
        .collect()
}

#[inline(always)]
fn rev_indexes(field: &Field) -> Vec<InsertedIndex> {
    field.inserted_indexes
        .iter()
        .filter_map(|i| match i {
            InsertedIndex::Direct { tree_name } =>
                Some(InsertedIndex::Rev { tree_name: tree_name.clone() }),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::schema::{FieldType, InsertedIndex, parse_schema};


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

        assert_eq!(projects_field.inserted_indexes, vec![InsertedIndex::Direct { tree_name: "User.projects".to_string() } ]);

        let users_field = &schema.models[1].fields[2];

        println!("{:#?}", users_field);

        assert_eq!(users_field.inserted_indexes.len(), 0);

        let FieldType::StructList(st) = &users_field.ty else {
            panic!("Field type is not StructList");
        };

        // В StructList добавляется parentID первым полем
        assert_eq!(st.fields.len(), 3);

        assert_eq!(st.fields[1].inserted_indexes, vec![InsertedIndex::Rev { tree_name: "User.projects".to_string() } ]);
    }

}