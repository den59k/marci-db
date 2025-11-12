use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct Schema {
    pub models: Vec<Model>,
}

impl Schema {
    fn get_field(&self, key: &ModelRef) -> &Field {
        return &self.models[key.model_index].fields[key.field_index];
    }
    fn get_field_mut(&mut self, key: &ModelRef) -> &mut Field {
        return &mut self.models[key.model_index].fields[key.field_index];
    }
    fn iter(&self) -> SchemaIter {
        let field_sizes = self.models.iter().map(|i| i.fields.len()).collect();
        return SchemaIter { field_sizes, field_index: 0, model_index: 0 }
    }
}

struct SchemaIter {
    field_sizes: Vec<usize>,
    model_index: usize,
    field_index: usize
}
impl Iterator for SchemaIter {
    type Item = ModelRef;
    fn next(&mut self) -> Option<Self::Item> {
        if self.field_index >= self.field_sizes[self.model_index] {
            self.field_index = 0;
            self.model_index += 1;
        }
        if self.model_index >= self.field_sizes.len() {
            return None;
        }

        let field = ModelRef::new(self.model_index, self.field_index);
        self.field_index += 1;
        Some(field)
    }
}

#[derive(Debug)]
pub struct Model {
    pub name: String,
    pub fields: Vec<Field>,
    pub counter_idx: usize,
    // Count of fields
    pub payload_offset: usize
}

#[derive(Debug,Clone)]
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
    // field offset index. In bytes offset is (3 + offset_index*3)
    pub offset_index: usize,
    pub offset_pos: usize,
    pub is_nullable: bool,
    pub inserted_indexes: Vec<InsertedIndex>,
    pub select_index: Option<String>,
    pub attributes: Vec<Attribute>,
    pub derived_from: Option<ModelRef>
}

#[derive(Debug,Clone)]
pub struct Struct {
    /// Полное имя (для таблицы) (base_table + base_field)
    pub name: String,
    pub fields: Vec<Field>,
    pub payload_offset: usize
}

pub trait WithFields {
    fn tree_name(&self) -> &[u8];
    fn fields(&self) -> &[Field];
    fn payload_offset(&self) -> usize;
    fn is_model(&self) -> bool;
}
impl WithFields for Model {
    fn tree_name(&self) -> &[u8] { &self.name.as_bytes() }
    fn fields(&self) -> &[Field] { &self.fields }
    fn payload_offset(&self) -> usize { self.payload_offset }
    fn is_model(&self) -> bool { true }
}
impl WithFields for Struct {
    fn tree_name(&self) -> &[u8] { &self.name.as_bytes() }
    fn fields(&self) -> &[Field] { &self.fields }
    fn payload_offset(&self) -> usize { self.payload_offset }
    fn is_model(&self) -> bool { false }
}

#[derive(Debug,Clone,PartialEq, Eq,Hash,PartialOrd)]
pub struct ModelRef {
    pub model_index: usize,
    pub field_index: usize
}
impl ModelRef {
    pub fn new(model_index: usize, field_index: usize) ->  ModelRef {
        return ModelRef { model_index, field_index };
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
    ModelRefDerived(usize),
    ModelRefList(usize),
    PrimitiveList(PrimitiveFieldType),
    Struct(Struct),
    StructList(Struct,usize)
}

#[derive(Debug,Clone)]
pub enum Attribute {
    Index,
    DerivedUnresolved { model: String, field: String },
}

fn parse_fields(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> (Vec<Field>, usize) {
    let mut offset_index: usize = 0;
    let mut fields = Vec::new();

    for line in lines {
        let line = line.trim();
        if line == "}" { break }
        if line.is_empty() { continue; }

        let mut field = parse_field_raw(line);

        let is_derived = field.attributes.iter().any(|f| matches!(f, Attribute::DerivedUnresolved { .. }));
        let is_virtual = matches!(field.ty, FieldType::RefListUnresolved(_));

        if !is_virtual && !is_derived { 
            field.offset_index = offset_index;
            field.offset_pos = 3 + offset_index * 4;
            offset_index += 1;
        }
        fields.push(field);
    }
    return (fields, offset_index);
}

pub fn parse_model_block(name: String, lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Model {

    let (fields, offset_index) = parse_fields(lines);

    let payload_offset = 3 + offset_index * 4;
    return Model { name, fields, payload_offset, counter_idx: 0 };
}

pub fn parse_struct_block(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Struct {
    let (fields, offset_index) = parse_fields(lines);
    let payload_offset = 3 + offset_index * 4;

    return Struct { name: String::new(), fields: fields, payload_offset }
}

pub fn parse_schema(input: &str) -> Schema {
    let mut models = Vec::new();
    let mut structs: HashMap<String, Struct> = HashMap::new();
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
    let field_by_name = build_field_map(&schema);

    let model_names: Vec<String> = schema.models.iter().map(|i| i.name.clone()).collect();

    let mut indexes: Vec<ModelRef> = vec![];
    let mut bindings: HashSet<(ModelRef,ModelRef)> = HashSet::new();

    // resolve types and attributes
    for field_ref in schema.iter() {
        let model_name = schema.models[field_ref.model_index].name.clone();
        let field = schema.get_field_mut(&field_ref);

        resolve_field_type(&mut field.ty, &model_by_name, &structs);

        if let FieldType::Struct(st) = &mut field.ty {
            st.name = format!("{}.{}", model_name, field.name)
        }
        if let FieldType::ModelRefList(_) = &field.ty {
            let index_name = format!("{}.{}", model_name, field.name);
            field.inserted_indexes.push(InsertedIndex::Direct { tree_name: index_name.clone() });
            field.select_index = Some(index_name)
        }

        for attr in &mut field.attributes {
            if let Attribute::DerivedUnresolved { model: model_name, field: field_name } = attr {
                let m = model_by_name[model_name];
                let f: usize = field_by_name[m][field_name];
                let derived_ref = ModelRef::new(m, f);
                field.derived_from = Some(derived_ref.clone());
                let field_ref = field_ref.clone();
                let key: (ModelRef,ModelRef) = if derived_ref > field_ref { (field_ref,derived_ref) } else { (field_ref,derived_ref) };
                bindings.insert(key);
            }
        }

        // let is_index = field.attributes.iter().any(|i| matches!(i, Attribute::Index));
        // if is_index {
        //     field.index_name = Some(format!("{}.{}.idx", model.name, field.name));
        // }
    }

    for (a, b) in bindings {
        let indexes_b = rev_indexes(schema.get_field(&a));
        let indexes_a = rev_indexes(schema.get_field(&b));

        schema.get_field_mut(&a).inserted_indexes.extend(indexes_a);
        schema.get_field_mut(&b).inserted_indexes.extend(indexes_b);
    }

    for model in schema.models.iter() {
        println!("{:#?}", model);
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

    Field { name, ty, offset_index: 0, offset_pos: 0, attributes, is_nullable, derived_from: None, inserted_indexes: vec![], select_index: None }
}

fn parse_attribute(s: &str) -> Vec<Attribute> {
    if s.starts_with("index") {
        return vec![Attribute::Index];
    }

    if let Some(inside) = s.strip_prefix("derived(").and_then(|x| x.strip_suffix(')')) {
        let mut parts = inside.split('.');
        let model = parts.next().unwrap().to_string();
        let field = parts.next().unwrap().to_string();
        return vec![Attribute::DerivedUnresolved { model, field }];
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

// fn is_primitive(s: &str) -> bool {
//     matches!(s, "String" | "DateTime" | "Bool" | "Int" | "Float")
// }

fn resolve_field_type(ty: &mut FieldType, model_by_name: &HashMap<String, usize>, structs: &HashMap<String, Struct>) {
    match ty {
        FieldType::RefUnresolved(name) => {
            if let Some(st) = structs.get(name) {
                *ty = FieldType::Struct(st.clone());
            } else {
                *ty = FieldType::ModelRef(*model_by_name.get(name).expect(&format!("Not found type {}", name)));
            }
        }
        FieldType::RefListUnresolved(name) => {
            if let Some(st) = structs.get(name) {
                *ty = FieldType::StructList(st.clone(),0);
            } else {
                *ty = FieldType::ModelRefList(*model_by_name.get(name).expect(&format!("Not found type {}", name)));
            }
        }
        _ => {}
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