use std::collections::HashMap;

#[derive(Debug)]
pub struct Schema {
    pub models: Vec<Model>,
}

#[derive(Debug)]
pub struct Model {
    pub name: String,
    pub fields: Vec<Field>,
    // Count of fields
    pub payload_offset: usize
}

#[derive(Debug,Clone)]
pub struct Field {
    pub name: String,
    pub ty: FieldType,
    // field offset index. In bytes offset is (3 + offset_index*3)
    pub offset_index: usize,
    pub offset_pos: usize,
    pub is_nullable: bool,
    pub attributes: Vec<Attribute>,
    pub index_name: Option<String>,
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
    fn field(&self, index: usize) -> &Field;
    fn payload_offset(&self) -> usize;
}
impl WithFields for Model {
    fn tree_name(&self) -> &[u8] { &self.name.as_bytes() }
    fn fields(&self) -> &[Field] { &self.fields }
    fn field(&self, index: usize) -> &Field { &self.fields[index] }
    fn payload_offset(&self) -> usize { self.payload_offset }
}
impl WithFields for Struct {
    fn tree_name(&self) -> &[u8] { &self.name.as_bytes() }
    fn fields(&self) -> &[Field] { &self.fields }
    fn field(&self, index: usize) -> &Field { &self.fields[index] }
    fn payload_offset(&self) -> usize { self.payload_offset }
}

#[derive(Debug,Clone)]
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
    ModelRefList(usize),
    PrimitiveList(PrimitiveFieldType),
    Struct(Struct),
    StructList(Struct)
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

        let is_virtual = field.attributes.iter().any(|f| matches!(f, Attribute::DerivedUnresolved { .. }));

        if !is_virtual { 
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
    return Model { name, fields, payload_offset };
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

    let mut indexes: Vec<(usize, usize)> = vec![];

    // resolve types and attributes
    for model in &mut schema.models.iter_mut(){
        for field in &mut model.fields.iter_mut() {
            resolve_field_type(&mut field.ty, &model_by_name, &structs);

            if let FieldType::Struct(st) = &mut field.ty {
                st.name = format!("{}.{}", model.name, field.name)
            }

            for attr in &mut field.attributes {
                if let Attribute::DerivedUnresolved { model: model_name, field: field_name } = attr {
                    let m = model_by_name[model_name];
                    let f = field_by_name[m][field_name];
                    field.derived_from = Some(ModelRef::new(m, f));
                    indexes.push((m, f));
                }
            }

            let is_index = field.attributes.iter().any(|i| matches!(i, Attribute::Index));
            if is_index {
                field.index_name = Some(format!("{}.{}.idx", model.name, field.name));
            }
        }
        println!("{:?}", model);
    }

    for key in indexes {
        let model = &mut schema.models[key.0];
        let field = &mut model.fields[key.1];
        field.index_name = Some(format!("{}.{}.idx", model.name, field.name));
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

    Field { name, ty, offset_index: 0, offset_pos: 0, attributes, is_nullable, derived_from: None, index_name: None }
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
                *ty = FieldType::StructList(st.clone());
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
