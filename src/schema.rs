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
    pub fields_size: u16,
    pub payload_offset: usize
}

#[derive(Debug)]
pub struct Field {
    pub name: String,
    pub ty: FieldType,
    // field offset index. In bytes offset is (3 + offset_index*3)
    pub offset_index: usize,
    pub offset_pos: usize,
    pub is_nullable: bool,
    pub attributes: Vec<Attribute>,
    pub index_name: Option<String>,
    pub derived_from: Option<ModelRef>,
    pub ext_indexes: Vec<IndexRef>
}

#[derive(Debug)]
pub struct ModelRef {
    pub model_index: usize,
    pub field_index: usize
}
impl ModelRef {
    pub fn new(model_index: usize, field_index: usize) ->  ModelRef {
        return ModelRef { model_index, field_index };
    }
}

#[derive(Debug)]
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
    ModelRefUnresolved(String),
    ModelRef(usize),
    ModelRefListUnresolved(String),
    ModelRefList(usize),
    PrimitiveList(PrimitiveFieldType),
}

#[derive(Debug)]
pub enum Attribute {
    Index,
    DerivedUnresolved { model: String, field: String },
}

pub fn parse_schema(input: &str) -> Schema {
    let mut models = Vec::new();
    let mut lines = input.lines().peekable();

    while let Some(line) = lines.next() {
        let line = line.trim();
        if let Some(name) = line.strip_prefix("model ") {
            let mut offset_index: usize = 0;
            let name = name.trim_end_matches('{').trim().to_string();
            let mut fields = Vec::new();

            for line in &mut lines {
                let line = line.trim();
                if line == "}" { break }
                if !line.is_empty() {
                    let mut field = parse_field_raw(line);

                    let is_virtual = field.attributes.iter().any(|f| matches!(f, Attribute::DerivedUnresolved { .. }));

                    if !is_virtual { 
                        field.offset_index = offset_index;
                        field.offset_pos = 3 + offset_index * 4;
                        offset_index += 1;
                    }
                    fields.push(field);
                }
            }

            let payload_offset = 3 + offset_index * 4;
            models.push(Model { name, fields_size: offset_index as u16, fields, payload_offset });
        }
    }

    let mut schema = Schema { models };

    // build name maps
    let model_by_name = build_model_map(&schema);
    let field_by_name = build_field_map(&schema);

    let mut ext_indexes_map: HashMap<(usize, usize), Vec<IndexRef>> = HashMap::new();

    // resolve types and attributes
    for (cur_model_idx, model) in &mut schema.models.iter_mut().enumerate() {
        for (cur_field_idx, field) in &mut model.fields.iter_mut().enumerate() {
            resolve_field_type(&mut field.ty, &model_by_name);

            for attr in &mut field.attributes {
                if let Attribute::DerivedUnresolved { model: model_name, field: field_name } = attr {
                    let m = model_by_name[model_name];
                    let f = field_by_name[m][field_name];
                    field.derived_from = Some(ModelRef::new(m, f));

                    let index_name = format!("{}.{}.idx", model.name, field.name);
                    field.index_name = Some(index_name.clone());
                    ext_indexes_map.entry((m, f)).or_default().push(IndexRef::new(cur_model_idx, cur_field_idx, index_name));
                }
            }

            let is_index = field.attributes.iter().any(|i| matches!(i, Attribute::Index));
            if is_index {
                field.index_name = Some(format!("{}.{}.idx", model.name, field.name));
            }
        }
        // println!("{:?}", model);
    }

    for (key, ext_indexes) in ext_indexes_map {
        schema.models[key.0].fields[key.1].ext_indexes = ext_indexes;
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

    Field { name, ty, offset_index: 0, offset_pos: 0, attributes, is_nullable, derived_from: None, index_name: None, ext_indexes: vec![] }
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
            FieldType::ModelRefListUnresolved(inner.to_string())
        }
    } else if let Some(primitive_field) = get_primitive_type(s) {
        FieldType::Primitive(primitive_field)
    } else {
        FieldType::ModelRefUnresolved(s.to_string())
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

fn resolve_field_type(ty: &mut FieldType, model_by_name: &HashMap<String, usize>) {
    match ty {
        FieldType::ModelRefUnresolved(name) => {
            *ty = FieldType::ModelRef(*model_by_name.get(name).expect(&format!("Not found type {}", name)));
        }
        FieldType::ModelRefListUnresolved(name) => {
            *ty = FieldType::ModelRefList(*model_by_name.get(name).expect(&format!("Not found type {}", name)));
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
