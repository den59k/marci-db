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
    pub is_virtual: bool
}

#[derive(Debug, Clone, Copy)]
pub enum PrimitiveFieldType {
    String,
    Int64,
    UInt64,
    Float,
    Double,
    Bool,
    DateTime, // пока кодируем как строку (ISO), но тип отличаем
}

#[derive(Debug, Clone)]
pub enum FieldType {
    Primitive(PrimitiveFieldType),
    ModelRefUnresolved(String),
    ModelRef(usize),
    List(Box<FieldType>),
}

#[derive(Debug)]
pub enum Attribute {
    Index,
    DerivedUnresolved { model: String, field: String },
    Derived { model: usize, field: usize },
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
                    if !field.is_virtual { 
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

    // resolve types and attributes
    for model in &mut schema.models {
        for field in &mut model.fields {
            resolve_field_type(&mut field.ty, &model_by_name);

            for attr in &mut field.attributes {
                if let Attribute::DerivedUnresolved { model, field } = attr {
                    let m = model_by_name[model];
                    let f = field_by_name[m][field];
                    *attr = Attribute::Derived { model: m, field: f };
                }
            }
        }

        println!("{:?}", model);
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

    let is_virtual = attributes.iter().any(|i| matches!(i, Attribute::DerivedUnresolved { .. }));

    Field { name, ty, offset_index: 0, offset_pos: 0, attributes, is_nullable, is_virtual }
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
        FieldType::List(Box::new(parse_type(inner)))
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
        "DateTime" => Some(PrimitiveFieldType::DateTime),
        _ => None
    }
}

// fn is_primitive(s: &str) -> bool {
//     matches!(s, "String" | "DateTime" | "Bool" | "Int" | "Float")
// }

fn resolve_field_type(ty: &mut FieldType, model_by_name: &HashMap<String, usize>) {
    match ty {
        FieldType::Primitive(_) | FieldType::ModelRef(_) => {}
        FieldType::ModelRefUnresolved(name) => {
            *ty = FieldType::ModelRef(*model_by_name.get(name).expect(&format!("Not found type {}", name)));
        }
        FieldType::List(inner) => resolve_field_type(inner, model_by_name),
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
