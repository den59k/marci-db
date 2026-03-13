use std::fmt;

use crate::schema::{schema_attributes::{Attribute, parse_attribute}};


#[derive(Debug,Clone)]
pub enum FieldLocation {
    Key { index: usize },
    Body { offset: usize },
    Virtual,
}

#[derive(Debug,Clone)]
pub struct Field {
    pub name: String,
    pub full_name: String,
    pub ty: FieldType,
    pub location: FieldLocation,
    pub nullable: bool,
    pub attributes: Vec<Attribute>,
    pub default_value: Option<FieldDefault>
}

#[derive(Debug,Clone)]
pub enum FieldDefault {
    Counter(usize)
}

impl Field {

    pub fn new_id() -> Self {
        Field { 
            name: "id".to_string(), 
            full_name: String::new(),
            ty: FieldType::Primitive(PrimitiveFieldType::UInt64), 
            nullable: false,
            location: FieldLocation::Key { index: 0 },
            attributes: vec![],
            default_value: Some(FieldDefault::Counter(0))
        }
    }

    pub fn is_id(&self) -> bool {
        return matches!(self.location, FieldLocation::Key { .. });
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PrimitiveFieldType {
    String,
    Int64,
    UInt64,
    Float,
    Double,
    Bool,
    DateTime,
}

impl PrimitiveFieldType {

    pub fn get_size(&self) -> Option<usize> {
        return match *self {
            PrimitiveFieldType::Bool => Some(1),
            PrimitiveFieldType::Float => Some(4),
            PrimitiveFieldType::Int64 | 
                PrimitiveFieldType::UInt64 | 
                PrimitiveFieldType::Double | 
                PrimitiveFieldType::DateTime => Some(8),

            _ => None
        }
    }
}

impl fmt::Display for PrimitiveFieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            PrimitiveFieldType::String => "string",
            PrimitiveFieldType::Int64 => "i64",
            PrimitiveFieldType::UInt64 => "u64",
            PrimitiveFieldType::Float => "float",
            PrimitiveFieldType::Double => "double",
            PrimitiveFieldType::Bool => "bool",
            PrimitiveFieldType::DateTime => "datetime",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone)]
pub enum FieldType {
    Primitive(PrimitiveFieldType),
    // Ссылка на либо model, либо struct
    RefUnresolved(String),
    // Ссылка на список либо model, либо struct
    RefListUnresolved(String),
    Ref { model_index: usize, rev_field_idx: Option<usize>, st_index: Option<usize> },
    RefList { model_index: usize, rev_field_idx: Option<usize>, st_index: Option<usize> },
    PrimitiveList(PrimitiveFieldType),
    PrimitiveFixedList(PrimitiveFieldType,usize),
    // Struct(Entity),
    // StructList(Entity),
    // Enum(EnumDef)
}

pub fn parse_field_raw(line: &str) -> Field {
    // имя и тип
    let mut parts = line.split_whitespace();

    let name = parts.next().expect("expected field name").to_string();

    let type_str = parts.next().expect("expected field type");

    let nullable = type_str.ends_with('?');
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
    let _is_unique = attributes.iter().any(|attr| matches!(attr, Attribute::Unique));

    let mut location = FieldLocation::Body { offset: 0 };
    if is_id {
        location = FieldLocation::Key { index: 0 };
    } else if matches!(ty, FieldType::RefListUnresolved(_)) {
        location = FieldLocation::Virtual;
    }

    Field {
      name,
      full_name: String::new(),
      ty,
      location,
      nullable,
      attributes,
      default_value: None
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

