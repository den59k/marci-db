use std::{collections::HashMap, fmt};

use crate::schema::{schema_attributes::{Attribute, FieldCustomFormat, parse_attribute}, schema_default_value::FieldDefault};

#[derive(Debug,Clone)]
pub enum FieldLocation {
    Key { index: usize },
    Body { offset_pos: usize },
    Virtual,
}

#[derive(Debug,Clone)]
pub enum FieldIndex {
    Value { tree_name: String, unique: bool },
    Number { tree_name: String, unique: bool, ty: FieldIndexNum },
    Custom { tree_name: String, index_idx: usize }
}

impl FieldIndex {
    pub fn tree_name(&self) -> &[u8] {
        return match self {
            FieldIndex::Value { tree_name, .. } => tree_name.as_bytes(),
            FieldIndex::Number { tree_name, .. } => tree_name.as_bytes(),
            FieldIndex::Custom { tree_name, .. } => tree_name.as_bytes(),
        }
    }

    pub fn is_unique(&self) -> bool {
        return match self {
            FieldIndex::Value { unique, .. } => *unique,
            FieldIndex::Number { unique, .. } => *unique,
            FieldIndex::Custom { .. } => false,
        }
    }
}

#[derive(Debug,Clone)]
pub enum FieldIndexNum {
    Int64,
    UInt64,
    Float,
    Double
}

#[derive(Debug,Clone)]
pub struct Field {
    pub name: String,
    pub full_name: String,
    pub ty: FieldType,
    pub location: FieldLocation,
    pub nullable: bool,
    pub attributes: Vec<Attribute>,
    pub default_value: Option<FieldDefault>,
    pub indexes: Vec<FieldIndex>,
    pub condition: FieldExistsCondition,
    pub format: Option<FieldCustomFormat>
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
            indexes: vec![],
            default_value: Some(FieldDefault::Counter(0)),
            condition: FieldExistsCondition::None,
            format: None
        }
    }

    pub fn is_id(&self) -> bool {
        return matches!(self.location, FieldLocation::Key { .. });
    }

    pub fn get_size(&self) -> Option<usize> {
        return match self.ty {
            FieldType::Primitive(primitive) => primitive.get_size(),
            FieldType::Enum(_) => Some(2),
            FieldType::PrimitiveList(ty, Some(size)) => ty.get_size().map(|f| f * size),
            _ => None
        }
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
    Byte
}

impl PrimitiveFieldType {
    pub fn get_size(&self) -> Option<usize> {
        return match *self {
            PrimitiveFieldType::Bool => Some(1),
            PrimitiveFieldType::Byte => Some(1),
            PrimitiveFieldType::Float => Some(4),
            PrimitiveFieldType::Int64 | 
                PrimitiveFieldType::UInt64 | 
                PrimitiveFieldType::Double | 
                PrimitiveFieldType::DateTime => Some(8),

            _ => None
        }
    }

    pub fn get_num_type(&self) -> Option<FieldIndexNum> {
        return match *self {
            PrimitiveFieldType::DateTime | PrimitiveFieldType::Int64 => Some(FieldIndexNum::Int64),
            PrimitiveFieldType::Float => Some(FieldIndexNum::Float),
            PrimitiveFieldType::Double => Some(FieldIndexNum::Double),
            PrimitiveFieldType::UInt64 => Some(FieldIndexNum::UInt64),
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
            PrimitiveFieldType::Byte => "u8",
            PrimitiveFieldType::DateTime => "datetime",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone)]
pub enum FieldType {
    Primitive(PrimitiveFieldType),
    // Unresolved тип используется только при парсинге
    RefUnresolved(String),
    RefListUnresolved(String),
    // Ref хранит в себе ID от model_index
    Ref(RefInfo),
    // RefList - это всегда Virtual поле, то есть он хранит инфо только в индексе
    RefList(RefInfo),
    PrimitiveList(PrimitiveFieldType, Option<usize>),
    Enum(EnumInfo)
}

#[derive(Debug, Clone)]
pub struct EnumInfo {
    pub variants: HashMap<u16,Vec<usize>>,
    pub variants_map: HashMap<String,u16>,
    pub variants_names_map: HashMap<u16,String> // Обратная map к variants_map
}

impl EnumInfo {
    pub fn keys_to_string(&self) -> String {
        self.variants_map
            .keys()
            .map(|s| [ "\"", s, "\""].concat() )
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

#[derive(Debug, Clone)]
pub struct RefInfo {
    pub model_index: usize, 
    pub rev_field_idx: Option<usize>, 
    pub parent_index: Option<usize>, 
    pub binding: RefBinding,
    pub is_unique: bool
}

impl RefInfo {
    pub fn new(model_index: usize) -> Self {
        return RefInfo { 
            model_index, 
            rev_field_idx: None, 
            parent_index: None, 
            binding: RefBinding::CurrentId,
            is_unique: false
        }
    }
}

/// Место хранения связи между таблицами
#[derive(Debug, Clone,PartialEq)]
pub enum RefBinding {
    CurrentId,
    FieldValue,
    IndexTree(String)
}

#[derive(Debug,Clone)]
/// Структура для проверки существования поля в Entity
pub enum FieldExistsCondition {
    None,
    EnumValue { field_index: usize, variant: u16 }
}

fn is_valid_string(s: &str) -> bool {
    s.chars().all(|c| {
        c.is_ascii_alphabetic() || c.is_ascii_digit() || c == '_'
    })
}

pub fn parse_field_raw(line: &str) -> Field {
    // имя и тип
    let mut parts = line.split_whitespace();

    let name = parts.next().expect("expected field name").to_string();
    if !is_valid_string(&name) {
        panic!("Invalid field name: \"{}\"", name);
    }

    let type_str = parts.next().expect("expected field type");

    let ty = parse_type(type_str.strip_suffix("?").unwrap_or(type_str));

    let nullable = type_str.ends_with('?') || matches!(ty, FieldType::RefListUnresolved(_));
    
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

    let mut location = FieldLocation::Body { offset_pos: 0 };
    if is_id {
        location = FieldLocation::Key { index: 0 };
    } else if matches!(ty, FieldType::RefListUnresolved(_)) {
        location = FieldLocation::Virtual;
    }

    let format = attributes.iter().find_map(|attr| if let Attribute::Format(fmt) = attr { Some(fmt.clone()) } else { None });

    Field {
      name,
      full_name: String::new(),
      ty,
      location,
      nullable,
      attributes,
      default_value: None,
      condition: FieldExistsCondition::None,
      indexes: vec![],
      format
    }
}

fn parse_type(s: &str) -> FieldType {
    if let Some((ty, bracket)) = s.strip_suffix(']').and_then(|s| s.split_once('[')) {
        let bracket = bracket.trim();
        if bracket.is_empty() {
            if let Some(prim) = get_primitive_type(ty) {
                return FieldType::PrimitiveList(prim, None);
            }
            return FieldType::RefListUnresolved(ty.to_string());
        }

        if let Ok(len) = bracket.parse::<usize>() {
            if let Some(prim) = get_primitive_type(ty) {
                return FieldType::PrimitiveList(prim, Some(len));
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
        "Boolean" => Some(PrimitiveFieldType::Bool),
        "bool" => Some(PrimitiveFieldType::Bool),

        "Byte" => Some(PrimitiveFieldType::Byte),
        "u8" => Some(PrimitiveFieldType::Byte),

        "Int" => Some(PrimitiveFieldType::Int64),
        "i64" => Some(PrimitiveFieldType::Int64),

        "UInt" => Some(PrimitiveFieldType::UInt64),
        "u64" => Some(PrimitiveFieldType::UInt64),

        "Float" => Some(PrimitiveFieldType::Float),
        "f32" => Some(PrimitiveFieldType::Float),

        "Double" => Some(PrimitiveFieldType::Double),
        "f64" => Some(PrimitiveFieldType::Double),
        
        "DateTime" => Some(PrimitiveFieldType::DateTime),
        _ => None
    }
}
