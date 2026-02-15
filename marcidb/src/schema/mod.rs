use std::fmt;
use std::{collections::HashMap, fmt::Debug};

use crate::schema::schema_enum::EnumDef;

pub use crate::schema::{schema_attributes::DeleteConstraint,schema_attributes::Attribute,schema_attributes::VectorIndexType};
pub use crate::schema::{schema_parse::parse_schema};

mod schema_enum;
mod schema_attributes;
mod schema_parse;

#[derive(Debug)]
pub struct Schema {
    pub models: Vec<Entity>,
    pub foreign_bindings: Vec<Vec<(FieldRef,DeleteConstraint)>>
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

// #[derive(Debug,Clone,PartialEq)]
// pub enum InsertedIndex {
//     /// Вставляем индекс на основе <A.id><B.id>
//     Direct { tree_name: String },
//     /// Вставляем индекс на основе <B.id><A.id>
//     Rev { tree_name: String },
//     // Вставляем lexical ordered index <field><A.id>
//     Field { tree_name: String }
// }
// impl InsertedIndex {
//     pub fn tree_name(&self) -> &[u8] {
//         match self {
//             InsertedIndex::Direct { tree_name } | 
//             InsertedIndex::Rev { tree_name } |
//             InsertedIndex::Field { tree_name } => 
//             tree_name.as_bytes(),
//         }
//     }
// }


#[derive(Debug,Clone,PartialEq)]
pub struct InsertedIndex {
    pub tree_name: String
}

impl InsertedIndex {
    pub fn tree_name(&self) -> &[u8] {
        return self.tree_name.as_bytes();
    }
}

#[derive(Debug,Clone,PartialEq)]
pub struct InsertedIndexSt {
    pub direct: Option<InsertedIndex>,
    pub rev: Option<InsertedIndex>,
    pub field: Option<InsertedIndex>
}

impl InsertedIndexSt {
    pub fn new() -> InsertedIndexSt {
        return InsertedIndexSt {
            direct: None,
            rev: None,
            field: None
        }
    }
    pub fn is_empty(&self) -> bool {
        return self.direct.is_none() && self.rev.is_none() && self.field.is_none()
    }
}

pub type Aliases = HashMap<String,String>;

#[derive(Debug,Clone)]
pub struct Field {
    pub name: String,
    pub full_name: String,
    pub ty: FieldType,
    /// Offset in bytes  (3 + offset_index*4)
    pub offset_pos: usize,
    pub is_nullable: bool,
    /// Position in ID key (just index, not bytes)
    pub id_idx: Option<usize>,
    pub counter_idx: Option<usize>,
    pub inserted_indexes: InsertedIndexSt,
    pub attributes: Vec<Attribute>,
    /// Ключи, которые можно добавить через inject (используется при запросе на derived элемент в структуре)
    pub injected_fields: Option<(FieldRef,Aliases)>
}

impl Field {
    pub fn is_derived(&self) -> bool {
        self.attributes.iter().any(|attr| matches!(attr, Attribute::DerivedUnresolved { .. }))
    }

    pub fn get_direct_index(&self) -> Option<&InsertedIndex> {
        return self.inserted_indexes.direct.as_ref()
    }

    pub fn get_rev_index(&self) -> Option<&InsertedIndex> {
        return self.inserted_indexes.rev.as_ref()
    }
    pub fn get_field_index(&self) -> Option<&InsertedIndex> {
        return self.inserted_indexes.field.as_ref()
    }

    pub fn get_size(&self) -> Option<usize> {
        match self.ty {
            FieldType::Primitive(primitive_type) => primitive_type.get_size(),
            FieldType::PrimitiveFixedList(primitive_type, size) => {
                primitive_type.get_size().map(|i| i * size)
            },
            _ => None
        }
    }
}

#[derive(Debug,Clone,PartialEq, Eq,Hash,PartialOrd)]
pub struct FieldRef {
    pub model_index: usize,
    pub field_index: usize,
    pub struct_field_index: Option<usize>,
    pub enum_variant_index: Option<(usize, usize)>
}
impl FieldRef {
    pub fn new(model_index: usize, field_index: usize) ->  FieldRef {
        return FieldRef { model_index, field_index, struct_field_index: None, enum_variant_index: None };
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
    ModelRef(usize),
    ModelRefList(usize),
    PrimitiveList(PrimitiveFieldType),
    PrimitiveFixedList(PrimitiveFieldType,usize),
    Struct(Entity),
    StructList(Entity),
    Enum(EnumDef)
}

impl Schema {
    pub fn get_field(&self, key: &FieldRef) -> &Field {
        let model = &self.models[key.model_index];
        let field = &model.fields[key.field_index];
        if let Some(struct_field_index) = &key.struct_field_index {
            let st = match &field.ty {
                FieldType::Struct(st) => st,
                FieldType::StructList(st) => st,
                _ => { panic!("Trying to get index from non-struct {}", field.full_name); }
            };
            return &st.fields[*struct_field_index];
        } else if let Some(enum_variant_index) = &key.enum_variant_index {
            let en = match &field.ty {
                FieldType::Enum(en) => en,
                _ => { panic!("Trying to get index from non-enum {}", field.full_name); }
            };
            return &en.variants[enum_variant_index.0].fields[enum_variant_index.1];
        } else {
            return field;
        }
    }

    pub fn get_field_entity(&self, key: &FieldRef) -> &Entity {
        let model = &self.models[key.model_index];
        let field = &model.fields[key.field_index];
        if let Some(_) = &key.struct_field_index {
            let st = match &field.ty {
                FieldType::Struct(st) => st,
                FieldType::StructList(st) => st,
                _ => { panic!("Trying to get index from non-struct {}", field.full_name); }
            };
            return st;
        } else if let Some(_enum_variant_index) = &key.enum_variant_index {
            // let en = match &field.ty {
            //     FieldType::Enum(en) => en,
            //     _ => { panic!("Trying to get index from non-enum {}", field.full_name); }
            // };
            // return &en.variants[enum_variant_index.0];
            todo!("Get entity from enum are not supported yet")
        } else {
            return model;
        }
    }

    fn get_field_mut(&mut self, key: &FieldRef) -> &mut Field {
        let model = &mut self.models[key.model_index];
        let field = &mut model.fields[key.field_index];
        if let Some(struct_field_index) = &key.struct_field_index {
            let st = match &mut field.ty {
                FieldType::Struct(st) => st,
                FieldType::StructList(st) => st,
                _ => { panic!("Trying to get index from non-struct {}", field.full_name); }
            };
            return &mut st.fields[*struct_field_index];
        } else if let Some(enum_variant_index) = &key.enum_variant_index {
            let en = match &mut field.ty {
                FieldType::Enum(en) => en,
                _ => { panic!("Trying to get index from non-enum {}", field.full_name); }
            };
            return &mut en.variants[enum_variant_index.0].fields[enum_variant_index.1];
        } else {
            return field;
        }
    }
    pub fn walk<F: FnMut(&Field, FieldRef)>(&self, mut f: F) {
        for (model_index, model) in self.models.iter().enumerate() {
            for (field_index, field) in model.fields.iter().enumerate() {

                f(field, FieldRef::new(model_index, field_index));

                match &field.ty {
                    FieldType::Struct(st) | FieldType::StructList(st) => {
                        for (sub_index, subfield) in st.fields.iter().enumerate() {
                            f(subfield, FieldRef { model_index, field_index, struct_field_index: Some(sub_index), enum_variant_index: None });
                        }
                    },
                    FieldType::Enum(en) => {
                        for (variant_idx, variant) in en.variants.iter().enumerate() {
                            for (variant_field_index, variant_field) in variant.fields.iter().enumerate() {
                                f(variant_field, FieldRef { model_index, field_index, struct_field_index: None, enum_variant_index: Some((variant_idx, variant_field_index)) })
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

