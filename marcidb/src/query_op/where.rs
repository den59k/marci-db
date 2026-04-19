use crate::{Field, FieldLocation, num_utils::NumberValue, query_op::PrefixKey, schema::Entity};

#[derive(Debug, Clone)]
pub enum Where<'a> {
    True,
    And(Vec<Where<'a>>),
    Or(Vec<Where<'a>>),
    Not(Box<Where<'a>>),
    Field(&'a Field, FieldCompare<'a>)
}

#[derive(Debug, Clone)]
pub enum FieldCompare<'a> {
    EqNull,
    NeNull,
    Ref(&'a Entity, PrefixKey<'a>, FieldCompareRef<'a>),
    In(Vec<Vec<u8>>,bool),
    NotIn(Vec<Vec<u8>>,bool),
    Eq(Vec<u8>),
    Ne(Vec<u8>),
    Gt(NumberValue),
    Gte(NumberValue),
    Lt(NumberValue),
    Lte(NumberValue),
    StringStartsWith(Vec<u8>),
    StringIncludes(Vec<u8>)
}

#[derive(Debug, Clone)]
pub enum FieldCompareRef<'a> {
    Every(Box<Where<'a>>),
    Some(Box<Where<'a>>),
    None(Box<Where<'a>>),
    Eq(Box<Where<'a>>),
    Ne(Box<Where<'a>>),
    Exists,
    NotExists
}


impl Where<'_> {
    pub fn only_id_required(&self, entity: &Entity) -> bool {
        return match self {
            Where::True => true,
            Where::And(items) => items.iter().all(|i| i.only_id_required(entity)),
            Where::Or(items) => items.iter().all(|i| i.only_id_required(entity)),
            Where::Not(item) => item.only_id_required(entity),
            Where::Field(field, _) => {
                return matches!(field.location, FieldLocation::Key { .. })
            },
        };
    }
}