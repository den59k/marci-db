use std::cmp::Ordering;

use crate::Field;

#[derive(Debug)]
pub enum Where<'a> {
    And(Vec<Where<'a>>),
    Or(Vec<Where<'a>>),
    Not(Box<Where<'a>>),
    Field(&'a Field, FieldCompare)
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldCompare {
    EqNull,
    NeNull,
    In(Vec<Vec<u8>>,bool),
    NotIn(Vec<Vec<u8>>,bool),
    Eq(Vec<u8>),
    Ne(Vec<u8>),
    Gt(WhereNumValue),
    Gte(WhereNumValue),
    Lt(WhereNumValue),
    Lte(WhereNumValue),
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereNumValue {
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    DateTime(i64),
}

impl WhereNumValue {
    pub fn compare_with_bytes(&self, data: &[u8]) -> Option<Ordering> {
        match self {
            WhereNumValue::Int64(f) => {
                Some(i64::from_be_bytes(data.try_into().ok()?).cmp(f))
            },
            WhereNumValue::UInt64(f) => {
                u64::from_be_bytes(data.try_into().ok()?).partial_cmp(f)
            },
            WhereNumValue::Float(f) => {
                f.partial_cmp(&f32::from_be_bytes(data.try_into().ok()?))
            },
            WhereNumValue::Double(f) => {
                f.partial_cmp(&f64::from_be_bytes(data.try_into().ok()?))
            },
            WhereNumValue::DateTime(f) => {
                Some(f.cmp(&i64::from_be_bytes(data.try_into().ok()?)))
            },
        }
    }
}

// #[derive(Debug)]
// pub enum Where<'a> {
//   And(Vec<Where<'a>>),
//   Or(Vec<Where<'a>>),
//   Fields(Vec<FieldCondition<'a>>),
// }

// #[derive(Debug)]
// pub struct FieldCondition<'a> {
//     pub field: &'a Field,
//     pub kind: FieldConditionKind,
// }

// #[derive(Debug)]
// pub enum FieldConditionKind {
//     Scalar(Vec<(Operator, WhereValue)>),
// }

// #[derive(Debug, Clone, PartialEq)]
// pub enum WhereValue {
//     Null,
//     Bool(bool),
//     Int64(i64),
//     UInt64(u64),
//     Float(f32),
//     Double(f64),
//     String(String),
//     DateTime(i64)
// }

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// pub enum Operator {
//     Eq
// }
