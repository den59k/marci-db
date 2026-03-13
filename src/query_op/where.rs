use crate::Field;

#[derive(Debug)]
pub enum Where<'a> {
  And(Vec<Where<'a>>),
  Or(Vec<Where<'a>>),
  Fields(Vec<FieldCondition<'a>>),
}

#[derive(Debug)]
pub struct FieldCondition<'a> {
    pub field: &'a Field,
    pub kind: FieldConditionKind,
}

#[derive(Debug)]
pub enum FieldConditionKind {
    Scalar(Vec<(Operator, WhereValue)>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereValue {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(String),
    DateTime(i64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    Eq
}
