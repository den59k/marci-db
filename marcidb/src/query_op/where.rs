use serde_json::Value;

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
    /// `$between: [min, max]` — inclusive on both ends. Kept as one operator rather than desugared into
    /// `$gte` + `$lte` so it fits the one-operator-per-field rule and plans as a single bounded index range.
    Between(NumberValue, NumberValue),
    StringStartsWith(Vec<u8>),
    StringIncludes(Vec<u8>),
    /// A module-index search operator (`$near`/`$search`) on a `@custom`-indexed field. The raw payload is
    /// interpreted by the provider. This is lifted out of the filter into `QueryOp.search` before planning;
    /// if it ever reaches row evaluation (e.g. inside an include), it matches everything (a no-op residual).
    Search(Value),
    /// One or more path conditions into a `Json` field's blob, all ANDed. A residual scan predicate: there's
    /// no index for a JSON path, so it's evaluated per row by navigating the blob (see `process_where`).
    Json(Vec<JsonFilter>)
}

/// A single `path → operator` condition into a JSON document. `path` is the dot-split segments
/// (e.g. `["address", "city"]`); a numeric segment indexes an array. See [`JsonOp`].
#[derive(Debug, Clone)]
pub struct JsonFilter {
    pub path: Vec<String>,
    pub op: JsonOp,
}

/// An operator applied to the leaf a [`JsonFilter`] path resolves to. Comparison values are kept as
/// `serde_json::Value` and matched type-aware at eval time (numbers compare by value; a missing leaf or a
/// type mismatch is simply "no match", except the negative `$ne`/`$notIn`, which also match a missing leaf).
#[derive(Debug, Clone)]
pub enum JsonOp {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    /// `$between: [min, max]` — inclusive on both ends, over two numbers or two strings.
    Between(Value, Value),
    In(Vec<Value>),
    NotIn(Vec<Value>),
    /// Prefix / substring on a string leaf.
    StringStartsWith(String),
    StringIncludes(String),
    /// The leaf is an array that contains this value.
    Contains(Value),
    /// Whether the path resolves to a value at all.
    Exists(bool),
    /// The leaf's JSON type name: `string｜number｜boolean｜object｜array｜null`.
    Type(String),
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