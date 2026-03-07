use std::collections::HashMap;

use serde_json::Value;

use crate::schema::{Aliases, Entity, Field, FieldRef, FieldType, PrimitiveFieldType, Schema};

#[derive(Debug)]
pub enum ParseWhereError {
    TypeMismatch { field: String, expected: String },
    UnknownFieldAlias(String),
    InvalidData,
}

fn type_mismatch(field: &Field, expected: impl Into<String>) -> ParseWhereError {
    ParseWhereError::TypeMismatch {
        field: field.full_name.clone(),
        expected: expected.into(),
    }
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
    DateTime(i64),
    UInt16(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug)]
pub struct MarciWhere<'a> {
    pub node: WhereNode<'a>,
}

#[derive(Debug)]
pub enum WhereNode<'a> {
    And(Vec<MarciWhere<'a>>),
    Or(Vec<MarciWhere<'a>>),
    Fields(Vec<FieldCondition<'a>>),
}

#[derive(Debug)]
pub struct FieldCondition<'a> {
    pub field: &'a Field,
    pub kind: FieldConditionKind<'a>,
}

#[derive(Debug)]
pub enum FieldConditionKind<'a> {
    Scalar(Vec<(Operator, WhereValue)>),

    StructWhere {
        st: &'a Entity,
        inner: Box<MarciWhere<'a>>,
    },

    StructListAll {
        st: &'a Entity,
        elements: Vec<Value>,
    },

    ModelRefListAll(Vec<u64>),

    Injected {
        st_ref: FieldRef,
        st: &'a Entity,
        inner: Box<MarciWhere<'a>>,
        parent_id_byte_start: usize,
    },

    VectorSearch {
        point: Vec<f32>,
        take: u64,
        threshold: f32,
    },
}

pub fn parse_where_json<'a>(
    model: &'a Entity,
    schema: &'a Schema,
    body_json: &Value,
) -> Result<Option<MarciWhere<'a>>, ParseWhereError> {
    let Some(where_obj) = body_json.get("$where") else {
        return Ok(None);
    };
    let mw = parse_where_node(model, schema, where_obj)?;
    Ok(Some(mw))
}

fn parse_where_node<'a>(
    model: &'a Entity,
    schema: &'a Schema,
    where_obj: &Value,
) -> Result<MarciWhere<'a>, ParseWhereError> {
    if let Some(and_array) = where_obj.get("$and").and_then(|v| v.as_array()) {
        let branches = and_array
            .iter()
            .map(|cond| parse_where_node(model, schema, cond))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(MarciWhere { node: WhereNode::And(branches) });
    }

    if let Some(or_array) = where_obj.get("$or").and_then(|v| v.as_array()) {
        let branches = or_array
            .iter()
            .map(|cond| parse_where_node(model, schema, cond))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(MarciWhere { node: WhereNode::Or(branches) });
    }

    let mut conditions: Vec<FieldCondition<'a>> = Vec::new();

    for field in model.fields.iter() {
        let Some(field_val) = where_obj.get(&field.name) else {
            continue;
        };

        let kind = parse_field_condition(field, schema, field_val)?;
        conditions.push(FieldCondition { field, kind });
    }

    Ok(MarciWhere { node: WhereNode::Fields(conditions) })
}

fn parse_field_condition<'a>(
    field: &'a Field,
    schema: &'a Schema,
    val: &Value,
) -> Result<FieldConditionKind<'a>, ParseWhereError> {
    if let Some((st_ref, aliases)) = &field.injected_fields {
        return parse_injected_condition(field, schema, st_ref, aliases, val);
    }

    match &field.ty {
        FieldType::Struct(st) => {
            let cond_obj = val.as_object().ok_or_else(|| {
                type_mismatch(field, "object with field conditions")
            })?;
            let sub_where = Value::Object(cond_obj.clone());
            let inner = parse_where_node(st, schema, &sub_where)?;
            return Ok(FieldConditionKind::StructWhere {
                st,
                inner: Box::new(inner),
            });
        }

        FieldType::StructList(st) => {
            let cond_obj = val.as_object().ok_or_else(|| {
                type_mismatch(field, "object with $all or field conditions")
            })?;
            if let Some(all_array) = cond_obj.get("$all").and_then(|v| v.as_array()) {
                return Ok(FieldConditionKind::StructListAll {
                    st,
                    elements: all_array.clone(),
                });
            }
            return Ok(FieldConditionKind::StructListAll { st, elements: vec![] });
        }

        FieldType::ModelRefList(_) => {
            let raw_ids: Vec<Value> = if let Some(all_array) =
                val.get("$all").and_then(|v| v.as_array())
            {
                all_array.clone()
            } else if val.is_array() {
                val.as_array().unwrap().clone()
            } else {
                vec![val.clone()]
            };

            let ids = raw_ids
                .iter()
                .map(extract_ref_id)
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(FieldConditionKind::ModelRefListAll(ids));
        }

        _ if field.attributes.iter().any(|a| {
            matches!(a, crate::schema::Attribute::VectorIndex(_))
        }) => {
            let obj = val.as_object();
            let point = obj
                .and_then(|o| o.get("$close"))
                .and_then(|v| v.as_array())
                .and_then(|arr| {
                    arr.iter()
                        .map(|v| v.as_f64().map(|f| f as f32))
                        .collect::<Option<Vec<f32>>>()
                })
                .ok_or_else(|| {
                    type_mismatch(field, "{ $close: f32[] }")
                })?;
            let take = obj
                .and_then(|o| o.get("$take"))
                .and_then(|v| v.as_u64())
                .unwrap_or(10);
            let threshold = obj
                .and_then(|o| o.get("$threshold"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0) as f32;
            return Ok(FieldConditionKind::VectorSearch { point, take, threshold });
        }

        _ => {
            let ops = parse_operator_conditions(field, val)?;
            return Ok(FieldConditionKind::Scalar(ops));
        }
    }
}

fn parse_injected_condition<'a>(
    field: &'a Field,
    schema: &'a Schema,
    st_ref: &FieldRef,
    aliases: &Aliases,
    val: &Value,
) -> Result<FieldConditionKind<'a>, ParseWhereError> {
    if !val.is_object() {
        return Err(type_mismatch(field, "object with field conditions"));
    }

    let field_def = schema.get_field(st_ref);
    let st = match &field_def.ty {
        FieldType::Struct(st) | FieldType::StructList(st) => st,
        _ => {
            return Err(ParseWhereError::TypeMismatch {
                field: field.full_name.clone(),
                expected: "struct or struct list backing field".into(),
            })
        }
    };

    let current_model_index = match &field.ty {
        FieldType::ModelRefList(idx) => *idx,
        _ => {
            return Err(ParseWhereError::TypeMismatch {
                field: field.full_name.clone(),
                expected: "ModelRefList with @inject".into(),
            })
        }
    };

    let parent_field = st
        .fields
        .iter()
        .find(|f| matches!(&f.ty, FieldType::ModelRef(idx) if *idx == current_model_index))
        .ok_or_else(|| ParseWhereError::TypeMismatch {
            field: field.full_name.clone(),
            expected: format!(
                "struct {} must contain a ModelRef back to the parent model",
                st.name
            ),
        })?;

    let parent_id_idx = parent_field
        .id_idx
        .expect("ModelRef back-reference must carry @id");

    let alias_to_field: HashMap<&str, String> = aliases
        .iter()
        .map(|(full_name, alias)| {
            let field_name = full_name.split('.').last().unwrap().to_string();
            (alias.as_str(), field_name)
        })
        .collect();

    let mut real_map = serde_json::Map::new();
    if let Some(obj) = val.as_object() {
        for (key, value) in obj {
            if key.starts_with('$') {
                real_map.insert(key.clone(), value.clone());
            } else if let Some(real_name) = alias_to_field.get(key.as_str()) {
                real_map.insert(real_name.clone(), value.clone());
            } else {
                return Err(ParseWhereError::UnknownFieldAlias(key.clone()));
            }
        }
    }

    let inner = parse_where_node(st, schema, &Value::Object(real_map))?;

    Ok(FieldConditionKind::Injected {
        st_ref: st_ref.clone(),
        st,
        inner: Box::new(inner),
        parent_id_byte_start: parent_id_idx * 8,
    })
}

pub fn parse_operator_conditions(
    field: &Field,
    val: &Value,
) -> Result<Vec<(Operator, WhereValue)>, ParseWhereError> {
    let mut ops = Vec::new();
    if let Some(obj) = val.as_object() {
        let mut found_op = false;
        for (k, v) in obj {
            let op = match k.as_str() {
                "$eq" => Operator::Eq,
                "$ne" => Operator::Ne,
                "$gt" => Operator::Gt,
                "$lt" => Operator::Lt,
                "$ge" => Operator::Ge,
                "$le" => Operator::Le,
                _ => continue,
            };
            found_op = true;
            ops.push((op, parse_json_value(field, v)?));
        }
        if !found_op {
            ops.push((Operator::Eq, parse_json_value(field, val)?));
        }
    } else {
        ops.push((Operator::Eq, parse_json_value(field, val)?));
    }
    Ok(ops)
}

pub fn parse_json_value(field: &Field, val: &Value) -> Result<WhereValue, ParseWhereError> {
    if val.is_null() {
        return Ok(WhereValue::Null);
    }
    match &field.ty {
        FieldType::Primitive(prim) => parse_primitive_json(field, prim, val),
        FieldType::ModelRef(_) => {
            let id = if let Some(n) = val.as_u64() {
                n
            } else if let Some(obj) = val.as_object() {
                obj.get("id")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| type_mismatch(field, "{ id: u64 }"))?
            } else {
                return Err(type_mismatch(field, "u64 or object with id"));
            };
            Ok(WhereValue::UInt64(id))
        }
        FieldType::Enum(en) => {
            let s = val
                .as_str()
                .ok_or_else(|| type_mismatch(field, "string (enum variant)"))?;
            let idx = *en
                .variants_map
                .get(s)
                .ok_or_else(|| type_mismatch(field, format!("one of: {}", en.variants_str())))?;
            Ok(WhereValue::UInt16(idx))
        }
        _ => Err(type_mismatch(field, "primitive, reference, or enum")),
    }
}

fn parse_primitive_json(
    field: &Field,
    prim: &PrimitiveFieldType,
    val: &Value,
) -> Result<WhereValue, ParseWhereError> {
    match prim {
        PrimitiveFieldType::String => Ok(WhereValue::String(
            val.as_str()
                .ok_or_else(|| type_mismatch(field, "string"))?
                .to_string(),
        )),
        PrimitiveFieldType::Int64 => Ok(WhereValue::Int64(
            val.as_i64().ok_or_else(|| type_mismatch(field, "i64"))?,
        )),
        PrimitiveFieldType::UInt64 => Ok(WhereValue::UInt64(
            val.as_u64().ok_or_else(|| type_mismatch(field, "u64"))?,
        )),
        PrimitiveFieldType::Float => Ok(WhereValue::Float(
            val.as_f64().ok_or_else(|| type_mismatch(field, "float"))? as f32,
        )),
        PrimitiveFieldType::Double => Ok(WhereValue::Double(
            val.as_f64().ok_or_else(|| type_mismatch(field, "double"))?,
        )),
        PrimitiveFieldType::Bool => Ok(WhereValue::Bool(
            val.as_bool().ok_or_else(|| type_mismatch(field, "bool"))?,
        )),
        PrimitiveFieldType::DateTime => {
            let epoch = if let Some(n) = val.as_i64() {
                n
            } else if let Some(s) = val.as_str() {
                s.parse::<chrono::DateTime<chrono::Utc>>()
                    .map_err(|_| type_mismatch(field, "ISO-8601 string"))?
                    .timestamp_millis()
            } else {
                return Err(type_mismatch(field, "i64 or ISO-8601 string"));
            };
            Ok(WhereValue::DateTime(epoch))
        }
    }
}

fn extract_ref_id(val: &Value) -> Result<u64, ParseWhereError> {
    match val {
        Value::Number(n) => n.as_u64().ok_or(ParseWhereError::InvalidData),
        Value::Object(obj) => obj
            .get("id")
            .and_then(|v| v.as_u64())
            .ok_or(ParseWhereError::InvalidData),
        _ => Err(ParseWhereError::InvalidData),
    }
}

pub fn decode_bytes_to_value(
    field: &Field,
    bytes: &[u8],
) -> Result<WhereValue, ParseWhereError> {
    match &field.ty {
        FieldType::Primitive(prim) => decode_primitive_bytes(prim, bytes),
        FieldType::ModelRef(_) => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(WhereValue::UInt64(u64::from_be_bytes(arr)))
        }
        FieldType::Enum(_) => {
            if bytes.len() < 2 {
                return Err(ParseWhereError::InvalidData);
            }
            let idx = u16::from_be_bytes(bytes[..2].try_into().unwrap());
            Ok(WhereValue::UInt16(idx))
        }
        _ => Err(ParseWhereError::TypeMismatch {
            field: field.full_name.clone(),
            expected: "primitive, reference, or enum".into(),
        }),
    }
}

fn decode_primitive_bytes(
    prim: &PrimitiveFieldType,
    bytes: &[u8],
) -> Result<WhereValue, ParseWhereError> {
    match prim {
        PrimitiveFieldType::Bool => {
            Ok(WhereValue::Bool(bytes.first().map_or(false, |&b| b != 0)))
        }
        PrimitiveFieldType::Int64 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(WhereValue::Int64(i64::from_be_bytes(arr)))
        }
        PrimitiveFieldType::UInt64 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(WhereValue::UInt64(u64::from_be_bytes(arr)))
        }
        PrimitiveFieldType::Float => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(WhereValue::Float(f32::from_be_bytes(arr)))
        }
        PrimitiveFieldType::Double => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(WhereValue::Double(f64::from_be_bytes(arr)))
        }
        PrimitiveFieldType::String => {
            let s = String::from_utf8(bytes.to_vec()).map_err(|_| ParseWhereError::InvalidData)?;
            Ok(WhereValue::String(s))
        }
        PrimitiveFieldType::DateTime => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(WhereValue::DateTime(i64::from_be_bytes(arr)))
        }
    }
}

pub fn encode_where_value(field: &Field, val: &WhereValue) -> Result<Vec<u8>, ParseWhereError> {
    let mut out = Vec::new();
    match (&field.ty, val) {
        (FieldType::Primitive(PrimitiveFieldType::String), WhereValue::String(s)) => {
            out.extend_from_slice(s.as_bytes());
        }
        (FieldType::Primitive(PrimitiveFieldType::Int64), WhereValue::Int64(n)) => {
            out.extend_from_slice(&n.to_be_bytes());
        }
        (FieldType::Primitive(PrimitiveFieldType::UInt64), WhereValue::UInt64(n)) => {
            out.extend_from_slice(&n.to_be_bytes());
        }
        (FieldType::Primitive(PrimitiveFieldType::Float), WhereValue::Float(n)) => {
            out.extend_from_slice(&n.to_be_bytes());
        }
        (FieldType::Primitive(PrimitiveFieldType::Double), WhereValue::Double(n)) => {
            out.extend_from_slice(&n.to_be_bytes());
        }
        (FieldType::Primitive(PrimitiveFieldType::Bool), WhereValue::Bool(b)) => {
            out.push(if *b { 1 } else { 0 });
        }
        (FieldType::Primitive(PrimitiveFieldType::DateTime), WhereValue::DateTime(ts)) => {
            out.extend_from_slice(&ts.to_be_bytes());
        }
        (FieldType::ModelRef(_), WhereValue::UInt64(id)) => {
            out.extend_from_slice(&id.to_be_bytes());
        }
        (FieldType::Enum(_), WhereValue::UInt16(idx)) => {
            out.extend_from_slice(&idx.to_be_bytes());
        }
        _ => {
            return Err(ParseWhereError::TypeMismatch {
                field: field.full_name.clone(),
                expected: "matching value type".into(),
            })
        }
    }
    if field.get_size().is_none()
        && !matches!(
            field.ty,
            FieldType::ModelRef(_) | FieldType::ModelRefList(_)
        )
    {
        out.push(0);
    }
    Ok(out)
}

pub fn check_condition(value: &WhereValue, op: Operator, target: &WhereValue) -> bool {
    match (value, target) {
        (WhereValue::Null, WhereValue::Null) => op == Operator::Eq,
        (WhereValue::Null, _) => op == Operator::Ne,
        (_, WhereValue::Null) => op == Operator::Ne,
        (WhereValue::Bool(a), WhereValue::Bool(b)) => cmp_op(a, b, op),
        (WhereValue::Int64(a), WhereValue::Int64(b)) => cmp_op(a, b, op),
        (WhereValue::UInt64(a), WhereValue::UInt64(b)) => cmp_op(a, b, op),
        (WhereValue::Float(a), WhereValue::Float(b)) => cmp_op(a, b, op),
        (WhereValue::Double(a), WhereValue::Double(b)) => cmp_op(a, b, op),
        (WhereValue::String(a), WhereValue::String(b)) => cmp_op(a, b, op),
        (WhereValue::DateTime(a), WhereValue::DateTime(b)) => cmp_op(a, b, op),
        (WhereValue::UInt16(a), WhereValue::UInt16(b)) => cmp_op(a, b, op),
        _ => false,
    }
}

#[inline]
fn cmp_op<T: PartialOrd + PartialEq>(a: &T, b: &T, op: Operator) -> bool {
    match op {
        Operator::Eq => a == b,
        Operator::Ne => a != b,
        Operator::Gt => a > b,
        Operator::Lt => a < b,
        Operator::Ge => a >= b,
        Operator::Le => a <= b,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::schema::parse_schema;

    fn scalar_schema() -> crate::schema::Schema {
        parse_schema("
enum RoleKind {
  admin
  user
}

model Post {
  title   String
}

model User {
  name      String
  surname   String
  age       Int
  score     Float
  rating    Double
  active    Bool
  created   DateTime
  post      Post
  posts     Post[]
  role      RoleKind
}
")
    }

    fn struct_schema() -> crate::schema::Schema {
        parse_schema("
model User {
  name   String
  info   UserInfo
}

struct UserInfo {
  bio    String
  age    Int
}
")
    }

    fn struct_list_schema() -> crate::schema::Schema {
        parse_schema("
model User {
  name   String
  items  Item[]
}

struct Item {
  label  String
}
")
    }

    fn inject_schema() -> crate::schema::Schema {
        parse_schema("
model Project {
  name   String
  users  UserRole[]
}

struct UserRole {
  user   User     @id
  role   String
}

model User {
  name      String
  projects  Project[]  @derived(Project.users.user) @inject(Project.users { role as user_role })
}
")
    }

    #[test]
    fn test_no_where_key_returns_none() {
        let schema = scalar_schema();
        let model = &schema.models[1]; 
        assert!(parse_where_json(model, &schema, &json!({ "name": true })).unwrap().is_none());
        assert!(parse_where_json(model, &schema, &json!({})).unwrap().is_none());
    }

    #[test]
    fn test_empty_where_object_produces_empty_fields() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": {} }))
            .unwrap()
            .unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        assert!(conds.is_empty());
    }

    #[test]
    fn test_unknown_field_is_silently_ignored() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "no_such_field": 42 } })
        ).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        assert!(conds.is_empty());
    }

    #[test]
    fn test_implicit_eq() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "name": "Alice" } }))
            .unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        assert_eq!(conds.len(), 1);
        let FieldConditionKind::Scalar(ops) = &conds[0].kind else { panic!() };
        assert_eq!(ops, &[(Operator::Eq, WhereValue::String("Alice".into()))]);
    }

    #[test]
    fn test_all_scalar_operators() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let body = json!({ "$where": { "age": { "$eq": 1, "$ne": 2, "$gt": 3, "$lt": 4, "$ge": 5, "$le": 6 } } });
        let mw = parse_where_json(model, &schema, &body).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &conds[0].kind else { panic!() };

        let op_map: std::collections::HashMap<Operator, i64> = ops
            .iter()
            .map(|(op, v)| (*op, if let WhereValue::Int64(n) = v { *n } else { panic!() }))
            .collect();

        assert_eq!(op_map[&Operator::Eq],  1);
        assert_eq!(op_map[&Operator::Ne],  2);
        assert_eq!(op_map[&Operator::Gt],  3);
        assert_eq!(op_map[&Operator::Lt],  4);
        assert_eq!(op_map[&Operator::Ge],  5);
        assert_eq!(op_map[&Operator::Le],  6);
    }

    #[test]
    fn test_unknown_operator_keys_are_ignored() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let body = json!({ "$where": { "age": { "$gt": 0, "$unknown": 99 } } });
        let mw = parse_where_json(model, &schema, &body).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &conds[0].kind else { panic!() };
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].0, Operator::Gt);
    }


    #[test]
    fn test_primitive_string() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "name": "Bob" } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops[0].1, WhereValue::String("Bob".into()));
    }

    #[test]
    fn test_primitive_int() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "age": -5 } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops[0].1, WhereValue::Int64(-5));
    }

    #[test]
    fn test_primitive_float() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "score": 3.14 } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert!(matches!(ops[0].1, WhereValue::Float(_)));
    }

    #[test]
    fn test_primitive_double() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "rating": 2.718 } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert!(matches!(ops[0].1, WhereValue::Double(_)));
    }

    #[test]
    fn test_primitive_bool() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "active": true } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops[0].1, WhereValue::Bool(true));
    }

    #[test]
    fn test_primitive_datetime_as_epoch() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "created": 1_700_000_000_000i64 } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops[0].1, WhereValue::DateTime(1_700_000_000_000));
    }

    #[test]
    fn test_primitive_datetime_as_iso8601() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema,
            &json!({ "$where": { "created": "2023-11-14T22:13:20Z" } }),
        ).unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert!(matches!(ops[0].1, WhereValue::DateTime(_)));
        if let WhereValue::DateTime(ts) = ops[0].1 {
            assert!(ts > 1_699_000_000_000);
        }
    }

    #[test]
    fn test_enum_by_name() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "role": "admin" } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops[0].1, WhereValue::UInt16(0));
    }

    #[test]
    fn test_enum_second_variant() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "role": "user" } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops[0].1, WhereValue::UInt16(1));
    }

    #[test]
    fn test_enum_unknown_variant_error() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let err = parse_where_json(
            model, &schema, &json!({ "$where": { "role": "superuser" } })
        ).unwrap_err();
        assert!(matches!(err, ParseWhereError::TypeMismatch { .. }));
    }


    #[test]
    fn test_model_ref_bare_id() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "post": 42 } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops[0].1, WhereValue::UInt64(42));
    }

    #[test]
    fn test_model_ref_object_id() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "post": { "id": 99 } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops[0].1, WhereValue::UInt64(99));
    }


    #[test]
    fn test_model_ref_list_explicit_all() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "posts": { "$all": [6, 9] } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::ModelRefListAll(ids) = &c[0].kind else { panic!() };
        assert_eq!(ids, &[6u64, 9u64]);
    }

    #[test]
    fn test_model_ref_list_bare_array() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "posts": [1, 2, 3] } })
        ).unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::ModelRefListAll(ids) = &c[0].kind else { panic!() };
        assert_eq!(ids, &[1u64, 2u64, 3u64]);
    }

    #[test]
    fn test_model_ref_list_single_value() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "posts": 7 } })
        ).unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::ModelRefListAll(ids) = &c[0].kind else { panic!() };
        assert_eq!(ids, &[7u64]);
    }

    #[test]
    fn test_model_ref_list_object_ids() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "posts": { "$all": [{"id": 5}, {"id": 10}] } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::ModelRefListAll(ids) = &c[0].kind else { panic!() };
        assert_eq!(ids, &[5u64, 10u64]);
    }

    
    
    

    #[test]
    fn test_null_implicit_eq() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(model, &schema, &json!({ "$where": { "name": null } }))
            .unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops, &[(Operator::Eq, WhereValue::Null)]);
    }

    #[test]
    fn test_null_explicit_eq() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "name": { "$eq": null } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops, &[(Operator::Eq, WhereValue::Null)]);
    }

    #[test]
    fn test_null_ne() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "name": { "$ne": null } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(c) = &mw.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &c[0].kind else { panic!() };
        assert_eq!(ops, &[(Operator::Ne, WhereValue::Null)]);
    }

    #[test]
    fn test_struct_where_parsed() {
        let schema = struct_schema();
        let model = &schema.models[0]; 
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "info": { "bio": "developer" } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        assert_eq!(conds.len(), 1);
        let FieldConditionKind::StructWhere { st, inner } = &conds[0].kind else { panic!() };
        assert_eq!(st.name, "User.info");
        let WhereNode::Fields(inner_conds) = &inner.node else { panic!() };
        assert_eq!(inner_conds.len(), 1);
        assert_eq!(inner_conds[0].field.name, "bio");
    }

    #[test]
    fn test_struct_where_nested_operators() {
        let schema = struct_schema();
        let model = &schema.models[0];
        let mw = parse_where_json(
            model, &schema,
            &json!({ "$where": { "info": { "age": { "$gt": 18, "$lt": 65 } } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        let FieldConditionKind::StructWhere { inner, .. } = &conds[0].kind else { panic!() };
        let WhereNode::Fields(inner_conds) = &inner.node else { panic!() };
        let FieldConditionKind::Scalar(ops) = &inner_conds[0].kind else { panic!() };
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn test_struct_non_object_error() {
        let schema = struct_schema();
        let model = &schema.models[0];
        let err = parse_where_json(
            model, &schema, &json!({ "$where": { "info": 42 } })
        ).unwrap_err();
        assert!(matches!(err, ParseWhereError::TypeMismatch { .. }));
    }

    #[test]
    fn test_struct_list_all_parsed() {
        let schema = struct_list_schema();
        let model = &schema.models[0]; 
        let mw = parse_where_json(
            model, &schema,
            &json!({ "$where": { "items": { "$all": [{ "label": "x" }, { "label": "y" }] } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        let FieldConditionKind::StructListAll { st, elements } = &conds[0].kind else { panic!() };
        assert_eq!(st.name, "User.items");
        assert_eq!(elements.len(), 2);
    }

    #[test]
    fn test_struct_list_empty_all() {
        let schema = struct_list_schema();
        let model = &schema.models[0];
        let mw = parse_where_json(
            model, &schema, &json!({ "$where": { "items": { "$all": [] } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        let FieldConditionKind::StructListAll { elements, .. } = &conds[0].kind else { panic!() };
        assert!(elements.is_empty());
    }

    #[test]
    fn test_struct_list_non_object_error() {
        let schema = struct_list_schema();
        let model = &schema.models[0];
        let err = parse_where_json(
            model, &schema, &json!({ "$where": { "items": 42 } })
        ).unwrap_err();
        assert!(matches!(err, ParseWhereError::TypeMismatch { .. }));
    }

    #[test]
    fn test_injected_alias_resolved() {
        let schema = inject_schema();
        
        let user_model = schema.models.iter().find(|m| m.name == "User").unwrap();
        let mw = parse_where_json(
            user_model, &schema,
            &json!({ "$where": { "projects": { "user_role": "admin" } } })
        ).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        assert_eq!(conds.len(), 1);
        let FieldConditionKind::Injected { st, inner, .. } = &conds[0].kind else { panic!() };
        
        assert_eq!(st.name, "Project.users");
        
        let WhereNode::Fields(inner_conds) = &inner.node else { panic!() };
        assert_eq!(inner_conds[0].field.name, "role");
    }

    #[test]
    fn test_injected_unknown_alias_error() {
        let schema = inject_schema();
        let user_model = schema.models.iter().find(|m| m.name == "User").unwrap();
        let err = parse_where_json(
            user_model, &schema,
            &json!({ "$where": { "projects": { "no_such_alias": "x" } } })
        ).unwrap_err();
        assert!(matches!(err, ParseWhereError::UnknownFieldAlias(_)));
    }

    #[test]
    fn test_injected_non_object_error() {
        let schema = inject_schema();
        let user_model = schema.models.iter().find(|m| m.name == "User").unwrap();
        let err = parse_where_json(
            user_model, &schema,
            &json!({ "$where": { "projects": 42 } })
        ).unwrap_err();
        assert!(matches!(err, ParseWhereError::TypeMismatch { .. }));
    }

    #[test]
    fn test_and_two_branches() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema,
            &json!({ "$where": { "$and": [{ "name": "Alice" }, { "age": 30 }] } })
        ).unwrap().unwrap();
        let WhereNode::And(branches) = &mw.node else { panic!() };
        assert_eq!(branches.len(), 2);
    }

    #[test]
    fn test_or_two_branches() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema,
            &json!({ "$where": { "$or": [{ "name": "Alice" }, { "name": "Bob" }] } })
        ).unwrap().unwrap();
        let WhereNode::Or(branches) = &mw.node else { panic!() };
        assert_eq!(branches.len(), 2);
    }

    #[test]
    fn test_and_nested_or() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema,
            &json!({
                "$where": {
                    "$and": [
                        { "age": { "$gt": 18 } },
                        { "$or": [{ "name": "Alice" }, { "role": "admin" }] }
                    ]
                }
            })
        ).unwrap().unwrap();
        let WhereNode::And(branches) = &mw.node else { panic!() };
        assert_eq!(branches.len(), 2);
        let WhereNode::Or(or_branches) = &branches[1].node else { panic!() };
        assert_eq!(or_branches.len(), 2);
    }

    #[test]
    fn test_multiple_fields_implicit_and() {
        let schema = scalar_schema();
        let model = &schema.models[1];
        let mw = parse_where_json(
            model, &schema,
            &json!({ "$where": { "name": "Alice", "age": 30, "active": true } })
        ).unwrap().unwrap();
        let WhereNode::Fields(conds) = &mw.node else { panic!() };
        assert_eq!(conds.len(), 3);
    }


    #[test]
    fn test_check_condition_null_semantics() {
        assert!(check_condition(&WhereValue::Null, Operator::Eq, &WhereValue::Null));
        assert!(!check_condition(&WhereValue::Null, Operator::Ne, &WhereValue::Null));
        assert!(check_condition(&WhereValue::Null, Operator::Ne, &WhereValue::String("x".into())));
        assert!(!check_condition(&WhereValue::Null, Operator::Eq, &WhereValue::String("x".into())));
        assert!(check_condition(&WhereValue::String("x".into()), Operator::Ne, &WhereValue::Null));
        assert!(!check_condition(&WhereValue::String("x".into()), Operator::Eq, &WhereValue::Null));
    }

    #[test]
    fn test_check_condition_int_comparisons() {
        let a = WhereValue::Int64(5);
        let b = WhereValue::Int64(10);
        assert!( check_condition(&a, Operator::Lt, &b));
        assert!( check_condition(&a, Operator::Le, &b));
        assert!(!check_condition(&a, Operator::Gt, &b));
        assert!(!check_condition(&a, Operator::Ge, &b));
        assert!(!check_condition(&a, Operator::Eq, &b));
        assert!( check_condition(&a, Operator::Ne, &b));
        let c = WhereValue::Int64(5);
        assert!( check_condition(&a, Operator::Eq, &c));
        assert!( check_condition(&a, Operator::Ge, &c));
        assert!( check_condition(&a, Operator::Le, &c));
        assert!(!check_condition(&a, Operator::Ne, &c));
        assert!(!check_condition(&a, Operator::Gt, &c));
        assert!(!check_condition(&a, Operator::Lt, &c));
    }

    #[test]
    fn test_check_condition_string_comparisons() {
        let a = WhereValue::String("apple".into());
        let b = WhereValue::String("banana".into());
        assert!(check_condition(&a, Operator::Lt, &b));
        assert!(check_condition(&a, Operator::Ne, &b));
        assert!(!check_condition(&a, Operator::Eq, &b));
    }

    #[test]
    fn test_check_condition_type_mismatch_returns_false() {
        
        assert!(!check_condition(&WhereValue::Int64(1), Operator::Eq, &WhereValue::String("1".into())));
        assert!(!check_condition(&WhereValue::Bool(true), Operator::Eq, &WhereValue::Int64(1)));
    }

    
    
    

    fn field_of_type<'a>(schema: &'a crate::schema::Schema, model_idx: usize, field_name: &str) -> &'a Field {
        schema.models[model_idx].fields.iter().find(|f| f.name == field_name).unwrap()
    }

    #[test]
    fn test_decode_bytes_string() {
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "name");
        let result = decode_bytes_to_value(f, b"hello").unwrap();
        assert_eq!(result, WhereValue::String("hello".into()));
    }

    #[test]
    fn test_decode_bytes_int64() {
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "age");
        let bytes = (-42i64).to_be_bytes();
        assert_eq!(decode_bytes_to_value(f, &bytes).unwrap(), WhereValue::Int64(-42));
    }

    #[test]
    fn test_decode_bytes_bool_true() {
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "active");
        assert_eq!(decode_bytes_to_value(f, &[1u8]).unwrap(), WhereValue::Bool(true));
        assert_eq!(decode_bytes_to_value(f, &[0u8]).unwrap(), WhereValue::Bool(false));
    }

    #[test]
    fn test_decode_bytes_model_ref() {
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "post");
        let bytes = 123u64.to_be_bytes();
        assert_eq!(decode_bytes_to_value(f, &bytes).unwrap(), WhereValue::UInt64(123));
    }

    #[test]
    fn test_decode_bytes_enum() {
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "role");
        let bytes = 1u16.to_be_bytes(); 
        assert_eq!(decode_bytes_to_value(f, &bytes).unwrap(), WhereValue::UInt16(1));
    }

    #[test]
    fn test_decode_bytes_too_short_error() {
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "age"); 
        assert!(decode_bytes_to_value(f, &[1, 2, 3]).is_err());
    }

    
    
    

    #[test]
    fn test_encode_string_gets_trailing_null() {
        
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "name");
        let encoded = encode_where_value(f, &WhereValue::String("hi".into())).unwrap();
        assert_eq!(encoded, b"hi\0");
    }

    #[test]
    fn test_encode_int64_no_trailing_null() {
        
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "age");
        let encoded = encode_where_value(f, &WhereValue::Int64(1)).unwrap();
        assert_eq!(encoded.len(), 8);
        assert_eq!(i64::from_be_bytes(encoded.try_into().unwrap()), 1i64);
    }

    #[test]
    fn test_encode_bool() {
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "active");
        assert_eq!(encode_where_value(f, &WhereValue::Bool(true)).unwrap(), vec![1u8]);
        assert_eq!(encode_where_value(f, &WhereValue::Bool(false)).unwrap(), vec![0u8]);
    }

    #[test]
    fn test_encode_model_ref_no_trailing_null() {
        
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "post");
        let encoded = encode_where_value(f, &WhereValue::UInt64(7)).unwrap();
        assert_eq!(encoded.len(), 8);
    }

    #[test]
    fn test_encode_type_mismatch_error() {
        let schema = scalar_schema();
        let f = field_of_type(&schema, 1, "age"); 
        let err = encode_where_value(f, &WhereValue::String("oops".into())).unwrap_err();
        assert!(matches!(err, ParseWhereError::TypeMismatch { .. }));
    }
}
