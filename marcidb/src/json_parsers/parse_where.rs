use serde_json::Value;

use crate::{Field, json_parsers::{parse_query_op::get_prefix_key, parsers::{EncodeError, encode_enum, encode_list, encode_primitive_value, parse_field_value_num}}, query_op::{FieldCompare, FieldCompareRef, JsonFilter, JsonOp, Where}, schema::{Entity, FieldType, PrimitiveFieldType, Schema}};

/// Parses a JSON object into a where condition
pub fn parse_where<'a>(schema: &'a Schema, entity: &'a Entity, where_obj: &Value) -> Result<Where<'a>,EncodeError> {
  let Some(where_obj) = where_obj.as_object() else {
    return Err(EncodeError::NotAnObject);
  };

  if let Some(or_condition) = where_obj.get("$or") {
    let or_condition = collect_where_conditions(schema, entity, or_condition, false)?;
    // True is OR's absorbing element: if any branch matches everything, the whole OR does.
    if or_condition.iter().any(|f| matches!(f, Where::True)) {
      return Ok(Where::True)
    }
    return match or_condition.len() {
      0 => Ok(Where::True),
      1 => Ok(or_condition.into_iter().next().unwrap()),
      _ => Ok(Where::Or(or_condition))
    }
  }

  if let Some(and_conditions) = where_obj.get("$and") {
    let and_conditions = collect_where_conditions(schema, entity, and_conditions, true)?;
    return match and_conditions.len() {
      0 => Ok(Where::True),
      1 => Ok(and_conditions.into_iter().next().unwrap()),
      _ => Ok(Where::And(and_conditions))
    }
  }

  if let Some(not_condition) = where_obj.get("$not") {
    return Ok(Where::Not(Box::new(parse_where(schema, entity, not_condition)?)))
  }

  let mut conditions: Vec<Where<'a>> = Vec::new();
  for field in entity.fields.iter() {
    let Some(field_val) = where_obj.get(&field.name) else {
        continue;
    };
    let field_compare = parse_field_compare(schema, field, field_val)?;
    conditions.push(Where::Field(field, field_compare));
  }
  
  return match conditions.len() {
    0 => Ok(Where::True),
    1 => Ok(conditions.into_iter().next().unwrap()),
    _ => Ok(Where::And(conditions))
  };
}

fn collect_where_conditions<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value, filter: bool) -> Result<Vec<Where<'a>>,EncodeError> {
  let Some(json_val) = json_val.as_array() else {
    return Err(EncodeError::NotAnArray)
  };

  if filter {
    json_val
        .iter()
        .map(|f| parse_where(schema, entity, f))
        .filter(|f| !matches!(f, Ok(Where::True)))
        .collect()
  } else {
    json_val
        .iter()
        .map(|f| parse_where(schema, entity, f))
        .collect()
  }
}

fn parse_field_compare<'a>(schema: &'a Schema, field: &'a Field, value: &Value) -> Result<FieldCompare<'a>,EncodeError> {
  match &field.ty {
    FieldType::Ref(ref_info) => {
      let entity = &schema.models[ref_info.model_index];
      let prefix = get_prefix_key(&ref_info.binding, field);

      if value.is_null() {
        return Ok(FieldCompare::Ref(entity, prefix, FieldCompareRef::NotExists))
      }
      let Some(obj) = value.as_object() else {
        return Err(EncodeError::type_mismatch(field, "object"))
      };

      if obj.len() == 1 {
        let (key, value) = obj.iter().next().unwrap();
        match key.as_str() {
          "$ne" | "$not" => {
            if value.is_null() { return Ok(FieldCompare::Ref(entity, prefix, FieldCompareRef::Exists))  }
            let filter = Box::new(parse_where(schema, entity, value)?);
            return Ok(FieldCompare::Ref(entity, prefix, FieldCompareRef::Ne(filter)))
          }
          _ => {}
        }
      }

      let filter = Box::new(parse_where(schema, entity, value)?);
      return Ok(FieldCompare::Ref(entity, prefix, FieldCompareRef::Eq(filter)))
    },
    FieldType::RefList(ref_info) => {
      let Some(obj) = value.as_object() else {
        return Err(EncodeError::type_mismatch(field, "object"))
      };
      if obj.len() != 1 {
        return Err(EncodeError::OnlyOneKeyExpected(field.full_name.clone(), value.to_string()))
      }
      let (key, value) = obj.iter().next().unwrap();
      let entity = &schema.models[ref_info.model_index];
      let prefix = get_prefix_key(&ref_info.binding, field);
      let filter = Box::new(parse_where(schema, entity, value)?);
      return match key.as_str() {
        "$every" => Ok(FieldCompare::Ref(entity, prefix, FieldCompareRef::Every(filter))),
        "$some" => Ok(FieldCompare::Ref(entity, prefix, FieldCompareRef::Some(filter))),
        "$none" => Ok(FieldCompare::Ref(entity, prefix, FieldCompareRef::None(filter))),
        _ => Err(EncodeError::UnsupportedOperation(key.clone()))
      };
    },
    _ => { }
  }

  // A Json field keyed by JSON paths (non-`$` keys) is path-mode. `$`-operators and bare values fall through
  // to whole-value handling below (a whole-blob `$eq`, which works via the canonical encoding).
  if matches!(field.ty, FieldType::Primitive(PrimitiveFieldType::Json)) {
    if let Some(obj) = value.as_object() {
      let has_path = obj.keys().any(|k| !k.starts_with('$'));
      let has_op = obj.keys().any(|k| k.starts_with('$'));
      if has_path && has_op {
        return Err(EncodeError::UnsupportedOperation(
          "a JSON filter cannot mix path keys and $-operators".to_string(),
        ));
      }
      if has_path {
        return Ok(FieldCompare::Json(parse_json_filters(obj)?));
      }
    }
  }

  if value.is_null() { return Ok(FieldCompare::EqNull) }

  if let Some(obj) = value.as_object() {
    if obj.len() != 1 {
      return Err(EncodeError::OnlyOneKeyExpected(field.full_name.clone(), value.to_string()))
    }
    let (key, value) = obj.iter().next().unwrap();

    match key.as_str() {
      "$eq" => {
        if value.is_null() { return Ok(FieldCompare::EqNull) }
        Ok(FieldCompare::Eq(parse_field_value_binary(field, value)?))
      },
      "$ne" | "$not" => {
        if value.is_null() { return Ok(FieldCompare::NeNull)  }
        Ok(FieldCompare::Ne(parse_field_value_binary(field, value)?))
      },
      "$gt" => Ok(FieldCompare::Gt(
        parse_field_value_num(field, value)?
      )),
      "$gte" => Ok(FieldCompare::Gte(
        parse_field_value_num(field, value)?
      )),
      "$lt" => Ok(FieldCompare::Lt(
        parse_field_value_num(field, value)?
      )),
      "$lte" => Ok(FieldCompare::Lte(
        parse_field_value_num(field, value)?
      )),
      "$in" => {
        let (buf, has_null) = parse_field_value_in(field, value)?;
        Ok(FieldCompare::In(buf, has_null))
      },
      "$notIn" => {
        let (buf, has_null) = parse_field_value_in(field, value)?;
        Ok(FieldCompare::NotIn(buf, has_null))
      },
      "$startsWith" => {
        let Some(value) = value.as_str() else { return Err(EncodeError::type_mismatch(field, "string")) };
        Ok(FieldCompare::StringStartsWith(value.as_bytes().to_vec()))
      },
      "$includes" => {
        let Some(value) = value.as_str() else { return Err(EncodeError::type_mismatch(field, "string")) };
        Ok(FieldCompare::StringIncludes(value.as_bytes().to_vec()))
      },
      // Module-index search: hand the raw payload to the provider (resolved during query execution).
      // The field must carry a `@custom` index; that (and payload validity) is checked at execution.
      "$near" | "$search" => Ok(FieldCompare::Search(value.clone())),
      _ => return Err(EncodeError::UnsupportedOperation(key.clone())),
    }
  } else {
    Ok(FieldCompare::Eq(parse_field_value_binary(field, value)?))
  }
}

// For Eq comparisons the binary representation of the data is enough
fn parse_field_value_binary<'a>(field: &'a Field, v: &Value) -> Result<Vec<u8>,EncodeError> {
  let mut dst = vec![];
  match &field.ty {
    FieldType::Enum(enum_def) => {
      encode_enum(&mut dst, field, enum_def, v)?;
    },
    FieldType::Primitive(primitive_field_type) => {
      encode_primitive_value(&mut dst, field, primitive_field_type, v)?;
    },
    FieldType::PrimitiveList(primitive_type, fixed_size) => {
      encode_list(&mut dst, v, field, &primitive_type, *fixed_size)?;
    },
    _ => return Err(EncodeError::UnavailableKeyField(field.full_name.clone()))
  }

  Ok(dst)
}

fn parse_field_value_in(field: &Field, value: &Value) -> Result<(Vec<Vec<u8>>,bool),EncodeError> {
  let Some(arr) = value.as_array() else { return Err(EncodeError::NotAnArray) };
  let mut buf = Vec::with_capacity(arr.len());
  let mut has_null = false;
  for v in arr {
    if v.is_null() {
      has_null = true;
      continue;
    }
    buf.push(parse_field_value_binary(field, v)?);
  }
  Ok((buf,has_null))
}

/// Parses the path-mode object of a JSON filter: each entry is `"<dot.path>": <condition>`, all ANDed.
fn parse_json_filters(obj: &serde_json::Map<String, Value>) -> Result<Vec<JsonFilter>, EncodeError> {
  obj.iter()
    .map(|(path, cond)| Ok(JsonFilter {
      path: path.split('.').map(str::to_string).collect(),
      op: parse_json_op(cond)?,
    }))
    .collect()
}

/// A condition on a single path. A single-`$`-key object is an operator; any other value (scalar, array, or
/// a plain object) is a literal `$eq` against the leaf (so a plain object means whole-subtree equality).
fn parse_json_op(cond: &Value) -> Result<JsonOp, EncodeError> {
  if let Some(obj) = cond.as_object() {
    if obj.keys().any(|k| k.starts_with('$')) {
      if obj.len() != 1 {
        return Err(EncodeError::UnsupportedOperation(
          "a JSON path condition takes exactly one operator".to_string(),
        ));
      }
      let (key, v) = obj.iter().next().unwrap();
      return parse_json_operator(key, v);
    }
  }
  Ok(JsonOp::Eq(cond.clone()))
}

fn parse_json_operator(key: &str, v: &Value) -> Result<JsonOp, EncodeError> {
  let as_str = |v: &Value| v.as_str().map(str::to_string)
    .ok_or_else(|| EncodeError::UnsupportedOperation(format!("{} expects a string", key)));
  let as_arr = |v: &Value| v.as_array().cloned().ok_or(EncodeError::NotAnArray);
  Ok(match key {
    "$eq" => JsonOp::Eq(v.clone()),
    "$ne" | "$not" => JsonOp::Ne(v.clone()),
    "$gt" => JsonOp::Gt(v.clone()),
    "$gte" => JsonOp::Gte(v.clone()),
    "$lt" => JsonOp::Lt(v.clone()),
    "$lte" => JsonOp::Lte(v.clone()),
    "$in" => JsonOp::In(as_arr(v)?),
    "$notIn" => JsonOp::NotIn(as_arr(v)?),
    "$startsWith" => JsonOp::StringStartsWith(as_str(v)?),
    "$includes" => JsonOp::StringIncludes(as_str(v)?),
    "$contains" => JsonOp::Contains(v.clone()),
    "$exists" => JsonOp::Exists(
      v.as_bool().ok_or_else(|| EncodeError::UnsupportedOperation("$exists expects a boolean".to_string()))?,
    ),
    "$type" => JsonOp::Type(parse_json_type(v)?),
    _ => return Err(EncodeError::UnsupportedOperation(key.to_string())),
  })
}

fn parse_json_type(v: &Value) -> Result<String, EncodeError> {
  let s = v.as_str().ok_or_else(|| EncodeError::UnsupportedOperation("$type expects a string".to_string()))?;
  match s {
    "string" | "number" | "boolean" | "object" | "array" | "null" => Ok(s.to_string()),
    _ => Err(EncodeError::UnsupportedOperation(format!("unknown $type '{}'", s))),
  }
}

#[cfg(test)]
mod tests {
  use serde_json::json;
  use std::assert_matches;

use crate::{json_parsers::parse_where::parse_where, num_utils::NumberValue, parse_schema, query_op::{FieldCompare, Where}};

  #[test]
  fn basic_where_test() {

    let schema = parse_schema("
      model User {
          name        String
          age         UInt
          email       String?
          createdAt   DateTime
          pages       Page[]
      }
      model Page {
          bio         String
      }
    ");

    let user_model = &schema.models[0];
      
    let where_op = parse_where(&schema, user_model, &json!({
      "email": { "$ne": null },
      "age": { "$gt": 20 }
    })).unwrap();

    let Where::And(ops) = where_op else {
      panic!("Wrong where_op type");
    };

    assert_eq!(ops.len(), 2);
    for op in ops.iter() {
      let Where::Field(field, compare) = op else {
        panic!("Wrong where_op type");
      };
      if field.name == "email" {
        assert_matches!(compare, &FieldCompare::NeNull);
      }
      if field.name == "age" {
        assert_matches!(compare, &FieldCompare::Gt(NumberValue::UInt64(20)));
      }
    }
  }
}