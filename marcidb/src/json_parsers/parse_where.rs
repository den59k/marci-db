use serde_json::Value;

use crate::{Field, json_parsers::{parse_query_op::get_prefix_key, parsers::{EncodeError, encode_enum, encode_list, encode_primitive_value, parse_field_value_num}}, query_op::{FieldCompare, FieldCompareRef, Where}, schema::{Entity, FieldType, Schema}};

/// Парсит JSON объект в where условие
pub fn parse_where<'a>(schema: &'a Schema, entity: &'a Entity, where_obj: &Value) -> Result<Where<'a>,EncodeError> {
  let Some(where_obj) = where_obj.as_object() else {
    return Err(EncodeError::NotAnObject);
  };

  if let Some(or_condition) = where_obj.get("$or") {
    let or_condition = collect_where_conditions(schema, entity, or_condition, false)?;
    if or_condition.iter().any(|f| !matches!(f, Where::True)) {
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
      _ => return Err(EncodeError::UnsupportedOperation(key.clone())),
    }
  } else {
    Ok(FieldCompare::Eq(parse_field_value_binary(field, value)?))
  }
}

// Для Eq сравнений нам достаточно бинарного представления данных
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