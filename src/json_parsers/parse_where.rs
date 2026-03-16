use serde_json::Value;

use crate::{Field, json_parsers::parsers::{EncodeError, as_datetime, as_f32, as_f64, as_i64, as_u64, encode_enum, encode_list, encode_primitive_value}, query_op::{FieldCompare, Where, WhereNumValue}, schema::{Entity, FieldType, PrimitiveFieldType, Schema}};

/// Парсит JSON объект в where условие
pub fn parse_where<'a>(schema: &'a Schema, entity: &'a Entity, where_obj: &Value) -> Result<Where<'a>,ParseWhereError> {
  let Some(where_obj) = where_obj.as_object() else {
    return Err(ParseWhereError::NotAnObject);
  };

  if let Some(or_condition) = where_obj.get("$or") {
    let or_condition = collect_where_conditions(schema, entity, or_condition)?;
    return match or_condition.len() {
      0 => Err(ParseWhereError::EmptyConditionArray),
      1 => Ok(or_condition.into_iter().next().unwrap()),
      _ => Ok(Where::Or(or_condition))
    }
  }

  if let Some(and_conditions) = where_obj.get("$and") {
    let and_conditions = collect_where_conditions(schema, entity, and_conditions)?;
    return match and_conditions.len() {
      0 => Err(ParseWhereError::EmptyConditionArray),
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
    conditions.push(Where::Field(field, parse_field_compare(schema, field, field_val)?));
  }
  
  return match conditions.len() {
    0 => Err(ParseWhereError::EmptyConditionArray),
    1 => Ok(conditions.into_iter().next().unwrap()),
    _ => Ok(Where::And(conditions))
  };
}

fn collect_where_conditions<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<Vec<Where<'a>>,ParseWhereError> {
  let Some(json_val) = json_val.as_array() else {
    return Err(ParseWhereError::NotAnArray)
  };
  json_val
      .iter()
      .map(|f| parse_where(schema, entity, f))
      .collect()
}

fn parse_field_compare<'a>(schema: &'a Schema, field: &'a Field, value: &Value) -> Result<FieldCompare,ParseWhereError> {
  match &field.ty {
    FieldType::Ref(ref_info) => {
      todo!("make FieldType::Ref query")
    },
    FieldType::RefList(ref_info) => {
      todo!("make FieldType::RefList query")
    },
    _ => { }
  }

  if value.is_null() { return Ok(FieldCompare::EqNull) }

  if let Some(obj) = value.as_object() {
    if obj.len() != 1 {
      return Err(ParseWhereError::OnlyOneKeyExpected(value.to_string()))
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
      "$gt" => Ok(FieldCompare::Gt(parse_field_value_num(field, value)?)),
      "$gte" => Ok(FieldCompare::Gte(parse_field_value_num(field, value)?)),
      "$lt" => Ok(FieldCompare::Lt(parse_field_value_num(field, value)?)),
      "$lte" => Ok(FieldCompare::Lte(parse_field_value_num(field, value)?)),
      "$in" => {
        let (buf, has_null) = parse_field_value_in(field, value)?;
        Ok(FieldCompare::In(buf, has_null))
      },
      "$notIn" => {
        let (buf, has_null) = parse_field_value_in(field, value)?;
        Ok(FieldCompare::NotIn(buf, has_null))
      },
      _ => return Err(ParseWhereError::UnsupportedOperation(key.clone())),
    }
  } else {
    Ok(FieldCompare::Eq(parse_field_value_binary(field, value)?))
  }
}

// Для Eq сравнений нам достаточно бинарного представления данных
fn parse_field_value_binary<'a>(field: &'a Field, v: &Value) -> Result<Vec<u8>,ParseWhereError> {
  let mut dst = vec![];
  match &field.ty {
    FieldType::Enum(enum_def) => {
      encode_enum(&mut dst, field, enum_def, v)
        .map_err(|err: EncodeError| ParseWhereError::EncodeError(err))?;
    },
    FieldType::Primitive(primitive_field_type) => {
      encode_primitive_value(&mut dst, field, primitive_field_type, v)
        .map_err(|err: EncodeError| ParseWhereError::EncodeError(err))?;
    },
    FieldType::PrimitiveList(primitive_type) => {
      encode_list(&mut dst, v, field, &primitive_type, None)
        .map_err(|err: EncodeError| ParseWhereError::EncodeError(err))?;
    },
    FieldType::PrimitiveFixedList(primitive_type, fixed_size) => {
      encode_list(&mut dst, v, field, &primitive_type, Some(*fixed_size))
        .map_err(|err: EncodeError| ParseWhereError::EncodeError(err))?;
    },
    _ => return Err(ParseWhereError::UnavailableKeyField(field.full_name.clone()))
  }

  Ok(dst)
}

fn parse_field_value_in(field: &Field, value: &Value) -> Result<(Vec<Vec<u8>>,bool),ParseWhereError> {
  let Some(arr) = value.as_array() else { return Err(ParseWhereError::NotAnArray) };
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

// Для сравнений lt, gt, lte, gte нам нужно числовое представление
fn parse_field_value_num<'a>(field: &'a Field, v: &Value) -> Result<WhereNumValue,ParseWhereError> {
  match &field.ty {
    FieldType::Primitive(primitive_field_type) => {
      match primitive_field_type {
        PrimitiveFieldType::DateTime => as_datetime(v, field)
          .map(|val| WhereNumValue::DateTime(val))
          .map_err(|err: EncodeError| ParseWhereError::EncodeError(err)),
        PrimitiveFieldType::Int64 => as_i64(v, field)
          .map(|val| WhereNumValue::Int64(val))
          .map_err(|err: EncodeError| ParseWhereError::EncodeError(err)),
        PrimitiveFieldType::UInt64 => as_u64(v, field)
          .map(|val| WhereNumValue::UInt64(val))
          .map_err(|err: EncodeError| ParseWhereError::EncodeError(err)),
        PrimitiveFieldType::Float => as_f32(v, field)
          .map(|val| WhereNumValue::Float(val))
          .map_err(|err: EncodeError| ParseWhereError::EncodeError(err)),
        PrimitiveFieldType::Double => as_f64(v, field)
          .map(|val| WhereNumValue::Double(val))
          .map_err(|err: EncodeError| ParseWhereError::EncodeError(err)),
        _ => Err(ParseWhereError::NotApplicable(field.full_name.clone()))
      }
    },
    _ => Err(ParseWhereError::NotApplicable(field.full_name.clone()))
  }
}

#[derive(Debug)]
pub enum ParseWhereError {
  NotAnObject,
  NotAnArray,
  EmptyConditionArray,
  NotApplicable(String),
  UnavailableKeyField(String),
  OnlyOneKeyExpected(String),
  UnsupportedOperation(String),
  EncodeError(EncodeError)
}

impl ParseWhereError {
  pub fn type_mismatch(field: &Field, expected: impl Into<String>) -> Self {
        ParseWhereError::EncodeError(EncodeError::TypeMismatch {
            field: field.name.clone(),
            expected: expected.into(),
        })
    }
}

#[cfg(test)]
mod tests {
  use serde_json::json;

use crate::{json_parsers::parse_where::parse_where, parse_schema, query_op::{FieldCompare, Where, WhereNumValue}};

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
        assert_eq!(compare, &FieldCompare::NeNull);
      }
      if field.name == "age" {
        assert_eq!(compare, &FieldCompare::Gt(WhereNumValue::UInt64(20)));
      }
    }
  }
  

}