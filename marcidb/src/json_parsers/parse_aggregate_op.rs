use serde_json::{Map, Value};

use crate::{Field, aggregate_op::{AggregateOp, AggregateResult, SumValue}, index_utils::generate_prefix_from_where, json_parsers::{json_decoder::decode_field_value, parse_query_op::ParseError, parse_where::parse_where}, query_op::Where, schema::{Entity, FieldExistsCondition, FieldLocation, FieldType, PrimitiveFieldType, Schema}};

pub fn parse_aggregate<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<AggregateOp<'a>, ParseError> {
  let Some(obj) = json_val.as_object() else {
    return Err(ParseError::NotAnObject)
  };

  let mut filter = None;
  let mut prefix_key = None;
  if let Some(where_value) = obj.get("$where") {
    let where_op = parse_where(schema, entity, where_value)
      .map_err(|err| ParseError::WhereError(err))?;

    if !matches!(where_op, Where::True) {
      prefix_key = generate_prefix_from_where(entity, &where_op);
      filter = Some(where_op);
    }
  }

  // Одиночное условие, из которого получен диапазон — фильтр покрыт диапазоном целиком
  let fully_covered = prefix_key.is_some() && matches!(&filter, Some(Where::Field(_, _)));

  let count = match obj.get("$count") {
    None => false,
    Some(Value::Bool(true)) => true,
    Some(_) => return Err(ParseError::WrongServiceValue("$count".to_string(), "true".to_string()))
  };

  Ok(AggregateOp {
    entity,
    filter,
    prefix_key,
    fully_covered,
    count,
    sum: parse_aggregate_field(entity, obj, "$sum", true)?,
    avg: parse_aggregate_field(entity, obj, "$avg", true)?,
    min: parse_aggregate_field(entity, obj, "$min", false)?,
    max: parse_aggregate_field(entity, obj, "$max", false)?,
  })
}

fn parse_aggregate_field<'a>(entity: &'a Entity, obj: &Map<String, Value>, key: &str, numeric: bool) -> Result<Option<&'a Field>, ParseError> {
  let Some(value) = obj.get(key) else {
    return Ok(None);
  };
  let Some(field_name) = value.as_str() else {
    return Err(ParseError::WrongServiceValue(key.to_string(), "field name".to_string()));
  };

  let Some(field) = entity.fields.iter().find(|f| f.name == field_name) else {
    return Err(ParseError::UnknownField(field_name.to_string()));
  };

  // Только обычные примитивные поля: без Virtual и без полей enum-вариантов
  let valid = matches!(field.location, FieldLocation::Key { .. } | FieldLocation::Body { .. })
    && matches!(field.condition, FieldExistsCondition::None)
    && match &field.ty {
      FieldType::Primitive(ty) => !numeric || is_numeric(ty),
      _ => false
    };

  if !valid {
    return Err(ParseError::TypeMismatch {
      field: field.full_name.clone(),
      expected: if numeric { "numeric field".to_string() } else { "primitive field".to_string() }
    });
  }

  Ok(Some(field))
}

fn is_numeric(ty: &PrimitiveFieldType) -> bool {
  matches!(ty, PrimitiveFieldType::Int64 | PrimitiveFieldType::UInt64 | PrimitiveFieldType::Float | PrimitiveFieldType::Double)
}

/// Собирает JSON-объект результата: только запрошенные агрегаты
pub fn aggregate_to_json(op: &AggregateOp, result: &AggregateResult) -> String {
  let mut parts: Vec<String> = vec![];

  if op.count {
    parts.push(format!("\"count\":{}", result.count));
  }
  if op.sum.is_some() {
    parts.push(format!("\"sum\":{}", sum_to_json(&result.sum)));
  }
  if op.avg.is_some() {
    parts.push(format!("\"avg\":{}", f64_to_json(result.avg)));
  }
  if let Some(field) = op.min {
    parts.push(format!("\"min\":{}", value_to_json(field, &result.min)));
  }
  if let Some(field) = op.max {
    parts.push(format!("\"max\":{}", value_to_json(field, &result.max)));
  }

  format!("{{{}}}", parts.join(","))
}

fn sum_to_json(sum: &Option<SumValue>) -> String {
  match sum {
    None => "null".to_string(),
    Some(SumValue::Int(value)) => {
      // Сумма может выйти за пределы i64/u64 — тогда отдаём как f64
      if let Ok(v) = i64::try_from(*value) {
        v.to_string()
      } else if let Ok(v) = u64::try_from(*value) {
        v.to_string()
      } else {
        f64_to_json(Some(*value as f64))
      }
    },
    Some(SumValue::Float(value)) => f64_to_json(Some(*value))
  }
}

fn f64_to_json(value: Option<f64>) -> String {
  match value {
    Some(v) if v.is_finite() => v.to_string(),
    _ => "null".to_string()
  }
}

fn value_to_json(field: &Field, value: &Option<Vec<u8>>) -> String {
  match value {
    Some(data) => decode_field_value(field, data).unwrap_or_else(|_| "null".to_string()),
    None => "null".to_string()
  }
}
