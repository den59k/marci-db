use serde_json::Value;

use crate::{Field, json_parsers::{parse_query_op::get_prefix_key, parsers::{EncodeError, encode_enum, encode_list, encode_primitive_value, parse_field_value_num}}, query_op::{EnumListFieldFilter, EnumListFilter, FieldCompare, FieldCompareRef, Where}, schema::{EnumInfo, Entity, FieldType, Schema}};

/// Парсит JSON объект в where условие
pub fn parse_where<'a>(schema: &'a Schema, entity: &'a Entity, where_obj: &Value) -> Result<Where<'a>,EncodeError> {
  let Some(where_obj) = where_obj.as_object() else {
    return Err(EncodeError::NotAnObject);
  };

  if let Some(or_condition) = where_obj.get("$or") {
    let or_condition = collect_where_conditions(schema, entity, or_condition, false)?;
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
    FieldType::EnumList(enum_info) => {
      // Допускаем строку как сокращение для { "$some": "variant" }
      if let Some(s) = value.as_str() {
        let filter = parse_enum_list_item_filter(field, enum_info, &serde_json::json!({ "$variant": s }))?;
        return Ok(FieldCompare::EnumListSome(filter));
      }

      let Some(obj) = value.as_object() else {
        return Err(EncodeError::type_mismatch(field, "object with $some/$every/$none"));
      };
      if obj.len() != 1 {
        return Err(EncodeError::OnlyOneKeyExpected(
          field.full_name.clone(), value.to_string()
        ));
      }
      let (key, filter_val) = obj.iter().next().unwrap();
      let filter = parse_enum_list_item_filter(field, enum_info, filter_val)?;

      return match key.as_str() {
        "$some"  => Ok(FieldCompare::EnumListSome(filter)),
        "$every" => Ok(FieldCompare::EnumListEvery(filter)),
        "$none"  => Ok(FieldCompare::EnumListNone(filter)),
        _ => Err(EncodeError::UnsupportedOperation(key.clone())),
      };
    },

    _ => {}
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

fn parse_enum_list_item_filter<'a>(
  field: &'a Field,
  enum_info: &'a EnumInfo,
  value: &Value,
) -> Result<EnumListFilter<'a>, EncodeError> {
  // Допускаем строку как { "$variant": "name" }
  if let Some(s) = value.as_str() {
    let &variant_idx = enum_info.variants_map.get(s)
        .ok_or_else(|| EncodeError::type_mismatch(field, enum_info.keys_to_string()))?;
    return Ok(EnumListFilter { variant_idx: Some(variant_idx), field_filters: vec![] });
  }

  let Some(obj) = value.as_object() else {
    return Err(EncodeError::type_mismatch(field, "string or object"));
  };

  // Читаем $variant если есть
  let variant_idx = if let Some(v) = obj.get("$variant") {
    let s = v.as_str()
        .ok_or_else(|| EncodeError::type_mismatch(field, "string"))?;
    Some(*enum_info.variants_map.get(s)
        .ok_or_else(|| EncodeError::type_mismatch(field, enum_info.keys_to_string()))?)
  } else {
    None
  };

  // Определяем, по каким вариантам искать поля
  let variants_to_search: Vec<u16> = if let Some(vi) = variant_idx {
    vec![vi]
  } else {
    enum_info.variant_fields.keys().copied().collect()
  };

  let mut field_filters = vec![];

  for &vi in &variants_to_search {
    let Some(vfields) = enum_info.variant_fields.get(&vi) else { continue };
    let num_variant_fields = vfields.len();

    for (fi, vf) in vfields.iter().enumerate() {
      let Some(field_val) = obj.get(&vf.name) else { continue };

      // Рекурсивно парсим сравнение для примитивного поля варианта
      let compare = parse_field_compare_primitive(vf, field_val)?;

      field_filters.push(EnumListFieldFilter {
        variant_idx: vi,
        field_idx: fi,
        num_variant_fields,
        field: vf,
        compare,
      });
    }
  }

  Ok(EnumListFilter { variant_idx, field_filters })
}

/// Упрощённый parse_field_compare для примитивных типов внутри EnumList-вариантов
fn parse_field_compare_primitive<'a>(
  field: &'a Field,
  value: &Value,
) -> Result<FieldCompare<'a>, EncodeError> {
  if value.is_null() {
    return Ok(FieldCompare::EqNull);
  }

  if let Some(obj) = value.as_object() {
    if obj.len() != 1 {
      return Err(EncodeError::OnlyOneKeyExpected(field.full_name.clone(), value.to_string()));
    }
    let (key, val) = obj.iter().next().unwrap();
    return match key.as_str() {
      "$eq"  => {
        if val.is_null() { return Ok(FieldCompare::EqNull); }
        Ok(FieldCompare::Eq(parse_field_value_binary(field, val)?))
      },
      "$ne" | "$not" => {
        if val.is_null() { return Ok(FieldCompare::NeNull); }
        Ok(FieldCompare::Ne(parse_field_value_binary(field, val)?))
      },
      "$gt"  => Ok(FieldCompare::Gt(parse_field_value_num(field, val)?)),
      "$gte" => Ok(FieldCompare::Gte(parse_field_value_num(field, val)?)),
      "$lt"  => Ok(FieldCompare::Lt(parse_field_value_num(field, val)?)),
      "$lte" => Ok(FieldCompare::Lte(parse_field_value_num(field, val)?)),
      "$in"  => {
        let (buf, has_null) = parse_field_value_in(field, val)?;
        Ok(FieldCompare::In(buf, has_null))
      },
      "$notIn" => {
        let (buf, has_null) = parse_field_value_in(field, val)?;
        Ok(FieldCompare::NotIn(buf, has_null))
      },
      _ => Err(EncodeError::UnsupportedOperation(key.clone())),
    };
  }

  Ok(FieldCompare::Eq(parse_field_value_binary(field, value)?))
}

#[cfg(test)]
mod tests {
  use serde_json::json;
  use std::assert_matches::assert_matches;
  use crate::{json_parsers::parse_where::parse_where, num_utils::NumberValue, parse_schema, query_op::{FieldCompare, Where}};

  fn simple_user_schema() -> crate::schema::Schema {
    parse_schema("
        model User {
            name  String
            age   UInt?
            score Int?
        }
    ")
  }

  fn ref_schema() -> crate::schema::Schema {
    parse_schema("
        model Post {
            title  String
            author User?
        }
        model User {
            name  String
            email String?
        }
    ")
  }

  fn ref_list_schema() -> crate::schema::Schema {
    parse_schema("
        model User {
            name  String
            posts Post[]
        }
        model Post {
            title  String
            author User?
        }
    ")
  }

  fn enum_list_schema() -> crate::schema::Schema {
    parse_schema("
        enum Role {
            viewer
            editor
            owner
        }
        model Project {
            name  String
            roles Role[]
        }
    ")
  }

  fn enum_list_with_fields_schema() -> crate::schema::Schema {
    parse_schema("
        enum Event {
            created {
                at UInt
            }
            updated {
                at   UInt
                note String?
            }
        }
        model Doc {
            name   String
            events Event[]
        }
    ")
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // $or
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn or_produces_or_variant_with_two_conditions() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({
        "$or": [{ "name": "Alice" }, { "name": "Bob" }]
    })).unwrap();
    assert_matches!(w, Where::Or(_));
    let Where::Or(items) = w else { unreachable!() };
    assert_eq!(items.len(), 2);
  }

  #[test]
  fn or_with_true_shortcircuits_to_true() {
    // Если хотя бы одно условие — Where::True, весь $or → Where::True
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({
        "$or": [{}, { "name": "Alice" }]
    })).unwrap();
    assert_matches!(w, Where::True);
  }

  #[test]
  fn or_empty_array_returns_true() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "$or": [] })).unwrap();
    assert_matches!(w, Where::True);
  }

  #[test]
  fn or_single_condition_unwraps_to_that_condition() {
    // $or с одним не-True элементом возвращает сам элемент (не Or)
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "$or": [{ "age": { "$gt": 18 } }] })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::Gt(_)));
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // $and
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn and_filters_out_true_conditions() {
    // $and фильтрует Where::True условия; одно оставшееся разворачивается
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({
        "$and": [{}, { "age": { "$gt": 5 } }]
    })).unwrap();
    // {} → Where::True → отфильтровано; осталось одно → разворачивается
    assert_matches!(w, Where::Field(_, FieldCompare::Gt(_)));
  }

  #[test]
  fn and_empty_after_filtering_returns_true() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "$and": [{}, {}] })).unwrap();
    assert_matches!(w, Where::True);
  }

  #[test]
  fn and_two_nontrue_conditions_produces_and() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({
        "$and": [{ "age": { "$gt": 0 } }, { "age": { "$lt": 100 } }]
    })).unwrap();
    assert_matches!(w, Where::And(_));
    let Where::And(items) = w else { unreachable!() };
    assert_eq!(items.len(), 2);
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // $not
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn not_wraps_inner_condition() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "$not": { "age": { "$gt": 5 } } })).unwrap();
    assert_matches!(w, Where::Not(_));
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // Скалярные операторы
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn eq_explicit_with_nonnull_value() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "name": { "$eq": "Alice" } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::Eq(_)));
  }

  #[test]
  fn eq_explicit_null_produces_eq_null() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "age": { "$eq": null } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EqNull));
  }

  #[test]
  fn ne_with_nonnull_value_produces_ne() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "name": { "$ne": "Bob" } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::Ne(_)));
  }

  #[test]
  fn not_operator_on_scalar_also_produces_ne() {
    // $not и $ne эквивалентны для скалярных полей
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "name": { "$not": "Bob" } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::Ne(_)));
  }

  #[test]
  fn lt_produces_lt() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "age": { "$lt": 30 } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::Lt(NumberValue::UInt64(30))));
  }

  #[test]
  fn lte_produces_lte() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "age": { "$lte": 30 } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::Lte(NumberValue::UInt64(30))));
  }

  #[test]
  fn gte_produces_gte() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "age": { "$gte": 18 } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::Gte(NumberValue::UInt64(18))));
  }

  #[test]
  fn in_without_null_has_null_false() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "name": { "$in": ["Alice", "Bob"] } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::In(_, false)));
    let Where::Field(_, FieldCompare::In(items, _)) = w else { unreachable!() };
    assert_eq!(items.len(), 2);
  }

  #[test]
  fn in_with_null_sets_has_null_true() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "age": { "$in": [null, 20] } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::In(_, true)));
    let Where::Field(_, FieldCompare::In(items, _)) = w else { unreachable!() };
    assert_eq!(items.len(), 1); // null не попадает в буфер
  }

  #[test]
  fn not_in_without_null() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "name": { "$notIn": ["Alice"] } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::NotIn(_, false)));
  }

  #[test]
  fn not_in_with_null_sets_has_null_true() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "age": { "$notIn": [null, 20] } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::NotIn(_, true)));
  }

  #[test]
  fn starts_with_produces_string_starts_with() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "name": { "$startsWith": "Al" } })).unwrap();
    let Where::Field(_, FieldCompare::StringStartsWith(bytes)) = w else {
      panic!("ожидался StringStartsWith");
    };
    assert_eq!(bytes, b"Al");
  }

  #[test]
  fn includes_produces_string_includes() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let w = parse_where(&schema, entity, &json!({ "name": { "$includes": "ice" } })).unwrap();
    let Where::Field(_, FieldCompare::StringIncludes(bytes)) = w else {
      panic!("ожидался StringIncludes");
    };
    assert_eq!(bytes, b"ice");
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // Ошибки скалярных операторов
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn error_not_an_object_for_parse_where() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let result = parse_where(&schema, entity, &json!([1, 2, 3]));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::NotAnObject));
  }

  #[test]
  fn error_unsupported_operation_unknown_op() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let result = parse_where(&schema, entity, &json!({ "age": { "$unknown": 5 } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::UnsupportedOperation(_)));
  }

  #[test]
  fn error_only_one_key_expected_multiple_ops() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let result = parse_where(&schema, entity, &json!({ "age": { "$gt": 5, "$lt": 10 } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::OnlyOneKeyExpected(_, _)));
  }

  #[test]
  fn error_in_not_array() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let result = parse_where(&schema, entity, &json!({ "age": { "$in": 5 } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::NotAnArray));
  }

  #[test]
  fn error_starts_with_not_string() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let result = parse_where(&schema, entity, &json!({ "name": { "$startsWith": 123 } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::TypeMismatch { .. }));
  }

  #[test]
  fn error_includes_not_string() {
    let schema = simple_user_schema();
    let entity = &schema.models[0];
    let result = parse_where(&schema, entity, &json!({ "name": { "$includes": 999 } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::TypeMismatch { .. }));
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // Ref-поле (FieldType::Ref)
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn ref_field_null_produces_not_exists() {
    let schema = ref_schema();
    let post = &schema.models[0];
    let w = parse_where(&schema, post, &json!({ "author": null })).unwrap();
    let Where::Field(_, crate::query_op::FieldCompare::Ref(_, _, ref_cmp)) = w else {
      panic!("ожидался Field с Ref");
    };
    assert_matches!(ref_cmp, crate::query_op::FieldCompareRef::NotExists);
  }

  #[test]
  fn ref_field_ne_null_produces_exists() {
    let schema = ref_schema();
    let post = &schema.models[0];
    let w = parse_where(&schema, post, &json!({ "author": { "$ne": null } })).unwrap();
    let Where::Field(_, crate::query_op::FieldCompare::Ref(_, _, ref_cmp)) = w else {
      panic!("ожидался Field с Ref");
    };
    assert_matches!(ref_cmp, crate::query_op::FieldCompareRef::Exists);
  }

  #[test]
  fn ref_field_ne_nonnull_produces_ne() {
    let schema = ref_schema();
    let post = &schema.models[0];
    let w = parse_where(&schema, post, &json!({ "author": { "$ne": { "name": "Bob" } } })).unwrap();
    let Where::Field(_, crate::query_op::FieldCompare::Ref(_, _, ref_cmp)) = w else {
      panic!("ожидался Field с Ref");
    };
    assert_matches!(ref_cmp, crate::query_op::FieldCompareRef::Ne(_));
  }

  #[test]
  fn ref_field_object_produces_eq() {
    let schema = ref_schema();
    let post = &schema.models[0];
    let w = parse_where(&schema, post, &json!({ "author": { "name": "Alice" } })).unwrap();
    let Where::Field(_, crate::query_op::FieldCompare::Ref(_, _, ref_cmp)) = w else {
      panic!("ожидался Field с Ref");
    };
    assert_matches!(ref_cmp, crate::query_op::FieldCompareRef::Eq(_));
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // RefList-поле (FieldType::RefList)
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn ref_list_every_parsed_correctly() {
    let schema = ref_list_schema();
    let user = &schema.models[0];
    let w = parse_where(&schema, user, &json!({ "posts": { "$every": {} } })).unwrap();
    let Where::Field(_, crate::query_op::FieldCompare::Ref(_, _, ref_cmp)) = w else {
      panic!("ожидался Ref");
    };
    assert_matches!(ref_cmp, crate::query_op::FieldCompareRef::Every(_));
  }

  #[test]
  fn ref_list_some_parsed_correctly() {
    let schema = ref_list_schema();
    let user = &schema.models[0];
    let w = parse_where(&schema, user, &json!({ "posts": { "$some": {} } })).unwrap();
    let Where::Field(_, crate::query_op::FieldCompare::Ref(_, _, ref_cmp)) = w else {
      panic!("ожидался Ref");
    };
    assert_matches!(ref_cmp, crate::query_op::FieldCompareRef::Some(_));
  }

  #[test]
  fn ref_list_none_parsed_correctly() {
    let schema = ref_list_schema();
    let user = &schema.models[0];
    let w = parse_where(&schema, user, &json!({ "posts": { "$none": {} } })).unwrap();
    let Where::Field(_, crate::query_op::FieldCompare::Ref(_, _, ref_cmp)) = w else {
      panic!("ожидался Ref");
    };
    assert_matches!(ref_cmp, crate::query_op::FieldCompareRef::None(_));
  }

  #[test]
  fn ref_list_error_unsupported_key() {
    let schema = ref_list_schema();
    let user = &schema.models[0];
    let result = parse_where(&schema, user, &json!({ "posts": { "$contains": {} } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::UnsupportedOperation(_)));
  }

  #[test]
  fn ref_list_error_only_one_key_expected() {
    let schema = ref_list_schema();
    let user = &schema.models[0];
    let result = parse_where(&schema, user, &json!({ "posts": { "$some": {}, "$every": {} } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::OnlyOneKeyExpected(_, _)));
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // EnumList-поле (FieldType::EnumList) — parse_field_compare
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn enum_list_shorthand_string_produces_some() {
    let schema = enum_list_schema();
    let project = &schema.models[0];
    let w = parse_where(&schema, project, &json!({ "roles": "owner" })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn enum_list_some_with_variant_filter() {
    let schema = enum_list_schema();
    let project = &schema.models[0];
    let w = parse_where(&schema, project, &json!({ "roles": { "$some": { "$variant": "editor" } } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn enum_list_every_parsed() {
    let schema = enum_list_schema();
    let project = &schema.models[0];
    let w = parse_where(&schema, project, &json!({ "roles": { "$every": "owner" } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListEvery(_)));
  }

  #[test]
  fn enum_list_none_parsed() {
    let schema = enum_list_schema();
    let project = &schema.models[0];
    let w = parse_where(&schema, project, &json!({ "roles": { "$none": "viewer" } })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListNone(_)));
  }

  #[test]
  fn enum_list_error_unsupported_key() {
    let schema = enum_list_schema();
    let project = &schema.models[0];
    let result = parse_where(&schema, project, &json!({ "roles": { "$contains": "owner" } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::UnsupportedOperation(_)));
  }

  #[test]
  fn enum_list_error_only_one_key_expected() {
    let schema = enum_list_schema();
    let project = &schema.models[0];
    let result = parse_where(&schema, project, &json!({ "roles": { "$some": "viewer", "$every": "owner" } }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::OnlyOneKeyExpected(_, _)));
  }

  #[test]
  fn enum_list_error_type_mismatch_non_object_non_string() {
    let schema = enum_list_schema();
    let project = &schema.models[0];
    let result = parse_where(&schema, project, &json!({ "roles": 123 }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::TypeMismatch { .. }));
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // parse_field_compare_primitive (через EnumList с вариантными полями)
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn primitive_cmp_eq_null_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "$variant": "updated", "note": null } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_ne_null_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "$variant": "updated", "note": { "$ne": null } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_gt_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "$variant": "updated", "at": { "$gt": 100 } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_lt_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$lt": 50 } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_lte_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$lte": 50 } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_gte_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$gte": 100 } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_in_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$in": [100, 200] } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_in_with_null_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "$variant": "updated", "note": { "$in": [null, "fix"] } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_not_in_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$notIn": [100] } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_eq_nonnull_in_enum_list_filter() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$eq": 200 } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  #[test]
  fn primitive_cmp_error_unsupported_op() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let result = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$startsWith": "x" } } }
    }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::UnsupportedOperation(_)));
  }

  #[test]
  fn primitive_cmp_error_only_one_key() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let result = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$gt": 5, "$lt": 100 } } }
    }));
    assert_matches!(result, Err(crate::json_parsers::parsers::EncodeError::OnlyOneKeyExpected(_, _)));
  }

  #[test]
  fn primitive_cmp_implicit_eq_in_enum_list_filter() {
    // Прямое значение (не объект) → Eq
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": 200 } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
  }

  // ═══════════════════════════════════════════════════════════════════════════════
  // parse_enum_list_item_filter: поиск без $variant (все варианты)
  // ═══════════════════════════════════════════════════════════════════════════════

  #[test]
  fn enum_list_filter_without_variant_matches_all_variants() {
    let schema = enum_list_with_fields_schema();
    let doc = &schema.models[0];
    // Без $variant — ищем поле 'at' во всех вариантах
    let w = parse_where(&schema, doc, &json!({
        "events": { "$some": { "at": { "$gt": 50 } } }
    })).unwrap();
    assert_matches!(w, Where::Field(_, FieldCompare::EnumListSome(_)));
    let Where::Field(_, FieldCompare::EnumListSome(filter)) = w else { unreachable!() };
    // variant_idx = None (все варианты)
    assert!(filter.variant_idx.is_none());
    // Должны быть field_filters для обоих вариантов (created.at + updated.at)
    assert!(!filter.field_filters.is_empty());
  }

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
