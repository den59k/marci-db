use serde_json::{Map, Value};
use bitvec::prelude::*;

use crate::{Field, index_utils::generate_prefix_from_where, json_parsers::{EncodeError, parse_aggregate_op::parse_aggregate, parse_where::parse_where, parsers::parse_id}, query_op::{IncludeQuery, PrefixKey, QueryInclude, QueryOp, QueryType, Sort, Where}, schema::{Entity, FieldExistsCondition, FieldIndex, FieldLocation, FieldType, RefBinding, Schema}};

const AGGREGATE_KEYS: [&str; 5] = ["$count", "$sum", "$avg", "$min", "$max"];

pub fn parse_query<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<QueryOp<'a>, ParseError> {
  let Some(obj) = json_val.as_object() else {
    return Err(ParseError::NotAnObject)
  };
  let mut op = parse_query_internal(schema, entity, obj)?;
  resolve_sort_plan(&mut op);
  return Ok(op);
}

pub fn parse_query_internal<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Map<String,Value>) -> Result<QueryOp<'a>, ParseError> {
  
  let mut mask = bitvec![0; entity.fields.len()];
  let mut includes = vec![];
  let mut prefix_key = None;
  
  let mut filter = None;
  if let Some(where_value) = json_val.get("$where") {
    let where_op = parse_where(schema, entity, where_value)
      .map_err(|err| ParseError::WhereError(err))?;

    if !matches!(where_op, Where::True) {
      prefix_key = generate_prefix_from_where(entity, &where_op);
      filter = Some(where_op);
    }
  }

  let sort = match json_val.get("$order") {
    Some(order_value) => Some(parse_order(entity, order_value)?),
    None => None
  };
  let limit = parse_service_number(json_val, "$limit")?;
  let skip = parse_service_number(json_val, "$skip")?;

  // $cursor — это id элемента: результаты идут строго после него (exclusive)
  let cursor = match json_val.get("$cursor") {
    Some(cursor_value) => Some(parse_id(schema, entity, cursor_value).map_err(|err| ParseError::CursorError(err))?),
    None => None
  };

  for (field_index, field) in entity.fields.iter().enumerate() {
    let Some(val) = json_val.get(&field.name) else {
      continue;
    };
    if matches!(val, Value::Bool(false)) {
      continue;
    }
    match &field.ty {
      FieldType::Ref (ref_info) => {
        if has_aggregate_keys(val) {
          return Err(ParseError::TypeMismatch { field: field.full_name.clone(), expected: "list relation for aggregate".to_string() });
        }
        let mut op = parse_query_ref(val, &schema.models[ref_info.model_index], field, schema)?;
        op.prefix_key = Some(get_prefix_key(&ref_info.binding, field));
        resolve_sort_plan(&mut op);
        includes.push(QueryInclude { query_type: QueryType::One, field, query: IncludeQuery::Query(op) });
      }
      FieldType::RefList (ref_info) => {
        if has_aggregate_keys(val) {
          let op = parse_aggregate_include(val, &schema.models[ref_info.model_index], field, schema, &ref_info.binding)?;
          includes.push(QueryInclude { query_type: QueryType::Many, field, query: IncludeQuery::Aggregate(op) });
          continue;
        }
        let mut op = parse_query_ref(val, &schema.models[ref_info.model_index], field, schema)?;
        op.prefix_key = Some(get_prefix_key(&ref_info.binding, field));
        resolve_sort_plan(&mut op);
        includes.push(QueryInclude { query_type: QueryType::Many, field, query: IncludeQuery::Query(op) });
      }
      _ => {
        mask.set(field_index, true);
      }
    }
  }

  Ok(QueryOp { mask, entity, sort, filter, prefix_key, includes, limit, skip, cursor, reverse: false, post_sort: false })
}

fn has_aggregate_keys(val: &Value) -> bool {
  match val.as_object() {
    Some(obj) => AGGREGATE_KEYS.iter().any(|key| obj.contains_key(*key)),
    None => false
  }
}

/// Агрегация по связанным записям: select-поля и порядок/лимиты внутри неё запрещены
fn parse_aggregate_include<'a>(val: &Value, ref_entity: &'a Entity, field: &'a Field, schema: &'a Schema, binding: &'a RefBinding) -> Result<crate::aggregate_op::AggregateOp<'a>, ParseError> {
  let Some(obj) = val.as_object() else {
    return Err(ParseError::NotAnObject);
  };

  for key in obj.keys() {
    if key != "$where" && !AGGREGATE_KEYS.contains(&key.as_str()) {
      return Err(ParseError::WrongServiceValue(
        format!("{}.{}", field.name, key),
        "only $where and aggregate keys are allowed in relation aggregate".to_string()
      ));
    }
  }

  let mut op = parse_aggregate(schema, ref_entity, val)?;
  // Скан идёт по привязке родителя; собственный префикс из $where ей замещается,
  // фильтр остаётся residual-условием
  op.prefix_key = Some(get_prefix_key(binding, field));
  op.fully_covered = false;
  Ok(op)
}

fn parse_service_number(json_val: &Map<String,Value>, key: &str) -> Result<Option<usize>, ParseError> {
  let Some(value) = json_val.get(key) else {
    return Ok(None);
  };
  let Some(number) = value.as_u64() else {
    return Err(ParseError::WrongServiceValue(key.to_string(), "positive number".to_string()));
  };
  Ok(Some(number as usize))
}

/// Парсит $order: { field: "asc" | "desc" }. Поддерживается одно поле
fn parse_order<'a>(entity: &'a Entity, order_value: &Value) -> Result<Sort<'a>, ParseError> {
  let Some(obj) = order_value.as_object() else {
    return Err(ParseError::WrongServiceValue("$order".to_string(), "object".to_string()));
  };
  if obj.len() != 1 {
    return Err(ParseError::WrongServiceValue("$order".to_string(), "exactly one field".to_string()));
  }
  let (field_name, direction) = obj.iter().next().unwrap();

  let Some(field) = entity.fields.iter().find(|f| &f.name == field_name) else {
    return Err(ParseError::UnknownField(field_name.clone()));
  };

  // Сортировать можно по ключевым полям и по примитивам/enum из body
  let sortable = match field.location {
    FieldLocation::Key { .. } => true,
    FieldLocation::Body { .. } => matches!(field.ty, FieldType::Primitive(_) | FieldType::Enum(_)),
    FieldLocation::Virtual => false
  };
  if !sortable {
    return Err(ParseError::NotSortable(field.full_name.clone()));
  }

  match direction.as_str() {
    Some("asc") => Ok(Sort::Asc(field)),
    Some("desc") => Ok(Sort::Desc(field)),
    _ => Err(ParseError::WrongServiceValue("$order".to_string(), "\"asc\" | \"desc\"".to_string()))
  }
}

/// Планировщик: выбирает дерево скана и направление под $order.
/// Корректность не зависит от выбора — фильтр перепроверяет все условия для каждой строки,
/// планировщик влияет только на скорость
pub fn resolve_sort_plan(query: &mut QueryOp) {
  query.reverse = false;
  query.post_sort = false;
  let Some(sort) = &query.sort else {
    // $cursor без $order означает порядок по id: скан индекса $where идёт
    // в порядке значения, поэтому от него приходится отказаться
    if query.cursor.is_some() && matches!(query.prefix_key, Some(PrefixKey::IndexRange { .. })) {
      query.prefix_key = None;
    }
    return;
  };
  let (field, desc) = (sort.field(), sort.is_desc());

  // Первичный ключ: деревья моделей и Parent-привязки уже идут в порядке id
  if matches!(field.location, FieldLocation::Key { index: 0 }) {
    match &query.prefix_key {
      // Скан индекса $where идёт в порядке значения, а не id
      Some(PrefixKey::IndexRange { .. }) => { query.post_sort = true; },
      _ => { query.reverse = desc; }
    }
    return;
  }

  // Поле с индексом: можно сканировать дерево индекса.
  // Только non-nullable и не из enum-варианта: sparse-индексы не содержат null-строк,
  // и скан по ним потерял бы записи
  if !field.nullable && matches!(field.condition, FieldExistsCondition::None) {
    let sort_index = field.indexes.iter().find(|i| matches!(i, FieldIndex::Value { .. } | FieldIndex::Number { .. }));
    if let Some(sort_index) = sort_index {
      let tree_name = match sort_index {
        FieldIndex::Value { tree_name, .. } | FieldIndex::Number { tree_name, .. } => tree_name,
        _ => unreachable!()
      };

      match &query.prefix_key {
        // $where уже сканирует индекс этого же поля — диапазон отсортирован по значению
        Some(PrefixKey::IndexRange { tree_name: where_tree, .. }) if where_tree == tree_name => {
          query.reverse = desc;
          return;
        }
        // Другого пути доступа нет — сканируем индекс сортировки целиком
        None => {
          query.prefix_key = Some(PrefixKey::IndexRange {
            start: None, end: None, tree_name: tree_name.clone(), fixed_size: field.get_size()
          });
          query.reverse = desc;
          return;
        }
        // Есть $limit — ранний выход по индексу сортировки важнее селективности $where
        Some(PrefixKey::IndexRange { .. }) if query.limit.is_some() => {
          query.prefix_key = Some(PrefixKey::IndexRange {
            start: None, end: None, tree_name: tree_name.clone(), fixed_size: field.get_size()
          });
          query.reverse = desc;
          return;
        }
        _ => {}
      }
    }
  }

  query.post_sort = true;
}

fn parse_query_ref<'a>(val: &Value, ref_entity: &'a Entity, field: &'a Field, schema: &'a Schema) -> Result<QueryOp<'a>, ParseError> {
  if matches!(val, Value::Bool(true)) {
    return Ok(QueryOp::all(ref_entity));
  }

  let Some(obj) = val.as_object() else {
    return Err(ParseError::type_mismatch(field, "object"))
  };

  return parse_query_internal(schema, &ref_entity, obj);
}

pub fn get_prefix_key<'a>(binding: &'a RefBinding, field: &'a Field) -> PrefixKey<'a> {
  match &binding {
    RefBinding::CurrentId => {
      return PrefixKey::ParentId;
    }
    RefBinding::FieldValue => {
      return PrefixKey::ParentField(field);
    },
    RefBinding::IndexTree(tree_name) => {
      return PrefixKey::ParentIndexTree(tree_name.clone())
    },
  }
}

#[derive(Debug)]
pub enum ParseError {
  NotAnObject,
  MissingIdField(String),
  WhereError(EncodeError),
  TypeMismatch { field: String, expected: String },
  UnknownField(String),
  NotSortable(String),
  WrongServiceValue(String, String),
  CursorError(EncodeError),
}

impl ParseError {
    pub fn type_mismatch(field: &Field, expected: impl Into<String>) -> Self {
        ParseError::TypeMismatch {
            field: field.name.clone(),
            expected: expected.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{parse_query, parse_schema, query_op::{PrefixKey}};

  
  #[test]
  fn test_encode_query_op() {
    let schema = parse_schema("
      model User {
          name        String
          info        UserInfo?
      }
      struct UserInfo {
          bio         String
      }
      model Project {
          name        String
          users       UserRole[]
      }
      struct UserRole {
        user        User          @id
        role        String
      }
    ");

    {
      let user_model = &schema.models[0];

      let input = json!({
          "name": true,
          "info": { "bio": true }
      });

      let encoded = parse_query(&schema, user_model, &input).unwrap();
      assert_eq!(encoded.includes.len(), 1);

      let crate::query_op::IncludeQuery::Query(include_query) = &encoded.includes[0].query else {
        panic!("Expected IncludeQuery::Query");
      };
      assert!(matches!(include_query.prefix_key, Some(PrefixKey::ParentId)));
    }

    {
      let project_model = &schema.models[1];
      let input = json!({
          "name": true,
          "users": { "role": true }
      });

      let encoded = parse_query(&schema, project_model, &input).unwrap();
      assert_eq!(encoded.includes.len(), 1);
    }

    {
      let user_model = &schema.models[0];
      let input = json!({
          "name": true,
          "$where": { "id": 1 }
      });

      let encoded = parse_query(&schema, user_model, &input).unwrap();
      match encoded.prefix_key {
        Some(PrefixKey::Id(value)) => { assert_eq!(value, vec![ 0, 0, 0, 0, 0, 0, 0, 1 ]) },
        _ => panic!("Wrong prefix key value: {:?}", encoded.prefix_key)
      }
    }
  }

  #[test]
  fn test_sort_plan() {
    let schema = parse_schema("
      model User {
          name        String
          age         UInt      @index
          rating      Int?      @index
      }
    ");
    let user_model = &schema.models[0];

    // $order по индексированному полю без $where → скан индекса сортировки
    {
      let q = parse_query(&schema, user_model, &json!({ "name": true, "$order": { "age": "desc" } })).unwrap();
      assert!(q.reverse);
      assert!(!q.post_sort);
      match &q.prefix_key {
        Some(PrefixKey::IndexRange { tree_name, start: None, end: None, .. }) => {
          assert_eq!(tree_name.as_str(), "index_User.age");
        },
        _ => panic!("Expected full IndexRange, got {:?}", q.prefix_key)
      }
    }

    // Диапазон $where по тому же полю — переиспользуется, post_sort не нужен
    {
      let q = parse_query(&schema, user_model, &json!({
        "name": true, "$where": { "age": { "$gte": 10 } }, "$order": { "age": "desc" }
      })).unwrap();
      assert!(q.reverse);
      assert!(!q.post_sort);
      match &q.prefix_key {
        Some(PrefixKey::IndexRange { start: Some(_), .. }) => {},
        _ => panic!("Expected range from $where, got {:?}", q.prefix_key)
      }
    }

    // Nullable поле: sparse-индекс не используется — сортировка в памяти
    {
      let q = parse_query(&schema, user_model, &json!({ "name": true, "$order": { "rating": "asc" } })).unwrap();
      assert!(q.post_sort);
      assert!(q.prefix_key.is_none());
    }

    // Сортировка по id — нативный порядок дерева
    {
      let q = parse_query(&schema, user_model, &json!({ "name": true, "$order": { "id": "desc" } })).unwrap();
      assert!(q.reverse);
      assert!(!q.post_sort);
    }
  }

}