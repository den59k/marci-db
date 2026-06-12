use smallvec::SmallVec;

use crate::{Entity, Field, FieldLocation, FieldType, num_utils::NumberValue, query_op::{FieldCompare, PrefixKey, Where}, schema::{FieldIndex, FieldIndexNum, Schema}, utils::get_data};

/// Увеличивает буффер на один бит
pub fn increase_bit(start: &[u8]) -> Option<Vec<u8>> {
  let mut end = SmallVec::<u8, 256>::from_slice_copy(start);
  for (i, b) in end.iter_mut().enumerate().rev() {
      if *b < u8::MAX {
          *b += 1;
          end.truncate(i + 1);
          return Some(end.to_vec());
      }
  }
  return None;
}

/// Уменьшает буффер на один бит
pub fn decrease_bit(start: &[u8]) -> Option<Vec<u8>> {
  let mut end = SmallVec::<u8, 256>::from_slice_copy(start);
  for (i, b) in end.iter_mut().enumerate().rev() {
      if *b > 0 {
          *b -= 1;
          end.truncate(i + 1);
          return Some(end.to_vec());
      }
  }
  return None;
}

#[inline]
/// Для точного сравнения по индексу (Eq) мы сравниваем по префиксу
fn generate_prefix<'a>(val: Vec<u8>, tree_name: &String) -> Option<PrefixKey<'a>> {
  let end = increase_bit(&val);
  let fixed_size = Some(val.len());

  Some(PrefixKey::IndexRange { start: Some(val), end, tree_name: tree_name.clone(), fixed_size })
}

#[inline]
fn generate_prefix_starts_with<'a>(val: Vec<u8>, tree_name: &String) -> Option<PrefixKey<'a>> {
  let end = increase_bit(&val);
  Some(PrefixKey::IndexRange { start: Some(val), end, tree_name: tree_name.clone(), fixed_size: None })
}

pub fn encode_full_index(field: &Field, index: &FieldIndex, id: &[u8], value: &[u8]) -> Vec<u8> {
  let value = encode_index(field, index, value);
  [ value.as_slice(), &id ].concat()
}

pub fn encode_index(field: &Field, index: &FieldIndex, value: &[u8]) -> Vec<u8> {
  match index {
    FieldIndex::Value { .. } => {
      encode_index_data(field, value)
    }
    FieldIndex::Number { ty, .. } => {
      encode_index_number(ty, value)
    }
    _ => panic!("Cannot use custom index in ID")
  }
}

/// Кодирует число для лексиграфического сравнения
pub fn encode_index_number(ty: &FieldIndexNum, val: &[u8]) -> Vec<u8> {
  match ty {
    FieldIndexNum::Int64 => encode_i64(val),
    FieldIndexNum::Float => encode_f32(val),
    FieldIndexNum::Double => encode_f64(val),
    _ => val.to_vec()
  }
}

/// Добавляет нуль-терминатор в конце, если размер неизвестен
pub fn encode_index_data(field: &Field, val: &[u8]) -> Vec<u8> {
    // if field.get_size().is_none() && val[val.len() - 1] != b'\0' {
    if field.get_size().is_none() {
        let mut result = val.to_vec();
        result.push(b'\0');
        return result;
    }
    val.to_vec()
}

/// Кодирует число для лексиграфического сравнения
fn encode_num_wh(value: &NumberValue) -> Vec<u8> {
  match value {
    NumberValue::DateTime(val) | NumberValue::Int64(val) => encode_i64(&val.to_be_bytes()),
    NumberValue::UInt64(val) => val.to_be_bytes().to_vec(),
    NumberValue::Float(val) => encode_f32(&val.to_be_bytes()),
    NumberValue::Double(val) => encode_f64(&val.to_be_bytes())
  }
}

/// Обратное преобразование к encode_index_number: байты ключа индекса → be-bytes значения
pub fn decode_index_number(ty: &FieldIndexNum, data: &[u8]) -> Vec<u8> {
  match ty {
    FieldIndexNum::Int64 => {
      let mut out = data.to_vec();
      out[0] ^= 0x80;
      out
    },
    FieldIndexNum::UInt64 => data.to_vec(),
    FieldIndexNum::Float | FieldIndexNum::Double => {
      let mut out = data.to_vec();
      if out[0] & 0x80 != 0 {
        // Было положительное — возвращаем знаковый бит
        out[0] ^= 0x80;
      } else {
        // Было отрицательное — инвертируем все биты обратно
        for b in &mut out {
          *b = !*b;
        }
      }
      out
    }
  }
}

/// Извлекает значение поля из ключа индекса (encoded_value ++ id)
/// и возвращает его в формате get_data (be-bytes значения)
pub fn decode_index_key_value(field: &Field, index: &FieldIndex, key: &[u8]) -> Vec<u8> {
  match index {
    FieldIndex::Number { ty, .. } => {
      let size = field.get_size().expect("Number index field must have fixed size");
      decode_index_number(ty, &key[..size])
    },
    FieldIndex::Value { .. } => {
      match field.get_size() {
        Some(size) => key[..size].to_vec(),
        // Динамический размер: значение до нуль-терминатора
        None => {
          let pos = key.iter().position(|&b| b == b'\0').unwrap_or(key.len());
          key[..pos].to_vec()
        }
      }
    },
    FieldIndex::Custom { .. } => panic!("Cannot decode value from custom index")
  }
}

/// i64 be_bytes → лексикографически сортируемые байты
/// Флипаем только старший бит (знаковый)
pub fn encode_i64(bytes: &[u8]) -> Vec<u8> {
    debug_assert_eq!(bytes.len(), 8, "i64 must be 8 bytes");
    let mut out = bytes.to_vec();
    out[0] ^= 0x80;
    out
}

/// f32 be_bytes → лексикографически сортируемые байты
pub fn encode_f32(bytes: &[u8]) -> Vec<u8> {
    debug_assert_eq!(bytes.len(), 4, "f32 must be 4 bytes");
    let mut out = bytes.to_vec();
    if out[0] & 0x80 != 0 {
        // Отрицательное число — флипаем все биты
        for b in &mut out {
            *b = !*b;
        }
    } else {
        // Положительное — флипаем только знаковый бит
        out[0] ^= 0x80;
    }
    out
}

/// f64 be_bytes → лексикографически сортируемые байты
pub fn encode_f64(bytes: &[u8]) -> Vec<u8> {
    debug_assert_eq!(bytes.len(), 8, "f64 must be 8 bytes");
    let mut out = bytes.to_vec();
    if out[0] & 0x80 != 0 {
        for b in &mut out {
            *b = !*b;
        }
    } else {
        out[0] ^= 0x80;
    }
    out
}

/// Позиция строки в индексном дереве поля: та же кодировка, в которой ключи лежат в индексе
/// (encoded_value ++ id). Используется для keyset-курсора при скане индекса
pub fn make_index_cursor_key(field: &Field, value: &[u8], id: &[u8]) -> Vec<u8> {
  let mut key = match &field.ty {
    FieldType::Primitive(ty) if ty.get_num_type().is_some() => {
      encode_index_number(&ty.get_num_type().unwrap(), value)
    },
    // Строки получают нуль-терминатор (как в индексах), остальное сравнивается как есть
    _ => encode_index_data(field, value)
  };
  key.extend_from_slice(id);
  key
}

/// Ключ сортировки в памяти. Байтовое сравнение ключей даёт тот же порядок,
/// что и скан соответствующего индексного дерева (та же кодировка значений + id как tie-break).
/// null-значения уходят в конец при asc и в начало при desc
pub fn make_sort_key(entity: &Entity, field: &Field, id: &[u8], data: &[u8], schema: &Schema) -> Vec<u8> {
  let Some(value) = get_data(entity, field, id, data, schema) else {
    let mut key = Vec::with_capacity(id.len() + 1);
    key.push(0xFF);
    key.extend_from_slice(id);
    return key;
  };

  let mut key = Vec::with_capacity(value.len() + id.len() + 2);
  key.push(0x00);
  key.extend(make_index_cursor_key(field, value, id));
  key
}

// Приоритеты условий для выбора индексного пути доступа (меньше — селективнее)
const SCORE_ID: u8 = 0;          // точный первичный ключ
const SCORE_UNIQUE_EQ: u8 = 1;   // eq по unique-индексу
const SCORE_ID_PREFIX: u8 = 2;   // префикс составного первичного ключа
const SCORE_EQ: u8 = 3;          // eq по обычному индексу
const SCORE_STARTS_WITH: u8 = 4;
const SCORE_RANGE: u8 = 5;

// Генеририрует индекс для Where
pub fn generate_prefix_from_where<'a>(entity: &'a Entity, where_op: &Where) -> Option<PrefixKey<'a>> {
  generate_prefix_scored(entity, where_op).map(|(prefix, _)| prefix)
}

fn id_prefix_score(prefix: &PrefixKey) -> u8 {
  match prefix {
    PrefixKey::Id(_) => SCORE_ID,
    _ => SCORE_ID_PREFIX
  }
}

/// Возвращает путь доступа вместе с приоритетом. Из нескольких индексированных
/// условий выбирается самое селективное по статическому приоритету
fn generate_prefix_scored<'a>(entity: &'a Entity, where_op: &Where) -> Option<(PrefixKey<'a>, u8)> {
  match where_op {
    Where::True => None,
    Where::And(items) => {

      // Пробуем собрать prefix для ID
      let key_fields: Vec<(&Field,&Vec<u8>)> = items.iter().filter_map(|f| {
        match f {
          Where::Field(field, FieldCompare::Eq(value)) => Some((*field, value)),
          _ => None
        }
      }).collect();
      let id_prefix = try_to_generate_id_prefix(entity, key_fields);
      if let Some(prefix) = &id_prefix && id_prefix_score(prefix) == SCORE_ID {
        return id_prefix.map(|p| (p, SCORE_ID));
      }

      let best = items.iter()
        .filter_map(|f| generate_prefix_scored(entity, f))
        .min_by_key(|(_, score)| *score);

      match (id_prefix, best) {
        (Some(prefix), Some((best_prefix, best_score))) => {
          let prefix_score = id_prefix_score(&prefix);
          if best_score < prefix_score { Some((best_prefix, best_score)) } else { Some((prefix, prefix_score)) }
        },
        (Some(prefix), None) => { let score = id_prefix_score(&prefix); Some((prefix, score)) },
        (None, best) => best
      }
    },
    Where::Or(_) => None,
    Where::Not(_) => None,
    Where::Field(field, field_compare) => {
      // Проверяем, это может быть единственное поле для ID
      if matches!(field.location, FieldLocation::Key { index } if index == 0) && let FieldCompare::Eq(value) = field_compare {
        if let Some(prefix) = try_to_generate_id_prefix(entity, vec![( field, value )]) {
          let score = id_prefix_score(&prefix);
          return Some((prefix, score))
        }
      }

      for index in field.indexes.iter() {
        match index {
            FieldIndex::Value { tree_name, unique } => {
              return match field_compare {
                  FieldCompare::Eq(val) => generate_prefix(encode_index_data(field, val), tree_name)
                    .map(|p| (p, if *unique { SCORE_UNIQUE_EQ } else { SCORE_EQ })),
                  // Здесь мы не ставим нуль терминатор в конец, поскольку не обязательно, чтобы длина строк совпадала
                  FieldCompare::StringStartsWith(val) => generate_prefix_starts_with(val.clone(), tree_name)
                    .map(|p| (p, SCORE_STARTS_WITH)),
                  _ => None
              }
            }
            FieldIndex::Number { tree_name, ty, unique } => {
              return match field_compare {
                FieldCompare::Eq(val) => generate_prefix( encode_index_number(ty, val), tree_name)
                  .map(|p| (p, if *unique { SCORE_UNIQUE_EQ } else { SCORE_EQ })),
                FieldCompare::Gte(val) => {
                  Some((PrefixKey::IndexRange { start: Some(encode_num_wh(val)), end: None, tree_name: tree_name.clone(), fixed_size: field.get_size() }, SCORE_RANGE))
                }
                FieldCompare::Gt(val) => {
                  Some((PrefixKey::IndexRange { start: increase_bit(&encode_num_wh(val)), end: None, tree_name: tree_name.clone(), fixed_size: field.get_size() }, SCORE_RANGE))
                }
                FieldCompare::Lte(val) => {
                  Some((PrefixKey::IndexRange { start: None, end: Some(encode_num_wh(val)), tree_name: tree_name.clone(), fixed_size: field.get_size() }, SCORE_RANGE))
                }
                FieldCompare::Lt(val) => {
                  Some((PrefixKey::IndexRange { start: None, end: decrease_bit(&encode_num_wh(val)), tree_name: tree_name.clone(), fixed_size: field.get_size() }, SCORE_RANGE))
                }
                _ => None
              }
            },
            FieldIndex::Custom { .. } => {
              todo!("Custom indexes are not supported yet")
            },
        }
      }

      return None
    },
  }
}

pub fn try_to_generate_id_prefix<'a>(entity: &Entity, items: Vec<(&Field,&Vec<u8>)>) -> Option<PrefixKey<'a>> {
  let mut prefix_value = vec![];

  for field in entity.fields.iter() {
    if !matches!(field.location, FieldLocation::Key { .. }) {
      return Some(PrefixKey::Id(prefix_value))
    }
    let Some(value) = items
      .iter()
      .find_map(|i| std::ptr::eq(field, i.0).then_some(i.1))
      else { break; };
    prefix_value.extend_from_slice(value); 
    if field.get_size().is_none() {
      prefix_value.push(b'\0');
    }
  }

  if prefix_value.is_empty() {
    return None
  }
  return Some(PrefixKey::IdPrefix(prefix_value))
}