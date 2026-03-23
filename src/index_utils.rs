use smallvec::SmallVec;

use crate::{Field, query_op::{FieldCompare, PrefixKey, Where, WhereNumValue}, schema::{FieldIndex, FieldIndexNum}};

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
fn encode_num_wh(value: &WhereNumValue) -> Vec<u8> {
  match value {
    WhereNumValue::DateTime(val) | WhereNumValue::Int64(val) => encode_i64(&val.to_be_bytes()),
    WhereNumValue::UInt64(val) => val.to_be_bytes().to_vec(),
    WhereNumValue::Float(val) => encode_f32(&val.to_be_bytes()),
    WhereNumValue::Double(val) => encode_f64(&val.to_be_bytes())
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

// Генеририрует индекс для Where
pub fn generate_prefix_from_where<'a>(where_op: &Where) -> Option<PrefixKey<'a>> {
  match where_op {
    Where::True => None,
    Where::And(items) => items.iter().find_map(|f| generate_prefix_from_where(f)),
    Where::Or(_) => None,
    Where::Not(_) => None,
    Where::Field(field, field_compare) => {
      for index in field.indexes.iter() {
        match index {
            FieldIndex::Value { tree_name,  .. } => {
              return match field_compare {
                  FieldCompare::Eq(val) => generate_prefix(encode_index_data(field, val), tree_name),
                  _ => None
              }
            }
            FieldIndex::Number { tree_name, ty, .. } => {
              return match field_compare {
                FieldCompare::Eq(val) => generate_prefix( encode_index_number(ty, val), tree_name),
                FieldCompare::Gte(val) => {
                  Some(PrefixKey::IndexRange { start: Some(encode_num_wh(val)), end: None, tree_name: tree_name.clone(), fixed_size: field.get_size() })
                }
                FieldCompare::Gt(val) => {
                  Some(PrefixKey::IndexRange { start: increase_bit(&encode_num_wh(val)), end: None, tree_name: tree_name.clone(), fixed_size: field.get_size() })
                }
                FieldCompare::Lte(val) => {
                  Some(PrefixKey::IndexRange { start: None, end: Some(encode_num_wh(val)), tree_name: tree_name.clone(), fixed_size: field.get_size() })
                }
                FieldCompare::Lt(val) => {
                  Some(PrefixKey::IndexRange { start: None, end: decrease_bit(&encode_num_wh(val)), tree_name: tree_name.clone(), fixed_size: field.get_size() })
                }
                _ => None
              }
            },
            FieldIndex::Custom { .. } => {
              todo!("Custom indexes not supported yet")
            },
        }
      }

      return None
    },
  }
}