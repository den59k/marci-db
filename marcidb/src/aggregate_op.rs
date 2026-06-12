use crate::{Field, index_utils::decode_index_key_value, query_op::{FieldCompare, ParentData, PrefixKey, TransationContext, Where, get_id_from_index_key, get_prefix, process_where, range_keys_iter}, schema::{Entity, FieldExistsCondition, FieldIndex, PrimitiveFieldType}, utils::get_data};

#[derive(Debug)]
pub struct AggregateOp<'a> {
  pub entity: &'a Entity,
  pub filter: Option<Where<'a>>,
  pub prefix_key: Option<PrefixKey<'a>>,
  /// Фильтр полностью выражен диапазоном prefix_key — count можно считать по ключам, без чтения строк
  pub fully_covered: bool,
  pub count: bool,
  pub sum: Option<&'a Field>,
  pub avg: Option<&'a Field>,
  pub min: Option<&'a Field>,
  pub max: Option<&'a Field>,
}

impl AggregateOp<'_> {
  pub fn has_aggregates(&self) -> bool {
    self.count || self.sum.is_some() || self.avg.is_some() || self.min.is_some() || self.max.is_some()
  }

  /// Нужно ли читать строки, или достаточно посчитать ключи
  fn needs_rows(&self) -> bool {
    self.sum.is_some() || self.avg.is_some() || self.min.is_some() || self.max.is_some()
      || (self.filter.is_some() && !self.fully_covered)
  }
}

/// Накопленная сумма: целые считаются точно (i128), дробные — в f64
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SumValue {
  Int(i128),
  Float(f64)
}

#[derive(Debug, Default)]
pub struct AggregateResult {
  pub count: u64,
  /// None — не было ни одного значения (как NULL у SUM в SQL)
  pub sum: Option<SumValue>,
  pub avg: Option<f64>,
  /// Сырые байты значения поля — декодируются на json-слое
  pub min: Option<Vec<u8>>,
  pub max: Option<Vec<u8>>,
}

pub fn process_aggregate<'a, U, F>(op: &'a AggregateOp, ctx: &mut TransationContext<'a, U, F>, parent: Option<ParentData>) -> AggregateResult {
  let mut result = AggregateResult::default();

  // Быстрые пути: count по ключам, без чтения и декодирования строк
  if !op.needs_rows() {
    result.count = match &op.prefix_key {
      None => ctx.get_tree(&op.entity.name).len(),
      Some(PrefixKey::IndexRange { start, end, tree_name, .. }) => {
        let index_tree = ctx.get_tree(tree_name);
        range_keys_iter(&index_tree, start, end, false).count() as u64
      },
      // Связи родителя: количество детей = количество ключей по префиксу parent_id
      Some(PrefixKey::ParentIndexTree(tree_name)) => {
        let index_tree = ctx.get_tree(tree_name);
        index_tree.prefix_keys(&parent.unwrap().1).unwrap().count() as u64
      },
      Some(prefix_key) => {
        let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
          return result;
        };
        let tree = ctx.get_tree(&op.entity.name);
        tree.prefix_keys(&prefix).unwrap().count() as u64
      },
    };
    return result;
  }

  // count по null/not-null индексированного поля: sparse-индекс не содержит null-строк,
  // поэтому разность размеров деревьев даёт точный ответ без скана
  if op.sum.is_none() && op.avg.is_none() && op.min.is_none() && op.max.is_none() && op.prefix_key.is_none() {
    if let Some(Where::Field(field, compare @ (FieldCompare::EqNull | FieldCompare::NeNull))) = &op.filter
      && matches!(field.condition, FieldExistsCondition::None)
      && let Some(index) = find_usable_index(field) {

      let index_len = ctx.get_tree(index_tree_name(index)).len();
      result.count = match compare {
        FieldCompare::NeNull => index_len,
        _ => ctx.get_tree(&op.entity.name).len() - index_len
      };
      return result;
    }
  }

  // min/max по крайним ключам индекса: O(log n) вместо скана.
  // Применимо без фильтра и без sum/avg, когда все запрошенные поля индексированы и non-nullable
  if op.filter.is_none() && op.prefix_key.is_none() && op.sum.is_none() && op.avg.is_none() {
    if let Some(fast_result) = try_minmax_by_index(op, ctx) {
      return fast_result;
    }
  }

  let mut acc = Accumulators::default();

  match &op.prefix_key {
    Some(PrefixKey::IndexRange { start, end, tree_name, fixed_size }) => {
      let index_tree = ctx.get_tree(tree_name);
      let tree = ctx.get_tree(&op.entity.name);
      let iter = range_keys_iter(&index_tree, start, end, false)
        .map(|key| {
          let id = get_id_from_index_key(&key, *fixed_size);
          let value = tree.get(&id).unwrap().unwrap();
          (id, value)
        });
      aggregate_rows(iter, ctx, op, &mut acc);
    },
    Some(PrefixKey::ParentIndexTree(tree_name)) => {
      let index_tree = ctx.get_tree(tree_name);
      let tree = ctx.get_tree(&op.entity.name);
      let parent_id = parent.unwrap().1;
      let parent_id_len = parent_id.len();
      let iter = index_tree.prefix_keys(&parent_id).unwrap()
        .map(|item| {
          let key = item.unwrap();
          let id = key[parent_id_len..].to_vec();
          let value = tree.get(&id).unwrap().unwrap();
          (id, value)
        });
      aggregate_rows(iter, ctx, op, &mut acc);
    },
    Some(prefix_key) => {
      let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
        return result;
      };
      let tree = ctx.get_tree(&op.entity.name);
      let iter = tree.prefix(&prefix).unwrap().map(|item| item.unwrap());
      aggregate_rows(iter, ctx, op, &mut acc);
    },
    None => {
      let tree = ctx.get_tree(&op.entity.name);
      let iter = tree.iter().unwrap().map(|item| item.unwrap());
      aggregate_rows(iter, ctx, op, &mut acc);
    },
  }

  result.count = acc.count;
  result.sum = acc.sum;
  result.avg = if acc.avg_count > 0 { Some(acc.avg_sum / acc.avg_count as f64) } else { None };
  result.min = acc.min.map(|(_, value)| value);
  result.max = acc.max.map(|(_, value)| value);
  result
}

fn find_usable_index(field: &Field) -> Option<&FieldIndex> {
  field.indexes.iter().find(|i| matches!(i, FieldIndex::Value { .. } | FieldIndex::Number { .. }))
}

fn index_tree_name(index: &FieldIndex) -> &str {
  match index {
    FieldIndex::Value { tree_name, .. } | FieldIndex::Number { tree_name, .. } => tree_name,
    _ => unreachable!()
  }
}

/// Поле пригодно для min/max по крайним ключам индекса: индексировано, non-nullable,
/// не из enum-варианта (sparse-индекс мог бы потерять строки)
fn minmax_index<'a>(field: Option<&'a Field>) -> Option<Option<(&'a Field, &'a FieldIndex)>> {
  let Some(field) = field else {
    return Some(None); // агрегат не запрошен — ограничений нет
  };
  if field.nullable || !matches!(field.condition, FieldExistsCondition::None) {
    return None;
  }
  find_usable_index(field).map(|index| Some((field, index)))
}

/// min/max через первый/последний ключ индекса, count через len()
fn try_minmax_by_index<'a, U, F>(op: &'a AggregateOp, ctx: &mut TransationContext<'a, U, F>) -> Option<AggregateResult> {
  let min_index = minmax_index(op.min)?;
  let max_index = minmax_index(op.max)?;

  let mut result = AggregateResult::default();
  if op.count {
    result.count = ctx.get_tree(&op.entity.name).len();
  }

  if let Some((field, index)) = min_index {
    let index_tree = ctx.get_tree(index_tree_name(index));
    result.min = index_tree.first().unwrap().map(|(key, _)| decode_index_key_value(field, index, &key));
  }
  if let Some((field, index)) = max_index {
    let index_tree = ctx.get_tree(index_tree_name(index));
    result.max = index_tree.last().unwrap().map(|(key, _)| decode_index_key_value(field, index, &key));
  }

  Some(result)
}

#[derive(Default)]
struct Accumulators {
  count: u64,
  sum: Option<SumValue>,
  avg_sum: f64,
  avg_count: u64,
  // (ключ сравнения, сырые байты значения)
  min: Option<(Vec<u8>, Vec<u8>)>,
  max: Option<(Vec<u8>, Vec<u8>)>,
}

fn aggregate_rows<'a, U, F, I, A, B>(iter: I, ctx: &mut TransationContext<'a, U, F>, op: &'a AggregateOp, acc: &mut Accumulators)
  where I: Iterator<Item = (A, B)>, A: AsRef<[u8]>, B: AsRef<[u8]> {

  for (id, data) in iter {
    let (id, data) = (id.as_ref(), data.as_ref());
    if let Some(where_op) = &op.filter && !process_where(id, data, ctx, op.entity, where_op) {
      continue;
    }
    acc.count += 1;

    if let Some(field) = op.sum && let Some(value) = get_data(op.entity, field, id, data, ctx.schema) {
      accumulate_sum(acc, field, value);
    }
    if let Some(field) = op.avg && let Some(value) = get_data(op.entity, field, id, data, ctx.schema) {
      acc.avg_sum += read_num_as_f64(field, value);
      acc.avg_count += 1;
    }
    if let Some(field) = op.min && let Some(value) = get_data(op.entity, field, id, data, ctx.schema) {
      let key = make_compare_key(field, value);
      if acc.min.as_ref().is_none_or(|(best, _)| &key < best) {
        acc.min = Some((key, value.to_vec()));
      }
    }
    if let Some(field) = op.max && let Some(value) = get_data(op.entity, field, id, data, ctx.schema) {
      let key = make_compare_key(field, value);
      if acc.max.as_ref().is_none_or(|(best, _)| &key > best) {
        acc.max = Some((key, value.to_vec()));
      }
    }
  }
}

/// Байтовый ключ для сравнения min/max — та же кодировка, что в индексах
fn make_compare_key(field: &Field, value: &[u8]) -> Vec<u8> {
  use crate::index_utils::encode_index_number;
  use crate::schema::FieldType;

  match &field.ty {
    FieldType::Primitive(ty) if ty.get_num_type().is_some() => {
      encode_index_number(&ty.get_num_type().unwrap(), value)
    },
    _ => value.to_vec()
  }
}

fn primitive_type(field: &Field) -> &PrimitiveFieldType {
  match &field.ty {
    crate::schema::FieldType::Primitive(ty) => ty,
    _ => panic!("Aggregate field {} must be primitive", field.full_name)
  }
}

fn accumulate_sum(acc: &mut Accumulators, field: &Field, value: &[u8]) {
  match primitive_type(field) {
    PrimitiveFieldType::Int64 => {
      let v = i64::from_be_bytes(value.try_into().unwrap()) as i128;
      acc.sum = Some(match acc.sum {
        Some(SumValue::Int(current)) => SumValue::Int(current + v),
        _ => SumValue::Int(v)
      });
    },
    PrimitiveFieldType::UInt64 => {
      let v = u64::from_be_bytes(value.try_into().unwrap()) as i128;
      acc.sum = Some(match acc.sum {
        Some(SumValue::Int(current)) => SumValue::Int(current + v),
        _ => SumValue::Int(v)
      });
    },
    PrimitiveFieldType::Float => {
      let v = f32::from_be_bytes(value.try_into().unwrap()) as f64;
      acc.sum = Some(match acc.sum {
        Some(SumValue::Float(current)) => SumValue::Float(current + v),
        _ => SumValue::Float(v)
      });
    },
    PrimitiveFieldType::Double => {
      let v = f64::from_be_bytes(value.try_into().unwrap());
      acc.sum = Some(match acc.sum {
        Some(SumValue::Float(current)) => SumValue::Float(current + v),
        _ => SumValue::Float(v)
      });
    },
    _ => panic!("Sum field {} must be numeric", field.full_name)
  }
}

fn read_num_as_f64(field: &Field, value: &[u8]) -> f64 {
  match primitive_type(field) {
    PrimitiveFieldType::Int64 => i64::from_be_bytes(value.try_into().unwrap()) as f64,
    PrimitiveFieldType::UInt64 => u64::from_be_bytes(value.try_into().unwrap()) as f64,
    PrimitiveFieldType::Float => f32::from_be_bytes(value.try_into().unwrap()) as f64,
    PrimitiveFieldType::Double => f64::from_be_bytes(value.try_into().unwrap()),
    _ => panic!("Avg field {} must be numeric", field.full_name)
  }
}
