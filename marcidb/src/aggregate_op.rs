use crate::{Field, StorageError, index_utils::decode_index_key_value, query_op::{FieldCompare, ParentData, PrefixKey, TransationContext, Where, get_id_from_index_key, get_prefix, process_where, range_keys_iter}, schema::{Entity, FieldExistsCondition, FieldIndex, PrimitiveFieldType}, utils::get_data};

#[derive(Debug)]
pub struct AggregateOp<'a> {
  pub entity: &'a Entity,
  pub filter: Option<Where<'a>>,
  pub prefix_key: Option<PrefixKey<'a>>,
  /// The filter is fully expressed by the prefix_key range — count can be computed from keys, without reading rows
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

  /// Whether rows need to be read, or counting keys is enough
  fn needs_rows(&self) -> bool {
    self.sum.is_some() || self.avg.is_some() || self.min.is_some() || self.max.is_some()
      || (self.filter.is_some() && !self.fully_covered)
  }
}

/// Accumulated sum: integers are counted exactly (i128), fractional ones — in f64
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SumValue {
  Int(i128),
  Float(f64)
}

#[derive(Debug, Default)]
pub struct AggregateResult {
  pub count: u64,
  /// None — there were no values at all (like NULL for SUM in SQL)
  pub sum: Option<SumValue>,
  pub avg: Option<f64>,
  /// Raw field value bytes — decoded at the json layer
  pub min: Option<Vec<u8>>,
  pub max: Option<Vec<u8>>,
}

pub fn process_aggregate<'a, U, F>(op: &'a AggregateOp, ctx: &mut TransationContext<'a, U, F>, parent: Option<ParentData>) -> Result<AggregateResult, StorageError> {
  let mut result = AggregateResult::default();

  // Fast paths: count by keys, without reading and decoding rows
  if !op.needs_rows() {
    result.count = match &op.prefix_key {
      None => ctx.get_tree(&op.entity.name)?.len(),
      Some(PrefixKey::IndexRange { start, end, tree_name, .. }) => {
        let index_tree = ctx.get_tree(tree_name)?;
        range_keys_iter(&index_tree, start, end, false)?.try_fold(0u64, |n, k| k.map(|_| n + 1))?
      },
      // Parent relations: number of children = number of keys under the parent_id prefix
      Some(PrefixKey::ParentIndexTree(tree_name)) => {
        let index_tree = ctx.get_tree(tree_name)?;
        index_tree.prefix_keys(&parent.unwrap().1)?.try_fold(0u64, |n, k| k.map(|_| n + 1))?
      },
      Some(prefix_key) => {
        let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
          return Ok(result);
        };
        let tree = ctx.get_tree(&op.entity.name)?;
        tree.prefix_keys(&prefix)?.try_fold(0u64, |n, k| k.map(|_| n + 1))?
      },
    };
    return Ok(result);
  }

  // count by null/not-null of an indexed field: a sparse index doesn't contain null rows,
  // so the difference of tree sizes gives the exact answer without a scan
  if op.sum.is_none() && op.avg.is_none() && op.min.is_none() && op.max.is_none() && op.prefix_key.is_none() {
    if let Some(Where::Field(field, compare @ (FieldCompare::EqNull | FieldCompare::NeNull))) = &op.filter
      && matches!(field.condition, FieldExistsCondition::None)
      && let Some(index) = find_usable_index(field) {

      let index_len = ctx.get_tree(index_tree_name(index))?.len();
      result.count = match compare {
        FieldCompare::NeNull => index_len,
        _ => ctx.get_tree(&op.entity.name)?.len() - index_len
      };
      return Ok(result);
    }
  }

  // min/max via the boundary keys of the index: O(log n) instead of a scan.
  // Applicable without a filter and without sum/avg, when all requested fields are indexed and non-nullable
  if op.filter.is_none() && op.prefix_key.is_none() && op.sum.is_none() && op.avg.is_none() {
    if let Some(fast_result) = try_minmax_by_index(op, ctx)? {
      return Ok(fast_result);
    }
  }

  let mut acc = Accumulators::default();

  match &op.prefix_key {
    Some(PrefixKey::IndexRange { start, end, tree_name, fixed_size }) => {
      let index_tree = ctx.get_tree(tree_name)?;
      let tree = ctx.get_tree(&op.entity.name)?;
      let iter = range_keys_iter(&index_tree, start, end, false)?
        .map(|key| {
          let id = get_id_from_index_key(&key?, *fixed_size);
          let value = tree.get(&id)?.unwrap();
          Ok((id, value))
        });
      aggregate_rows(iter, ctx, op, &mut acc)?;
    },
    Some(PrefixKey::ParentIndexTree(tree_name)) => {
      let index_tree = ctx.get_tree(tree_name)?;
      let tree = ctx.get_tree(&op.entity.name)?;
      let parent_id = parent.unwrap().1;
      let parent_id_len = parent_id.len();
      let iter = index_tree.prefix_keys(&parent_id)?
        .map(|item| {
          let key = item?;
          let id = key[parent_id_len..].to_vec();
          let value = tree.get(&id)?.unwrap();
          Ok((id, value))
        });
      aggregate_rows(iter, ctx, op, &mut acc)?;
    },
    Some(prefix_key) => {
      let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
        return Ok(result);
      };
      let tree = ctx.get_tree(&op.entity.name)?;
      aggregate_rows(tree.prefix(&prefix)?, ctx, op, &mut acc)?;
    },
    None => {
      let tree = ctx.get_tree(&op.entity.name)?;
      aggregate_rows(tree.iter()?, ctx, op, &mut acc)?;
    },
  }

  result.count = acc.count;
  result.sum = acc.sum;
  result.avg = if acc.avg_count > 0 { Some(acc.avg_sum / acc.avg_count as f64) } else { None };
  result.min = acc.min.map(|(_, value)| value);
  result.max = acc.max.map(|(_, value)| value);
  Ok(result)
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

/// A field is eligible for min/max via the boundary keys of the index: indexed, non-nullable,
/// not from an enum variant (a sparse index could lose rows)
fn minmax_index<'a>(field: Option<&'a Field>) -> Option<Option<(&'a Field, &'a FieldIndex)>> {
  let Some(field) = field else {
    return Some(None); // aggregate not requested — no constraints
  };
  if field.nullable || !matches!(field.condition, FieldExistsCondition::None) {
    return None;
  }
  find_usable_index(field).map(|index| Some((field, index)))
}

/// min/max via the first/last index key, count via len()
fn try_minmax_by_index<'a, U, F>(op: &'a AggregateOp, ctx: &mut TransationContext<'a, U, F>) -> Result<Option<AggregateResult>, StorageError> {
  let Some(min_index) = minmax_index(op.min) else { return Ok(None) };
  let Some(max_index) = minmax_index(op.max) else { return Ok(None) };

  let mut result = AggregateResult::default();
  if op.count {
    result.count = ctx.get_tree(&op.entity.name)?.len();
  }

  if let Some((field, index)) = min_index {
    let index_tree = ctx.get_tree(index_tree_name(index))?;
    result.min = index_tree.first()?.map(|(key, _)| decode_index_key_value(field, index, &key));
  }
  if let Some((field, index)) = max_index {
    let index_tree = ctx.get_tree(index_tree_name(index))?;
    result.max = index_tree.last()?.map(|(key, _)| decode_index_key_value(field, index, &key));
  }

  Ok(Some(result))
}

#[derive(Default)]
struct Accumulators {
  count: u64,
  sum: Option<SumValue>,
  avg_sum: f64,
  avg_count: u64,
  // (comparison key, raw value bytes)
  min: Option<(Vec<u8>, Vec<u8>)>,
  max: Option<(Vec<u8>, Vec<u8>)>,
}

fn aggregate_rows<'a, U, F, I, A, B>(iter: I, ctx: &mut TransationContext<'a, U, F>, op: &'a AggregateOp, acc: &mut Accumulators) -> Result<(), StorageError>
  where I: Iterator<Item = Result<(A, B), canopydb::Error>>, A: AsRef<[u8]>, B: AsRef<[u8]> {

  for item in iter {
    let (id, data) = item?;
    let (id, data) = (id.as_ref(), data.as_ref());
    if let Some(where_op) = &op.filter && !process_where(id, data, ctx, op.entity, where_op)? {
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
  Ok(())
}

/// Byte key for min/max comparison — the same encoding as in indexes
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
