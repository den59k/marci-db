use std::{collections::HashMap, sync::Arc};

use bitvec::vec::BitVec;
use canopydb::{Bytes, Transaction, Tree};

use crate::{Field, aggregate_op::{AggregateOp, AggregateResult, process_aggregate}, query_op::{IncludeQuery, PrefixKey, QueryOp, QueryType, process_query_many, process_query_one::process_query_one, process_where::process_where}, schema::{Entity, Schema}, utils::get_data};

pub type ParentData<'a> = (&'a Entity, &'a[u8], &'a[u8]);

pub fn get_prefix<'a>(prefix_key: &'a PrefixKey, parent: Option<ParentData<'a>>, schema: &'a Schema) -> Option<&'a [u8]> {
  match prefix_key {
    PrefixKey::ParentId => {
      let (_,parent_id,_) = parent.unwrap();
      return Some(parent_id);
    },
    PrefixKey::ParentField(field) => {
      let (entity,parent_id,parent_body) = &parent.unwrap();
      return get_data(entity, field, parent_id, parent_body, schema);    
    },
    PrefixKey::Id(value) | PrefixKey::IdPrefix(value) => {
      return Some(value)
    },
    _ => {
      panic!("Cannot get prefix from {:?}", prefix_key)
    }
  }
}

pub fn get_ids_by_prefix(index_tree: &Tree, item_id: &[u8]) -> Vec<Vec<u8>> {
  let item_id_len = item_id.len();
  index_tree
    .prefix_keys(&item_id)
    .unwrap()
    .map(|e| e.unwrap()[item_id_len..].to_vec())
    .collect()
}

pub fn get_first_id_by_prefix(index_tree: &Tree, item_id: &[u8]) -> Option<Vec<u8>> {
  let item_id_len = item_id.len();
  index_tree
    .prefix_keys(&item_id)
    .unwrap()
    .map(|e| e.unwrap()[item_id_len..].to_vec())
    .next()
}

/// Итератор для опционально-обратного обхода без аллокаций
pub enum MaybeRev<I> {
  Fwd(I),
  Rev(std::iter::Rev<I>)
}

impl<I: DoubleEndedIterator> Iterator for MaybeRev<I> {
  type Item = I::Item;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self {
      MaybeRev::Fwd(iter) => iter.next(),
      MaybeRev::Rev(iter) => iter.next()
    }
  }
}

pub fn maybe_rev<I: DoubleEndedIterator>(iter: I, reverse: bool) -> MaybeRev<I> {
  if reverse { MaybeRev::Rev(iter.rev()) } else { MaybeRev::Fwd(iter) }
}

/// Ленивый обход ключей индексного дерева в заданном диапазоне и направлении
pub fn range_keys_iter<'t>(
    index_tree: &'t Tree,
    start: &Option<Vec<u8>>,
    end: &Option<Vec<u8>>,
    reverse: bool
) -> impl Iterator<Item = Bytes> + 't {
  let iter = match (start.as_deref(), end.as_deref()) {
      (Some(s), Some(e)) => index_tree.range_keys(s..e),
      (Some(s), None) => index_tree.range_keys(s..),
      (None, Some(e)) => index_tree.range_keys(..e),
      // Полный обход: диапазон от пустого ключа покрывает всё дерево
      (None, None) => index_tree.range_keys((&[] as &[u8])..),
  };

  maybe_rev(iter.unwrap(), reverse).map(|item| item.unwrap())
}

#[inline]
pub fn get_id_from_index_key(key: &[u8], fixed_size: Option<usize>) -> Vec<u8> {
  if let Some(fixed_size) = fixed_size {
    key[fixed_size..].to_vec()
  } else {
    let Some(pos) = key.iter().position(|&b| b == b'\0') else {
      panic!("Not found null-terminator in variable length index")
    };
    return key[pos+1..].to_vec();
  }
}

// Обрабатывает данные. Если элемент не подходит по условию, возвращает None
pub fn process_data<'a, 'b, U, F>(
  id: &'b [u8],
  data: &'b [u8],
  ctx: &mut TransationContext<'a, U, F>,
  query: &'a QueryOp,
) -> Option<U> where U: Clone, F: Fn(DecodeCtx<U>) -> U {

  if let Some(where_op) = &query.filter && !process_where(id, data, ctx, query.entity, where_op) {
    return None
  }

  Some(decode_row(id, data, ctx, query))
}

// Декодирует строку вместе с includes. Фильтр должен быть проверен до вызова
pub fn decode_row<'a, 'b, U, F>(
  id: &'b [u8],
  data: &'b [u8],
  ctx: &mut TransationContext<'a, U, F>,
  query: &'a QueryOp,
) -> U where U: Clone, F: Fn(DecodeCtx<U>) -> U {

  let mut includes: Vec<IncludeResult<U>> = Vec::with_capacity(query.includes.len());

  for include in query.includes.iter() {
    match &include.query {
      IncludeQuery::Query(include_query) => {
        match include.query_type {
          QueryType::One => {
            if let Some(result) = process_query_one(include_query, ctx, Some((query.entity,id,data))) {
              includes.push(IncludeResult::One(include.field, result));
            } else {
              includes.push(IncludeResult::None(include.field));
            }
          },
          QueryType::Many => {
            let result = process_query_many(include_query, ctx, Some((query.entity,id,data)));
            includes.push(IncludeResult::Many(include.field, result));
          },
          QueryType::First => {
            todo!("Make QueryType::First method")
          }
        }
      },
      IncludeQuery::Aggregate(aggregate_op) => {
        let result = process_aggregate(aggregate_op, ctx, Some((query.entity,id,data)));
        includes.push(IncludeResult::Aggregate(include.field, aggregate_op, result));
      }
    }
  }

  return (ctx.f)(DecodeCtx { id, data, entity: query.entity, mask: &query.mask, includes, schema: ctx.schema });
}

/// Кэш include-результатов одного вложенного запроса (id записи → результат декода).
/// Хранится U — сейчас это JSON-строка, при переходе на бинарный формат код не меняется
pub struct IncludeCache<U> {
  pub hits: u32,
  pub misses: u32,
  pub items: HashMap<Vec<u8>, Option<U>>
}

impl<U> Default for IncludeCache<U> {
  fn default() -> Self {
    Self { hits: 0, misses: 0, items: HashMap::new() }
  }
}

pub struct TransationContext<'a, U, F> {
  pub trees: HashMap<String, Arc<Tree<'a>>>,
  /// Любая транзакция, поддерживающая чтение. ReadTransaction и WriteTransaction
  /// разыменовываются в &Transaction, поэтому запросы работают и внутри write-транзакции
  /// (видят её незакоммиченные изменения — read-your-writes)
  pub rx: &'a Transaction,
  pub schema: &'a Schema,
  pub f: F,
  /// Кэши include-результатов на время одного запроса, по идентичности вложенного запроса
  pub include_cache: HashMap<usize, IncludeCache<U>>
}

impl<'a, U, F> TransationContext<'a, U, F> {
  pub fn new(rx: &'a Transaction, schema: &'a Schema, f: F) -> Self {
    Self { trees: HashMap::new(), rx, f, schema, include_cache: HashMap::new() }
  }
  pub fn get_tree(&mut self, key: &str) -> Arc<Tree<'a>> {
    self.trees
        .entry(key.to_string())
        .or_insert_with(|| {
          let tree = self.rx.get_tree(key.as_bytes()).unwrap().unwrap();
          return Arc::new(tree);
        })
        .clone()
  }
}

pub struct DecodeCtx<'a, U> {
  pub id: &'a [u8],
  pub data: &'a [u8],
  pub entity: &'a Entity,
  pub mask: &'a BitVec,
  pub includes: Vec<IncludeResult<'a, U>>,
  pub schema: &'a Schema
}

pub enum IncludeResult<'a, U> {
  None(&'a Field),
  One(&'a Field,U),
  Many(&'a Field,Vec<U>),
  // Агрегация по связанным записям: форматируется на json-слое
  Aggregate(&'a Field, &'a AggregateOp<'a>, AggregateResult)
}