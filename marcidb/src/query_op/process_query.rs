use std::{collections::HashMap, sync::Arc};

use bitvec::vec::BitVec;
use canopydb::{ReadTransaction, Tree};

use crate::{Field, query_op::{PrefixKey, QueryOp, QueryType, process_query_many, process_query_one::process_query_one, process_where::process_where}, schema::{Entity, Schema}, utils::{get_data, check_exists_condition}};

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

pub fn get_ids_by_range(
    index_tree: &Tree,
    start: &Option<Vec<u8>>,
    end: &Option<Vec<u8>>,
    fixed_size: Option<usize>,
    limit: Option<usize>
) -> Vec<Vec<u8>> {
  let iter = match (start.as_deref(), end.as_deref()) {
      (Some(s), Some(e)) => index_tree.range_keys(s..e),
      (Some(s), None) => index_tree.range_keys(s..),
      (None, Some(e)) => index_tree.range_keys(..e),
      (None, None) => panic!("Start or end of range must be defined"),
  };

  if let Some(limit) = limit {
    iter
      .unwrap()
      .take(limit)
      .map(|e| get_id_from_index_key(&e.unwrap(), fixed_size))
      .collect()
  } else {
    iter
      .unwrap()
      .map(|e| get_id_from_index_key(&e.unwrap(), fixed_size))
      .collect()
  }
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
  ctx: &mut TransationContext<'a, F>,
  query: &'a QueryOp,
) -> Option<U> where F: Fn(DecodeCtx<U>) -> U { 

  if let Some(where_op) = &query.filter && !process_where(id, data, ctx, query.entity, where_op) {
    return None
  }

  let mut includes: Vec<IncludeResult<U>> = Vec::with_capacity(query.includes.len());

  for include in query.includes.iter() {
    if !check_exists_condition(query.entity, &include.field.condition, id, data, ctx.schema) {
      continue;
    }
    match include.query_type {
      QueryType::One => {
        if let Some(result) = process_query_one(&include.query, ctx, Some((query.entity,id,data))) {
          includes.push(IncludeResult::One(include.field, result));
        } else {
          includes.push(IncludeResult::None(include.field));
        }
      },
      QueryType::Many => {
        let result = process_query_many(&include.query, ctx, Some((query.entity,id,data)));
        includes.push(IncludeResult::Many(include.field, result));
      },
      QueryType::First => {
        todo!("Make QueryType::First method")
      }
    }
  }

  return Some((ctx.f)(DecodeCtx { id, data, entity: query.entity, mask: &query.mask, includes, schema: ctx.schema }));
}

pub struct TransationContext<'a, F> {
  pub trees: HashMap<String, Arc<Tree<'a>>>,
  pub rx: &'a ReadTransaction,
  pub schema: &'a Schema,
  pub f: F
}

impl<'a, F> TransationContext<'a, F> {
  pub fn new(rx: &'a ReadTransaction, schema: &'a Schema, f: F) -> Self {
    Self { trees: HashMap::new(), rx, f, schema }
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
  Many(&'a Field,Vec<U>)
}