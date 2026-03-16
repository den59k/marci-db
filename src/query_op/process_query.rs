use std::{collections::HashMap, sync::Arc};

use bitvec::vec::BitVec;
use canopydb::{ReadTransaction, Tree};

use crate::{Field, query_op::{PrefixKey, QueryOp, QueryType, process_where::process_where}, schema::{Entity, Schema}, utils::get_data};

type ParentData<'a> = (&'a Entity, &'a[u8], &'a[u8]);

pub fn process_query_many<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, F>, parent: Option<ParentData>) -> Vec<U>
  where F: Fn(DecodeCtx<U>) -> U {

  if let Some(PrefixKey::ParentIndexTree(index_tree_name)) = &query.prefix_key {
    let index_tree = ctx.get_tree(index_tree_name);
    let ids = get_ids_by_prefix(&index_tree, parent.unwrap().1);
    if ids.is_empty() {
      return vec![];
    }
    let tree = ctx.get_tree(&query.entity.name);

    return ids.iter().filter_map(|id| {
      let value = tree.get(&id).unwrap().unwrap();
      process_data(&id, &value, ctx, query)
    }).collect()
  }
    
  if let Some(prefix_key) = &query.prefix_key {
    let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
      return vec![];
    };
    let tree = ctx.get_tree(&query.entity.name);
    return tree.prefix(&prefix).unwrap().filter_map(|item| {
      let (id, value) = item.unwrap();
      process_data(&id, &value, ctx, query)
    }).collect();
  }
  
  let tree = ctx.get_tree(&query.entity.name);
  return tree.iter().unwrap().filter_map(|item| {
    let (id, value) = item.unwrap();
    process_data(&id, &value, ctx, query)
  }).collect()
}

pub fn process_query_one<'a, U,F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, F>, parent: Option<ParentData>) -> Option<U>
  where F: Fn(DecodeCtx<U>) -> U {

  let Some(prefix_key) = &query.prefix_key else {
    panic!("QueryOne without prefix is not supported");
  };
  
  let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
    return None;
  };
    
  let tree = ctx.get_tree(&query.entity.name);
  return tree.get(prefix).unwrap().and_then(|value| {
    process_data(prefix, &value, ctx, query)
  });
}

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