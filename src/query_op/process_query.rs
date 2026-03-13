use std::{collections::HashMap, sync::Arc};

use bitvec::vec::BitVec;
use canopydb::{ReadTransaction, Tree};

use crate::{Field, query_op::{PrefixKey, QueryOp, QueryType}, schema::{Entity, Schema}};

pub fn process_query_many<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, F>, parent_id: Option<&[u8]>) -> Vec<U>
  where F: Fn(DecodeCtx<U>) -> U {

  let tree = ctx.get_tree(&query.entity.name);

  if let Some(prefix_key) = &query.prefix_key {
    match prefix_key {
      PrefixKey::ParentId => {
        let prefix = &parent_id.unwrap();
        return tree.prefix(prefix).unwrap().map(|item| {
          let (id, value) = item.unwrap();
          process_data(&id, &value, ctx, query)
        }).collect();
      }
    }
  }

  return tree.iter().unwrap().map(|item| {
    let (id, value) = item.unwrap();
    process_data(&id, &value, ctx, query)
  }).collect()
}

pub fn process_query_one<'a, U,F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, F>, parent_id: Option<&[u8]>) -> Option<U>
  where F: Fn(DecodeCtx<U>) -> U {

  let tree = ctx.get_tree(&query.entity.name);

  let Some(prefix_key) = &query.prefix_key else {
    panic!("QueryOne without prefix is not supported");
  };

  match prefix_key {
    PrefixKey::ParentId => {
      let id = &parent_id.unwrap();
      return tree.get(*id).unwrap().map(|value| {
        process_data(id, &value, ctx, query)
      });
    }
  }
}


pub fn process_data<'a, 'b, U, F>(
  id: &'b [u8],
  data: &'b [u8],
  ctx: &mut TransationContext<'a, F>,
  query: &'a QueryOp,
) -> U where F: Fn(DecodeCtx<U>) -> U { 

  let mut includes: Vec<IncludeResult<U>> = Vec::with_capacity(query.includes.len());

  for include in query.includes.iter() {
    match include.query_type {
      QueryType::One => {
        if let Some(result) = process_query_one(&include.query, ctx, Some(id)) {
          includes.push(IncludeResult::One(include.field, result));
        } else {
          includes.push(IncludeResult::None(include.field));
        }
      },
      QueryType::Many => {
        let result = process_query_many(&include.query, ctx, Some(id));
        includes.push(IncludeResult::Many(include.field, result));
      },
      QueryType::First => {
        todo!("Make QueryType::First method")
      }
    }
  }

  return (ctx.f)(DecodeCtx { id, data, entity: query.entity, mask: &query.mask, includes, schema: ctx.schema });
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