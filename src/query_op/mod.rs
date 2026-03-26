use bitvec::{bitvec, vec::BitVec};

mod r#where;
mod process_query;
mod process_where;
use crate::{Field, schema::{Entity, FieldLocation, FieldType}};

pub use process_query::{process_query_many,process_query_one,DecodeCtx,TransationContext,IncludeResult};
pub use r#where::{Where,FieldCompare,FieldCompareRef};

#[derive(Debug)]
pub struct QueryOp<'a> {
  pub mask: BitVec,
  pub entity: &'a Entity,
  pub sort: Option<Sort<'a>>,
  pub filter: Option<Where<'a>>,
  pub take: Option<usize>,
  pub skip: Option<usize>,
  pub prefix_key: Option<PrefixKey<'a>>,
  pub includes: Vec<QueryInclude<'a>>
}

#[derive(Debug)]
pub enum QueryType {
  // Получает элемент по ID
  One,
  // Получает первый элемент
  First,
  // Получает все элементы, удволетворяющие запросу
  Many
}

#[derive(Debug)]
pub enum Sort<'a> {
  Asc(&'a Field),
  Desc(&'a Field)
}

#[derive(Debug)]
pub struct QueryInclude<'a> {
  pub query_type: QueryType,
  pub field: &'a Field,
  pub query: QueryOp<'a>,
}

#[derive(Debug,Clone)]
pub enum PrefixKey<'a> {
  ParentId,
  ParentField(&'a Field),
  ParentIndexTree(String),
  IndexRange { start: Option<Vec<u8>>, end: Option<Vec<u8>>, tree_name: String, fixed_size: Option<usize> }
}

impl<'a> QueryOp<'a> {
  pub fn only_key(&self) -> bool {
    for (idx, field) in self.entity.fields.iter().enumerate() {
      if self.mask[idx] && !matches!(field.location, FieldLocation::Key { .. }) {
        return false;
      }
    }
    return true;
  }

  pub fn all(entity: &'a Entity) -> Self {
    let mut mask = bitvec![1; entity.fields.len()];
    for (field_index,field) in entity.fields.iter().enumerate() {
      if matches!(field.ty, FieldType::Ref(_) | FieldType::RefList(_)) {
        mask.set(field_index, false);
      }
    }
    return QueryOp { 
      mask,
      entity, 
      sort: None, 
      filter: None, 
      take: None, 
      skip: None, 
      prefix_key: None, 
      includes: vec![]
    }
  }
}