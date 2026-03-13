use bitvec::vec::BitVec;

mod r#where;
mod process_query;
use crate::{Field, query_op::r#where::Where, schema::{Entity, FieldLocation}};

pub use process_query::{process_query_many,process_query_one,DecodeCtx,TransationContext,IncludeResult};

#[derive(Debug)]
pub struct QueryOp<'a> {
  pub mask: BitVec,
  pub entity: &'a Entity,
  pub sort: Option<Sort<'a>>,
  pub filter: Option<Where<'a>>,
  pub take: Option<usize>,
  pub skip: Option<usize>,
  pub prefix_key: Option<PrefixKey>,
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

#[derive(Debug)]
pub enum PrefixKey {
  ParentId
}

impl QueryOp<'_> {
  pub fn only_key(&self) -> bool {
    for (idx, field) in self.entity.fields.iter().enumerate() {
      if self.mask[idx] && !matches!(field.location, FieldLocation::Key { .. }) {
        return false;
      }
    }
    return true;
  }
}