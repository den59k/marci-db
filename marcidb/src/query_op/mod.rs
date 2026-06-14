use bitvec::{bitvec, vec::BitVec};

mod r#where;
mod process_query;
mod process_where;
mod process_query_one;
mod process_query_many;
use crate::{Field, schema::{Entity, FieldLocation, FieldType}};

pub use process_query::{DecodeCtx,TransationContext,IncludeResult};
pub use process_query_one::process_query_one;
pub use process_query_many::process_query_many;
pub use r#where::{Where,FieldCompare,FieldCompareRef};

pub(crate) use process_query::{ParentData, get_id_from_index_key, get_prefix, range_keys_iter};
pub(crate) use process_where::process_where;

#[derive(Debug)]
pub struct QueryOp<'a> {
  pub mask: BitVec,
  pub entity: &'a Entity,
  pub sort: Option<Sort<'a>>,
  pub filter: Option<Where<'a>>,
  pub limit: Option<usize>,
  pub skip: Option<usize>,
  /// Keyset pagination: id of the item strictly after which the results begin
  pub cursor: Option<Vec<u8>>,
  /// Scan direction (set by the planner)
  pub reverse: bool,
  /// The scan does not produce the required order — rows are sorted in memory after filtering
  pub post_sort: bool,
  pub prefix_key: Option<PrefixKey<'a>>,
  pub includes: Vec<QueryInclude<'a>>
}

#[derive(Debug)]
pub enum QueryType {
  // Fetches an item by ID
  One,
  // Fetches all items matching the query
  Many
}

#[derive(Debug)]
pub enum Sort<'a> {
  Asc(&'a Field),
  Desc(&'a Field)
}

impl<'a> Sort<'a> {
  pub fn field(&self) -> &'a Field {
    match self {
      Sort::Asc(field) | Sort::Desc(field) => field
    }
  }

  pub fn is_desc(&self) -> bool {
    matches!(self, Sort::Desc(_))
  }
}

#[derive(Debug)]
pub struct QueryInclude<'a> {
  pub query_type: QueryType,
  pub field: &'a Field,
  pub query: IncludeQuery<'a>,
}

#[derive(Debug)]
pub enum IncludeQuery<'a> {
  // Nested select of related records
  Query(QueryOp<'a>),
  // Aggregation over related records
  Aggregate(crate::aggregate_op::AggregateOp<'a>)
}

#[derive(Debug,Clone)]
pub enum PrefixKey<'a> {
  ParentId,
  ParentField(&'a Field),
  ParentIndexTree(String),
  IndexRange { start: Option<Vec<u8>>, end: Option<Vec<u8>>, tree_name: String, fixed_size: Option<usize> },
  Id(Vec<u8>),
  IdPrefix(Vec<u8>),
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
      limit: None,
      skip: None,
      cursor: None,
      reverse: false,
      post_sort: false,
      prefix_key: None,
      includes: vec![]
    }
  }
}