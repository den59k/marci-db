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

#[derive(Debug)]
pub struct QueryOp<'a> {
  pub mask: BitVec,
  pub entity: &'a Entity,
  pub sort: Option<Sort<'a>>,
  pub filter: Option<Where<'a>>,
  pub limit: Option<usize>,
  pub skip: Option<usize>,
  /// Keyset-пагинация: id элемента, строго после которого идут результаты
  pub cursor: Option<Vec<u8>>,
  /// Направление скана (выставляется планировщиком)
  pub reverse: bool,
  /// Скан не даёт нужного порядка — строки сортируются в памяти после фильтра
  pub post_sort: bool,
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
  pub query: QueryOp<'a>,
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