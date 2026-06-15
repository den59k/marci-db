mod process_delete;
mod prepare_delete_op;

pub use process_delete::{process_delete};
pub use prepare_delete_op::prepare_delete;

use crate::{Field, ProviderError, StorageError, json_parsers::EncodeError, schema::{Entity, FieldIndex, RefBinding}};

#[derive(Debug)]
pub struct DeleteOp<'a> {
  pub indexes_to_delete: Vec<DeleteIndex<'a>>,
  pub dependencies: Vec<DependencyAction<'a>>,
  pub refs_to_delete: Vec<RefToDelete<'a>>
}

impl DeleteOp<'_> {
  pub fn is_body_need(&self) -> bool {
    // A `Custom` entry needs the old field value, which is read from the body — so force the body read too.
    return self.indexes_to_delete.iter().any(|f| matches!(f, DeleteIndex::BodyValue { .. } | DeleteIndex::Custom { .. })) ||
      self.dependencies.iter().any(|f| matches!(f.binding, Some((_, RefBinding::FieldValue))))
  }
  pub fn is_empty(&self) -> bool {
    return self.indexes_to_delete.is_empty() && self.dependencies.is_empty() && self.refs_to_delete.is_empty()
  }
}

#[derive(Debug)]
pub enum DeleteIndex<'a> {
  Value { index: &'a FieldIndex, key: Vec<u8> },
  BodyValue { index: &'a FieldIndex, field: &'a Field, offset_pos: usize },
  KeyValue { index: &'a FieldIndex, field: &'a Field },
  /// A live `@custom` (module) index on `field`: its `on_delete` hook is dispatched with the old field value
  /// read from the row body. One entry per indexed field; the provider iterates the field's custom indexes.
  Custom { field: &'a Field },
}

#[derive(Debug)]
pub enum RefToDelete<'a> {
  Index { tree_name: String },
  ChildEntity { entity: &'a Entity, delete_op: DeleteOp<'a> }
}

#[derive(Debug)]
pub struct DependencyAction<'a> {
  pub rev_entity: &'a Entity,
  pub rev_field: &'a Field,
  pub rev_binding: &'a RefBinding,
  pub binding: Option<(&'a Field, &'a RefBinding)>,
  pub action_type: DependencyActionType<'a>
  // Value { tree_name: String, key: Vec<u8> },
}

#[derive(Debug)]
pub enum DependencyActionType<'a> {
  Delete (DeleteOp<'a>),
  SetNull { offset_pos: usize },
  Restrict,
  RemoveIndex { tree_name: String }
}

#[derive(Debug,PartialEq)]
pub enum DeleteError {
  ItemNotFound,
  RestrictConstraints(String,Vec<Vec<u8>>),
  EncodeError(EncodeError),
  /// A cascade/dependency of this form is not implemented yet (nested owned collections, IndexTree-rev)
  Unsupported(&'static str),
  /// A live `@custom` index hook (e.g. full-text `on_delete`) failed while maintaining the index.
  IndexError(ProviderError),
  Storage(StorageError)
}

impl From<canopydb::Error> for DeleteError {
  fn from(e: canopydb::Error) -> Self {
    DeleteError::Storage(StorageError::Backend(e))
  }
}

impl From<StorageError> for DeleteError {
  fn from(e: StorageError) -> Self {
    DeleteError::Storage(e)
  }
}

impl From<ProviderError> for DeleteError {
  fn from(e: ProviderError) -> Self {
    DeleteError::IndexError(e)
  }
}
