mod process_delete;
mod prepare_delete_op;

pub use process_delete::{process_delete};
pub use prepare_delete_op::prepare_delete;

use crate::{Field, ProviderError, StorageError, json_parsers::EncodeError, schema::{Entity, FieldIndex, RefBinding, UniqueGroup}};

#[derive(Debug)]
pub struct DeleteOp<'a> {
  pub indexes_to_delete: Vec<DeleteIndex<'a>>,
  pub dependencies: Vec<DependencyAction<'a>>,
  pub refs_to_delete: Vec<RefToDelete<'a>>
}

impl DeleteOp<'_> {
  pub fn is_body_need(&self) -> bool {
    // A `Custom` entry needs the old field value, which is read from the body — so force the body read too.
    // A `@list` id array (own field or a dependency's forward binding) is likewise read from the body.
    return self.indexes_to_delete.iter().any(|f| matches!(f, DeleteIndex::BodyValue { .. } | DeleteIndex::Custom { .. } | DeleteIndex::UniqueGroup { .. })) ||
      self.dependencies.iter().any(|f| matches!(f.binding, Some((_, RefBinding::FieldValue)) | Some((_, RefBinding::IdList { .. })))) ||
      self.refs_to_delete.iter().any(|f| matches!(f, RefToDelete::IdListRev { .. }))
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
  /// A composite unique index: the entry is recomputed from the key and the body (`encode_unique_group`).
  UniqueGroup { group: &'a UniqueGroup },
}

#[derive(Debug)]
pub enum RefToDelete<'a> {
  Index { tree_name: String },
  ChildEntity { entity: &'a Entity, delete_op: DeleteOp<'a> },
  /// Owner of a `@list` relation with no declared back-reference is deleted: the hidden reverse tree
  /// still holds `member_id ++ owner_id` entries for every member in the body array — tear them down
  IdListRev { tree_name: String, field: &'a Field },
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
  RemoveIndex { tree_name: String },
  /// A row referenced by a `@list` id array is deleted: splice its id out of every owner's inline
  /// array (owners are found via the reverse tree) and drop the reverse-tree entries
  RemoveFromIdList { tree_name: String },
}

#[derive(Debug,PartialEq)]
pub enum DeleteError {
  ItemNotFound,
  /// Rows still point at the one being deleted through a `Restrict` relation: the field's full name
  /// (`Enrollment.class`) and the ids of those rows (the client's form of the id).
  RestrictConstraints(String, Vec<String>),
  EncodeError(EncodeError),
  /// A cascade/dependency of this form is not implemented yet (nested owned collections, IndexTree-rev)
  Unsupported(&'static str),
  /// A live `@custom` index hook (e.g. full-text `on_delete`) failed while maintaining the index.
  IndexError(ProviderError),
  /// The deleted row could not be written to a journal of its model (journal.rs).
  Journal(String),
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

impl std::fmt::Display for DeleteError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      DeleteError::ItemNotFound => write!(f, "the row does not exist"),
      DeleteError::RestrictConstraints(field, ids) => write!(f, "{}: rows {} still point at this one; delete or repoint them first", field, ids.join(", ")),
      DeleteError::EncodeError(e) => write!(f, "{}", e),
      DeleteError::Unsupported(what) => write!(f, "not supported: {}", what),
      DeleteError::IndexError(e) => write!(f, "{}", e),
      DeleteError::Journal(e) => write!(f, "journal: {}", e),
      DeleteError::Storage(e) => write!(f, "{}", e),
    }
  }
}
impl std::error::Error for DeleteError {}

impl DeleteError {
  /// The class of the refusal, as clients see it — see `InsertError::kind`; `restrict` is the delete refused
  /// by a `Restrict` relation.
  pub fn kind(&self) -> &'static str {
    match self {
      DeleteError::ItemNotFound => "not_found",
      DeleteError::RestrictConstraints(..) => "restrict",
      DeleteError::Journal(_) | DeleteError::Storage(_) => "internal",
      _ => "bad_request",
    }
  }
  /// The field the refusal is about (`Model.field`), when it is about one.
  pub fn field(&self) -> Option<&str> {
    match self {
      DeleteError::RestrictConstraints(field, _) => Some(field),
      _ => None,
    }
  }
}
