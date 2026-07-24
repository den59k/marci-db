mod process_update;

use crate::{Field, InsertError, ProviderError, StorageError, delete_op::{DeleteError, DeleteOp}, num_utils::NumberDelta, schema::{Entity, RefInfo}, write_op::{WriteIndexesError, WriteOp}};
pub use process_update::process_update;

#[derive(Debug)]
pub struct UpdateOp<'a> {
  pub update_fields: Vec<UpdateField<'a>>,
  pub update_refs: Vec<UpdateRelation<'a>>,
}

#[derive(Debug)]
pub struct UpdateField<'a> {
  pub field: &'a Field,
  pub offset_pos: usize,
  pub value: UpdateValue
}

#[derive(Debug)]
pub enum UpdateValue {
  Null,
  Value(Vec<u8>),
  Increment(NumberDelta)
}

#[derive(Debug,PartialEq)]
pub enum UpdateError {
  ItemNotFound,
  UniqueViolation(String, Vec<u8>),
  DeleteError(DeleteError),
  InsertError(InsertError),
  WriteIndexesError(WriteIndexesError),
  /// A live `@custom` index hook (e.g. full-text `on_update`) failed while maintaining the index.
  IndexError(ProviderError),
  /// The relation operation is not implemented yet.
  Unsupported(&'static str),
  /// An `$increment` would take the field outside its range — a negative result on an unsigned field, or
  /// past the type's min/max. The write is rejected and the whole transaction rolls back, rather than
  /// storing a wrapped value.
  IncrementOutOfRange(String),
  Storage(StorageError)
}

impl From<canopydb::Error> for UpdateError {
  fn from(e: canopydb::Error) -> Self {
    UpdateError::Storage(StorageError::Backend(e))
  }
}

impl From<StorageError> for UpdateError {
  fn from(e: StorageError) -> Self {
    UpdateError::Storage(e)
  }
}

impl From<ProviderError> for UpdateError {
  fn from(e: ProviderError) -> Self {
    UpdateError::IndexError(e)
  }
}

#[derive(Debug)]
pub struct UpdateRelation<'a> {
  pub field: &'a Field,
  pub ref_info: &'a RefInfo,
  pub rev_ref_info: Option<&'a RefInfo>,
  pub st: &'a Entity,
  pub op: UpdateRelationOp<'a>
}

#[derive(Debug)]
pub enum UpdateRelationOp<'a> {
  /// Deletes objects
  Remove(DeleteOp<'a>),
  /// Removes relations without deleting objects
  DisconnectAll,
  /// Replaces all objects with a new object
  // Replace(WriteOp<'a>,DeleteOp<'a>),
  /// Creates an object only where the field = null
  Create(WriteOp<'a>),
  /// Updates the object if it is not null
  Update(UpdateOp<'a>),

  /// Adds elements to the array
  Push(Vec<WriteOp<'a>>),
  // /// Replaces all elements with elements from the array
  RemoveAll(DeleteOp<'a>),
  /// Removes elements from the array
  RemoveItems(Vec<Vec<u8>>,DeleteOp<'a>),

  /// Links the entity to existing objects
  Connect(Vec<Vec<u8>>),
  /// Removes relations to existing objects
  Disconnect(Vec<Vec<u8>>),

  /// `@list` relation: replaces the whole inline id array (also the reorder operation —
  /// same members in a new order rewrite the body without touching the reverse tree)
  SetList(Vec<Vec<u8>>),
  /// `@list` relation: appends ids to the end of the array (already-present ids are skipped)
  ConnectList(Vec<Vec<u8>>),
  /// `@list` relation: removes ids from the array
  DisconnectList(Vec<Vec<u8>>),
}