mod process_update;

use crate::{Field, InsertError, StorageError, delete_op::{DeleteError, DeleteOp}, num_utils::NumberValue, schema::{Entity, RefInfo}, write_op::{WriteIndexesError, WriteOp}};
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
  Increment(NumberValue)
}

#[derive(Debug,PartialEq)]
pub enum UpdateError {
  ItemNotFound,
  UniqueViolation(String, Vec<u8>),
  DeleteError(DeleteError),
  InsertError(InsertError),
  WriteIndexesError(WriteIndexesError),
  /// The relation operation is not implemented yet (disconnect of a single relation, $remove of list items)
  Unsupported(&'static str),
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
}