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
  Increment(NumberDelta),
  /// In-place edit of a variable-length primitive list; the elements are individually encoded
  ListOp { op: ListOp, items: Vec<Vec<u8>>, elem_size: Option<usize> },
}

/// The edit a primitive-list operator applies. The array is a sequence — the same
/// value may appear several times (mirrors the `@list` relation semantics)
#[derive(Debug)]
pub enum ListOp {
  /// Append at the end — an already-present value gains another occurrence
  Push,
  /// Append only the values not already present
  PushUnique,
  /// Remove every occurrence of the given values
  Remove,
}

#[derive(Debug,PartialEq)]
pub enum UpdateError {
  ItemNotFound,
  /// A `@unique` value is taken: the field's full name and the id of the row holding it (the client's form of the id).
  UniqueViolation(String, String),
  DeleteError(DeleteError),
  InsertError(InsertError),
  WriteIndexesError(WriteIndexesError),
  /// A relation field was pointed at a row that does not exist: the field's full name and the id.
  ForeignKeyViolation(String, String),
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

impl UpdateError {
  /// The error of linking rows through `field`, with the field's name where the message needs it.
  pub(crate) fn link(e: WriteIndexesError, field: &Field) -> UpdateError {
    match e {
      WriteIndexesError::ForeignKeyViolation(id) => UpdateError::ForeignKeyViolation(field.full_name.clone(), id),
      e => UpdateError::WriteIndexesError(e),
    }
  }
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
  /// Replaces link membership with exactly the given set: missing links are disconnected,
  /// new ones connected. Never creates or deletes the rows themselves
  SetLinks(Vec<Vec<u8>>),
  /// Owned (struct) list: partial update of single children, addressed by the child's own key
  /// fields (the parent prefix of the storage key is supplied by the update context)
  UpdateItems(Vec<(Vec<u8>, UpdateOp<'a>)>),

  /// `@list` relation: replaces the whole inline id array (also the reorder operation —
  /// same members in a new order rewrite the body without touching the reverse tree).
  /// The array is a sequence: the same id may appear several times
  SetList(Vec<Vec<u8>>),
  /// `@list` relation: appends ids to the end of the array — an already-present id gains
  /// another occurrence
  ConnectList(Vec<Vec<u8>>),
  /// `@list` relation: appends only the ids not already present in the array
  ConnectUniqueList(Vec<Vec<u8>>),
  /// `@list` relation: removes every occurrence of the given ids
  DisconnectList(Vec<Vec<u8>>),
}
impl std::fmt::Display for UpdateError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      UpdateError::ItemNotFound => write!(f, "the row does not exist"),
      UpdateError::UniqueViolation(field, id) => write!(f, "{}: the value is already taken (by the row with id {})", field, id),
      UpdateError::DeleteError(e) => write!(f, "{}", e),
      UpdateError::InsertError(e) => write!(f, "{}", e),
      UpdateError::WriteIndexesError(e) => write!(f, "{}", e),
      UpdateError::ForeignKeyViolation(field, id) => write!(f, "{}: no row with id {} to point at", field, id),
      UpdateError::IndexError(e) => write!(f, "{}", e),
      UpdateError::Unsupported(what) => write!(f, "not supported: {}", what),
      UpdateError::IncrementOutOfRange(field) => write!(f, "{}: the increment takes the value out of the field's range", field),
      UpdateError::Storage(e) => write!(f, "{}", e),
    }
  }
}
impl std::error::Error for UpdateError {}

impl UpdateError {
  /// The class of the refusal, as clients see it — see `InsertError::kind`.
  pub fn kind(&self) -> &'static str {
    match self {
      UpdateError::ItemNotFound => "not_found",
      UpdateError::UniqueViolation(..) => "unique",
      UpdateError::ForeignKeyViolation(..) => "foreign_key",
      UpdateError::DeleteError(e) => e.kind(),
      UpdateError::InsertError(e) => e.kind(),
      UpdateError::WriteIndexesError(e) => e.kind(),
      UpdateError::Storage(_) => "internal",
      _ => "bad_request",
    }
  }
  /// The field the refusal is about (`Model.field`), when it is about one.
  pub fn field(&self) -> Option<&str> {
    match self {
      UpdateError::UniqueViolation(field, _) | UpdateError::ForeignKeyViolation(field, _) | UpdateError::IncrementOutOfRange(field) => Some(field),
      UpdateError::DeleteError(e) => e.field(),
      UpdateError::InsertError(e) => e.field(),
      _ => None,
    }
  }
}
