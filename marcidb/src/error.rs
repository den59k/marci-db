use std::fmt;

use canopydb::{Transaction, Tree};

/// Storage-layer error.
///
/// - `Backend` — an error from the underlying KV store (`canopydb`): I/O, DB unavailability, etc.
/// - `MissingTree` — an expected tree (entity/index) was absent. This is a structural invariant
///   (schema ⟺ storage) rather than a transient fault; it surfaces only on corruption or a desync, and is
///   propagated rather than panicked so a single request fails cleanly instead of taking the thread down.
///
/// The wrapper is needed because `canopydb::Error` doesn't implement `PartialEq`/`Clone`
/// (it contains `io::Error` inside), while domain errors (`InsertError` and others) are compared in tests.
/// Two `Backend` errors are considered equal if their variant matches (ignoring the payload).
#[derive(Debug)]
pub enum StorageError {
  Backend(canopydb::Error),
  MissingTree(String),
}

impl PartialEq for StorageError {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (StorageError::Backend(a), StorageError::Backend(b)) => std::mem::discriminant(a) == std::mem::discriminant(b),
      (StorageError::MissingTree(a), StorageError::MissingTree(b)) => a == b,
      _ => false,
    }
  }
}

impl fmt::Display for StorageError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      StorageError::Backend(e) => write!(f, "storage error: {}", e),
      StorageError::MissingTree(name) => write!(f, "storage error: tree '{}' is missing (database may be corrupt)", name),
    }
  }
}

impl std::error::Error for StorageError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      StorageError::Backend(e) => Some(e),
      StorageError::MissingTree(_) => None,
    }
  }
}

impl From<canopydb::Error> for StorageError {
  fn from(e: canopydb::Error) -> Self {
    StorageError::Backend(e)
  }
}

/// Opens a tree that is expected to exist, turning the "not found" case into [`StorageError::MissingTree`]
/// instead of an `Option::unwrap` panic. Implemented on `Transaction`; `WriteTransaction` reaches it through
/// `Deref`, so it works for both read and write transactions.
pub(crate) trait RequireTree {
  fn require_tree(&self, name: &[u8]) -> Result<Tree<'_>, StorageError>;
}

impl RequireTree for Transaction {
  fn require_tree(&self, name: &[u8]) -> Result<Tree<'_>, StorageError> {
    self.get_tree(name)?
      .ok_or_else(|| StorageError::MissingTree(String::from_utf8_lossy(name).into_owned()))
  }
}
