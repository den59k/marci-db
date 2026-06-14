use std::fmt;

/// Storage layer error (canopydb): I/O, DB unavailability, etc.
///
/// The wrapper is needed because `canopydb::Error` doesn't implement `PartialEq`/`Clone`
/// (it contains `io::Error` inside), while domain errors (`InsertError` and others) are compared in tests.
/// Two storage errors are considered equal if their variant matches (ignoring the payload).
#[derive(Debug)]
pub struct StorageError(pub canopydb::Error);

impl PartialEq for StorageError {
  fn eq(&self, other: &Self) -> bool {
    std::mem::discriminant(&self.0) == std::mem::discriminant(&other.0)
  }
}

impl fmt::Display for StorageError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "storage error: {}", self.0)
  }
}

impl std::error::Error for StorageError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    Some(&self.0)
  }
}

impl From<canopydb::Error> for StorageError {
  fn from(e: canopydb::Error) -> Self {
    StorageError(e)
  }
}
