use std::fmt;

/// Ошибка слоя хранения (canopydb): ввод-вывод, недоступность БД и т.п.
///
/// Обёртка нужна, потому что `canopydb::Error` не реализует `PartialEq`/`Clone`
/// (внутри `io::Error`), а доменные ошибки (`InsertError` и пр.) сравниваются в тестах.
/// Две storage-ошибки считаются равными, если совпадает их вариант (без учёта payload).
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
