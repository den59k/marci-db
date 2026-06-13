use canopydb::WriteTransaction;

use crate::{MarciDB, aggregate_op::{AggregateOp, AggregateResult, process_aggregate}, delete_op::{DeleteError, prepare_delete, process_delete}, query_op::{DecodeCtx, QueryOp, TransationContext, process_query_many, process_query_one}, schema::Entity, update_op::{UpdateError, UpdateOp, process_update}, write_op::{InsertError, WriteOp, process_write}};

/// Открытая write-транзакция уровня API.
///
/// Оборачивает одну canopydb write-транзакцию: все insert/update/delete и запросы
/// внутри `MarciTransaction` применяются атомарно. Изменения становятся видимы
/// другим читателям только после [`MarciTransaction::commit`]. При `drop` без коммита
/// (в том числе при выходе по ошибке или панике) транзакция откатывается.
///
/// Методы запросов (`find_many` / `find_first` / `aggregate` / `count`) читают
/// собственные незакоммиченные изменения транзакции (read-your-writes).
///
/// Пока транзакция открыта, она держит эксклюзивную write-блокировку БД — другие
/// писатели (включая короткоживущие методы [`MarciDB`]) ждут её завершения, поэтому
/// транзакции стоит держать недолго и не вызывать write-методы самого `MarciDB` внутри.
pub struct MarciTransaction<'db> {
  db: &'db MarciDB,
  tx: WriteTransaction,
}

impl<'db> MarciTransaction<'db> {
  pub(crate) fn new(db: &'db MarciDB, tx: WriteTransaction) -> Self {
    Self { db, tx }
  }

  pub fn insert_item(&self, entity: &Entity, insert: &WriteOp) -> Result<Vec<u8>, InsertError> {
    process_write(&self.tx, entity, insert, self.db, None)
  }

  pub fn update_item(&self, entity: &Entity, id: &[u8], update_op: &UpdateOp) -> Result<bool, UpdateError> {
    process_update(&self.tx, entity, id, update_op, self.db)
  }

  pub fn delete_item(&self, entity: &Entity, id: &[u8]) -> Result<bool, DeleteError> {
    let action = prepare_delete(&self.db.schema, entity, Some(id), None);
    process_delete(&self.tx, id, entity, &action, &self.db.schema, None)
  }

  pub fn find_many<U, F>(&self, query: &QueryOp, f: F) -> Vec<U> where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    let mut ctx = TransationContext::new(&self.tx, &self.db.schema, f);
    return process_query_many(query, &mut ctx, None);
  }

  pub fn find_first<U, F>(&self, query: &QueryOp, f: F) -> Option<U> where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    let mut ctx = TransationContext::new(&self.tx, &self.db.schema, f);
    return process_query_one(query, &mut ctx, None);
  }

  pub fn aggregate(&self, op: &AggregateOp) -> AggregateResult {
    // Декод строк агрегациям не нужен — колбэк-заглушка нужна только для типа контекста
    let mut ctx: TransationContext<(), _> = TransationContext::new(&self.tx, &self.db.schema, |_: DecodeCtx<()>| ());
    return process_aggregate(op, &mut ctx, None);
  }

  pub fn count(&self, entity: &Entity) -> u64 {
    let tree = self.tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
    return tree.len();
  }

  /// Фиксирует все изменения транзакции. После коммита они видны новым читателям
  pub fn commit(self) -> Result<(), canopydb::Error> {
    self.tx.commit()?;
    Ok(())
  }

  /// Явно откатывает транзакцию. Эквивалентно простому `drop`, но возвращает ошибку,
  /// если откат не удался
  pub fn rollback(self) -> Result<(), canopydb::Error> {
    self.tx.rollback()
  }
}
