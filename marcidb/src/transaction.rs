use canopydb::WriteTransaction;

use crate::{MarciDB, StorageError, aggregate_op::{AggregateOp, AggregateResult, process_aggregate}, delete_op::{DeleteError, prepare_delete, process_delete}, query_op::{DecodeCtx, QueryOp, TransationContext, process_query_many, process_query_one}, schema::Entity, update_op::{UpdateError, UpdateOp, process_update}, write_op::{InsertError, WriteOp, process_write}};

/// An open API-level write transaction.
///
/// Wraps a single canopydb write transaction: all insert/update/delete and queries
/// within `MarciTransaction` are applied atomically. Changes become visible
/// to other readers only after [`MarciTransaction::commit`]. On `drop` without a commit
/// (including on exit via an error or a panic) the transaction is rolled back.
///
/// Query methods (`find_many` / `find_first` / `aggregate` / `count`) read
/// the transaction's own uncommitted changes (read-your-writes).
///
/// While the transaction is open, it holds an exclusive write lock on the DB — other
/// writers (including the short-lived methods of [`MarciDB`]) wait for it to finish, so
/// transactions should be kept short and the write methods of `MarciDB` itself should not be called inside.
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

  pub fn find_many<U, F>(&self, query: &QueryOp, f: F) -> Result<Vec<U>, StorageError> where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    let mut ctx = TransationContext::new(&self.tx, &self.db.schema, f);
    return process_query_many(query, &mut ctx, None);
  }

  pub fn find_first<U, F>(&self, query: &QueryOp, f: F) -> Result<Option<U>, StorageError> where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    let mut ctx = TransationContext::new(&self.tx, &self.db.schema, f);
    return process_query_one(query, &mut ctx, None);
  }

  pub fn aggregate(&self, op: &AggregateOp) -> Result<AggregateResult, StorageError> {
    // Aggregations don't need row decoding — the callback stub is only needed for the context type
    let mut ctx: TransationContext<(), _> = TransationContext::new(&self.tx, &self.db.schema, |_: DecodeCtx<()>| ());
    return process_aggregate(op, &mut ctx, None);
  }

  pub fn count(&self, entity: &Entity) -> Result<u64, StorageError> {
    let tree = self.tx.get_tree(entity.name.as_bytes())?.unwrap();
    return Ok(tree.len());
  }

  /// Commits all the transaction's changes. After the commit they are visible to new readers
  pub fn commit(self) -> Result<(), StorageError> {
    self.tx.commit()?;
    Ok(())
  }

  /// Explicitly rolls back the transaction. Equivalent to a plain `drop`, but returns an error
  /// if the rollback failed
  pub fn rollback(self) -> Result<(), StorageError> {
    self.tx.rollback()?;
    Ok(())
  }
}
