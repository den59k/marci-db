use canopydb::WriteTransaction;

use crate::{MarciDB, StorageError, aggregate_op::{AggregateOp, AggregateResult, process_aggregate}, delete_op::{DeleteError, prepare_delete, process_delete}, error::RequireTree, query_op::{DecodeCtx, QueryOp, TransationContext, process_query_many, process_query_one}, schema::Entity, update_op::{UpdateError, UpdateOp, process_update}, write_op::{InsertError, WriteOp, process_write}};

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

  /// Applies `update` to every row matching `query`'s filter; returns how many rows matched.
  ///
  /// The count is rows *matched*, not rows whose stored bytes actually changed — re-applying a value a
  /// row already holds still counts, matching what SQL `UPDATE` reports.
  ///
  /// Matching ids are resolved up front and only then updated, so the filter observes the pre-update
  /// state: a row that stops matching part-way through is still updated, and one that starts matching
  /// is not. Everything happens in this transaction, so an error part-way leaves nothing committed.
  pub fn update_many(&self, entity: &Entity, query: &QueryOp, update: &UpdateOp) -> Result<u64, UpdateError> {
    // `find_many` on a transaction ignores `query.search` (unlike `MarciDB::find_many`), and
    // `split_search` has already lifted the search clause out of `filter` — so a `$near`/`$search`
    // here would silently degrade into an unfiltered scan and update every row in the model.
    if query.search.is_some() {
      return Err(UpdateError::Unsupported("$near/$search is not supported in updateMany"));
    }
    // Relation ops recurse into related rows, so the work is unbounded in the number of matches.
    if !update.update_refs.is_empty() {
      return Err(UpdateError::Unsupported("relation operations are not supported in updateMany"));
    }
    // A bounded update without a total order would hit an arbitrary subset of the matches.
    if query.limit.is_some() || query.skip.is_some() || query.cursor.is_some() {
      return Err(UpdateError::Unsupported("$limit/$skip/$cursor are not supported in updateMany"));
    }

    // The scope here is load-bearing: the context keeps every tree the scan touched open, canopydb
    // permits only one live handle per tree per write transaction, and `process_update` reopens the
    // model tree (plus the index trees of any field it changes). The context must drop first.
    let ids: Vec<Vec<u8>> = {
      let mut ctx = TransationContext::new(&self.tx, &self.db.schema, |c: DecodeCtx<Vec<u8>>| c.id.to_vec());
      process_query_many(query, &mut ctx, None)?
    };

    let mut updated = 0;
    for id in ids.iter() {
      if process_update(&self.tx, entity, id, update, self.db)? {
        updated += 1;
      }
    }
    Ok(updated)
  }

  pub fn delete_item(&self, entity: &Entity, id: &[u8]) -> Result<bool, DeleteError> {
    let action = prepare_delete(&self.db.schema, entity, Some(id), None);
    process_delete(&self.tx, id, entity, &action, &self.db.schema, &self.db.providers, None)
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
    let tree = self.tx.require_tree(entity.name.as_bytes())?;
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
