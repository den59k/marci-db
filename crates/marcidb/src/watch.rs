//! Watches: a signal that the result of a read may have changed. Ephemeral and empty — the subscriber
//! re-reads; nothing is persisted, nothing survives a reopen.
//!
//! A subscriber (a socket of the HTTP host, an embedded handle) is a [`Watcher`] with read ops under ids
//! of its own (`MarciDB::watch`). Each written row is checked IN its transaction, from the places every
//! insert, update and delete goes through (cascades included), against its model's watches: an insert by
//! the new body, a delete by the old, an update by both, against the op's `$where`. A model the op reaches
//! through a relation (an include, a relation condition) fires it on any write. `commit` hands the hits to
//! their watchers and wakes them; a rollback drops them. A model nobody watches pays nothing.
//!
//! The check over-approximates — a false hit is one re-read: `$order` / `$limit` / `$cursor` are ignored,
//! a relation condition "may match". A schema change fires every watch; a closed database (restored over,
//! dropped) detaches its watchers. A parsed `Where` borrows the schema, so a watch keeps its op as JSON and
//! parses its `$where` per written row, until it is hit in that transaction.

use std::{collections::HashMap, sync::{Arc, Mutex, RwLock, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}}};

use canopydb::Transaction;
use serde_json::Value;

use crate::{MarciDB, OpError, aggregate_op::AggregateOp, json_parsers::{parse_aggregate, parse_query, parse_where}, query_op::{DecodeCtx, FieldCompare, FieldCompareRef, IncludeQuery, QueryOp, TransationContext, Where, process_where}, schema::{Entity, Schema}};

/// The actions a watch takes: the reads.
const WATCHABLE: [&str; 4] = ["findMany", "findFirst", "count", "aggregate"];

/// One subscriber: the ids of its watches whose results may have changed since it last looked.
pub struct Watcher {
  state: Mutex<WatcherState>,
  /// Called after a commit that changed something for it (on the committing thread — keep it short).
  wake: Box<dyn Fn() + Send + Sync>,
}

#[derive(Default)]
struct WatcherState {
  changed: Vec<u64>,
  detached: bool,
}

impl Watcher {
  pub fn new(wake: impl Fn() + Send + Sync + 'static) -> Arc<Watcher> {
    Arc::new(Watcher { state: Mutex::default(), wake: Box::new(wake) })
  }

  /// The ids changed since the last call, each once, in the order they were first hit.
  pub fn take_changed(&self) -> Vec<u64> {
    std::mem::take(&mut self.state.lock().unwrap().changed)
  }

  /// The database it watched was closed (restored over, dropped): its watches will not fire again.
  pub fn is_detached(&self) -> bool {
    self.state.lock().unwrap().detached
  }

  fn push(&self, id: u64) {
    let mut state = self.state.lock().unwrap();
    if !state.changed.contains(&id) { state.changed.push(id); }
  }
}

pub(crate) struct Watch {
  id: u64,
  watcher: Arc<Watcher>,
  /// `{ model, action, query }` as registered: re-read after a schema change.
  op: Value,
  model: String,
  /// The op's `$where`; none = every row of the model is in it.
  filter: Option<Value>,
  /// Models the op reads through relations: any write of them fires it.
  related: Vec<String>,
  /// The op does not parse on the current schema: it fires again only at the next schema change.
  broken: bool,
  /// Hit in the open write transaction (there is one at a time).
  hit: AtomicBool,
}

/// A watch as one model's writes see it: `rows` — check the row against the filter; otherwise any write
/// of the model fires it.
struct Check {
  watch: Arc<Watch>,
  rows: bool,
}

#[derive(Default)]
struct Registry {
  all: Vec<Arc<Watch>>,
  by_model: HashMap<String, Arc<Vec<Check>>>,
}

impl Registry {
  fn rebuild(&mut self) -> usize {
    let mut by_model: HashMap<String, Vec<Check>> = HashMap::new();
    let mut live = 0;
    for watch in self.all.iter().filter(|w| !w.broken) {
      live += 1;
      let coarse = watch.related.contains(&watch.model);
      by_model.entry(watch.model.clone()).or_default().push(Check { watch: watch.clone(), rows: !coarse });
      for model in watch.related.iter().filter(|m| **m != watch.model) {
        by_model.entry(model.clone()).or_default().push(Check { watch: watch.clone(), rows: false });
      }
    }
    self.by_model = by_model.into_iter().map(|(model, checks)| (model, Arc::new(checks))).collect();
    live
  }
}

/// The watches of one database, as the write path sees them.
#[derive(Default)]
pub(crate) struct Watches {
  registry: RwLock<Registry>,
  /// How many watches are live: the write path of a database with none reads this and nothing else.
  count: AtomicUsize,
  /// Hit by the open write transaction.
  hits: Mutex<Vec<Arc<Watch>>>,
  /// Rows checked against a filter, ever (diagnostics: `MarciDB::watch_checks_dev`).
  checks: AtomicU64,
}

/// What an op reads: its model, its `$where`, the models behind its relations.
pub(crate) struct Reads {
  model: String,
  filter: Option<Value>,
  related: Vec<String>,
}

impl Watches {
  fn of(&self, model: &str) -> Option<Arc<Vec<Check>>> {
    if self.count.load(Ordering::Relaxed) == 0 {
      return None;
    }
    self.registry.read().unwrap().by_model.get(model).cloned()
  }

  /// Whether a delete of this model's row must read the body (a watch checks its rows).
  pub(crate) fn reads_rows(&self, model: &str) -> bool {
    self.of(model).is_some_and(|checks| checks.iter().any(|c| c.rows && c.watch.filter.is_some()))
  }

  /// A row of `entity` was written in the open transaction: `old` is its body before (none for an
  /// insert), `new` after (none for a delete, or when it did not change). Neither: the watches fire unchecked.
  pub(crate) fn written<'a>(&self, tx: &'a Transaction, schema: &'a Schema, entity: &'a Entity, id: &[u8], old: Option<&[u8]>, new: Option<&[u8]>) {
    let Some(checks) = self.of(&entity.name) else { return };
    // Only for the schema: a relation condition, the one part that would read other trees, is not evaluated.
    let mut ctx: TransationContext<(), _> = TransationContext::new(tx, schema, |_: DecodeCtx<()>| ());
    for check in checks.iter() {
      let watch = &check.watch;
      if watch.hit.load(Ordering::Relaxed) { continue }
      let hit = match (&watch.filter, check.rows) {
        (Some(filter), true) if old.is_some() || new.is_some() => {
          self.checks.fetch_add(1, Ordering::Relaxed);
          // A filter that no longer parses: the schema change that broke it has fired it already.
          parse_where(schema, entity, filter).is_ok_and(|w| {
            [old, new].into_iter().flatten().any(|body| may_match(id, body, &mut ctx, entity, &w, true))
          })
        }
        _ => true,
      };
      if hit { self.hit(watch) }
    }
  }

  /// Rows of `entity` were written without their bodies at hand (a range delete): every watch of it fires.
  pub(crate) fn touched(&self, entity: &Entity) {
    let Some(checks) = self.of(&entity.name) else { return };
    for check in checks.iter() {
      if !check.watch.hit.load(Ordering::Relaxed) { self.hit(&check.watch) }
    }
  }

  fn hit(&self, watch: &Arc<Watch>) {
    if !watch.hit.swap(true, Ordering::Relaxed) {
      self.hits.lock().unwrap().push(watch.clone());
    }
  }

  /// Drops the hits of a transaction that did not commit. Called when a write transaction begins and on
  /// an explicit rollback — both while the write lock is held.
  pub(crate) fn discard(&self) {
    drop(self.take_hits());
  }

  /// The open transaction's hits, taken BEFORE it commits: once the write lock is released, the next
  /// transaction starts from none.
  pub(crate) fn take_hits(&self) -> Vec<Arc<Watch>> {
    let hits = std::mem::take(&mut *self.hits.lock().unwrap());
    for watch in hits.iter() { watch.hit.store(false, Ordering::Relaxed); }
    hits
  }

  /// After the commit: each hit id goes to its watcher, and each watcher concerned is woken once.
  pub(crate) fn publish(&self, hits: Vec<Arc<Watch>>) {
    let mut woken: Vec<Arc<Watcher>> = Vec::new();
    for watch in hits {
      watch.watcher.push(watch.id);
      if !woken.iter().any(|w| Arc::ptr_eq(w, &watch.watcher)) { woken.push(watch.watcher.clone()); }
    }
    for watcher in woken { (watcher.wake)() }
  }

  /// Fires, outside any transaction, every watch `which` picks.
  fn fire(&self, which: impl Fn(&Watch) -> bool) {
    let picked: Vec<Arc<Watch>> = self.registry.read().unwrap().all.iter().filter(|w| which(w)).cloned().collect();
    self.publish(picked);
  }

  /// The schema changed (a migration, `$sync`): every op is read again on it — one that no longer
  /// parses waits for the next change — and every watch fires.
  pub(crate) fn schema_changed(&self, reads: impl Fn(&Value) -> Result<Reads, OpError>) {
    let mut registry = self.registry.write().unwrap();
    registry.all = registry.all.iter().map(|w| Arc::new(Watch::new(w.id, w.watcher.clone(), w.op.clone(), reads(&w.op)))).collect();
    self.count.store(registry.rebuild(), Ordering::Relaxed);
    drop(registry);
    self.fire(|_| true);
  }

  /// Fires the watches of one model (its `@custom` indexes were rebuilt: a search may answer otherwise).
  pub(crate) fn fire_model(&self, model: &str) {
    self.fire(|w| w.model == model);
  }

  fn change(&self, f: impl FnOnce(&mut Vec<Arc<Watch>>)) {
    let mut registry = self.registry.write().unwrap();
    f(&mut registry.all);
    self.count.store(registry.rebuild(), Ordering::Relaxed);
  }
}

impl Drop for Watches {
  /// The database is closed: nothing will fire any more — its watchers are told so.
  fn drop(&mut self) {
    let registry = self.registry.get_mut().unwrap_or_else(|e| e.into_inner());
    let mut told: Vec<Arc<Watcher>> = Vec::new();
    for watch in registry.all.iter() {
      if told.iter().any(|w| Arc::ptr_eq(w, &watch.watcher)) { continue }
      watch.watcher.state.lock().unwrap_or_else(|e| e.into_inner()).detached = true;
      told.push(watch.watcher.clone());
    }
    for watcher in told { (watcher.wake)() }
  }
}

impl Watch {
  fn new(id: u64, watcher: Arc<Watcher>, op: Value, reads: Result<Reads, OpError>) -> Watch {
    let (model, filter, related, broken) = match reads {
      Ok(r) => (r.model, r.filter, r.related, false),
      Err(_) => (String::new(), None, vec![], true),
    };
    Watch { id, watcher, op, model, filter, related, broken, hit: AtomicBool::new(false) }
  }
}

/// Whether the row may be in `w` — `upper`, or surely is — `!upper`: a relation condition reads other rows,
/// so it counts as true for the first and false for the second. `$not` swaps them.
fn may_match<'a, U, F>(id: &[u8], body: &[u8], ctx: &mut TransationContext<'a, U, F>, entity: &Entity, w: &Where<'a>, upper: bool) -> bool {
  match w {
    Where::And(items) => items.iter().all(|i| may_match(id, body, ctx, entity, i, upper)),
    Where::Or(items) => items.iter().any(|i| may_match(id, body, ctx, entity, i, upper)),
    Where::Not(inner) => !may_match(id, body, ctx, entity, inner, !upper),
    Where::Field(_, FieldCompare::Ref(..)) => upper,
    other => process_where(id, body, ctx, entity, other).unwrap_or(upper),
  }
}

/// The models a condition reads through relations.
fn where_models(w: &Where, out: &mut Vec<String>) {
  match w {
    Where::And(items) | Where::Or(items) => items.iter().for_each(|i| where_models(i, out)),
    Where::Not(inner) => where_models(inner, out),
    Where::Field(_, FieldCompare::Ref(entity, _, compare)) => {
      out.push(entity.name.clone());
      if let FieldCompareRef::Every(w) | FieldCompareRef::Some(w) | FieldCompareRef::None(w) | FieldCompareRef::Eq(w) | FieldCompareRef::Ne(w) = compare {
        where_models(w, out);
      }
    }
    _ => {}
  }
}

/// The models a query reads besides its own rows: its relation conditions and includes, recursively.
fn query_models(q: &QueryOp, out: &mut Vec<String>) {
  if let Some(filter) = &q.filter { where_models(filter, out) }
  for include in q.includes.iter() {
    match &include.query {
      IncludeQuery::Query(inner) => {
        out.push(inner.entity.name.clone());
        query_models(inner, out);
      }
      IncludeQuery::Aggregate(agg) => aggregate_models(agg, out),
    }
  }
}

fn aggregate_models(agg: &AggregateOp, out: &mut Vec<String>) {
  out.push(agg.entity.name.clone());
  if let Some(filter) = &agg.filter { where_models(filter, out) }
}

impl MarciDB {
  /// What the read `op` (`{ model, action, query }`) reads, on the current schema — the parse its action
  /// would run. A write, or an op that does not parse, is refused.
  fn watch_reads(&self, op: &Value) -> Result<Reads, OpError> {
    let obj = op.as_object().ok_or(OpError::NotAnObject)?;
    let model = obj.get("model").and_then(Value::as_str).ok_or(OpError::MissingField("model"))?;
    let action = obj.get("action").and_then(Value::as_str).ok_or(OpError::MissingField("action"))?;
    if !WATCHABLE.contains(&action) {
      return Err(OpError::Parse(format!("'{}' is not a read — only findMany, findFirst, count and aggregate can be watched", action)));
    }
    let entity = self.get_model(model).ok_or_else(|| OpError::UnknownModel(model.to_string()))?;
    let query = obj.get("query").ok_or(OpError::MissingField("query"))?;

    let mut related = Vec::new();
    if action == "findMany" || action == "findFirst" {
      query_models(&parse_query(&self.schema, entity, query).map_err(|e| OpError::Parse(e.to_string()))?, &mut related);
    } else {
      let agg = parse_aggregate(&self.schema, entity, query).map_err(|e| OpError::Parse(e.to_string()))?;
      if let Some(filter) = &agg.filter { where_models(filter, &mut related) }
    }
    related.sort();
    related.dedup();
    Ok(Reads { model: entity.name.clone(), filter: query.get("$where").cloned(), related })
  }

  /// Registers the read `op` (`{ model, action, query }`) as `watcher`'s watch `id`, replacing one it had
  /// under that id: from now on every commit that may change the op's result puts `id` into the watcher's
  /// changed set and wakes it. Only reads can be watched (`findMany`, `findFirst`, `count`, `aggregate`).
  pub fn watch(&self, watcher: &Arc<Watcher>, id: u64, op: &Value) -> Result<(), OpError> {
    let reads = self.watch_reads(op)?;
    // Registered while the write lock is held: no transaction is half-way through its rows, unchecked.
    let lock = self.raw_begin_write().map_err(OpError::Storage)?;
    let watch = Arc::new(Watch::new(id, watcher.clone(), op.clone(), Ok(reads)));
    self.watches.change(|all| {
      all.retain(|w| !(w.id == id && Arc::ptr_eq(&w.watcher, watcher)));
      all.push(watch);
    });
    drop(lock);
    Ok(())
  }

  /// Removes `watcher`'s watch `id`; returns whether it had one.
  pub fn unwatch(&self, watcher: &Arc<Watcher>, id: u64) -> bool {
    let mut found = false;
    self.watches.change(|all| {
      let before = all.len();
      all.retain(|w| !(w.id == id && Arc::ptr_eq(&w.watcher, watcher)));
      found = all.len() != before;
    });
    found
  }

  /// Removes every watch of `watcher` (its socket closed).
  pub fn unwatch_all(&self, watcher: &Arc<Watcher>) {
    self.watches.change(|all| all.retain(|w| !Arc::ptr_eq(&w.watcher, watcher)));
  }

  /// Test/diagnostic helper: how many times a written row was checked against a watch's filter.
  pub fn watch_checks_dev(&self) -> u64 {
    self.watches.checks.load(Ordering::Relaxed)
  }

  /// Called by `commit_schema` once the new schema is in place.
  pub(crate) fn watches_schema_changed(&self) {
    self.watches.schema_changed(|op| self.watch_reads(op));
  }
}
