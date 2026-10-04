//! Journals: named, durable logs of the changes of ONE model.
//!
//! A journal exists because a reader asked for it (`MarciDB::journal_open`); a model nobody journals
//! pays nothing on its write path. An entry is written in the SAME transaction as the change it
//! describes, from the one place every delete goes through (`process_delete`) — so a row removed by a
//! cascade is journaled exactly like one removed by name. Entries stay until the reader confirms them:
//! a read carries `after = <seq>` ("everything up to here is handled"), which drops those entries and
//! returns the next ones. A reader that died before confirming gets the same entries again.
//!
//! Storage: `__marci_journals__` holds one record per journal (`<model>\0<name>` → ops + the confirmed
//! seq); `__journal__/<model>/<name>` holds its entries (`seq` u64 BE → op byte + the row as JSON
//! text). The row is stored DECODED: an entry must stay readable after any later migration.
//!
//! Only `delete` is journaled so far; the stored ops mask and the op byte leave room for the rest.

use std::{collections::HashMap, sync::{Arc, Condvar, Mutex, RwLock, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}}, time::Duration};

use canopydb::{Transaction, WriteTransaction};

use crate::{MarciDB, StorageError, json_parsers::decode_document, query_op::{DecodeCtx, QueryOp}, schema::{Entity, Schema}};

pub const JOURNALS_TREE: &[u8] = b"__marci_journals__";

const OP_DELETE: u8 = 1;

#[derive(Debug, PartialEq)]
pub enum JournalError {
  /// A journal name is 1–64 characters of `A-Z a-z 0-9 _ -`.
  InvalidName(String),
  /// `on` is empty or names an operation that is not journaled (only `delete` is).
  UnsupportedOp(String),
  /// The journal exists with other operations — a journal is never redefined in place.
  Conflict { name: String },
  NotFound { name: String },
  Storage(StorageError),
}

impl std::fmt::Display for JournalError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      JournalError::InvalidName(name) => write!(f, "invalid journal name '{}': 1-64 characters of A-Z a-z 0-9 _ -", name),
      JournalError::UnsupportedOp(op) if op.is_empty() => write!(f, "a journal needs 'on': the operations it records (only 'delete' is supported)"),
      JournalError::UnsupportedOp(op) => write!(f, "a journal cannot record '{}' (only 'delete' is supported)", op),
      JournalError::Conflict { name } => write!(f, "journal '{}' already exists with other operations — drop it first", name),
      JournalError::NotFound { name } => write!(f, "journal '{}' does not exist", name),
      JournalError::Storage(e) => write!(f, "{:?}", e),
    }
  }
}
impl std::error::Error for JournalError {}

impl From<canopydb::Error> for JournalError { fn from(e: canopydb::Error) -> Self { JournalError::Storage(StorageError::Backend(e)) } }
impl From<StorageError> for JournalError { fn from(e: StorageError) -> Self { JournalError::Storage(e) } }

/// Wakes the readers that wait for new entries. One per database: a reader takes [`generation`] BEFORE
/// it reads, and when the read came back empty waits for the generation to move — an entry committed
/// between the two is never missed.
///
/// [`generation`]: JournalSignal::generation
pub struct JournalSignal {
  generation: Mutex<u64>,
  changed: Condvar,
}

impl JournalSignal {
  pub fn generation(&self) -> u64 {
    *self.generation.lock().unwrap()
  }

  /// Blocks until the generation is no longer `seen` or `timeout` passes; returns the current one.
  pub fn wait(&self, seen: u64, timeout: Duration) -> u64 {
    let guard = self.generation.lock().unwrap();
    let (guard, _) = self.changed.wait_timeout_while(guard, timeout, |g| *g == seen).unwrap();
    *guard
  }

  fn notify(&self) {
    *self.generation.lock().unwrap() += 1;
    self.changed.notify_all();
  }
}

struct Journal {
  name: String,
  ops: u8,
  tree_name: Vec<u8>,
  next_seq: AtomicU64,
}

/// The journals of one database, as the write path sees them.
pub(crate) struct Journals {
  by_model: RwLock<HashMap<String, Arc<Vec<Arc<Journal>>>>>,
  /// How many journals exist: the write path of a database with none reads this and nothing else.
  count: AtomicUsize,
  /// An entry was written in the open write transaction (there is one at a time).
  dirty: AtomicBool,
  signal: Arc<JournalSignal>,
}

fn config_key(model: &str, name: &str) -> Vec<u8> {
  [model.as_bytes(), &[0], name.as_bytes()].concat()
}

fn entries_tree_name(model: &str, name: &str) -> Vec<u8> {
  format!("__journal__/{}/{}", model, name).into_bytes()
}

fn encode_config(ops: u8, acked: u64) -> Vec<u8> {
  let mut value = vec![ops];
  value.extend_from_slice(&acked.to_be_bytes());
  value
}

fn decode_config(value: &[u8]) -> (u8, u64) {
  (value[0], u64::from_be_bytes(value[1..9].try_into().expect("a journal record is 9 bytes")))
}

fn parse_ops(ops: &[String]) -> Result<u8, JournalError> {
  let mut mask = 0;
  for op in ops {
    match op.as_str() {
      "delete" => mask |= OP_DELETE,
      other => return Err(JournalError::UnsupportedOp(other.to_string())),
    }
  }
  if mask == 0 {
    return Err(JournalError::UnsupportedOp(String::new()));
  }
  Ok(mask)
}

fn check_name(name: &str) -> Result<(), JournalError> {
  let ok = !name.is_empty() && name.len() <= 64 && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
  if ok { Ok(()) } else { Err(JournalError::InvalidName(name.to_string())) }
}

impl Journals {
  /// Rebuilds the registry from what the database holds (called when it is opened).
  pub(crate) fn load(rx: &Transaction) -> Result<Journals, StorageError> {
    let mut by_model: HashMap<String, Vec<Arc<Journal>>> = HashMap::new();
    let mut count = 0;

    if let Some(config) = rx.get_tree(JOURNALS_TREE)? {
      for entry in config.iter()? {
        let (key, value) = entry?;
        let split = key.iter().position(|&b| b == 0).expect("a journal key is <model>\\0<name>");
        let model = String::from_utf8_lossy(&key[..split]).into_owned();
        let name = String::from_utf8_lossy(&key[split + 1..]).into_owned();
        let (ops, acked) = decode_config(&value);

        // The next seq continues after everything ever handed out: the confirmed seq when the reader
        // emptied the journal, the last stored entry otherwise.
        let tree_name = entries_tree_name(&model, &name);
        let last = match rx.get_tree(&tree_name)? {
          Some(tree) => tree.last()?.map(|(key, _)| u64::from_be_bytes(key.as_ref().try_into().expect("a journal seq is 8 bytes"))),
          None => None,
        };
        let next_seq = last.unwrap_or(0).max(acked) + 1;

        by_model.entry(model).or_default().push(Arc::new(Journal { name, ops, tree_name, next_seq: AtomicU64::new(next_seq) }));
        count += 1;
      }
    }

    Ok(Journals {
      by_model: RwLock::new(by_model.into_iter().map(|(model, list)| (model, Arc::new(list))).collect()),
      count: AtomicUsize::new(count),
      dirty: AtomicBool::new(false),
      signal: Arc::new(JournalSignal { generation: Mutex::new(0), changed: Condvar::new() }),
    })
  }

  fn of(&self, model: &str) -> Option<Arc<Vec<Arc<Journal>>>> {
    if self.count.load(Ordering::Relaxed) == 0 {
      return None;
    }
    self.by_model.read().unwrap().get(model).cloned()
  }

  fn find(&self, model: &str, name: &str) -> Option<Arc<Journal>> {
    self.of(model)?.iter().find(|j| j.name == name).cloned()
  }

  /// Whether a delete of this model's row is journaled — the delete path then reads the row's body
  /// and deletes row by row where it would otherwise drop a key range.
  pub(crate) fn records_delete(&self, model: &str) -> bool {
    self.of(model).is_some_and(|list| list.iter().any(|j| j.ops & OP_DELETE != 0))
  }

  /// Writes the deleted row into every journal of its model, in the transaction that deletes it.
  pub(crate) fn record_delete(&self, tx: &WriteTransaction, entity: &Entity, schema: &Schema, id: &[u8], body: &[u8]) -> Result<(), String> {
    let Some(list) = self.of(&entity.name) else { return Ok(()) };

    // The row as a query without a selection returns it: the id and every scalar field.
    let mask = QueryOp::all(entity).mask;
    let row = decode_document(DecodeCtx { id, data: body, entity, mask: &mask, includes: vec![], schema })
      .map_err(|e| format!("journal: the deleted {} row could not be decoded: {:?}", entity.name, e))?;

    let mut value = Vec::with_capacity(row.len() + 1);
    value.push(OP_DELETE);
    value.extend_from_slice(row.as_bytes());

    for journal in list.iter().filter(|j| j.ops & OP_DELETE != 0) {
      let seq = journal.next_seq.fetch_add(1, Ordering::Relaxed);
      let mut tree = tx.get_or_create_tree(&journal.tree_name).map_err(|e| e.to_string())?;
      tree.insert(&seq.to_be_bytes(), &value).map_err(|e| e.to_string())?;
    }
    self.dirty.store(true, Ordering::Relaxed);
    Ok(())
  }

  /// Called after a commit: wakes the waiting readers when the transaction wrote an entry.
  pub(crate) fn committed(&self) {
    if self.dirty.swap(false, Ordering::Relaxed) {
      self.signal.notify();
    }
  }

  /// A dropped model takes its journals with it. Runs inside the migration's transaction;
  /// [`Journals::forget_model`] follows the commit.
  pub(crate) fn drop_model_trees(&self, tx: &WriteTransaction, model: &str) -> Result<(), StorageError> {
    let Some(list) = self.of(model) else { return Ok(()) };
    let mut config = tx.get_or_create_tree(JOURNALS_TREE)?;
    for journal in list.iter() {
      config.delete(&config_key(model, &journal.name))?;
      tx.delete_tree(&journal.tree_name)?;
    }
    Ok(())
  }

  pub(crate) fn forget_model(&self, model: &str) {
    if let Some(list) = self.by_model.write().unwrap().remove(model) {
      self.count.fetch_sub(list.len(), Ordering::Relaxed);
    }
  }

  fn add(&self, model: &str, journal: Arc<Journal>) {
    let mut by_model = self.by_model.write().unwrap();
    let mut list = by_model.get(model).map(|l| l.as_ref().clone()).unwrap_or_default();
    list.push(journal);
    by_model.insert(model.to_string(), Arc::new(list));
    self.count.fetch_add(1, Ordering::Relaxed);
  }

  fn remove(&self, model: &str, name: &str) {
    let mut by_model = self.by_model.write().unwrap();
    let Some(list) = by_model.get(model) else { return };
    let rest: Vec<Arc<Journal>> = list.iter().filter(|j| j.name != name).cloned().collect();
    self.count.fetch_sub(list.len() - rest.len(), Ordering::Relaxed);
    if rest.is_empty() { by_model.remove(model); } else { by_model.insert(model.to_string(), Arc::new(rest)); }
  }
}

impl MarciDB {
  /// Creates the journal `name` of `entity` recording the operations `on`, or does nothing when it
  /// already exists with the same ones. Returns whether it was created. Changes made before this call
  /// are not in the journal; every later one is, until [`MarciDB::journal_drop`].
  pub fn journal_open(&self, entity: &Entity, name: &str, on: &[String]) -> Result<bool, JournalError> {
    check_name(name)?;
    let ops = parse_ops(on)?;

    if let Some(existing) = self.journals.find(&entity.name, name) {
      return if existing.ops == ops { Ok(false) } else { Err(JournalError::Conflict { name: name.to_string() }) };
    }

    let tx = self.raw_begin_write()?;
    let tree_name = entries_tree_name(&entity.name, name);
    {
      let mut config = tx.get_or_create_tree(JOURNALS_TREE)?;
      config.insert(&config_key(&entity.name, name), &encode_config(ops, 0))?;
      tx.get_or_create_tree(&tree_name)?;
    }
    // Registered while the write lock is held: no delete can commit between the two.
    self.journals.add(&entity.name, Arc::new(Journal { name: name.to_string(), ops, tree_name, next_seq: AtomicU64::new(1) }));
    if let Err(e) = tx.commit() {
      self.journals.remove(&entity.name, name);
      return Err(e.into());
    }
    Ok(true)
  }

  /// Reads up to `limit` entries of a journal, oldest first, each a JSON object
  /// `{"seq":…,"op":"delete","row":{…}}`. `after` confirms every entry up to that seq — they are
  /// dropped — and the read continues behind it; without it the read starts at the oldest entry kept.
  pub fn journal_read(&self, entity: &Entity, name: &str, after: Option<u64>, limit: usize) -> Result<Vec<String>, JournalError> {
    let journal = self.journals.find(&entity.name, name).ok_or_else(|| JournalError::NotFound { name: name.to_string() })?;

    if let Some(after) = after {
      // A seq that was never handed out confirms nothing beyond the last one that was.
      let after = after.min(journal.next_seq.load(Ordering::Relaxed).saturating_sub(1));
      let tx = self.raw_begin_write()?;
      let key = config_key(&entity.name, name);
      let mut config = tx.get_or_create_tree(JOURNALS_TREE)?;
      let acked = config.get(&key)?.map(|v| decode_config(&v).1).unwrap_or(0);
      if after > acked {
        config.insert(&key, &encode_config(journal.ops, after))?;
        let mut tree = tx.get_or_create_tree(&journal.tree_name)?;
        tree.delete_range((&[] as &[u8])..(after + 1).to_be_bytes().as_slice())?;
        drop(tree);
        drop(config);
        tx.commit()?;
      }
    }

    let rx = self.raw_begin_read()?;
    let Some(tree) = rx.get_tree(&journal.tree_name)? else { return Ok(vec![]) };
    let mut out = Vec::new();
    for entry in tree.iter()? {
      if out.len() >= limit { break; }
      let (key, value) = entry?;
      let seq = u64::from_be_bytes(key.as_ref().try_into().expect("a journal seq is 8 bytes"));
      let op = match value[0] { OP_DELETE => "delete", other => panic!("unknown journal op {}", other) };
      let row = std::str::from_utf8(&value[1..]).expect("a journal row is UTF-8 JSON");
      out.push(format!("{{\"seq\":{},\"op\":\"{}\",\"row\":{}}}", seq, op, row));
    }
    Ok(out)
  }

  /// Drops a journal with whatever it still holds. Returns whether it existed.
  pub fn journal_drop(&self, entity: &Entity, name: &str) -> Result<bool, JournalError> {
    let Some(journal) = self.journals.find(&entity.name, name) else { return Ok(false) };

    let tx = self.raw_begin_write()?;
    {
      let mut config = tx.get_or_create_tree(JOURNALS_TREE)?;
      config.delete(&config_key(&entity.name, name))?;
    }
    tx.delete_tree(&journal.tree_name)?;
    self.journals.remove(&entity.name, name);
    if let Err(e) = tx.commit() {
      self.journals.add(&entity.name, journal);
      return Err(e.into());
    }
    Ok(true)
  }

  /// The signal a reader waits on for new entries of this database's journals.
  pub fn journal_signal(&self) -> Arc<JournalSignal> {
    self.journals.signal.clone()
  }
}
