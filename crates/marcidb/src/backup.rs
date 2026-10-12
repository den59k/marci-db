//! Backup and restore: the `.marcibin` tree dump.
//!
//! Everything MarciDB owns lives in canopydb trees — rows, secondary indexes, relation binding trees,
//! journals, the `@custom` index stores and the schema itself (`__marci_meta__`). A backup is therefore
//! a dump of every tree, taken inside ONE read transaction (an MVCC snapshot: writers are never blocked,
//! and nothing committed after the snapshot leaks in). Restoring writes those bytes back one-to-one, so
//! the result is the database exactly as it was: same ids, same index entries, same journal positions.
//! The restored database sits at the schema snapshot of the backup; the regular migration path
//! (`migrate push`, `db.migrate(dir)`) brings it forward from there.
//!
//! File layout (all integers little-endian; `varint` is unsigned LEB128):
//!
//! ```text
//! "MARCIBIN"  u16 format  u32 header_len  header JSON
//! per tree, in header order:
//!   u16 name_len  name  u64 entries  { varint key_len  key  varint value_len  value }*  u64 xxh3(section)
//! "MARCIEND"  u32 sections  u64 xxh3(everything before this field)
//! ```
//!
//! The header JSON is `{ engine, createdAt, schemaVersion, schema, trees: [{ name, entries }] }` — the
//! schema snapshot text makes the file self-describing (an inspector can decode rows with no database
//! around), and the tree list lets a reader show a summary without scanning the body. Each section is
//! checksummed, and the trailer checksums the whole file: a truncated or bit-flipped backup is refused
//! at restore time instead of producing a half database. A backup written to a file goes to a temporary
//! name and is renamed only after the trailer, so a crash mid-backup never leaves a file that looks
//! complete.

use std::fmt;
use std::fs;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use canopydb::{Database, ReadTransaction, Transaction};
use serde_json::{Value, json};
use xxhash_rust::xxh3::Xxh3;

use crate::error::RequireTree;
use crate::marci_db::{OpenOptions, open_database};
use crate::migrate::META_TREE;
use crate::schema::serialize_snapshot;
use crate::{MarciDB, StorageError};

/// The eight bytes every `.marcibin` file starts with.
pub const BACKUP_MAGIC: &[u8; 8] = b"MARCIBIN";
/// The eight bytes that open the trailer.
const TRAILER_MAGIC: &[u8; 8] = b"MARCIEND";
/// Version of the file layout. Bumped only when the layout above changes; a reader refuses a newer
/// version than it knows. Independent of the engine version (which the header carries for information).
pub const BACKUP_FORMAT_VERSION: u16 = 1;
/// A restore commits a write transaction every time this many bytes of entries were inserted, so a
/// large backup never builds one huge uncommitted transaction (canopydb spills past 64 MB).
const RESTORE_BATCH_BYTES: usize = 32 * 1024 * 1024;
/// The header is small (a schema snapshot + a tree list); anything past this is not a backup file.
const MAX_HEADER_LEN: u32 = 64 * 1024 * 1024;

// ───────────────────────────────── public types ─────────────────────────────────

/// One tree as the header lists it.
#[derive(Debug, Clone, PartialEq)]
pub struct TreeInfo {
  pub name: String,
  pub entries: u64,
}

/// What a backup file says about itself, before any tree data.
#[derive(Debug, Clone)]
pub struct BackupHeader {
  /// [`BACKUP_FORMAT_VERSION`] of the file.
  pub format: u16,
  /// Engine version that wrote it (informational).
  pub engine: String,
  /// RFC 3339 UTC timestamp of the snapshot.
  pub created_at: String,
  /// The `__marci_meta__` version counter (bumped per applied migration); 0 for a database with no schema.
  pub schema_version: u64,
  /// The materialized schema snapshot text (same text as `__marci_meta__/schema`).
  pub schema: String,
  pub trees: Vec<TreeInfo>,
}

impl BackupHeader {
  fn to_json(&self) -> String {
    json!({
      "engine": self.engine,
      "createdAt": self.created_at,
      "schemaVersion": self.schema_version,
      "schema": self.schema,
      "trees": self.trees.iter().map(|t| json!({ "name": t.name, "entries": t.entries })).collect::<Vec<_>>(),
    }).to_string()
  }

  fn from_json(format: u16, text: &str) -> Result<Self, BackupError> {
    let v: Value = serde_json::from_str(text).map_err(|e| BackupError::Format(format!("header is not valid JSON: {}", e)))?;
    let trees = v.get("trees").and_then(Value::as_array)
      .ok_or_else(|| BackupError::Format("header has no tree list".to_string()))?
      .iter()
      .map(|t| Ok(TreeInfo {
        name: t.get("name").and_then(Value::as_str).ok_or_else(|| BackupError::Format("tree without a name in header".to_string()))?.to_string(),
        entries: t.get("entries").and_then(Value::as_u64).ok_or_else(|| BackupError::Format("tree without an entry count in header".to_string()))?,
      }))
      .collect::<Result<Vec<_>, BackupError>>()?;
    Ok(BackupHeader {
      format,
      engine: v.get("engine").and_then(Value::as_str).unwrap_or("").to_string(),
      created_at: v.get("createdAt").and_then(Value::as_str).unwrap_or("").to_string(),
      schema_version: v.get("schemaVersion").and_then(Value::as_u64).unwrap_or(0),
      schema: v.get("schema").and_then(Value::as_str).unwrap_or("").to_string(),
      trees,
    })
  }
}

/// The outcome of a backup or a restore: how much went through.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupInfo {
  /// Bytes written (backup) or read (restore), the whole file.
  pub bytes: u64,
  pub trees: usize,
  pub entries: u64,
  pub schema_version: u64,
  pub created_at: String,
}

impl BackupInfo {
  pub fn to_json(&self) -> Value {
    json!({ "bytes": self.bytes, "trees": self.trees, "entries": self.entries, "schemaVersion": self.schema_version, "createdAt": self.created_at })
  }
}

/// How a restore treats its target directory and opens the result.
#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
  /// Replace an existing, non-empty target directory. The old database is moved aside during the swap
  /// and removed once the restored one is in place. Without this, a non-empty target is an error.
  pub overwrite: bool,
  /// Storage options for both the staging database and the final open.
  pub open: OpenOptions,
}

#[derive(Debug)]
pub enum BackupError {
  Storage(StorageError),
  Io(io::Error),
  /// The input is not a backup this engine can read: bad magic, a newer format, a checksum mismatch,
  /// a truncated file, a malformed header.
  Format(String),
  /// The restore target cannot be used: it exists and `overwrite` is off, it is not a named directory,
  /// or the swap failed (typically because the database there is still open).
  Target(String),
}

impl fmt::Display for BackupError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      BackupError::Storage(e) => write!(f, "{}", e),
      BackupError::Io(e) => write!(f, "i/o error: {}", e),
      BackupError::Format(s) => write!(f, "not a valid backup: {}", s),
      BackupError::Target(s) => write!(f, "{}", s),
    }
  }
}

impl std::error::Error for BackupError {}
impl From<StorageError> for BackupError { fn from(e: StorageError) -> Self { BackupError::Storage(e) } }
impl From<canopydb::Error> for BackupError { fn from(e: canopydb::Error) -> Self { BackupError::Storage(StorageError::Backend(e)) } }
impl From<io::Error> for BackupError { fn from(e: io::Error) -> Self { BackupError::Io(e) } }

// ───────────────────────────────── varint ─────────────────────────────────

fn write_varint<W: Write>(w: &mut W, mut n: u64) -> io::Result<()> {
  let mut buf = [0u8; 10];
  let mut i = 0;
  loop {
    let byte = (n & 0x7f) as u8;
    n >>= 7;
    if n == 0 {
      buf[i] = byte;
      i += 1;
      break;
    }
    buf[i] = byte | 0x80;
    i += 1;
  }
  w.write_all(&buf[..i])
}

// ───────────────────────────────── writer ─────────────────────────────────

/// Counts bytes and feeds two xxh3 streams: the whole file and the current section.
struct HashingWriter<W: Write> {
  inner: W,
  total: Xxh3,
  section: Xxh3,
  bytes: u64,
}

impl<W: Write> HashingWriter<W> {
  fn new(inner: W) -> Self { HashingWriter { inner, total: Xxh3::new(), section: Xxh3::new(), bytes: 0 } }

  /// Writes bytes that belong to no section (the file header, the section checksums, the trailer).
  fn put_raw(&mut self, data: &[u8]) -> io::Result<()> {
    self.inner.write_all(data)?;
    self.total.update(data);
    self.bytes += data.len() as u64;
    Ok(())
  }

  /// Writes bytes of the current section (hashed into both streams).
  fn put(&mut self, data: &[u8]) -> io::Result<()> {
    self.section.update(data);
    self.put_raw(data)
  }
}

/// A `Write` view over the section stream, so `write_varint` can target it.
struct SectionSink<'a, W: Write>(&'a mut HashingWriter<W>);
impl<W: Write> Write for SectionSink<'_, W> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> { self.0.put(buf)?; Ok(buf.len()) }
  fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

/// A consistent snapshot of the database, ready to be written out. Holds a canopydb read transaction:
/// the snapshot point is fixed when [`MarciDB::backup_begin`] returns, writers continue meanwhile, and
/// pages they free stay pinned until this is dropped (the data file may grow a little during a long
/// backup; the space is reclaimed afterwards).
///
/// It does not borrow the [`MarciDB`] it came from: the transaction keeps the storage alive on its own,
/// so a host may release whatever lock it holds on the `MarciDB` right after `backup_begin` and let a
/// migration commit while the copy runs — the copy still sees only its snapshot. The storage directory
/// stays locked until the snapshot is dropped, so a host closing the database should wait for it.
pub struct BackupSnapshot {
  rx: ReadTransaction,
  header: BackupHeader,
}

impl BackupSnapshot {
  pub fn header(&self) -> &BackupHeader { &self.header }

  /// Streams the snapshot into `out` in the `.marcibin` layout. `out` is buffered internally.
  pub fn write_to<W: Write>(self, out: W) -> Result<BackupInfo, BackupError> {
    let mut w = HashingWriter::new(BufWriter::with_capacity(256 * 1024, out));

    w.put_raw(BACKUP_MAGIC)?;
    w.put_raw(&BACKUP_FORMAT_VERSION.to_le_bytes())?;
    let header_json = self.header.to_json();
    w.put_raw(&(header_json.len() as u32).to_le_bytes())?;
    w.put_raw(header_json.as_bytes())?;

    let mut entries_total = 0u64;
    for info in self.header.trees.iter() {
      let tree = self.rx.require_tree(info.name.as_bytes())?;
      let name_len = u16::try_from(info.name.len())
        .map_err(|_| BackupError::Format(format!("tree name '{}…' is too long for the backup format", info.name.chars().take(32).collect::<String>())))?;

      w.section.reset();
      w.put(&name_len.to_le_bytes())?;
      w.put(info.name.as_bytes())?;
      w.put(&info.entries.to_le_bytes())?;

      let mut written = 0u64;
      for entry in tree.iter()? {
        let (key, value) = entry?;
        let (key, value) = (key.as_ref(), value.as_ref());
        write_varint(&mut SectionSink(&mut w), key.len() as u64)?;
        w.put(key)?;
        write_varint(&mut SectionSink(&mut w), value.len() as u64)?;
        w.put(value)?;
        written += 1;
      }
      // The count was read from the same transaction as the scan, so a mismatch is a storage fault.
      if written != info.entries {
        return Err(BackupError::Storage(StorageError::MissingTree(format!("{} (entry count changed under a read snapshot)", info.name))));
      }
      entries_total += written;

      let digest = w.section.digest();
      w.put_raw(&digest.to_le_bytes())?;
    }

    w.put_raw(TRAILER_MAGIC)?;
    w.put_raw(&(self.header.trees.len() as u32).to_le_bytes())?;
    let total = w.total.digest();
    w.put_raw(&total.to_le_bytes())?;
    w.inner.flush()?;

    Ok(BackupInfo {
      bytes: w.bytes,
      trees: self.header.trees.len(),
      entries: entries_total,
      schema_version: self.header.schema_version,
      created_at: self.header.created_at.clone(),
    })
  }

  /// [`BackupSnapshot::write_to`] into `path`, through `<path>.tmp` renamed into place only once the
  /// trailer is written; on any failure the temporary file is removed and nothing is left behind.
  pub fn write_to_file(self, path: impl AsRef<Path>) -> Result<BackupInfo, BackupError> {
    write_backup_file(self, path.as_ref())
  }
}

// ───────────────────────────────── reader ─────────────────────────────────

struct HashingReader<R: Read> {
  inner: BufReader<R>,
  total: Xxh3,
  section: Xxh3,
  bytes: u64,
}

impl<R: Read> HashingReader<R> {
  fn fill(&mut self, buf: &mut [u8], in_section: bool) -> Result<(), BackupError> {
    self.inner.read_exact(buf).map_err(|e| match e.kind() {
      io::ErrorKind::UnexpectedEof => BackupError::Format("file is truncated".to_string()),
      _ => BackupError::Io(e),
    })?;
    self.total.update(buf);
    if in_section { self.section.update(buf); }
    self.bytes += buf.len() as u64;
    Ok(())
  }

  fn u8(&mut self, in_section: bool) -> Result<u8, BackupError> { let mut b = [0u8; 1]; self.fill(&mut b, in_section)?; Ok(b[0]) }
  fn u16(&mut self, in_section: bool) -> Result<u16, BackupError> { let mut b = [0u8; 2]; self.fill(&mut b, in_section)?; Ok(u16::from_le_bytes(b)) }
  fn u32(&mut self, in_section: bool) -> Result<u32, BackupError> { let mut b = [0u8; 4]; self.fill(&mut b, in_section)?; Ok(u32::from_le_bytes(b)) }
  fn u64(&mut self, in_section: bool) -> Result<u64, BackupError> { let mut b = [0u8; 8]; self.fill(&mut b, in_section)?; Ok(u64::from_le_bytes(b)) }

  /// Reads `len` section bytes into `buf`. The buffer grows in bounded steps, so a corrupt length cannot
  /// request a huge allocation up front — the file runs out first and the read fails as truncated.
  fn fill_vec(&mut self, buf: &mut Vec<u8>, len: usize) -> Result<(), BackupError> {
    buf.clear();
    while buf.len() < len {
      let start = buf.len();
      buf.resize(start + (len - start).min(1 << 20), 0);
      self.fill(&mut buf[start..], true)?;
    }
    Ok(())
  }

  fn varint(&mut self) -> Result<u64, BackupError> {
    let mut n = 0u64;
    let mut shift = 0u32;
    loop {
      let byte = self.u8(true)?;
      if shift >= 64 { return Err(BackupError::Format("length varint is too long".to_string())); }
      n |= ((byte & 0x7f) as u64) << shift;
      if byte & 0x80 == 0 { return Ok(n); }
      shift += 7;
    }
  }
}

/// Streaming reader of a `.marcibin` file: header first, then the sections in order, then the trailer.
/// Used by [`MarciDB::restore`] and by inspection tools; it never needs the whole file in memory.
pub struct BackupReader<R: Read> {
  r: HashingReader<R>,
  header: BackupHeader,
  /// Index of the next section to open.
  next: usize,
  /// Entries left in the open section, and whether its checksum was already consumed.
  remaining: u64,
  section_closed: bool,
  key: Vec<u8>,
  value: Vec<u8>,
}

impl<R: Read> BackupReader<R> {
  /// Reads and validates the file header. Fails on a wrong magic, a format newer than
  /// [`BACKUP_FORMAT_VERSION`], or a malformed header.
  pub fn open(input: R) -> Result<Self, BackupError> {
    let mut r = HashingReader { inner: BufReader::with_capacity(256 * 1024, input), total: Xxh3::new(), section: Xxh3::new(), bytes: 0 };
    let mut magic = [0u8; 8];
    r.fill(&mut magic, false)?;
    if &magic != BACKUP_MAGIC {
      return Err(BackupError::Format("missing MARCIBIN signature — not a MarciDB backup".to_string()));
    }
    let format = r.u16(false)?;
    if format == 0 || format > BACKUP_FORMAT_VERSION {
      return Err(BackupError::Format(format!("format version {} is newer than this engine supports ({})", format, BACKUP_FORMAT_VERSION)));
    }
    let header_len = r.u32(false)?;
    if header_len > MAX_HEADER_LEN {
      return Err(BackupError::Format("header length is implausible".to_string()));
    }
    let mut header_bytes = vec![0u8; header_len as usize];
    r.fill(&mut header_bytes, false)?;
    let header_text = String::from_utf8(header_bytes).map_err(|_| BackupError::Format("header is not UTF-8".to_string()))?;
    let header = BackupHeader::from_json(format, &header_text)?;
    Ok(BackupReader { r, header, next: 0, remaining: 0, section_closed: true, key: Vec::new(), value: Vec::new() })
  }

  pub fn header(&self) -> &BackupHeader { &self.header }

  /// Bytes consumed so far.
  pub fn bytes_read(&self) -> u64 { self.r.bytes }

  /// Opens the next section. Any entries left unread in the previous one are skipped (and its checksum
  /// still verified). Returns `None` after the last section.
  pub fn next_section(&mut self) -> Result<Option<SectionReader<'_, R>>, BackupError> {
    self.close_section()?;
    if self.next >= self.header.trees.len() {
      return Ok(None);
    }
    let expected = &self.header.trees[self.next];
    self.r.section.reset();
    let name_len = self.r.u16(true)? as usize;
    let mut name = vec![0u8; name_len];
    self.r.fill(&mut name, true)?;
    let entries = self.r.u64(true)?;
    if name != expected.name.as_bytes() || entries != expected.entries {
      return Err(BackupError::Format(format!(
        "section {} is '{}' with {} entries, header says '{}' with {}",
        self.next, String::from_utf8_lossy(&name), entries, expected.name, expected.entries
      )));
    }
    self.next += 1;
    self.remaining = entries;
    self.section_closed = false;
    let index = self.next - 1;
    Ok(Some(SectionReader { reader: self, index }))
  }

  /// Drains the open section (if any) and verifies its checksum.
  fn close_section(&mut self) -> Result<(), BackupError> {
    if self.section_closed { return Ok(()); }
    while self.remaining > 0 {
      self.read_entry()?;
    }
    let digest = self.r.section.digest();
    let stored = self.r.u64(false)?;
    if digest != stored {
      return Err(BackupError::Format(format!("checksum mismatch in tree '{}'", self.header.trees[self.next - 1].name)));
    }
    self.section_closed = true;
    Ok(())
  }

  fn read_entry(&mut self) -> Result<(), BackupError> {
    let klen = self.r.varint()? as usize;
    self.r.fill_vec(&mut self.key, klen)?;
    let vlen = self.r.varint()? as usize;
    self.r.fill_vec(&mut self.value, vlen)?;
    self.remaining -= 1;
    Ok(())
  }

  /// Reads the trailer and verifies the whole-file checksum. Sections still unread are skipped (each
  /// still verified). Returns the byte count of the file.
  pub fn finish(mut self) -> Result<u64, BackupError> {
    while self.next_section()?.is_some() {}
    let mut magic = [0u8; 8];
    self.r.fill(&mut magic, false)?;
    if &magic != TRAILER_MAGIC {
      return Err(BackupError::Format("trailer signature missing — file is truncated or corrupt".to_string()));
    }
    let sections = self.r.u32(false)?;
    if sections as usize != self.header.trees.len() {
      return Err(BackupError::Format(format!("trailer counts {} sections, header lists {}", sections, self.header.trees.len())));
    }
    let digest = self.r.total.digest();
    let stored = self.r.u64(false)?;
    if digest != stored {
      return Err(BackupError::Format("whole-file checksum mismatch".to_string()));
    }
    Ok(self.r.bytes)
  }
}

/// A `(key, value)` pair as a section yields it, borrowed until the next read.
type Entry<'e> = (&'e [u8], &'e [u8]);

/// One tree's entries, in key order. Dropping it before the end is fine — the reader skips the rest.
pub struct SectionReader<'a, R: Read> {
  reader: &'a mut BackupReader<R>,
  index: usize,
}

impl<R: Read> SectionReader<'_, R> {
  pub fn info(&self) -> &TreeInfo { &self.reader.header.trees[self.index] }
  pub fn name(&self) -> &str { &self.reader.header.trees[self.index].name }
  pub fn entries(&self) -> u64 { self.reader.header.trees[self.index].entries }
  pub fn remaining(&self) -> u64 { self.reader.remaining }

  /// The next `(key, value)`; `None` at the end of the section (the checksum is verified then).
  pub fn next_entry(&mut self) -> Result<Option<Entry<'_>>, BackupError> {
    if self.reader.remaining == 0 {
      self.reader.close_section()?;
      return Ok(None);
    }
    self.reader.read_entry()?;
    Ok(Some((&self.reader.key, &self.reader.value)))
  }
}

// ───────────────────────────────── restore ─────────────────────────────────

/// A restore that has been fully written to a staging directory next to its target, waiting to be
/// swapped in by [`RestoreStaged::commit`]. Dropping it without committing removes the staging directory.
/// The two phases exist so a host can do the long part (the copy) without holding anything, then close
/// the live database and swap in one short step.
pub struct RestoreStaged {
  target: PathBuf,
  staging: PathBuf,
  options: RestoreOptions,
  info: BackupInfo,
  committed: bool,
}

impl RestoreStaged {
  pub fn info(&self) -> &BackupInfo { &self.info }
  pub fn target(&self) -> &Path { &self.target }

  /// Puts the restored database in place of `target` and opens it. With `overwrite`, an existing
  /// database there is moved aside first and removed after the swap; the swap fails if that database
  /// is still open (close it first — in the server, evict its handle).
  pub fn commit(mut self) -> Result<(MarciDB, BackupInfo), BackupError> {
    let target = self.target.clone();
    let mut old: Option<PathBuf> = None;

    if target.exists() {
      ensure_not_open(&target)?;
      if dir_is_empty(&target)? {
        fs::remove_dir(&target)?;
      } else if self.options.overwrite {
        let aside = sibling(&target, &format!(".old-{}", unique_suffix()))?;
        fs::rename(&target, &aside).map_err(|e| BackupError::Target(format!(
          "cannot move the existing database '{}' aside ({}) — is it still open?", target.display(), e
        )))?;
        old = Some(aside);
      } else {
        return Err(BackupError::Target(format!("'{}' already holds a database — pass overwrite to replace it", target.display())));
      }
    } else if let Some(parent) = target.parent().filter(|p| !p.as_os_str().is_empty()) {
      fs::create_dir_all(parent)?;
    }

    if let Err(e) = fs::rename(&self.staging, &target) {
      // Put the old database back so the host is not left without one.
      if let Some(aside) = &old { let _ = fs::rename(aside, &target); }
      return Err(BackupError::Target(format!("cannot move the restored database into '{}': {}", target.display(), e)));
    }
    self.committed = true;

    if let Some(aside) = old {
      // Best effort: a leftover `.old-*` directory costs disk space, not correctness.
      let _ = fs::remove_dir_all(aside);
    }

    let db = MarciDB::try_open_with_options(&target.to_string_lossy(), &self.options.open)?;
    Ok((db, self.info.clone()))
  }

  /// Discards the staged restore (also what dropping does).
  pub fn abandon(self) {}
}

impl Drop for RestoreStaged {
  fn drop(&mut self) {
    if !self.committed {
      let _ = fs::remove_dir_all(&self.staging);
    }
  }
}

/// Fails when a process (this one included) has the database in `dir` open: canopydb holds an exclusive
/// lock on `<dir>/LOCK` for as long as a handle lives. Without this check a rename on Linux would
/// succeed under the open handle, which would keep writing into the moved-aside directory.
fn ensure_not_open(dir: &Path) -> Result<(), BackupError> {
  let lock_path = dir.join("LOCK");
  if !lock_path.exists() { return Ok(()); }
  let mut lock = fslock::LockFile::open(&lock_path)?;
  if !lock.try_lock()? {
    return Err(BackupError::Target(format!("the database in '{}' is open (in this process or another) — close it before restoring over it", dir.display())));
  }
  lock.unlock()?;
  Ok(())
}

fn dir_is_empty(path: &Path) -> Result<bool, BackupError> {
  Ok(fs::read_dir(path)?.next().is_none())
}

fn unique_suffix() -> String {
  let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
  format!("{}-{}", nanos, std::process::id())
}

/// `<parent>/<name><suffix>` for a target directory `<parent>/<name>`.
fn sibling(target: &Path, suffix: &str) -> Result<PathBuf, BackupError> {
  let name = target.file_name().and_then(|n| n.to_str())
    .ok_or_else(|| BackupError::Target(format!("restore target '{}' must be a named directory", target.display())))?;
  let parent = target.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
  Ok(parent.join(format!("{}{}", name, suffix)))
}

impl MarciDB {
  /// Fixes the snapshot point: opens the read transaction and lists every tree. The copy itself happens
  /// in [`BackupSnapshot::write_to`], which a host may run on another thread while this database keeps
  /// serving reads and writes.
  pub fn backup_begin(&self) -> Result<BackupSnapshot, BackupError> {
    let rx = self.db.begin_read()?;
    let trees = tree_list(&rx)?;

    // The schema as the snapshot sees it (not the in-memory copy, which a migration could be swapping).
    let (schema, schema_version) = match rx.get_tree(META_TREE)? {
      Some(meta) => (
        meta.get(b"schema")?.map(|v| String::from_utf8_lossy(v.as_ref()).into_owned()).unwrap_or_else(|| serialize_snapshot(&self.schema)),
        meta.get(b"version")?.and_then(|v| v.as_ref().try_into().ok().map(u64::from_be_bytes)).unwrap_or(0),
      ),
      None => (serialize_snapshot(&self.schema), 0),
    };

    let header = BackupHeader {
      format: BACKUP_FORMAT_VERSION,
      engine: env!("CARGO_PKG_VERSION").to_string(),
      created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
      schema_version,
      schema,
      trees,
    };
    Ok(BackupSnapshot { rx, header })
  }

  /// Writes a consistent backup of the whole database to `out`.
  pub fn backup<W: Write>(&self, out: W) -> Result<BackupInfo, BackupError> {
    self.backup_begin()?.write_to(out)
  }

  /// Writes a backup to `path`, through a temporary file next to it that is renamed into place only
  /// once the trailer is written — a crash mid-way never leaves a complete-looking file.
  pub fn backup_to_file(&self, path: impl AsRef<Path>) -> Result<BackupInfo, BackupError> {
    self.backup_begin()?.write_to_file(path)
  }

  /// Restores a backup into `dir` and opens it. The copy goes to a staging directory beside `dir`; the
  /// swap into place is the last step (see [`RestoreStaged::commit`]). The returned database has no
  /// `@custom` index providers installed — attach them with [`MarciDB::with_providers`] as after `open`.
  pub fn restore<R: Read>(dir: &str, input: R, options: &RestoreOptions) -> Result<(MarciDB, BackupInfo), BackupError> {
    // The swap checks this too; checking first saves copying a whole backup only to be refused.
    let target = Path::new(dir);
    if options.overwrite && target.is_dir() {
      ensure_not_open(target)?;
    }
    MarciDB::restore_prepare(dir, input, options)?.commit()
  }

  /// [`MarciDB::restore`] from a file.
  pub fn restore_from_file(dir: &str, path: impl AsRef<Path>, options: &RestoreOptions) -> Result<(MarciDB, BackupInfo), BackupError> {
    let file = fs::File::open(path.as_ref())?;
    MarciDB::restore(dir, file, options)
  }

  /// The first phase of a restore: validates the backup and writes it into a staging directory next to
  /// `dir`, without touching `dir` itself. Fails early if `dir` holds a database and `overwrite` is off.
  pub fn restore_prepare<R: Read>(dir: &str, input: R, options: &RestoreOptions) -> Result<RestoreStaged, BackupError> {
    let target = PathBuf::from(dir);
    if target.exists() && !target.is_dir() {
      return Err(BackupError::Target(format!("restore target '{}' exists and is not a directory", target.display())));
    }
    if target.exists() && !options.overwrite && !dir_is_empty(&target)? {
      return Err(BackupError::Target(format!("'{}' already holds a database — pass overwrite to replace it", target.display())));
    }
    let staging = sibling(&target, ".restoring")?;
    if staging.exists() {
      fs::remove_dir_all(&staging)?;
    }
    fs::create_dir_all(&staging)?;

    let mut staged = RestoreStaged { target, staging, options: options.clone(), info: BackupInfo { bytes: 0, trees: 0, entries: 0, schema_version: 0, created_at: String::new() }, committed: false };
    staged.info = write_staging(&staged.staging, input, &options.open)?; // on error `staged` drops and removes the directory
    Ok(staged)
  }

  /// Every tree of the database with its entry count, by name. For inspection tools.
  pub fn list_trees(&self) -> Result<Vec<TreeInfo>, StorageError> {
    let rx = self.db.begin_read()?;
    tree_list(&rx)
  }

  /// Walks a raw tree in key order, calling `f(key, value)` until it returns `false` or the tree ends.
  /// `None` when there is no such tree. For inspection tools.
  pub fn scan_tree(&self, name: &str, mut f: impl FnMut(&[u8], &[u8]) -> bool) -> Result<Option<()>, StorageError> {
    let rx = self.db.begin_read()?;
    let Some(tree) = rx.get_tree(name.as_bytes())? else { return Ok(None) };
    for entry in tree.iter()? {
      let (k, v) = entry?;
      if !f(k.as_ref(), v.as_ref()) { break; }
    }
    Ok(Some(()))
  }
}

/// Every tree a transaction sees, with its entry count, sorted by name (all MarciDB tree names are UTF-8).
fn tree_list(rx: &Transaction) -> Result<Vec<TreeInfo>, StorageError> {
  let mut trees = Vec::new();
  for name in rx.list_trees()? {
    let tree = rx.require_tree(name.as_ref())?;
    trees.push(TreeInfo { name: String::from_utf8_lossy(name.as_ref()).into_owned(), entries: tree.len() });
  }
  trees.sort_by(|a, b| a.name.cmp(&b.name));
  Ok(trees)
}

/// `BackupSnapshot::write_to` into `path` via `<path>.tmp` + rename.
fn write_backup_file(snapshot: BackupSnapshot, path: &Path) -> Result<BackupInfo, BackupError> {
  if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
    fs::create_dir_all(parent)?;
  }
  let tmp = path.with_extension(format!("{}.tmp", path.extension().and_then(|e| e.to_str()).unwrap_or("marcibin")));
  let result = (|| {
    let file = fs::File::create(&tmp)?;
    let info = snapshot.write_to(&file)?;
    file.sync_all()?;
    Ok::<_, BackupError>(info)
  })();
  match result.and_then(|info| { fs::rename(&tmp, path)?; Ok(info) }) {
    Ok(info) => Ok(info),
    Err(e) => {
      let _ = fs::remove_file(&tmp);
      Err(e)
    }
  }
}

/// Reads the whole backup into a fresh canopydb database at `staging`, committing in bounded batches,
/// and verifies every checksum. The database is checkpointed and closed before returning so the
/// directory can be renamed.
fn write_staging<R: Read>(staging: &Path, input: R, open: &OpenOptions) -> Result<BackupInfo, BackupError> {
  let db: Database = open_database(&staging.to_string_lossy(), open)?;
  let mut reader = BackupReader::open(input)?;
  let header = reader.header().clone();

  let mut entries_total = 0u64;
  while let Some(mut section) = reader.next_section()? {
    let name = section.name().as_bytes().to_vec();
    loop {
      let tx = db.begin_write()?;
      let mut batch = 0usize;
      let done = {
        let mut tree = tx.get_or_create_tree(&name)?;
        loop {
          match section.next_entry()? {
            None => break true,
            Some((k, v)) => {
              tree.insert(k, v)?;
              entries_total += 1;
              batch += k.len() + v.len() + 16;
              if batch >= RESTORE_BATCH_BYTES { break false; }
            }
          }
        }
      };
      tx.commit()?;
      if done { break; }
    }
  }
  let bytes = reader.finish()?;

  // Fold the WAL into the data file and close, so the staging directory is a complete, quiescent database.
  db.checkpoint()?;
  drop(db);

  Ok(BackupInfo { bytes, trees: header.trees.len(), entries: entries_total, schema_version: header.schema_version, created_at: header.created_at })
}
