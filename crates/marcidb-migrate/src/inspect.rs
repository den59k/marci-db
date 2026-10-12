//! `marci-migrate inspect` — reads a `.marcibin` backup or a closed database directory with no server
//! around: the header and the schema, the tree table, and the entries of one tree decoded through the
//! schema (or as hex with `--raw`, row headers annotated, so a damaged row can be read by eye).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::{fs, process};

use marcidb::{BackupReader, Entity, Field, FieldIndex, FieldLocation, FieldType, MarciDB, RefBinding, Schema, TreeInfo, decode_field_value, decode_id, decode_row_json, parse_id, parse_snapshot, serialize_snapshot, split_index_key};
use serde_json::{Value, json};

use crate::die;

struct Options {
  tree: Option<String>,
  limit: usize,
  id: Option<String>,
  raw: bool,
}

pub fn cmd_inspect(args: &[String]) {
  let mut target = None;
  let mut opts = Options { tree: None, limit: 20, id: None, raw: false };
  let mut it = args.iter();
  while let Some(arg) = it.next() {
    let mut value = |flag: &str| it.next().cloned().unwrap_or_else(|| { eprintln!("inspect: '{}' needs a value", flag); process::exit(2); });
    match arg.as_str() {
      "--tree" => opts.tree = Some(value("--tree")),
      "--limit" => opts.limit = die(value("--limit").parse::<usize>(), "inspect: --limit"),
      "--id" => opts.id = Some(value("--id")),
      "--raw" => opts.raw = true,
      flag if flag.starts_with("--") => { eprintln!("inspect: unknown option '{}'", flag); process::exit(2); }
      path if target.is_none() => target = Some(path.to_string()),
      extra => { eprintln!("inspect: unexpected argument '{}'", extra); process::exit(2); }
    }
  }
  let Some(target) = target else {
    eprintln!("Usage: marci-migrate inspect <file.marcibin | db-dir> [--tree <name>] [--limit N] [--id <json>] [--raw]");
    process::exit(2);
  };
  if opts.id.is_some() && opts.tree.is_none() {
    eprintln!("inspect: --id needs --tree <model>");
    process::exit(2);
  }

  // A damaged entry is reported in place (see `EntryPrinter::print`); keep the panic message off stderr.
  std::panic::set_hook(Box::new(|_| {}));

  if Path::new(&target).is_dir() { inspect_dir(&target, &opts) } else { inspect_file(&target, &opts) }
}

// ───────────────────────────────── sources ─────────────────────────────────

fn inspect_file(path: &str, opts: &Options) {
  let file = die(fs::File::open(path), &format!("Cannot open {}", path));
  let mut reader = die(BackupReader::open(file), path);
  let header = reader.header().clone();
  let schema = schema_of(&header.schema);

  let Some(tree) = &opts.tree else {
    print_summary(&format!("Backup {}", path), &[
      ("format", header.format.to_string()),
      ("engine", header.engine.clone()),
      ("created", header.created_at.clone()),
      ("schema version", header.schema_version.to_string()),
    ], &header.schema, &schema, &header.trees);
    let bytes = die(reader.finish(), "Verification failed");
    println!();
    println!("Checksums: ok ({} bytes)", bytes);
    return;
  };

  let info = find_tree(&header.trees, tree);
  let mut printer = EntryPrinter::new(&schema, tree, opts);
  // Sections before the one asked for are skipped (still checksummed); the rest is not read.
  while let Some(mut section) = die(reader.next_section(), "Corrupt backup") {
    if section.name() != tree { continue; }
    while let Some((key, value)) = die(section.next_entry(), "Corrupt backup") {
      if !printer.entry(key, value) { break; }
    }
    break;
  }
  printer.finish(info.entries);
}

fn inspect_dir(dir: &str, opts: &Options) {
  let db = MarciDB::try_open(dir).unwrap_or_else(|e| {
    eprintln!("Cannot open the database in '{}' — is it open in another process (a server or an app)? ({})", dir, e);
    process::exit(1);
  });
  let trees = die(db.list_trees(), "Cannot list trees");
  let schema_text = serialize_snapshot(&db.schema);

  let Some(tree) = &opts.tree else {
    let mut version = 0u64;
    die(db.scan_tree("__marci_meta__", |key, value| {
      if key == b"version" { version = value.try_into().map(u64::from_be_bytes).unwrap_or(0); }
      true
    }), "Cannot read the schema version");
    print_summary(&format!("Database {}", dir), &[("schema version", version.to_string())], &schema_text, &db.schema, &trees);
    return;
  };

  let info = find_tree(&trees, tree);
  let mut printer = EntryPrinter::new(&db.schema, tree, opts);
  die(db.scan_tree(tree, |key, value| printer.entry(key, value)), "Cannot read the tree");
  printer.finish(info.entries);
}

fn schema_of(snapshot: &str) -> Schema {
  if snapshot.trim().is_empty() { Schema { models: vec![] } } else { die(parse_snapshot(snapshot), "Corrupt schema snapshot") }
}

fn find_tree<'a>(trees: &'a [TreeInfo], name: &str) -> &'a TreeInfo {
  trees.iter().find(|t| t.name == name).unwrap_or_else(|| {
    eprintln!("No tree '{}' (run inspect without --tree for the list)", name);
    process::exit(1);
  })
}

// ───────────────────────────────── summary ─────────────────────────────────

fn print_summary(title: &str, facts: &[(&str, String)], schema_text: &str, schema: &Schema, trees: &[TreeInfo]) {
  println!("{}", title);
  for (label, value) in facts {
    println!("  {:<15} {}", label, value);
  }
  println!("  {:<15} {} trees, {} entries", "contents", trees.len(), trees.iter().map(|t| t.entries).sum::<u64>());

  println!();
  println!("Schema:");
  if schema_text.trim().is_empty() {
    println!("  (empty)");
  }
  for line in schema_text.lines() {
    println!("  {}", line);
  }

  println!();
  println!("Trees:");
  let width = trees.iter().map(|t| t.name.len()).max().unwrap_or(0).max(4);
  println!("  {:<width$}  {:>10}  KIND", "NAME", "ENTRIES");
  for t in trees {
    println!("  {:<width$}  {:>10}  {}", t.name, t.entries, Kind::of(schema, &t.name).label());
  }
}

// ───────────────────────────────── tree kinds ─────────────────────────────────

/// What a tree holds, found through the schema (tree names are derived from it).
enum Kind<'a> {
  Model(&'a Entity),
  Index(&'a Entity, &'a Field, &'a FieldIndex),
  Custom,
  Unique,
  Binding,
  /// A journal's entries: `seq` → op byte + the row as JSON.
  Journal,
  /// `__marci_journals__`: `<model>\0<name>` → ops + the confirmed seq.
  Journals,
  Meta,
  Unknown,
}

impl<'a> Kind<'a> {
  fn of(schema: &'a Schema, name: &str) -> Kind<'a> {
    match name {
      "__marci_meta__" => return Kind::Meta,
      "__marci_journals__" => return Kind::Journals,
      _ if name.starts_with("__journal__/") => return Kind::Journal,
      _ => {}
    }
    for entity in schema.models.iter() {
      if entity.name == name { return Kind::Model(entity); }
      if entity.unique_groups.iter().any(|g| g.tree_name == name) { return Kind::Unique; }
      for field in entity.fields.iter() {
        if let Some(index) = field.indexes.iter().find(|i| i.tree_name() == name.as_bytes()) {
          return match index { FieldIndex::Custom { .. } => Kind::Custom, _ => Kind::Index(entity, field, index) };
        }
        if let FieldType::Ref(info) | FieldType::RefList(info) = &field.ty {
          match &info.binding {
            RefBinding::IndexTree(tree) | RefBinding::IdList { rev_tree: tree } if tree == name => return Kind::Binding,
            _ => {}
          }
        }
      }
    }
    Kind::Unknown
  }

  fn label(&self) -> &'static str {
    match self {
      Kind::Model(_) => "model",
      Kind::Index(..) => "index",
      Kind::Custom => "custom",
      Kind::Unique => "unique",
      Kind::Binding => "binding",
      Kind::Journal | Kind::Journals => "journal",
      Kind::Meta => "meta",
      Kind::Unknown => "?",
    }
  }
}

// ───────────────────────────────── entries ─────────────────────────────────

struct EntryPrinter<'a> {
  schema: &'a Schema,
  kind: Kind<'a>,
  raw: bool,
  /// `--id`: the key of the one row to show.
  id: Option<Vec<u8>>,
  limit: usize,
  shown: usize,
}

impl<'a> EntryPrinter<'a> {
  fn new(schema: &'a Schema, tree: &str, opts: &Options) -> Self {
    let kind = Kind::of(schema, tree);
    let id = opts.id.as_ref().map(|text| {
      let Kind::Model(entity) = kind else {
        eprintln!("inspect: --id works on a model tree; '{}' is a {} tree", tree, kind.label());
        process::exit(2);
      };
      let value: Value = die(serde_json::from_str(text), "inspect: --id is not JSON");
      die(parse_id(schema, entity, &id_object(entity, value)), "inspect: --id")
    });
    EntryPrinter { schema, kind, raw: opts.raw, id, limit: opts.limit, shown: 0 }
  }

  /// Shows one entry if it passes `--id`; false when the scan can stop.
  fn entry(&mut self, key: &[u8], value: &[u8]) -> bool {
    if let Some(id) = &self.id {
      if key < id.as_slice() { return true; }
      if key > id.as_slice() { return false; }
    }
    if self.shown >= self.limit { return false; }
    self.shown += 1;
    self.print(key, value);
    self.id.is_none()
  }

  fn finish(&self, entries: u64) {
    if self.id.is_some() && self.shown == 0 {
      println!("(no row with that id)");
    } else if self.id.is_none() && entries > self.shown as u64 {
      println!("… {} more (--limit)", entries - self.shown as u64);
    }
  }

  fn print(&self, key: &[u8], value: &[u8]) {
    if self.raw {
      println!("key   {}", hex(key));
      println!("value {}", hex(value));
      if let Kind::Model(entity) = self.kind { annotate_row(entity, value); }
      println!();
      return;
    }
    // A damaged entry must not end the listing: decode failures and panics print the raw bytes instead.
    match catch_unwind(AssertUnwindSafe(|| self.decode(key, value))) {
      Ok(Ok(text)) => println!("{}", text),
      Ok(Err(e)) => println!("<undecodable: {}> key {} value {}", e, hex(key), hex(value)),
      Err(_) => println!("<undecodable> key {} value {}", hex(key), hex(value)),
    }
  }

  fn decode(&self, key: &[u8], value: &[u8]) -> Result<String, String> {
    match self.kind {
      Kind::Model(entity) => decode_row_json(self.schema, entity, key, value),
      Kind::Index(entity, field, index) => {
        let (bytes, id) = split_index_key(field, index, key);
        let value = decode_field_value(field, &bytes).map_err(|e| format!("{:?}", e))?;
        Ok(format!("{{\"value\":{},\"id\":{}}}", value, decode_id(&id, entity, self.schema)))
      }
      Kind::Journal => {
        let seq = u64::from_be_bytes(key.try_into().map_err(|_| "a seq is 8 bytes".to_string())?);
        let (op, row) = value.split_first().ok_or("empty entry")?;
        let row: Value = serde_json::from_slice(row).map_err(|e| e.to_string())?;
        Ok(format!("{{\"seq\":{},\"op\":\"{}\",\"row\":{}}}", seq, op_name(*op), row))
      }
      Kind::Journals => {
        let split = key.iter().position(|&b| b == 0).ok_or("a journal key is <model>\\0<name>")?;
        let (ops, acked) = value.split_first().ok_or("empty record")?;
        let acked = u64::from_be_bytes(acked.try_into().map_err(|_| "a journal record is 9 bytes".to_string())?);
        let ops: Vec<&str> = [1u8].iter().filter(|bit| ops & **bit != 0).map(|bit| op_name(*bit)).collect();
        Ok(format!(
          "{{\"model\":{},\"journal\":{},\"on\":{},\"confirmed\":{}}}",
          json!(String::from_utf8_lossy(&key[..split])), json!(String::from_utf8_lossy(&key[split + 1..])), json!(ops), acked,
        ))
      }
      Kind::Meta => match key {
        b"version" => Ok(format!("version: {}", u64::from_be_bytes(value.try_into().map_err(|_| "a version is 8 bytes".to_string())?))),
        b"schema" => Ok(format!("schema:\n{}", String::from_utf8_lossy(value).lines().map(|l| format!("  {}", l)).collect::<Vec<_>>().join("\n"))),
        _ => Ok(format!("{}  {}", String::from_utf8_lossy(key), hex(value))),
      },
      Kind::Custom | Kind::Unique | Kind::Binding | Kind::Unknown if value.is_empty() => Ok(hex(key)),
      Kind::Custom | Kind::Unique | Kind::Binding | Kind::Unknown => Ok(format!("{}  {}", hex(key), hex(value))),
    }
  }
}

/// `--id 5` for a model with one key field means `--id '{"id":5}'`.
fn id_object(entity: &Entity, value: Value) -> Value {
  if value.is_object() { return value; }
  let mut keys = entity.fields.iter().filter(|f| matches!(f.location, FieldLocation::Key { .. }));
  match (keys.next(), keys.next()) {
    (Some(key), None) => json!({ key.name.clone(): value }),
    _ => value,
  }
}

fn op_name(op: u8) -> &'static str {
  match op {
    1 => "delete",
    _ => "?",
  }
}

/// The row header of a stored row: `[version u8][reserved u8][payload_offset u16]`, then one `u32`
/// offset per body slot (0 = null), each named by the field that owns the slot.
fn annotate_row(entity: &Entity, row: &[u8]) {
  if row.len() < 4 {
    println!("  (shorter than the 4-byte row header)");
    return;
  }
  let payload_offset = u16::from_be_bytes([row[2], row[3]]) as usize;
  println!("  version {}  payload_offset {}  ({} bytes)", row[0], payload_offset, row.len());
  let mut pos = 4;
  while pos + 4 <= payload_offset.min(row.len()) {
    let offset = u32::from_be_bytes(row[pos..pos + 4].try_into().unwrap());
    let owner = entity.fields.iter()
      .find(|f| matches!(f.location, FieldLocation::Body { offset_pos } if offset_pos == pos))
      .map_or("(no field)", |f| f.name.as_str());
    let shown = if offset == 0 { "null".to_string() } else { offset.to_string() };
    println!("  slot @{:<4} {:<24} {}", pos, owner, shown);
    pos += 4;
  }
  if payload_offset > row.len() {
    println!("  payload_offset is past the end of the row");
  }
}

fn hex(bytes: &[u8]) -> String {
  bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
