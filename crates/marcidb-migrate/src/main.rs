//! `marci-migrate` — MarciDB migration CLI (smart client, dumb server).
//!
//! A migration file (`NNNN_name.march`) is a list of SELF-CONTAINED actions (each carries the field definition).
//! The developer sees the CHANGES in it (that's what gets reviewed); the full schema lives in `schema.marci`, there's no
//! snapshot inside a migration. The server is dumb: `$migrate` lays the sent actions onto its state.
//! All the smarts (what changed, what to send) live here: `generate` diffs, `plan` computes the unapplied tail.
//!
//! It also works on a local database directory that no process has open: `backup` / `restore` a
//! `.marcibin` file, and `inspect` a backup or a directory (see inspect.rs).

use std::{env, fs, io::Read, path::Path, process};

use marcidb::{BackupInfo, MarciDB, MigrateOp, RestoreOptions, Schema, parse_snapshot, serialize_snapshot, try_parse_schema};
use marcidb_schema::{diff, evolve, reconcile, serialize_migration};

mod inspect;

const EXT: &str = ".march";

fn main() {
  let args: Vec<String> = env::args().collect();
  match args.get(1).map(String::as_str) {
    Some("snapshot") => cmd_snapshot(
      args.get(2).map(String::as_str).unwrap_or("schema.marci"),
      args.get(3).map(String::as_str),
    ),
    Some("generate") => cmd_generate(
      args.get(2).map(String::as_str).unwrap_or("schema.marci"),
      args.get(3).map(String::as_str).unwrap_or("migrations"),
      args.get(4).map(String::as_str).unwrap_or("migration"),
    ),
    Some("plan") => cmd_plan(args.get(2).map(String::as_str).unwrap_or("migrations")),
    Some("backup") => match (args.get(2), args.get(3)) {
      (Some(dir), Some(out)) => cmd_backup(dir, out),
      _ => usage_error("backup <db-dir> <out.marcibin>"),
    },
    Some("restore") => {
      let overwrite = args.iter().skip(2).any(|a| a == "--overwrite");
      let positional: Vec<&String> = args.iter().skip(2).filter(|a| !a.starts_with("--")).collect();
      match positional.as_slice() {
        [dir, file] => cmd_restore(dir, file, overwrite),
        _ => usage_error("restore <db-dir> <in.marcibin> [--overwrite]"),
      }
    }
    Some("inspect") => inspect::cmd_inspect(&args[2..]),
    Some(other) => {
      eprintln!("Unknown command: {}\n", other);
      print_usage();
      process::exit(2);
    }
    None => {
      print_usage();
      process::exit(2);
    }
  }
}

fn print_usage() {
  eprintln!(
    "marci-migrate — MarciDB migration tool\n\nUsage:\n  \
     marci-migrate snapshot <schema.marci> [out]            materialize schema → flat snapshot\n  \
     marci-migrate generate <schema.marci> [dir] [name]     diff schema vs history → new {EXT} migration\n  \
     marci-migrate plan [dir]                               read server snapshot on STDIN → print actions to apply\n\n\
     On a local database directory that is not open in any process:\n  \
     marci-migrate backup  <db-dir> <out.marcibin>          consistent backup (out may be a directory)\n  \
     marci-migrate restore <db-dir> <in.marcibin> [--overwrite]   restore a backup into db-dir\n  \
     marci-migrate inspect <file.marcibin | db-dir> [--tree <name>] [--limit N] [--id <json>] [--raw]\n  \
                                                            header, schema and trees; --tree lists entries"
  );
}

fn usage_error(usage: &str) -> ! {
  eprintln!("Usage: marci-migrate {}", usage);
  process::exit(2);
}

/// Opens a database directory for the local commands. canopydb locks the directory: a database a
/// server or an app has open is refused (after canopydb's lock timeout) rather than read under it.
fn open_local(dir: &str) -> MarciDB {
  if !Path::new(dir).is_dir() {
    eprintln!("'{}' is not a database directory", dir);
    process::exit(1);
  }
  MarciDB::try_open(dir).unwrap_or_else(|e| {
    eprintln!("Cannot open the database in '{}' — is it open in another process (a server or an app)? ({})", dir, e);
    process::exit(1);
  })
}

fn describe(info: &BackupInfo) -> String {
  format!("{} trees, {} entries, {} bytes, schema version {}, taken {}", info.trees, info.entries, info.bytes, info.schema_version, info.created_at)
}

/// `backup <db-dir> <out>`: `out` is the file, or a directory to put `<db>-<YYYYMMDD-HHMMSS>.marcibin` in.
fn cmd_backup(dir: &str, out: &str) {
  let db = open_local(dir);
  let snapshot = die(db.backup_begin(), "Backup failed");
  let file = if Path::new(out).is_dir() {
    let name = Path::new(dir).canonicalize().ok()
      .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
      .unwrap_or_else(|| "db".to_string());
    // `2026-10-12T12:00:00Z` → `20261012-120000`
    let stamp: String = snapshot.header().created_at.chars().filter(|c| c.is_ascii_digit() || *c == 'T').collect::<String>().replace('T', "-");
    Path::new(out).join(format!("{}-{}.marcibin", name, stamp))
  } else {
    Path::new(out).to_path_buf()
  };
  let info = die(snapshot.write_to_file(&file), "Backup failed");
  println!("Backed up {} to {} ({})", dir, file.display(), describe(&info));
}

fn cmd_restore(dir: &str, file: &str, overwrite: bool) {
  let options = RestoreOptions { overwrite, ..Default::default() };
  let (_, info) = MarciDB::restore_from_file(dir, file, &options).unwrap_or_else(|e| {
    eprintln!("Restore failed: {}", e);
    if !overwrite && matches!(e, marcidb::BackupError::Target(_)) && Path::new(dir).is_dir() {
      eprintln!("(add --overwrite to replace the database in '{}')", dir);
    }
    process::exit(1);
  });
  println!("Restored {} into {} ({})", file, dir, describe(&info));
}

/// Reads the schema text, stripping the UTF-8 BOM (otherwise the first schema block "shifts")
fn read_schema(path: &str) -> String {
  let raw = fs::read_to_string(path).unwrap_or_else(|e| { eprintln!("Cannot read {}: {}", path, e); process::exit(1); });
  raw.strip_prefix('\u{feff}').unwrap_or(&raw).to_string()
}

fn die<T, E: std::fmt::Display>(r: Result<T, E>, ctx: &str) -> T {
  r.unwrap_or_else(|e| { eprintln!("{}: {}", ctx, e); process::exit(1); })
}

/// Materializes `schema.marci` into a flat snapshot (structs expanded, enum inlined).
fn cmd_snapshot(schema_path: &str, out: Option<&str>) {
  let schema = die(try_parse_schema(&read_schema(schema_path)), "Schema error");
  let snapshot = serialize_snapshot(&schema);
  match out {
    Some(path) => {
      die(fs::write(path, &snapshot), "Cannot write");
      println!("Wrote snapshot to {}", path);
    }
    None => print!("{}", snapshot),
  }
}

/// Replays the migration history from an empty state → snapshot text of the latest version
fn replay_history(files: &[String]) -> String {
  let mut snapshot = String::new();
  for f in files {
    let text = fs::read_to_string(f).unwrap_or_else(|e| { eprintln!("Cannot read {}: {}", f, e); process::exit(1); });
    snapshot = die(evolve(&snapshot, &text), &format!("Corrupt migration {}", f));
  }
  snapshot
}

fn snapshot_to_schema(text: &str) -> Schema {
  if text.trim().is_empty() { Schema { models: vec![] } } else { die(parse_snapshot(text), "Corrupt snapshot") }
}

/// Diffs `schema.marci` against the replayed history → a new `NNNN_name.march`
/// (only a list of self-contained actions — the changes). Variant slots/ids are inherited from
/// the previous state via [`reconcile`], so the server's data doesn't break.
fn cmd_generate(schema_path: &str, migrations_dir: &str, name: &str) {
  let mut new_schema = die(try_parse_schema(&read_schema(schema_path)), "Schema error");

  let files = list_migrations(migrations_dir);
  let prev = snapshot_to_schema(&replay_history(&files));

  reconcile(&mut new_schema, &prev);
  let ops = die(diff(&prev, &new_schema), "Migration error");

  if ops.is_empty() {
    println!("No schema changes — migration not created");
    return;
  }

  let id = format!("{:04}", files.len());
  let filename = format!("{}_{}{}", id, name, EXT);
  fs::create_dir_all(migrations_dir).unwrap();
  fs::write(format!("{}/{}", migrations_dir, filename), serialize_migration(&ops, &new_schema)).unwrap();
  println!("Created migration {} ({})", filename, summarize(&ops));
}

/// Push planning: STDIN is the server's current snapshot (from `GET /:db/$snapshot`, empty if the DB doesn't exist).
/// We replay migrations from the first one until the snapshot matches the server's; we print to STDOUT the actions
/// of the not-yet-applied migrations (which is what `marcidb migrate push` sends to `$migrate`).
fn cmd_plan(migrations_dir: &str) {
  let mut server_raw = String::new();
  std::io::stdin().read_to_string(&mut server_raw).ok();
  let server = canon(server_raw.trim());

  let files = list_migrations(migrations_dir);

  // Find after which migration the snapshot matches the server's; start is the index of the first unapplied one
  let mut cumulative = String::new();
  let mut start = None;
  if server.is_empty() {
    start = Some(0); // server is empty — send everything
  } else {
    for (i, f) in files.iter().enumerate() {
      let text = fs::read_to_string(f).unwrap_or_else(|e| { eprintln!("Cannot read {}: {}", f, e); process::exit(1); });
      cumulative = die(evolve(&cumulative, &text), &format!("Corrupt migration {}", f));
      if canon(&cumulative) == server { start = Some(i + 1); break; }
    }
  }

  let Some(start) = start else {
    eprintln!("plan: server schema doesn't match any point in the migration history — drift?");
    process::exit(1);
  };

  let tail: Vec<String> = files[start..].iter()
    .map(|f| fs::read_to_string(f).unwrap_or_default().trim().to_string())
    .filter(|s| !s.is_empty())
    .collect();
  print!("{}", tail.join("\n\n"));
}

/// Normalizes the snapshot text to its canonical form (for comparison with the server's)
fn canon(snapshot_text: &str) -> String {
  if snapshot_text.trim().is_empty() { return String::new(); }
  serialize_snapshot(&snapshot_to_schema(snapshot_text))
}

/// Paths to migration files (`*.march`), sorted by name
fn list_migrations(dir: &str) -> Vec<String> {
  let mut files: Vec<String> = match fs::read_dir(dir) {
    Ok(entries) => entries.filter_map(|e| e.ok()).map(|e| e.path())
      .filter(|p| p.extension().map(|x| x == "march").unwrap_or(false))
      .filter_map(|p| p.to_str().map(String::from)).collect(),
    Err(_) => vec![],
  };
  files.sort();
  files
}

/// Short summary of the operations (`1 create entity, 2 add field`)
fn summarize(ops: &[MigrateOp]) -> String {
  let (mut ce, mut de, mut af, mut df, mut alf, mut idx) = (0, 0, 0, 0, 0, 0);
  for op in ops {
    match op {
      MigrateOp::CreateEntity { .. } => ce += 1,
      MigrateOp::DropEntity { .. } => de += 1,
      MigrateOp::AddField { .. } => af += 1,
      MigrateOp::DropField { .. } => df += 1,
      MigrateOp::AlterField { .. } => alf += 1,
      MigrateOp::AddIndex { .. } | MigrateOp::DropIndex { .. } | MigrateOp::AddUniqueGroup { .. } | MigrateOp::DropUniqueGroup { .. } => idx += 1,
    }
  }
  [
    (ce, "create entity"), (de, "drop entity"), (af, "add field"),
    (df, "drop field"), (alf, "alter field"), (idx, "index"),
  ].iter().filter(|(n, _)| *n > 0).map(|(n, l)| format!("{} {}", n, l)).collect::<Vec<_>>().join(", ")
}
