//! `marci-migrate` — CLI миграций MarciDB (движок поверх materialized-снапшота).
//!
//! Тонкая обёртка над крейтом `marcidb`: вся логика (материализация схемы, снапшот, сверка слотов,
//! diff) живёт в либе и переиспользуется сервером. Здесь — разбор аргументов и работа с файлами.
//!
//! Файл миграции = materialized-снапшот версии схемы (плоские entities, struct развёрнуты, enum
//! впечатан, слоты/биндинги запинены). Сервер диффит соседние снапшоты и применяет — без отдельного
//! формата операций. Снапшот соответствует тому, что лежит в памяти marcidb, а не тексту `.marci`.

use std::{env, fs, process};

use marcidb::{MigrateOp, Schema, diff, parse_snapshot, reconcile, serialize_snapshot, try_parse_schema};

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
     marci-migrate snapshot <schema.marci> [out]                materialize schema → flat snapshot\n  \
     marci-migrate generate <schema.marci> [dir] [name]         diff schema vs last snapshot → new migration file"
  );
}

/// Читает текст схемы, срезая UTF-8 BOM (иначе первый блок схемы «сползёт»)
fn read_schema(path: &str) -> String {
  let raw = fs::read_to_string(path).unwrap_or_else(|e| {
    eprintln!("Cannot read {}: {}", path, e);
    process::exit(1);
  });
  raw.strip_prefix('\u{feff}').unwrap_or(&raw).to_string()
}

/// Материализует `schema.marci` в плоский снапшот (struct развёрнуты, enum впечатан, биндинги/слоты явные).
fn cmd_snapshot(schema_path: &str, out: Option<&str>) {
  let schema = try_parse_schema(&read_schema(schema_path)).unwrap_or_else(|e| {
    eprintln!("Schema error: {}", e);
    process::exit(1);
  });
  let snapshot = serialize_snapshot(&schema);

  match out {
    Some(path) => {
      fs::write(path, &snapshot).unwrap_or_else(|e| { eprintln!("Cannot write {}: {}", path, e); process::exit(1); });
      println!("Wrote snapshot to {}", path);
    }
    None => print!("{}", snapshot),
  }
}

/// Диффит `schema.marci` против последнего снапшота (`<dir>/meta/snapshot`) → новый файл-миграцию
/// (полный снапшот версии). Слоты/варианты наследуются от предыдущего снапшота через [`reconcile_slots`],
/// поэтому существующие данные не ломаются. Номер берётся из числа уже существующих файлов.
fn cmd_generate(schema_path: &str, migrations_dir: &str, name: &str) {
  let mut new_schema = try_parse_schema(&read_schema(schema_path)).unwrap_or_else(|e| {
    eprintln!("Schema error: {}", e);
    process::exit(1);
  });

  let meta_dir = format!("{}/meta", migrations_dir);
  let snapshot_path = format!("{}/snapshot", meta_dir);
  let prev_text = fs::read_to_string(&snapshot_path).unwrap_or_default();
  let prev = if prev_text.trim().is_empty() {
    Schema { models: vec![] }
  } else {
    parse_snapshot(&prev_text).unwrap_or_else(|e| { eprintln!("Corrupt snapshot {}: {}", snapshot_path, e); process::exit(1); })
  };

  // Переносим слоты/порядок/id вариантов из предыдущего снапшота, затем диффим
  reconcile(&mut new_schema, &prev);
  let ops = diff(&prev, &new_schema).unwrap_or_else(|e| {
    eprintln!("Migration error: {}", e);
    process::exit(1);
  });

  if ops.is_empty() {
    println!("No schema changes — migration not created");
    return;
  }

  let id = format!("{:04}", count_migrations(migrations_dir));
  let filename = format!("{}_{}.snapshot", id, name);
  let snapshot_text = serialize_snapshot(&new_schema);

  fs::create_dir_all(&meta_dir).unwrap();
  fs::write(format!("{}/{}", migrations_dir, filename), &snapshot_text).unwrap();
  fs::write(&snapshot_path, &snapshot_text).unwrap();

  println!("Created migration {} ({})", filename, summarize(&ops));
}

/// Сколько файлов-миграций (`*.snapshot`) уже есть в каталоге (для следующего номера)
fn count_migrations(dir: &str) -> usize {
  fs::read_dir(dir)
    .map(|entries| entries.filter_map(|e| e.ok())
      .filter(|e| e.file_name().to_string_lossy().ends_with(".snapshot"))
      .count())
    .unwrap_or(0)
}

/// Короткая сводка операций для вывода (`2 create, 1 add field, …`)
fn summarize(ops: &[MigrateOp]) -> String {
  let (mut ce, mut de, mut af, mut df, mut alf, mut idx) = (0, 0, 0, 0, 0, 0);
  for op in ops {
    match op {
      MigrateOp::CreateEntity { .. } => ce += 1,
      MigrateOp::DropEntity { .. } => de += 1,
      MigrateOp::AddField { .. } => af += 1,
      MigrateOp::DropField { .. } => df += 1,
      MigrateOp::AlterField { .. } => alf += 1,
      MigrateOp::AddIndex { .. } | MigrateOp::DropIndex { .. } => idx += 1,
    }
  }
  let parts: Vec<String> = [
    (ce, "create entity"), (de, "drop entity"), (af, "add field"),
    (df, "drop field"), (alf, "alter field"), (idx, "index"),
  ].iter().filter(|(n, _)| *n > 0).map(|(n, label)| format!("{} {}", n, label)).collect();
  parts.join(", ")
}
