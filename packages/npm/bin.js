#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Платформенный суффикс пребилт-бинарей. `marci-generate` генерит TS-типы,
// `marci-migrate` — файлы миграций (отдельный бинарь после рефактора миграций).
const PLATFORM = {
  "linux-x64":    "linux-x64",
  "darwin-arm64": "darwin-arm64",
  "win32-x64":    "win32-x64.exe",
};

function binPath(base) {
  const key = `${os.platform()}-${os.arch()}`;
  const suffix = PLATFORM[key];
  if (!suffix) {
    console.error(`marcidb: unsupported platform ${key}`);
    process.exit(1);
  }
  return path.join(__dirname, "bin", `${base}-${suffix}`);
}

// Разбирает позиционные аргументы и флаги (--flag value | --flag)
function parseArgs(args) {
  const positional = [];
  const flags = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = args[i + 1];
      if (next === undefined || next.startsWith("--")) {
        flags[key] = true;
      } else {
        flags[key] = next;
        i++;
      }
    } else {
      positional.push(a);
    }
  }
  return { positional, flags };
}

function help() {
  console.log(`
marcidb <command> [options]

Commands:
  generate [schema] [output]      Generate TS types (always) and a migration file
    --migrations <dir>            migrations directory (default: migrations)
    --name <name>                 migration name (default: migration)
    --no-migrations               types only, skip the migration file

  migrate push <db> [options]     Push migration files (.snapshot) to a server; it applies the new ones
    --url <url>                   server url (default: http://localhost:3000)
    --migrations <dir>            migrations directory (default: migrations)

Examples:
  marcidb generate
  marcidb generate schema.marci --name add_users
  marcidb migrate push myapp --url http://localhost:3000
`);
}

function cmdGenerate(args) {
  const { positional, flags } = parseArgs(args);
  const schema = positional[0] ?? "schema.marci";
  const output = positional[1] ?? path.join(__dirname, "..", ".marcidb", "client");

  // 1) TypeScript-типы (всегда — без них миграция бессмысленна)
  if (fs.existsSync(output)) {
    fs.rmSync(output, { recursive: true, force: true });
  }
  console.log(`Generating types from ${schema} into ${output}...`);
  execFileSync(binPath("marci-generate"), ["types", schema, output], { stdio: "inherit" });
  fs.writeFileSync(path.join(output, "package.json"), `{
  "name": "marci-client",
  "main": "index.js",
  "types": "index.d.ts"
}`);

  // 2) Файл миграции (полный снапшот версии) — отдельным бинарём marci-migrate
  if (!flags["no-migrations"]) {
    const dir = flags.migrations ?? "migrations";
    const name = flags.name ?? "migration";
    execFileSync(binPath("marci-migrate"), ["generate", schema, dir, name], { stdio: "inherit" });
  }

  console.log("Done.");
}

async function cmdMigrate(args) {
  const [sub, ...rest] = args;
  if (sub === "push") return migratePush(rest);
  console.error(`marcidb migrate: unknown subcommand "${sub ?? ""}" (expected "push")`);
  help();
  process.exit(1);
}

// Императивный push: отправляет .mig-файлы; сервер по ledger'у применяет только новые
async function migratePush(rest) {
  const { positional, flags } = parseArgs(rest);
  const db = positional[0];
  if (!db) {
    console.error("marcidb migrate push: <db> name required");
    process.exit(1);
  }

  const url = (flags.url ?? "http://localhost:3000").replace(/\/+$/, "");
  const dir = flags.migrations ?? "migrations";

  if (!fs.existsSync(dir)) {
    console.error(`marcidb migrate push: migrations directory "${dir}" not found — run "marcidb generate" first`);
    process.exit(1);
  }

  // Все .snapshot по порядку имён → [{ id, ops }]; id — имя файла без расширения.
  // `ops` — это materialized-снапшот версии (сервер диффит соседние снапшоты и применяет)
  const migrations = fs.readdirSync(dir)
    .filter((f) => f.endsWith(".snapshot"))
    .sort()
    .map((f) => ({ id: f.replace(/\.snapshot$/, ""), ops: fs.readFileSync(path.join(dir, f), "utf8") }));

  if (migrations.length === 0) {
    console.log("No migration files — nothing to push");
    return;
  }

  const res = await fetch(`${url}/${db}/$migrate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(migrations),
  });

  if (!res.ok) {
    console.error(`Migration failed [${res.status}]: ${await res.text()}`);
    process.exit(1);
  }
  const { applied } = await res.json();
  if (applied && applied.length) {
    console.log(`Applied ${applied.length} migration(s) to '${db}': ${applied.join(", ")}`);
  } else {
    console.log(`'${db}' is up to date — no new migrations`);
  }
}

const COMMANDS = {
  generate: cmdGenerate,
  migrate: cmdMigrate,
};

// --- main ---

const [command, ...args] = process.argv.slice(2);

if (!command || command === "help" || command === "--help" || command === "-h") {
  help();
  process.exit(0);
}

const fn = COMMANDS[command];

if (!fn) {
  console.error(`marcidb: unknown command "${command}"`);
  help();
  process.exit(1);
}

await fn(args);
