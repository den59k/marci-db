#!/usr/bin/env node

import { execFileSync, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Platform suffix for the prebuilt binaries. `marci-generate` generates TS types,
// `marci-migrate` — migration files (a separate binary after the migration refactor).
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

// Flags that never take a value (so `--yes backup.marcibin` keeps the file as a positional argument)
const BOOLEAN_FLAGS = new Set(["yes", "no-migrations"]);

// Parses positional arguments and flags (--flag value | --flag)
function parseArgs(args) {
  const positional = [];
  const flags = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = args[i + 1];
      if (BOOLEAN_FLAGS.has(key) || next === undefined || next.startsWith("--")) {
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

  migrate push <db> [options]     Push migration files (.march) to a server; it applies the new ones
    --url <url>                   server url (default: http://localhost:3000)
    --migrations <dir>            migrations directory (default: migrations)

  migrate check <db> [options]    Compare the server's current snapshot with your latest migration
    --url <url>                   server url (default: http://localhost:3000)
    --migrations <dir>            migrations directory (default: migrations)

  backup <db> [options]           Download a consistent backup of a database (.marcibin)
    --url <url>                   server url (default: http://localhost:3000)
    --out <file|dir>              where to write it (default: <db>-<YYYYMMDD-HHMMSS>.marcibin in cwd)

  restore <db> <file> [options]   Upload a backup; replaces the live database <db>
    --url <url>                   server url (default: http://localhost:3000)
    --into <name>                 restore under another name instead (created if missing)
    --yes                         confirm replacing a database that exists

  inspect <file|dir> [options]    Read a backup, or a database directory no process has open
    --tree <name>                 list one tree's entries (rows decoded through the schema)
    --limit <n>                   entries to show (default: 20)
    --id <json>                   one row of a model tree, by id
    --raw                         hex keys/values; row headers annotated

Every command talking to a server sends --token <token> (or $MARCI_TOKEN) as a bearer token.

Examples:
  marcidb generate
  marcidb generate schema.marci --name add_users
  marcidb migrate push myapp --url http://localhost:3000
  marcidb migrate check myapp
  marcidb backup myapp --out backups/
  marcidb restore myapp backups/myapp-20261012-030000.marcibin --into myapp_check
  marcidb inspect backups/myapp-20261012-030000.marcibin --tree User --limit 5
`);
}

// The server url (--url, default local) without a trailing slash
function serverUrl(flags) {
  return (flags.url ?? "http://localhost:3000").replace(/\/+$/, "");
}

// `Authorization: Bearer` for a server started with --token / MARCI_TOKEN
function authHeaders(flags) {
  const token = typeof flags.token === "string" ? flags.token : process.env.MARCI_TOKEN;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

function cmdGenerate(args) {
  const { positional, flags } = parseArgs(args);
  const schema = positional[0] ?? "schema.marci";
  const output = positional[1] ?? path.join(__dirname, "..", ".marcidb", "client");

  // 1) TypeScript types (always — without them a migration is pointless)
  if (fs.existsSync(output)) {
    fs.rmSync(output, { recursive: true, force: true });
  }
  console.log(`Generating client from ${schema}...`);
  execFileSync(binPath("marci-generate"), ["types", schema, output], { stdio: "inherit" });
  fs.writeFileSync(path.join(output, "package.json"), `{
  "name": "marci-client",
  "type": "module",
  "main": "index.js",
  "types": "index.d.ts"
}`);

  // 2) Migration file (full snapshot of the version) — via the separate marci-migrate binary
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
  if (sub === "check") return migrateCheck(rest);
  console.error(`marcidb migrate: unknown subcommand "${sub ?? ""}" (expected "push" or "check")`);
  help();
  process.exit(1);
}

// The server's current snapshot ("" if the DB doesn't exist yet)
async function fetchServerSnapshot(url, db, flags) {
  const res = await fetch(`${url}/${db}/$snapshot`, { headers: authHeaders(flags) });
  if (res.ok) return await res.text();
  if (res.status === 404) return "";
  console.error(`Cannot read server snapshot [${res.status}]: ${await res.text()}`);
  process.exit(1);
}

// Smart client: marci-migrate plan (STDIN = server snapshot) → actions of the not-yet-applied migrations
function planActions(dir, serverSnapshot) {
  try {
    return { out: execFileSync(binPath("marci-migrate"), ["plan", dir], { input: serverSnapshot, encoding: "utf8" }) };
  } catch (e) {
    return { error: (e.stderr || e.message || "").toString().trim() };
  }
}

// Push: the client computes the unapplied tail and sends it to the dumb $migrate (the server just applies it)
async function migratePush(rest) {
  const { positional, flags } = parseArgs(rest);
  const db = positional[0];
  if (!db) { console.error("marcidb migrate push: <db> name required"); process.exit(1); }

  const url = serverUrl(flags);
  const dir = flags.migrations ?? "migrations";
  if (!fs.existsSync(dir)) {
    console.error(`marcidb migrate push: migrations directory "${dir}" not found — run "marcidb generate" first`);
    process.exit(1);
  }

  const serverSnap = await fetchServerSnapshot(url, db, flags);
  const plan = planActions(dir, serverSnap);
  if (plan.error !== undefined) { console.error(`Migration planning failed: ${plan.error}`); process.exit(1); }
  if (!plan.out.trim()) { console.log(`'${db}' is up to date — nothing to push`); return; }

  const res = await fetch(`${url}/${db}/$migrate`, {
    method: "POST",
    headers: { "Content-Type": "text/plain", ...authHeaders(flags) },
    body: plan.out,
  });
  if (!res.ok) { console.error(`Migration failed [${res.status}]: ${await res.text()}`); process.exit(1); }
  console.log(`Applied pending migrations to '${db}'`);
}

// Check: whether the server matches the latest local migration (same planner: empty plan = matched)
async function migrateCheck(rest) {
  const { positional, flags } = parseArgs(rest);
  const db = positional[0];
  if (!db) { console.error("marcidb migrate check: <db> name required"); process.exit(1); }

  const url = serverUrl(flags);
  const dir = flags.migrations ?? "migrations";

  const serverSnap = await fetchServerSnapshot(url, db, flags);
  const plan = planActions(dir, serverSnap);
  if (plan.error !== undefined) {
    console.error(`'${db}' has DIVERGED — the server schema isn't a point in your migration history.`);
    process.exit(1);
  }
  if (!plan.out.trim()) {
    console.log(`'${db}' matches your latest migration ✓`);
  } else {
    console.error(`'${db}' is behind — there are pending migrations (run "marcidb migrate push ${db}").`);
    process.exit(1);
  }
}

// `20261012-120000` for a local timestamp (the server's file name carries the snapshot time instead)
function stamp() {
  return new Date().toISOString().replace(/\.\d+Z$/, "").replace(/[-:]/g, "").replace("T", "-");
}

// Backup: GET /:db/$backup streamed to <out>.tmp, renamed when complete. A download that breaks off
// (the server aborts the response when its copy fails) leaves nothing behind.
async function cmdBackup(args) {
  const { positional, flags } = parseArgs(args);
  const db = positional[0];
  if (!db) { console.error("marcidb backup: <db> name required"); process.exit(1); }

  const url = serverUrl(flags);
  const res = await fetch(`${url}/${db}/$backup`, { headers: authHeaders(flags) });
  if (!res.ok) { console.error(`Backup failed [${res.status}]: ${await res.text()}`); process.exit(1); }

  const offered = /filename="([^"]+)"/.exec(res.headers.get("content-disposition") ?? "")?.[1];
  const name = path.basename(offered ?? `${db}-${stamp()}.marcibin`);
  const outFlag = typeof flags.out === "string" ? flags.out : undefined;
  const out = outFlag === undefined ? name
    : fs.existsSync(outFlag) && fs.statSync(outFlag).isDirectory() ? path.join(outFlag, name)
    : outFlag;

  const tmp = `${out}.tmp`;
  try {
    await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(tmp));
    fs.renameSync(tmp, out);
  } catch (e) {
    fs.rmSync(tmp, { force: true });
    console.error(`Backup failed: the download broke off (${e.message ?? e}) — nothing was written`);
    process.exit(1);
  }
  console.log(`Backed up '${db}' to ${out} (${fs.statSync(out).size} bytes)`);
}

// Restore: POST the file to /:db/$restore[?into=]. Replacing a database that exists needs --yes.
async function cmdRestore(args) {
  const { positional, flags } = parseArgs(args);
  const [db, file] = positional;
  if (!db || !file) { console.error("marcidb restore: usage: marcidb restore <db> <file.marcibin> [--into <name>] [--yes]"); process.exit(1); }
  if (!fs.existsSync(file)) { console.error(`marcidb restore: "${file}" not found`); process.exit(1); }

  const url = serverUrl(flags);
  const into = typeof flags.into === "string" ? flags.into : undefined;
  const target = into ?? db;
  if (!flags.yes) {
    // Without --into the live database is replaced; with it, only when that name already exists.
    const exists = into === undefined || (await fetch(`${url}/${into}/$snapshot`, { headers: authHeaders(flags) })).status !== 404;
    if (exists) {
      console.error(
        `This replaces the database '${target}' on ${url} with the contents of ${file}:\n` +
        `everything written to '${target}' after that backup was taken is lost.\n` +
        `Run again with --yes to proceed${into === undefined ? ", or restore under another name with --into <name>" : ""}.`,
      );
      process.exit(1);
    }
  }

  const query = into === undefined ? "" : `?into=${encodeURIComponent(into)}`;
  const body = typeof fs.openAsBlob === "function" ? await fs.openAsBlob(file) : fs.readFileSync(file);
  const res = await fetch(`${url}/${db}/$restore${query}`, {
    method: "POST",
    headers: { "Content-Type": "application/x-marcidb-backup", ...authHeaders(flags) },
    body,
  });
  if (!res.ok) { console.error(`Restore failed [${res.status}]: ${await res.text()}`); process.exit(1); }
  const info = await res.json();
  console.log(`Restored ${file} into '${info.restored}' (${info.trees} trees, ${info.entries} entries, schema version ${info.schemaVersion}, taken ${info.createdAt})`);
}

// Inspect runs locally (no server): marci-migrate reads the file or the closed directory.
function cmdInspect(args) {
  const r = spawnSync(binPath("marci-migrate"), ["inspect", ...args], { stdio: "inherit" });
  if (r.error) { console.error(`marcidb inspect: ${r.error.message}`); process.exit(1); }
  process.exit(r.status ?? 1);
}

const COMMANDS = {
  generate: cmdGenerate,
  migrate: cmdMigrate,
  backup: cmdBackup,
  restore: cmdRestore,
  inspect: cmdInspect,
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
