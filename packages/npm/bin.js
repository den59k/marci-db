#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
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

// Parses positional arguments and flags (--flag value | --flag)
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

  migrate push <db> [options]     Push migration files (.march) to a server; it applies the new ones
    --url <url>                   server url (default: http://localhost:3000)
    --migrations <dir>            migrations directory (default: migrations)

  migrate check <db> [options]    Compare the server's current snapshot with your latest migration
    --url <url>                   server url (default: http://localhost:3000)
    --migrations <dir>            migrations directory (default: migrations)

Examples:
  marcidb generate
  marcidb generate schema.marci --name add_users
  marcidb migrate push myapp --url http://localhost:3000
  marcidb migrate check myapp
`);
}

function cmdGenerate(args) {
  const { positional, flags } = parseArgs(args);
  const schema = positional[0] ?? "schema.marci";
  const output = positional[1] ?? path.join(__dirname, "..", ".marcidb", "client");

  // 1) TypeScript types (always — without them a migration is pointless)
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
async function fetchServerSnapshot(url, db) {
  const res = await fetch(`${url}/${db}/$snapshot`);
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

  const url = (flags.url ?? "http://localhost:3000").replace(/\/+$/, "");
  const dir = flags.migrations ?? "migrations";
  if (!fs.existsSync(dir)) {
    console.error(`marcidb migrate push: migrations directory "${dir}" not found — run "marcidb generate" first`);
    process.exit(1);
  }

  const serverSnap = await fetchServerSnapshot(url, db);
  const plan = planActions(dir, serverSnap);
  if (plan.error !== undefined) { console.error(`Migration planning failed: ${plan.error}`); process.exit(1); }
  if (!plan.out.trim()) { console.log(`'${db}' is up to date — nothing to push`); return; }

  const res = await fetch(`${url}/${db}/$migrate`, {
    method: "POST",
    headers: { "Content-Type": "text/plain" },
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

  const url = (flags.url ?? "http://localhost:3000").replace(/\/+$/, "");
  const dir = flags.migrations ?? "migrations";

  const serverSnap = await fetchServerSnapshot(url, db);
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
