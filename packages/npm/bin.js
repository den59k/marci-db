#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import os from "node:os";
import fs from "node:fs"

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const BINS = {
  "linux-x64":    "marci-generate-linux-x64",
  "darwin-arm64": "marci-generate-darwin-arm64",
  "win32-x64":    "marci-generate-win32-x64.exe",
};

const COMMANDS = {
  generate: cmdGenerate,
};

function help() {
  console.log(`
marcidb <command> [options]

Commands:
  generate [schema] [output]   Generate TypeScript types from schema
                               schema  — path to .marci file (default: schema.marci)
                               output  — output directory  (default: node_modules/marci-db/generated)

Examples:
  marcidb generate
  marcidb generate schema.marci
  marcidb generate schema.marci node_modules/.marcidb/client
  `);
}

function cmdGenerate(args) {
  const schema = args[0] ?? "schema.marci";
  const output = args[1] ?? "node_modules/.marcidb/client";

  const key = `${os.platform()}-${os.arch()}`;
  const name = BINS[key];

  if (!name) {
    console.error(`marci-db: unsupported platform ${key}`);
    process.exit(1);
  }

  const bin = path.join(__dirname, "bin", name);

  // Удаляем папку перед генерацией
  if (fs.existsSync(output)) {
    fs.rmSync(output, { recursive: true, force: true });
    console.log(`Cleared ${output}`);
  }

  console.log(`Generating from ${schema} into ${output}...`);

  try {
    execFileSync(bin, [schema, output], { stdio: "inherit" });

    fs.writeFileSync(output + "/package.json", `{
  "name": "maci-client",
  "main": "index.js",
  "types": "index.d.ts"
}`)

    console.log("Done.");
  } catch (e) {
    process.exit(e.status ?? 1);
  }
}
// --- main ---

const [command, ...args] = process.argv.slice(2);

if (!command || command === "help" || command === "--help" || command === "-h") {
  help();
  process.exit(0);
}

const fn = COMMANDS[command];

if (!fn) {
  console.error(`marci: unknown command "${command}"`);
  help();
  process.exit(1);
}

fn(args);