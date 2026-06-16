// Bun test preload (see bunfig.toml) — makes `bun test` self-contained. Imported for its side effect both by
// the test runner and by the benchmark script; the build steps are idempotent and guarded so it runs once.
//
// It (1) builds the release native lib unless MARCIDB_LIB already points at one, then sets MARCIDB_LIB so the
// loader finds it; (2) rebuilds the marcidb-client runtime (the generated clients import it) and (3) this
// package's dist (the tests import it), so edits to either TS source are picked up without a manual build.
import path from "node:path";
import { spawnSync } from "node:child_process";

const HERE = import.meta.dir;
const EMBEDDED = path.resolve(HERE, "..");
const REPO = path.resolve(EMBEDDED, "..", "..");
const CLIENT = path.join(REPO, "packages", "marcidb-client");

function run(command: string, cwd: string) {
  const r = spawnSync(command, { cwd, stdio: "inherit", shell: true });
  if (r.status !== 0) throw new Error(`setup: \`${command}\` failed (exit ${r.status})`);
}

let done = false;

/** Builds the native lib + runtime + dist (once). Safe to call repeatedly. */
export function ensureBuilt() {
  if (done) return;
  done = true;

  // 1. Native library — build the release cdylib unless the caller already pointed MARCIDB_LIB at one.
  if (!process.env.MARCIDB_LIB) {
    run("cargo build --release -p marcidb-ffi", REPO);
    const file =
      process.platform === "win32" ? "marcidb_ffi.dll" : process.platform === "darwin" ? "libmarcidb_ffi.dylib" : "libmarcidb_ffi.so";
    process.env.MARCIDB_LIB = path.join(REPO, "target", "release", file);
  }

  // 2. marcidb-client runtime (generated clients import it) and 3. this package's dist (tests import it).
  run("bunx bunup src/index.ts --external .marcidb/client --out-dir runtime", CLIENT);
  run("bunx bunup src/index.ts --external koffi --external bun:ffi --out-dir dist", EMBEDDED);
}

ensureBuilt();
