// Shared test helper: generate a typed client from a `.marci` schema (via the marcidb-ts codegen), patch its
// `marcidb-client/runtime` import to the locally-built runtime, and import it. Memoized by schema path, so
// two tests using the same schema share one codegen run.
import path from "node:path";
import fs from "node:fs";
import { spawnSync } from "node:child_process";

const HERE = import.meta.dir;
const REPO = path.resolve(HERE, "..", "..", "..");
const RUNTIME = path.join(REPO, "packages", "marcidb-client", "runtime", "index.js");

const cache = new Map<string, Promise<any>>();

/** Codegen + import a typed client for `schemaPath`. Returns the client module (`{ marcidb, ref, ... }`). */
export function generateClient(schemaPath: string): Promise<any> {
  const cached = cache.get(schemaPath);
  if (cached) return cached;

  const name = path.basename(schemaPath).replace(/\.[^.]+$/, "");
  const outDir = path.join(HERE, ".gen", name);

  const promise = (async () => {
    fs.mkdirSync(outDir, { recursive: true });
    const r = spawnSync("cargo", ["run", "-q", "-p", "marcidb-ts", "--", schemaPath, outDir], { cwd: REPO, stdio: "inherit" });
    if (r.status !== 0) throw new Error(`codegen failed for ${schemaPath} (exit ${r.status})`);

    const idx = path.join(outDir, "index.js");
    const rel = path.relative(outDir, RUNTIME).replace(/\\/g, "/");
    fs.writeFileSync(idx, fs.readFileSync(idx, "utf8").replace("marcidb-client/runtime", rel));
    return import(`file://${idx.replace(/\\/g, "/")}`);
  })();

  cache.set(schemaPath, promise);
  return promise;
}

/** Absolute path to a fixture schema next to the test files. */
export function schemaPath(file: string): string {
  return path.join(HERE, file);
}

/** Directory the typed client for `schemaPath` is generated into (mirrors `generateClient`). */
export function generatedDir(schemaPath: string): string {
  const name = path.basename(schemaPath).replace(/\.[^.]+$/, "");
  return path.join(HERE, ".gen", name);
}

/** The `SCHEMA_HASH` baked into the generated client (the fingerprint it sends on binary HTTP reads). */
export function clientSchemaHash(schemaPath: string): string {
  const src = fs.readFileSync(path.join(generatedDir(schemaPath), "index.js"), "utf8");
  const m = src.match(/const SCHEMA_HASH = "([0-9a-f]+)"/);
  if (!m) throw new Error(`SCHEMA_HASH not found in generated client for ${schemaPath}`);
  return m[1]!;
}
