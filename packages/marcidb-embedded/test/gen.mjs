// Generates the typed client fixture used by full-client.mjs: runs the marcidb-ts codegen against
// schema.marci into ./.gen, then rewrites the `marcidb-client/runtime` import to the local runtime so the
// fixture resolves without installing marcidb-client. Runs under both Node and Bun.
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, "..", "..", ".."); // packages/marcidb-embedded/test -> repo root
const genDir = path.join(HERE, ".gen");
const schema = path.join(HERE, "schema.marci");

fs.mkdirSync(genDir, { recursive: true });

console.log("[gen] running marcidb-ts codegen…");
execFileSync("cargo", ["run", "-q", "-p", "marcidb-ts", "--", schema, genDir], {
  cwd: REPO,
  stdio: "inherit",
});

// The generated client imports `marcidb-client/runtime`; point it at the local runtime for the fixture.
const idx = path.join(genDir, "index.js");
const runtime = path.relative(genDir, path.join(REPO, "packages", "marcidb-client", "runtime", "index.js")).replace(/\\/g, "/");
fs.writeFileSync(idx, fs.readFileSync(idx, "utf8").replace("marcidb-client/runtime", runtime));
console.log(`[gen] patched runtime import -> ${runtime}`);
