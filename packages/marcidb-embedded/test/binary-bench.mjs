// Micro-benchmark isolating the binary read transport: the *same* embedded DB, read through the binary fast
// path (`marcidb(db)`) vs forced JSON (a transport without `queryBinary`). Reports ops/s and the speedup, so
// we can see how much of the read JSON tax the binary path removes (and whether Part 2 is worth pursuing).
//
// Run:  MARCIDB_LIB=… node test/binary-bench.mjs   |   bun test/binary-bench.mjs   (env: N=, READS=, POINTS=)
import path from "node:path";
import fs from "node:fs";
import { execFileSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

import { openTestDatabase } from "../src/index.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, "..", "..", "..");
const genDir = path.join(HERE, ".gen-bench");
const isBun = typeof Bun !== "undefined";
const RUNTIME = isBun ? `bun ${Bun.version}` : `node ${process.version}`;

const N = Number(process.env.N ?? 20000);
const READS = Number(process.env.READS ?? 50);
const POINTS = Number(process.env.POINTS ?? 20000);
const REL_POSTS = Number(process.env.REL_POSTS ?? 10000);
const REL_AUTHORS = Number(process.env.REL_AUTHORS ?? 100);
const REL_READS = Number(process.env.REL_READS ?? 20);
const IDX_READS = Number(process.env.IDX_READS ?? 5000);

const SCHEMA = `model User {
  name  String
  age   Int @index
  email String
  posts Post[] @bind(Post.author)
}
model Post {
  title  String
  author User?
}`;

// Generate the typed client and point its runtime import at the local build.
fs.mkdirSync(genDir, { recursive: true });
fs.writeFileSync(path.join(genDir, "schema.marci"), SCHEMA);
execFileSync("cargo", ["run", "-q", "-p", "marcidb-ts", "--", path.join(genDir, "schema.marci"), genDir], { cwd: REPO, stdio: "inherit" });
const idxPath = path.join(genDir, "index.js");
const runtimeRel = path.relative(genDir, path.join(REPO, "packages", "marcidb-client", "runtime", "index.js")).replace(/\\/g, "/");
fs.writeFileSync(idxPath, fs.readFileSync(idxPath, "utf8").replace("marcidb-client/runtime", runtimeRel));
const { marcidb } = await import(`file://${idxPath.replace(/\\/g, "/")}`);

async function timeOps(iters, op) {
  const t0 = performance.now();
  for (let i = 0; i < iters; i++) await op(i);
  return performance.now() - t0;
}
const fmt = (n) => Math.round(n).toLocaleString("en-US");
const randId = () => 1 + Math.floor(Math.random() * N);
const randAge = () => 18 + Math.floor(Math.random() * 60);

const db = await openTestDatabase(SCHEMA);
const binaryClient = marcidb(db);
const jsonClient = marcidb({ exec: db.exec, batch: db.batch }); // no queryBinary → JSON path

try {
  // seed users
  const users = Array.from({ length: N }, (_, i) => ({ name: `user${i}`, age: 18 + (i % 60), email: `user${i}@example.com` }));
  for (let i = 0; i < N; i += 1000) await jsonClient.$transaction(users.slice(i, i + 1000).map((u) => jsonClient.user.insert(u)));
  // seed posts with shared authors (first REL_AUTHORS users)
  const posts = Array.from({ length: REL_POSTS }, (_, i) => ({ title: `post${i}`, author: { id: 1 + (i % REL_AUTHORS) } }));
  for (let i = 0; i < REL_POSTS; i += 1000) await jsonClient.$transaction(posts.slice(i, i + 1000).map((p) => jsonClient.post.insert(p)));

  const benches = [
    { title: `Select all (${fmt(N)} rows × ${READS})`, iters: READS, op: (c) => c.user.findMany({ name: true, age: true, email: true }) },
    { title: `Point query by id (${fmt(POINTS)})`, iters: POINTS, op: (c) => c.user.findFirst({ name: true, age: true, $where: { id: randId() } }) },
    { title: `Index filter WHERE age=? (×${fmt(IDX_READS)})`, iters: IDX_READS, op: (c) => c.user.findMany({ name: true, $where: { age: randAge() } }) },
    { title: `Nested select — ${fmt(REL_POSTS)} posts + author (×${REL_READS})`, iters: REL_READS, op: (c) => c.post.findMany({ title: true, author: { name: true, email: true } }) },
  ];

  console.log(`MarciDB binary vs JSON transport — ${RUNTIME}`);
  console.log(`N=${fmt(N)} users, ${fmt(REL_POSTS)} posts / ${REL_AUTHORS} authors\n`);

  for (const b of benches) {
    // warmup both paths (compile decoder / hot JIT), then measure
    await b.op(binaryClient); await b.op(jsonClient);
    const jsonMs = await timeOps(b.iters, () => b.op(jsonClient));
    const binMs = await timeOps(b.iters, () => b.op(binaryClient));
    const jsonOps = b.iters / (jsonMs / 1000);
    const binOps = b.iters / (binMs / 1000);
    console.log(`## ${b.title}`);
    console.log(`  JSON    ${(jsonMs.toFixed(1) + " ms").padStart(11)}  ${(fmt(jsonOps) + " ops/s").padStart(17)}`);
    console.log(`  binary  ${(binMs.toFixed(1) + " ms").padStart(11)}  ${(fmt(binOps) + " ops/s").padStart(17)}   ${(binOps / jsonOps).toFixed(2)}x\n`);
  }
} finally {
  db.close();
  fs.rmSync(genDir, { recursive: true, force: true });
}
