// Micro-benchmark for the binary read transport over HTTP: the *same* running `marcidb-server`, read through
// the binary fast path (`marcidb(url)`, whose httpTransport advertises `Accept: <binary>` + the schema hash)
// vs forced JSON (a transport that posts plain JSON). Reports ops/s + speedup, and the on-the-wire payload
// size for a large read. Network latency dominates small reads — the win is wire size + skipped JSON.parse on
// big result sets. Not a unit test — run it directly:
//   bun test/http-bench.ts        (env: N=, READS=, POINTS=, REL_POSTS=, REL_AUTHORS=, REL_READS=)
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync, spawn } from "node:child_process";
import { performance } from "node:perf_hooks";

const HERE = path.dirname(fileURLToPath(import.meta.url)); // cross-runtime (Bun's import.meta.dir is Bun-only)
const REPO = path.resolve(HERE, "..", "..", "..");
const genDir = path.join(HERE, ".gen", "httpbench");
const isBun = typeof (globalThis as any).Bun !== "undefined";
const RUNTIME = isBun ? `bun ${(globalThis as any).Bun.version}` : `node ${process.version}`;

const N = Number(process.env.N ?? 20000);
const READS = Number(process.env.READS ?? 50);
const POINTS = Number(process.env.POINTS ?? 2000);
const REL_POSTS = Number(process.env.REL_POSTS ?? 10000);
const REL_AUTHORS = Number(process.env.REL_AUTHORS ?? 100);
const REL_READS = Number(process.env.REL_READS ?? 50);
const PORT = Number(process.env.PORT ?? 39818);
const DB = "bench";
const BINARY_MEDIA_TYPE = "application/x-marcidb-rows";

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

/** A transport that always posts plain JSON (no queryBinary) — the JSON baseline. */
function jsonTransport(baseUrl: string): any {
  const post = async (route: string, body?: any) => {
    const res = await fetch(`${baseUrl}/${route}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
    return res.json();
  };
  return {
    exec(op: any) {
      switch (op.action) {
        case "findMany": return post(`${op.model}/findMany`, op.query);
        case "findFirst": return post(`${op.model}/findFirst`, op.query);
        case "insert": return post(`${op.model}/insert`, op.data);
        default: throw new Error(`jsonTransport: unhandled action '${op.action}'`);
      }
    },
    batch(ops: any[]) { return post(`$transaction`, ops); },
  };
}

async function timeOps(iters: number, op: (i: number) => Promise<any>) {
  const t0 = performance.now();
  for (let i = 0; i < iters; i++) await op(i);
  return performance.now() - t0;
}
const fmt = (n: number) => Math.round(n).toLocaleString("en-US");
const randId = () => 1 + Math.floor(Math.random() * N);
const randAge = () => 18 + Math.floor(Math.random() * 60);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Build + launch the server.
const b = spawnSync("cargo", ["build", "-q", "--release", "-p", "marcidb-server"], { cwd: REPO, stdio: "inherit" });
if (b.status !== 0) throw new Error(`marcidb-server build failed (exit ${b.status})`);
const exe = path.join(REPO, "target", "release", process.platform === "win32" ? "marcidb-server.exe" : "marcidb-server");
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "marci-httpbench-"));
const server = spawn(exe, ["--host", "127.0.0.1", "--port", String(PORT), "--data", dataDir], { stdio: "ignore" });
const origin = `http://127.0.0.1:${PORT}`;
const base = `${origin}/${DB}`;

try {
  for (let i = 0; i < 200; i++) {
    try { await fetch(`${base}/$snapshot`); break; } catch { await sleep(50); }
    if (i === 199) throw new Error("server did not become ready");
  }
  const sync = await fetch(`${base}/$sync`, { method: "POST", body: SCHEMA });
  if (!sync.ok) throw new Error(`$sync failed: ${sync.status} ${await sync.text()}`);

  // Generate the typed client and point its runtime import at the local build.
  fs.mkdirSync(genDir, { recursive: true });
  fs.writeFileSync(path.join(genDir, "schema.marci"), SCHEMA);
  spawnSync("cargo", ["run", "-q", "-p", "marcidb-ts", "--", path.join(genDir, "schema.marci"), genDir], { cwd: REPO, stdio: "inherit" });
  const idxPath = path.join(genDir, "index.js");
  const runtimeRel = path.relative(genDir, path.join(REPO, "packages", "marcidb-client", "runtime", "index.js")).replace(/\\/g, "/");
  fs.writeFileSync(idxPath, fs.readFileSync(idxPath, "utf8").replace("marcidb-client/runtime", runtimeRel));
  const { marcidb } = await import(`file://${idxPath.replace(/\\/g, "/")}`);

  const binaryClient = marcidb(base);
  const jsonClient = marcidb(jsonTransport(base));

  // Seed.
  const users = Array.from({ length: N }, (_, i) => ({ name: `user${i}`, age: 18 + (i % 60), email: `user${i}@example.com` }));
  for (let i = 0; i < N; i += 1000) await jsonClient.$transaction(users.slice(i, i + 1000).map((u: any) => jsonClient.user.insert(u)));
  const posts = Array.from({ length: REL_POSTS }, (_, i) => ({ title: `post${i}`, author: { id: 1 + (i % REL_AUTHORS) } }));
  for (let i = 0; i < REL_POSTS; i += 1000) await jsonClient.$transaction(posts.slice(i, i + 1000).map((p: any) => jsonClient.post.insert(p)));

  // Payload size: same large read, binary vs JSON body bytes on the wire.
  const selectAll = { name: true, age: true, email: true };
  const jsonBytes = (await (await fetch(`${base}/User/findMany`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(selectAll) })).arrayBuffer()).byteLength;
  const hash = (fs.readFileSync(idxPath, "utf8").match(/const SCHEMA_HASH = "([0-9a-f]+)"/) ?? [])[1]!;
  const binRes = await fetch(`${base}/User/findMany`, { method: "POST", headers: { "Content-Type": "application/json", Accept: `${BINARY_MEDIA_TYPE}, application/json`, "X-Marci-Schema": hash }, body: JSON.stringify(selectAll) });
  if (!(binRes.headers.get("content-type") ?? "").includes(BINARY_MEDIA_TYPE)) throw new Error(`server did not return binary for the size probe (status ${binRes.status})`);
  const binBytes = (await binRes.arrayBuffer()).byteLength;

  const benches = [
    { title: `Select all (${fmt(N)} rows × ${READS})`, iters: READS, op: (c: any) => c.user.findMany(selectAll) },
    { title: `Point query by id (×${fmt(POINTS)})`, iters: POINTS, op: (c: any) => c.user.findFirst({ name: true, age: true, $where: { id: randId() } }) },
    { title: `Index filter WHERE age=? (×${fmt(POINTS)})`, iters: POINTS, op: (c: any) => c.user.findMany({ name: true, $where: { age: randAge() } }) },
    { title: `Nested select — ${fmt(REL_POSTS)} posts + author (×${REL_READS})`, iters: REL_READS, op: (c: any) => c.post.findMany({ title: true, author: { name: true, email: true } }) },
  ];

  console.log(`MarciDB HTTP binary vs JSON transport — ${RUNTIME}`);
  console.log(`server: ${origin}  N=${fmt(N)} users, ${fmt(REL_POSTS)} posts / ${REL_AUTHORS} authors\n`);
  console.log(`## Payload size — Select all (${fmt(N)} rows)`);
  console.log(`  JSON    ${(fmt(jsonBytes) + " B").padStart(13)}`);
  console.log(`  binary  ${(fmt(binBytes) + " B").padStart(13)}   ${(jsonBytes / binBytes).toFixed(2)}x smaller\n`);

  for (const bm of benches) {
    await bm.op(binaryClient); await bm.op(jsonClient); // warmup
    const jsonMs = await timeOps(bm.iters, () => bm.op(jsonClient));
    const binMs = await timeOps(bm.iters, () => bm.op(binaryClient));
    const jsonOps = bm.iters / (jsonMs / 1000);
    const binOps = bm.iters / (binMs / 1000);
    console.log(`## ${bm.title}`);
    console.log(`  JSON    ${(jsonMs.toFixed(1) + " ms").padStart(11)}  ${(fmt(jsonOps) + " ops/s").padStart(17)}`);
    console.log(`  binary  ${(binMs.toFixed(1) + " ms").padStart(11)}  ${(fmt(binOps) + " ops/s").padStart(17)}   ${(binOps / jsonOps).toFixed(2)}x\n`);
  }
} finally {
  server.kill();
  try { fs.rmSync(genDir, { recursive: true, force: true }); } catch { /* best-effort */ }
  try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
}
