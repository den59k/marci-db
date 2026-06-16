// End-to-end test of the *generated* client over the embedded transport: marcidb(db.transport) →
// typed `client.user.insert(...)`, including a @custom(fulltext) search and an atomic $transaction.
// Requires the generated client at ./.gen/index.js (run `node test/gen.mjs` first).
import assert from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { openTestDatabase } from "../src/index.js";
import { marcidb } from "./.gen/index.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const schema = fs.readFileSync(path.join(HERE, "schema.marci"), "utf8");
const runtime = typeof Bun !== "undefined" ? `bun ${Bun.version}` : `node ${process.version}`;
console.log(`[full-client] runtime: ${runtime}`);

const db = await openTestDatabase(schema);
try {
  const client = marcidb(db); // db is itself the transport

  // Typed CRUD through the generated client
  const id = await client.user.insert({ name: "Alice", age: 30 });
  assert.ok(id != null, "insert returns id");
  const users = await client.user.findMany({ name: true, age: true });
  assert.equal(users.length, 1);
  assert.equal(users[0].name, "Alice");

  // Atomic transaction built from lazy ops
  const res = await client.$transaction([
    client.user.insert({ name: "B", age: 1 }),
    client.user.insert({ name: "C", age: 2 }),
  ]);
  assert.equal(res.length, 2);
  assert.equal(await client.user.count(), 3);

  // @custom(fulltext) search through the generated client (live-maintained, no explicit reindex)
  await client.doc.insert({ title: "A", body: "the quick brown fox" });
  await client.doc.insert({ title: "B", body: "lazy dogs sleeping" });
  const hits = await client.doc.findMany({ title: true, $where: { body: { $search: "quick fox" } } });
  assert.equal(hits.length, 1, "fulltext should match exactly one doc");
  assert.equal(hits[0].title, "A");

  console.log("[full-client] ALL PASSED");
} finally {
  db.close();
}
