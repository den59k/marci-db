// Public API for embedded MarciDB. `openDatabase` opens an in-process DB and exposes a `transport` that
// plugs into the generated `marcidb()` client (from `marcidb-client`); `openTestDatabase` adds an
// ephemeral temp-dir DB with fsync disabled and automatic cleanup on `close()`.

import os from "node:os";
import path from "node:path";
import fs from "node:fs";

import { loadFfi } from "./loader.js";

// Resolved once at module load (top-level await): keeps `openDatabase` synchronous for callers.
const ffi = await loadFfi();

/** Error thrown when a native operation returns an `ok:false` envelope. `kind` mirrors the server's. */
export class MarciEmbeddedError extends Error {
  constructor(message, kind) {
    super(message);
    this.name = "MarciEmbeddedError";
    this.kind = kind;
  }
}

/** Parses a result envelope string, throwing on `ok:false`, returning `data` on success. */
function unwrap(envelope) {
  if (envelope == null) throw new MarciEmbeddedError("native call returned no result", "internal");
  let parsed;
  try {
    parsed = JSON.parse(envelope);
  } catch {
    throw new MarciEmbeddedError(`malformed native envelope: ${envelope}`, "internal");
  }
  if (parsed.ok) return parsed.data;
  throw new MarciEmbeddedError(parsed.error ?? "unknown error", parsed.kind ?? "internal");
}

/**
 * Opens (creating if needed) an embedded database at `dir`.
 * @param {string} dir filesystem directory for the database
 * @param {{ disableFsync?: boolean }} [options]
 * @returns a handle with `transport`, `$sync`, `$migrate`, `$snapshot`, `reindexAll`, `close`.
 */
export function openDatabase(dir, options = {}) {
  const handle = ffi.open(dir, JSON.stringify({ disableFsync: !!options.disableFsync }));
  if (!handle) {
    throw new MarciEmbeddedError(ffi.lastError() ?? `failed to open database at '${dir}'`, "internal");
  }

  let closed = false;
  const ensureOpen = () => {
    if (closed) throw new MarciEmbeddedError("database handle is closed", "bad_request");
  };

  // The transport handed to marcidb(): a single op object → marci_exec; an array → atomic transaction.
  const transport = {
    async exec(op) {
      ensureOpen();
      return unwrap(ffi.exec(handle, JSON.stringify(op)));
    },
    async batch(ops) {
      ensureOpen();
      return unwrap(ffi.exec(handle, JSON.stringify(ops)));
    },
  };

  return {
    transport,
    /** Declarative schema sync from `.marci` text. */
    async $sync(schemaText) {
      ensureOpen();
      return unwrap(ffi.sync(handle, schemaText));
    },
    /** Imperative migration from a single `.march` action text (not idempotent — prefer `migrate`). */
    async $migrate(migrationText) {
      ensureOpen();
      return unwrap(ffi.migrate(handle, migrationText));
    },
    /**
     * Idempotent, drift-aware migrator: applies the un-applied `.march` files from `migrationsDir` (sorted
     * by name) in order, skipping ones already applied. Safe to call on every startup. Returns
     * `{ applied, total }`. Throws (`kind: "bad_request"`) on drift — the DB matches no point in the history.
     */
    async migrate(migrationsDir) {
      ensureOpen();
      const texts = fs
        .readdirSync(migrationsDir)
        .filter((f) => f.endsWith(".march"))
        .sort()
        .map((f) => fs.readFileSync(path.join(migrationsDir, f), "utf8"));
      return unwrap(ffi.migrateApply(handle, JSON.stringify(texts)));
    },
    /** Current materialized schema snapshot text. */
    async $snapshot() {
      ensureOpen();
      return unwrap(ffi.snapshot(handle));
    },
    /** Rebuild every model's `@custom` indexes. */
    async reindexAll() {
      ensureOpen();
      return unwrap(ffi.reindexAll(handle));
    },
    /** Close and release the database. Idempotent. */
    close() {
      if (closed) return;
      closed = true;
      ffi.close(handle);
    },
    get closed() {
      return closed;
    },
  };
}

/**
 * Opens an ephemeral database in a fresh temp directory with fsync disabled, applies `schema` (if given)
 * via `$sync`, and arranges for `close()` to also remove the temp directory. Ideal for integration tests.
 * @param {string} [schema] `.marci` schema text to sync on open
 * @param {{ disableFsync?: boolean }} [options]
 */
export async function openTestDatabase(schema, options = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "marcidb-"));
  const db = openDatabase(dir, { disableFsync: true, ...options });

  const close = db.close;
  db.close = () => {
    close();
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {
      /* best-effort cleanup */
    }
  };
  db.path = dir;

  if (schema) await db.$sync(schema);
  return db;
}

export { resolveLibPath } from "./loader.js";
