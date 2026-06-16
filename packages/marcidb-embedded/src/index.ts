// Public API for embedded MarciDB. `openDatabase` opens an in-process DB; the returned handle is itself a
// transport, so `marcidb(db)` (from `marcidb-client`) gives you the typed client. `openTestDatabase` adds
// an ephemeral temp-dir DB with fsync disabled and automatic cleanup on `close()`.

import os from "node:os";
import path from "node:path";
import fs from "node:fs";

import { loadFfi } from "./loader.js";

// Resolved once at module load (top-level await): keeps `openDatabase` synchronous for callers.
const ffi = await loadFfi();

/** A transport-neutral operation descriptor (matches `MarciOp` from the generated client). */
export type MarciOp = { model: string; action: string; query?: any; data?: any; id?: any };

/** Error kinds mirror the server's HTTP error taxonomy. */
export type MarciErrorKind = "bad_request" | "not_found" | "internal";

/**
 * The transport object consumed by `marcidb()` from `marcidb-client`. `queryBinary` is the optional binary
 * read fast path the client uses when present (the HTTP transport omits it and stays JSON).
 */
export interface MarciTransport {
  exec(op: MarciOp): Promise<any>;
  batch(ops: MarciOp[]): Promise<any[]>;
  /** Returns the raw binary result buffer for a read op, or `null` to signal "fall back to JSON". */
  queryBinary?(op: MarciOp): Promise<Uint8Array | null>;
}

export interface EmbeddedOptions {
  /** Disable fsync — faster, durability-unsafe. Intended for ephemeral/test databases. */
  disableFsync?: boolean;
}

/**
 * An open embedded database. It *is* a `MarciTransport` (has `exec`/`batch`/`queryBinary`), so pass it
 * straight to `marcidb(db)` to get the typed client; it also carries the admin/lifecycle methods below.
 */
export interface EmbeddedDatabase extends MarciTransport {
  exec(op: MarciOp): Promise<any>;
  batch(ops: MarciOp[]): Promise<any[]>;
  queryBinary(op: MarciOp): Promise<Uint8Array | null>;
  /** Back-compat alias of the database itself — `marcidb(db)` and `marcidb(db.transport)` are equivalent. */
  readonly transport: MarciTransport;
  /** Declarative schema sync from `.marci` text. */
  $sync(schemaText: string): Promise<null>;
  /** Imperative migration from a single `.march` action text (not idempotent — prefer `migrate`). */
  $migrate(migrationText: string): Promise<null>;
  /**
   * Idempotent, drift-aware migrator: applies the un-applied `.march` files from `migrationsDir` (sorted by
   * name) in order. Safe to call on every startup. Throws on drift (DB matches no point in the history).
   */
  migrate(migrationsDir: string): Promise<{ applied: number; total: number }>;
  /** Current materialized schema snapshot text. */
  $snapshot(): Promise<string>;
  /** Rebuild every model's `@custom` indexes. */
  reindexAll(): Promise<{ ok: boolean; indexed: number }>;
  /** Close and release the database. Idempotent. */
  close(): void;
  readonly closed: boolean;
}

export interface TestDatabase extends EmbeddedDatabase {
  /** The temp directory backing this database (removed on `close()`). */
  path: string;
}

/** Error thrown when a native operation returns an `ok:false` envelope. `kind` mirrors the server's. */
export class MarciEmbeddedError extends Error {
  override name = "MarciEmbeddedError" as const;
  kind: MarciErrorKind;
  constructor(message: string, kind: MarciErrorKind) {
    super(message);
    this.kind = kind;
  }
}

/** Parses a result envelope string, throwing on `ok:false`, returning `data` on success. */
function unwrap(envelope: string | null): any {
  if (envelope == null) throw new MarciEmbeddedError("native call returned no result", "internal");
  let parsed: any;
  try {
    parsed = JSON.parse(envelope);
  } catch {
    throw new MarciEmbeddedError(`malformed native envelope: ${envelope}`, "internal");
  }
  if (parsed.ok) return parsed.data;
  throw new MarciEmbeddedError(parsed.error ?? "unknown error", parsed.kind ?? "internal");
}

/**
 * Opens (creating if needed) an embedded database at `dir`. The returned handle is a transport — pass it
 * straight to `marcidb(db)` — and also carries the admin/lifecycle methods.
 */
export function openDatabase(dir: string, options: EmbeddedOptions = {}): EmbeddedDatabase {
  const handle = ffi.open(dir, JSON.stringify({ disableFsync: !!options.disableFsync }));
  if (!handle) {
    throw new MarciEmbeddedError(ffi.lastError() ?? `failed to open database at '${dir}'`, "internal");
  }

  let closed = false;
  const ensureOpen = () => {
    if (closed) throw new MarciEmbeddedError("database handle is closed", "bad_request");
  };

  // The db is itself the transport `marcidb()` expects: a single op object → marci_exec; an array → an
  // atomic transaction. So you can write `marcidb(db)` directly — no `.transport` step.
  async function exec(op: MarciOp): Promise<any> {
    ensureOpen();
    return unwrap(ffi.exec(handle, JSON.stringify(op)));
  }
  async function batch(ops: MarciOp[]): Promise<any[]> {
    ensureOpen();
    return unwrap(ffi.exec(handle, JSON.stringify(ops)));
  }
  // Binary read fast path consumed by `marcidb(db)` (marcidb-client): returns the raw result payload bytes
  // for a `findMany`/`findFirst` op, or `null` when the engine can't binary-encode this shape (→ the client
  // falls back to `exec`/JSON). Throws `MarciEmbeddedError` on a real fault.
  async function queryBinary(op: MarciOp): Promise<Uint8Array | null> {
    ensureOpen();
    const body = ffi.queryBinary(handle, JSON.stringify(op));
    if (!body) return null; // NULL pointer (OOM) → fall back to JSON
    const status = body[0];
    if (status === 2) return null; // unsupported shape → fall back to JSON
    if (status === 1) throw new MarciEmbeddedError(new TextDecoder().decode(body.subarray(1)), "bad_request");
    return body.subarray(1); // status 0 → the binary result buffer
  }

  return {
    exec,
    batch,
    queryBinary,
    transport: { exec, batch, queryBinary },
    async $sync(schemaText: string) {
      ensureOpen();
      return unwrap(ffi.sync(handle, schemaText));
    },
    async $migrate(migrationText: string) {
      ensureOpen();
      return unwrap(ffi.migrate(handle, migrationText));
    },
    async migrate(migrationsDir: string) {
      ensureOpen();
      const texts = fs
        .readdirSync(migrationsDir)
        .filter((f) => f.endsWith(".march"))
        .sort()
        .map((f) => fs.readFileSync(path.join(migrationsDir, f), "utf8"));
      return unwrap(ffi.migrateApply(handle, JSON.stringify(texts)));
    },
    async $snapshot() {
      ensureOpen();
      return unwrap(ffi.snapshot(handle));
    },
    async reindexAll() {
      ensureOpen();
      return unwrap(ffi.reindexAll(handle));
    },
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
 */
export async function openTestDatabase(schema?: string, options: EmbeddedOptions = {}): Promise<TestDatabase> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "marcidb-"));
  const db = openDatabase(dir, { disableFsync: true, ...options }) as TestDatabase;

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
