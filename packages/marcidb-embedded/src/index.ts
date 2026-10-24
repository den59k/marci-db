// Public API for embedded MarciDB. `openDatabase` opens an in-process DB; the returned handle is itself a
// transport, so `marcidb(db)` (from `marcidb-client`) gives you the typed client. `openTestDatabase` adds
// an ephemeral temp-dir DB with fsync disabled and automatic cleanup on `close()`. `restoreDatabase` opens
// a database restored from a `.marcibin` backup (`db.backup(path)`).

import os from "node:os";
import path from "node:path";
import fs from "node:fs";

import { loadFfi } from "./loader.js";

// Resolved once at module load (top-level await): keeps `openDatabase` synchronous for callers.
const ffi = await loadFfi();

/** A transport-neutral operation descriptor (matches `MarciOp` from the generated client). */
export type MarciOp = { model: string; action: string; query?: any; data?: any; id?: any; journal?: { name: string; on?: readonly string[]; after?: number; limit?: number; wait?: number } };

/** Error kinds mirror the server's HTTP error taxonomy. */
/** The class of a refused operation (the envelope's `kind`): a constraint — `unique`, `foreign_key`, `restrict`,
 *  `duplicate_id` —, `not_found`, `bad_request`, or `internal` for a fault of the engine. */
export type MarciErrorKind = "unique" | "foreign_key" | "restrict" | "duplicate_id" | "not_found" | "bad_request" | "internal";

/**
 * The transport object consumed by `marcidb()` from `marcidb-client`. `queryBinary` is the optional binary
 * read fast path the client uses when present (the HTTP transport omits it and stays JSON).
 */
export interface MarciTransport {
  exec(op: MarciOp): Promise<any>;
  batch(ops: MarciOp[]): Promise<any[]>;
  /** Returns the raw binary result buffer for a read op, or `null` to signal "fall back to JSON". `schemaHash`
   *  is the fingerprint of the schema the calling client was generated from (the generated client passes its
   *  own); the engine answers binary only for a database on that schema — without it, always `null`. */
  queryBinary?(op: MarciOp, schemaHash?: string): Promise<Uint8Array | null>;
}

export interface EmbeddedOptions {
  /** Disable fsync — faster, durability-unsafe. Intended for ephemeral/test databases. */
  disableFsync?: boolean;
}

/** What a finished backup (or restore) went through. */
export interface BackupInfo {
  /** Size of the `.marcibin` file. */
  bytes: number;
  /** Storage trees in it (models, indexes, relation trees, journals, the schema). */
  trees: number;
  /** Key/value entries over all trees. */
  entries: number;
  /** The schema version (one per applied migration) the backup sits at. */
  schemaVersion: number;
  /** The snapshot time, RFC 3339 UTC. */
  createdAt: string;
}

export interface ListenOptions {
  /** TCP port; 0 picks a free one (read it back from `port`). */
  port: number;
  /** Bind address. Default `127.0.0.1`. Any non-loopback host requires `token`. */
  host?: string;
  /** Bearer token every request must carry (`GET /$health` excepted). */
  token?: string;
  /** Database name in the URL (`/<name>/…`). Default: the basename of the database directory. */
  name?: string;
}

/** A running maintenance port (see `EmbeddedDatabase.listen`). */
export interface MaintenanceServer {
  readonly port: number;
  readonly host: string;
  /** `http://<host>:<port>/<name>` — what `marcidb(url)` takes; the CLI takes `--url http://<host>:<port>` and `<name>`. */
  readonly url: string;
  /** Stops the port (idempotent): no new connections, in-flight requests get up to 5 s. Blocks meanwhile.
   *  `db.close()` does this too. */
  close(): void;
}

export interface RestoreOptions extends EmbeddedOptions {
  /** Replace the database already in `dir` (it must be closed). Without it, a non-empty `dir` is refused. */
  overwrite?: boolean;
}

/**
 * An open embedded database. It *is* a `MarciTransport` (has `exec`/`batch`/`queryBinary`), so pass it
 * straight to `marcidb(db)` to get the typed client; it also carries the admin/lifecycle methods below.
 */
export interface EmbeddedDatabase extends MarciTransport {
  exec(op: MarciOp): Promise<any>;
  batch(ops: MarciOp[]): Promise<any[]>;
  queryBinary(op: MarciOp, schemaHash?: string): Promise<Uint8Array | null>;
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
  /**
   * Consistent backup of the whole database to `path` (`.marcibin`). The snapshot point is fixed when the
   * promise is created; the copy runs on a native thread, reads and writes continue meanwhile. The file is
   * written under a temporary name and renamed into place when complete. Nothing waits for a running
   * backup — schema changes included; it keeps seeing its own snapshot.
   */
  backup(path: string): Promise<BackupInfo>;
  /**
   * Serves the HTTP API of `marcidb-server` for this database on a local port — the maintenance port: cron
   * backups (`marcidb backup`), maintenance scripts (`marcidb(url)`) and the CLI reach the live database
   * through it. It refuses what would replace or drop the database (`$restore`, `DELETE`): the app owns it.
   * Synchronous — the port is bound when this returns. One port per database.
   */
  listen(options: ListenOptions): MaintenanceServer;
  /** Close and release the database. Idempotent. Waits for running backups; their promises still settle. */
  close(): void;
  readonly closed: boolean;
}

export interface TestDatabase extends EmbeddedDatabase {
  /** The temp directory backing this database (removed on `close()`). */
  path: string;
}

/** Error thrown when a native operation returns an `ok:false` envelope. `kind` mirrors the server's; `field` is
 *  the `Model.field` a constraint failure is about. */
export class MarciEmbeddedError extends Error {
  override name = "MarciEmbeddedError" as const;
  kind: MarciErrorKind;
  field?: string;
  constructor(message: string, kind: MarciErrorKind, field?: string) {
    super(message);
    this.kind = kind;
    this.field = field;
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
  throw new MarciEmbeddedError(parsed.error ?? "unknown error", parsed.kind ?? "internal", parsed.field);
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
  return wrapHandle(handle, dir);
}

/**
 * Restores the `.marcibin` file at `backupPath` into `dir` and opens it — use instead of `openDatabase` at
 * startup. The directory must not be open by anyone; pass `overwrite: true` to replace an existing database
 * there. Synchronous: the restore runs on the calling thread. The restored database sits at the backup's
 * schema version — `db.migrate(dir)` brings it forward.
 */
export function restoreDatabase(dir: string, backupPath: string, options: RestoreOptions = {}): EmbeddedDatabase {
  const opts = JSON.stringify({ disableFsync: !!options.disableFsync, overwrite: !!options.overwrite });
  const handle = ffi.restore(dir, backupPath, opts);
  if (!handle) {
    throw new MarciEmbeddedError(ffi.lastError() ?? `failed to restore '${backupPath}' into '${dir}'`, "internal");
  }
  return wrapHandle(handle, dir);
}

/** How often a running backup is polled. */
const BACKUP_POLL_MS = 50;

/** Blocks the calling thread for `ms` (used only by `close()`, which blocks anyway until backups end). */
function sleepSync(ms: number) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/** A database name as the server's URLs take it. */
const DB_NAME = /^[A-Za-z0-9_-]+$/;

/** The host part of a URL that reaches `host`: a wildcard bind is reached on loopback; IPv6 gets brackets. */
function urlHost(host: string): string {
  if (host === "0.0.0.0") return "127.0.0.1";
  if (host === "::") return "[::1]";
  return host.includes(":") ? `[${host}]` : host;
}

/** The `EmbeddedDatabase` object over a native handle (shared by `openDatabase` and `restoreDatabase`). */
function wrapHandle(handle: unknown, dir: string): EmbeddedDatabase {
  let closed = false;
  /** Backups not yet settled, by job id: the poll loop and `close()` settle them. */
  const pending = new Map<number, { resolve: (info: BackupInfo) => void; reject: (e: unknown) => void }>();
  const ensureOpen = () => {
    if (closed) throw new MarciEmbeddedError("database handle is closed", "bad_request");
  };

  // The db is itself the transport `marcidb()` expects: a single op object → marci_exec; an array → an
  // atomic transaction. So you can write `marcidb(db)` directly — no `.transport` step.
  async function exec(op: MarciOp): Promise<any> {
    ensureOpen();
    const result = unwrap(ffi.exec(handle, JSON.stringify(op)));
    // A journal read that waits: the engine call is synchronous and must not hold the JS thread, so an
    // empty answer is given after a pause — the reader's loop asks again, as it does when a wait runs out.
    if (op.action === "$journalRead" && op.journal?.wait && result.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    return result;
  }
  async function batch(ops: MarciOp[]): Promise<any[]> {
    ensureOpen();
    return unwrap(ffi.exec(handle, JSON.stringify(ops)));
  }
  // Binary read fast path consumed by `marcidb(db)` (marcidb-client): returns the raw result payload bytes
  // for a `findMany`/`findFirst` op, or `null` when the engine won't binary-encode it — the client's schema
  // fingerprint (`schemaHash`, baked into the generated client) differs from the database's live schema, or
  // the shape isn't binary-encodable (→ the client falls back to `exec`/JSON). Throws `MarciEmbeddedError`
  // on a real fault.
  async function queryBinary(op: MarciOp, schemaHash?: string): Promise<Uint8Array | null> {
    ensureOpen();
    const body = ffi.queryBinary(handle, JSON.stringify(schemaHash === undefined ? op : { ...op, schema: schemaHash }));
    if (!body) return null; // NULL pointer (OOM) → fall back to JSON
    const status = body[0];
    if (status === 2) return null; // schema mismatch or unsupported shape → fall back to JSON
    if (status === 1) throw new MarciEmbeddedError(new TextDecoder().decode(body.subarray(1)), "bad_request");
    return body.subarray(1); // status 0 → the binary result buffer
  }

  /** One poll of `job`; settles its promise and returns true once the backup is over. */
  function pollBackup(job: number): boolean {
    const waiter = pending.get(job);
    if (!waiter) return true;
    let result: any;
    try {
      result = unwrap(ffi.backupPoll(handle, job));
    } catch (e) {
      pending.delete(job);
      waiter.reject(e);
      return true;
    }
    if (result.state === "running") return false;
    pending.delete(job);
    const { state: _state, ...info } = result;
    waiter.resolve(info as BackupInfo);
    return true;
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
    async backup(path: string): Promise<BackupInfo> {
      // An async body runs synchronously up to its first `await`: the snapshot is taken by `backupStart`
      // before the promise is handed back.
      ensureOpen();
      const { job } = unwrap(ffi.backupStart(handle, path));
      return new Promise<BackupInfo>((resolve, reject) => {
        pending.set(job, { resolve, reject });
        const tick = () => {
          if (!closed && !pollBackup(job)) setTimeout(tick, BACKUP_POLL_MS);
        };
        setTimeout(tick, BACKUP_POLL_MS);
      });
    },
    listen(options: ListenOptions): MaintenanceServer {
      ensureOpen();
      const name = options.name ?? path.basename(path.resolve(dir));
      if (!DB_NAME.test(name)) {
        throw new MarciEmbeddedError(
          `'${name}' cannot be the database name in a URL (A-Z a-z 0-9 _ -) — pass listen({ name })`,
          "bad_request",
        );
      }
      const started = unwrap(ffi.listen(handle, JSON.stringify({ port: options.port, host: options.host, token: options.token, name })));
      let stopped = false;
      return {
        port: started.port,
        host: started.host,
        url: `http://${urlHost(started.host)}:${started.port}/${name}`,
        close() {
          if (stopped) return;
          stopped = true;
          // A closed database has stopped its port already (and the handle is gone).
          if (!closed) unwrap(ffi.listenStop(handle));
        },
      };
    },
    close() {
      if (closed) return;
      closed = true;
      // The native close stops the maintenance port first.
      // The handle is freed below, so running backups are polled to the end here (the native close would
      // wait for them anyway) and every pending promise settles with its real outcome.
      for (const job of [...pending.keys()]) {
        while (!pollBackup(job)) sleepSync(5);
      }
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
