/** A transport-neutral operation descriptor (matches `MarciOp` from the generated client). */
export type MarciOp = { model: string; action: string; query?: any; data?: any; id?: any };

/** The transport object consumed by `marcidb()` from `marcidb-client`. */
export interface MarciTransport {
  exec(op: MarciOp): Promise<any>;
  batch(ops: MarciOp[]): Promise<any[]>;
}

/** Error kinds mirror the server's HTTP error taxonomy. */
export type MarciErrorKind = "bad_request" | "not_found" | "internal";

export class MarciEmbeddedError extends Error {
  name: "MarciEmbeddedError";
  kind: MarciErrorKind;
  constructor(message: string, kind: MarciErrorKind);
}

export interface EmbeddedOptions {
  /** Disable fsync — faster, durability-unsafe. Intended for ephemeral/test databases. */
  disableFsync?: boolean;
}

export interface EmbeddedDatabase {
  /** Pass to `marcidb(db.transport)` to get the typed client. */
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

/** Open (creating if needed) an embedded database at `dir`. */
export function openDatabase(dir: string, options?: EmbeddedOptions): EmbeddedDatabase;

/** Open an ephemeral temp-dir database (fsync off), optionally applying `schema`, cleaned up on `close()`. */
export function openTestDatabase(schema?: string, options?: EmbeddedOptions): Promise<TestDatabase>;

/** Absolute path to the native library for this platform (override via `MARCIDB_LIB`). */
export function resolveLibPath(): string;
