// Native library loader. Detects the runtime (Bun → `bun:ffi`, Node → `koffi`) and the platform, then
// binds the MarciDB C ABI behind a uniform `Ffi` object. String calls return the raw JSON envelope *string*
// (index.ts parses + unwraps it); `queryBinary` returns the raw framed result buffer body. Pointers returned
// by the library are read and freed here so callers never see a raw pointer.

import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));

/** Opaque native pointer (a `bun:ffi` Pointer or a koffi external) — never dereferenced by callers. */
type Ptr = unknown;

/** The uniform native binding both runtime backends produce. */
export interface Ffi {
  open(path: string, opts: string | null): Ptr | null;
  lastError(): string | null;
  close(handle: Ptr): void;
  exec(handle: Ptr, op: string): string | null;
  /** Binary read path: returns the framed body (`[u8 status][payload]`) or null on a NULL pointer. */
  queryBinary(handle: Ptr, op: string): Uint8Array | null;
  sync(handle: Ptr, schemaText: string): string | null;
  migrate(handle: Ptr, migrationText: string): string | null;
  migrateApply(handle: Ptr, migrationsJson: string): string | null;
  snapshot(handle: Ptr): string | null;
  reindexAll(handle: Ptr): string | null;
}

// `<platform>-<arch>` → native filename. Extend as more prebuilt targets are published.
const PLATFORM_FILE: Record<string, string> = {
  "linux-x64": "marcidb-linux-x64.so",
  "linux-arm64": "marcidb-linux-arm64.so",
  "darwin-x64": "marcidb-darwin-x64.dylib",
  "darwin-arm64": "marcidb-darwin-arm64.dylib",
  "win32-x64": "marcidb-win32-x64.dll",
};

/** Absolute path to the native library for this platform (override with `MARCIDB_LIB`). */
export function resolveLibPath(): string {
  const override = process.env.MARCIDB_LIB;
  if (override) return override;

  const key = `${process.platform}-${process.arch}`;
  const file = PLATFORM_FILE[key];
  if (!file) {
    throw new Error(
      `marcidb-embedded: no prebuilt native library for '${key}'. ` +
        `Build marcidb-ffi and point MARCIDB_LIB at the resulting library.`,
    );
  }
  // The bundled output lives in dist/, a sibling of native/ at the package root.
  return path.join(HERE, "..", "native", file);
}

const isBun = typeof (globalThis as any).Bun !== "undefined";

/** Loads and binds the native library, returning the uniform `Ffi` object. */
export async function loadFfi(): Promise<Ffi> {
  const libPath = resolveLibPath();
  return isBun ? await bunBackend(libPath) : await nodeBackend(libPath);
}

// ───────────────────────────────── Bun (bun:ffi) ─────────────────────────────────

async function bunBackend(libPath: string): Promise<Ffi> {
  const { dlopen, FFIType, CString, toArrayBuffer } = (await import("bun:ffi")) as any;

  // String args are passed as pointers to NUL-terminated buffers; results are pointers we read + free.
  const P = FFIType.ptr;
  const { symbols: s } = dlopen(libPath, {
    marci_open: { args: [P, P], returns: P },
    marci_close: { args: [P], returns: FFIType.void },
    marci_last_error: { args: [], returns: P },
    marci_free_string: { args: [P], returns: FFIType.void },
    marci_exec: { args: [P, P], returns: P },
    marci_query_binary: { args: [P, P], returns: P },
    marci_free_buffer: { args: [P, FFIType.u64], returns: FFIType.void },
    marci_sync: { args: [P, P], returns: P },
    marci_migrate: { args: [P, P], returns: P },
    marci_migrate_apply: { args: [P, P], returns: P },
    marci_snapshot: { args: [P], returns: P },
    marci_reindex_all: { args: [P], returns: P },
  });

  const enc = new TextEncoder();
  const cstr = (str: string) => enc.encode(str + "\0"); // NUL-terminated buffer for a `char*` argument

  const readFree = (p: any): string | null => {
    if (!p) return null;
    const out = new CString(p).toString();
    s.marci_free_string(p);
    return out;
  };

  // Reads a `[u32 n][n bytes]`-framed buffer (see marci_query_binary), copies the `n` body bytes out of
  // native memory, frees it, and returns the body (`[u8 status][payload]`) — or null on a NULL pointer.
  const readFreeBuffer = (p: any): Uint8Array | null => {
    if (!p) return null;
    const header = new Uint8Array(toArrayBuffer(p, 0, 4));
    const n = header[0]! | (header[1]! << 8) | (header[2]! << 16) | header[3]! * 0x1000000;
    const total = 4 + n;
    const body = new Uint8Array(toArrayBuffer(p, 4, n)).slice(); // copy before freeing
    s.marci_free_buffer(p, total);
    return body;
  };

  return {
    open: (p, opts) => s.marci_open(cstr(p), opts == null ? null : cstr(opts)) || null,
    lastError: () => {
      const p = s.marci_last_error();
      return p ? new CString(p).toString() : null;
    },
    close: (h) => s.marci_close(h),
    exec: (h, op) => readFree(s.marci_exec(h, cstr(op))),
    queryBinary: (h, op) => readFreeBuffer(s.marci_query_binary(h, cstr(op))),
    sync: (h, t) => readFree(s.marci_sync(h, cstr(t))),
    migrate: (h, t) => readFree(s.marci_migrate(h, cstr(t))),
    migrateApply: (h, json) => readFree(s.marci_migrate_apply(h, cstr(json))),
    snapshot: (h) => readFree(s.marci_snapshot(h)),
    reindexAll: (h) => readFree(s.marci_reindex_all(h)),
  };
}

// ───────────────────────────────── Node (koffi) ─────────────────────────────────

async function nodeBackend(libPath: string): Promise<Ffi> {
  const koffi = ((await import("koffi")) as any).default;
  const lib = koffi.load(libPath);

  // `str` marshals JS string → `char*` for arguments. Results are returned as opaque `void*` so we can
  // read the string AND free the buffer (returning `str` directly would leak the Rust allocation).
  const open = lib.func("void* marci_open(str, str)");
  const close = lib.func("void marci_close(void*)");
  const lastError = lib.func("void* marci_last_error()");
  const freeString = lib.func("void marci_free_string(void*)");
  const exec = lib.func("void* marci_exec(void*, str)");
  const queryBinary = lib.func("void* marci_query_binary(void*, str)");
  const freeBuffer = lib.func("void marci_free_buffer(void*, size_t)");
  const sync = lib.func("void* marci_sync(void*, str)");
  const migrate = lib.func("void* marci_migrate(void*, str)");
  const migrateApply = lib.func("void* marci_migrate_apply(void*, str)");
  const snapshot = lib.func("void* marci_snapshot(void*)");
  const reindexAll = lib.func("void* marci_reindex_all(void*)");

  const readString = (p: any): string | null => (p ? koffi.decode(p, "char", -1) : null);
  const readFree = (p: any): string | null => {
    if (!p) return null;
    const out = koffi.decode(p, "char", -1); // NUL-terminated string at the pointer
    freeString(p);
    return out;
  };

  // Reads a `[u32 n][n bytes]`-framed buffer, copies the `n` body bytes out, frees it, returns the body.
  const readFreeBuffer = (p: any): Uint8Array | null => {
    if (!p) return null;
    const header = koffi.decode(p, "uint8_t", 4); // [u32 n] little-endian
    const n = header[0] | (header[1] << 8) | (header[2] << 16) | header[3] * 0x1000000;
    const total = 4 + n;
    const all = koffi.decode(p, "uint8_t", total);
    freeBuffer(p, total);
    return Uint8Array.from(all.slice(4)); // the n body bytes ([u8 status][payload])
  };

  return {
    open: (p, opts) => open(p, opts ?? null) || null,
    lastError: () => readString(lastError()),
    close: (h) => close(h),
    exec: (h, op) => readFree(exec(h, op)),
    queryBinary: (h, op) => readFreeBuffer(queryBinary(h, op)),
    sync: (h, t) => readFree(sync(h, t)),
    migrate: (h, t) => readFree(migrate(h, t)),
    migrateApply: (h, json) => readFree(migrateApply(h, json)),
    snapshot: (h) => readFree(snapshot(h)),
    reindexAll: (h) => readFree(reindexAll(h)),
  };
}
