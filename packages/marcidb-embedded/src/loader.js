// Native library loader. Detects the runtime (Bun → `bun:ffi`, Node → `koffi`) and the platform, then
// binds the MarciDB C ABI behind a uniform `ffi` object. Each call returns the raw JSON envelope *string*
// (index.js parses + unwraps it); pointers returned by the library are read and freed here so callers
// never see a raw pointer.

import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));

// `<platform>-<arch>` → native filename. Extend as more prebuilt targets are published.
const PLATFORM_FILE = {
  "linux-x64": "marcidb-linux-x64.so",
  "linux-arm64": "marcidb-linux-arm64.so",
  "darwin-x64": "marcidb-darwin-x64.dylib",
  "darwin-arm64": "marcidb-darwin-arm64.dylib",
  "win32-x64": "marcidb-win32-x64.dll",
};

/** Absolute path to the native library for this platform (override with `MARCIDB_LIB`). */
export function resolveLibPath() {
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
  return path.join(HERE, "..", "native", file);
}

const isBun = typeof globalThis.Bun !== "undefined";

/** Loads and binds the native library, returning the uniform `ffi` object. */
export async function loadFfi() {
  const libPath = resolveLibPath();
  return isBun ? await bunBackend(libPath) : await nodeBackend(libPath);
}

// ───────────────────────────────── Bun (bun:ffi) ─────────────────────────────────

async function bunBackend(libPath) {
  const { dlopen, FFIType, CString } = await import("bun:ffi");

  // String args are passed as pointers to NUL-terminated buffers; results are pointers we read + free.
  const P = FFIType.ptr;
  const { symbols: s } = dlopen(libPath, {
    marci_open: { args: [P, P], returns: P },
    marci_close: { args: [P], returns: FFIType.void },
    marci_last_error: { args: [], returns: P },
    marci_free_string: { args: [P], returns: FFIType.void },
    marci_exec: { args: [P, P], returns: P },
    marci_sync: { args: [P, P], returns: P },
    marci_migrate: { args: [P, P], returns: P },
    marci_snapshot: { args: [P], returns: P },
    marci_reindex_all: { args: [P], returns: P },
  });

  const enc = new TextEncoder();
  const cstr = (str) => enc.encode(str + "\0"); // NUL-terminated buffer for a `char*` argument

  const readFree = (p) => {
    if (!p) return null;
    const out = new CString(p).toString();
    s.marci_free_string(p);
    return out;
  };

  return {
    open: (p, opts) => s.marci_open(cstr(p), opts == null ? null : cstr(opts)) || null,
    lastError: () => {
      const p = s.marci_last_error();
      return p ? new CString(p).toString() : null;
    },
    close: (h) => s.marci_close(h),
    exec: (h, op) => readFree(s.marci_exec(h, cstr(op))),
    sync: (h, t) => readFree(s.marci_sync(h, cstr(t))),
    migrate: (h, t) => readFree(s.marci_migrate(h, cstr(t))),
    snapshot: (h) => readFree(s.marci_snapshot(h)),
    reindexAll: (h) => readFree(s.marci_reindex_all(h)),
  };
}

// ───────────────────────────────── Node (koffi) ─────────────────────────────────

async function nodeBackend(libPath) {
  const koffi = (await import("koffi")).default;
  const lib = koffi.load(libPath);

  // `str` marshals JS string → `char*` for arguments. Results are returned as opaque `void*` so we can
  // read the string AND free the buffer (returning `str` directly would leak the Rust allocation).
  const open = lib.func("void* marci_open(str, str)");
  const close = lib.func("void marci_close(void*)");
  const lastError = lib.func("void* marci_last_error()");
  const freeString = lib.func("void marci_free_string(void*)");
  const exec = lib.func("void* marci_exec(void*, str)");
  const sync = lib.func("void* marci_sync(void*, str)");
  const migrate = lib.func("void* marci_migrate(void*, str)");
  const snapshot = lib.func("void* marci_snapshot(void*)");
  const reindexAll = lib.func("void* marci_reindex_all(void*)");

  const readString = (p) => (p ? koffi.decode(p, "char", -1) : null);
  const readFree = (p) => {
    if (!p) return null;
    const out = koffi.decode(p, "char", -1); // NUL-terminated string at the pointer
    freeString(p);
    return out;
  };

  return {
    open: (p, opts) => open(p, opts ?? null) || null,
    lastError: () => readString(lastError()),
    close: (h) => close(h),
    exec: (h, op) => readFree(exec(h, op)),
    sync: (h, t) => readFree(sync(h, t)),
    migrate: (h, t) => readFree(migrate(h, t)),
    snapshot: (h) => readFree(snapshot(h)),
    reindexAll: (h) => readFree(reindexAll(h)),
  };
}
