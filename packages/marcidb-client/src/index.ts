export async function request(method: string, url: string, body?: any) {
  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`MarciDB [${res.status}]: ${text}`);
  }

  // 204 No Content — delete/update may not return a body
  // const contentType = res.headers.get("content-type");
  // if (res.status === 204 || !contentType?.includes("application/json")) {
  //   return undefined;
  // }

  return res.json();
}


// ───────────────────────────────── binary result decoder ─────────────────────────────────
//
// The embedded FFI transport can return query results as a compact binary buffer instead of JSON. Decoding
// is driven by shape-specialized functions compiled (and cached) from the `select` + the per-model field
// descriptors the codegen emits — see `createDecoderRegistry`. This removes the JSON serialize/parse tax on
// the hot read path. Wire format (little-endian) — produced by the engine's `binary_encode`:
//
//   result buffer : [u8 version=1][u32 row_count][row…]
//   row           : selected fields in slot order, then includes (relations) in field order
//   key field     : value (no tag — ids are always present)
//   body field    : [u8 tag] (0=null, 1=present) then, if present, the value
//   to-one rel    : [u8] (0=null, 1=present) then, if present, the related row inline
//   to-many rel   : [u32 count] then that many related rows inline
//   scalar values : i64/u64/f64 → 8 bytes; bool/u8 → 1 byte; string → [u32 len][utf-8 bytes]

const BINARY_VERSION = 1;
const AGG_KEYS = ["$count", "$sum", "$avg", "$min", "$max"];

/** Forward cursor over a result buffer. One instance walks the whole buffer (rows are sequential). */
class BinReader {
  o = 0;
  private dv: DataView;
  private bytes: Uint8Array;
  private static decoder = new TextDecoder();
  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
    this.dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }
  u8() { return this.dv.getUint8(this.o++); }
  u32() { const v = this.dv.getUint32(this.o, true); this.o += 4; return v; }
  i64() { const v = Number(this.dv.getBigInt64(this.o, true)); this.o += 8; return v; }
  u64() { const v = Number(this.dv.getBigUint64(this.o, true)); this.o += 8; return v; }
  f64() { const v = this.dv.getFloat64(this.o, true); this.o += 8; return v; }
  str() {
    const len = this.u32();
    const s = BinReader.decoder.decode(this.bytes.subarray(this.o, this.o + len));
    this.o += len;
    return s;
  }
}

function readScalar(r: BinReader, t: string): any {
  switch (t) {
    case "str": return r.str();
    case "i64": return r.i64();
    case "u64": return r.u64();
    case "f64": return r.f64();
    case "bool": return r.u8() !== 0;
    case "u8": return r.u8();
    default: throw new Error(`marcidb: unknown binary type code '${t}'`);
  }
}

type FieldDesc = { n: string; k: "key" | "body" | "one" | "many"; t?: string | null; m?: string };
type Decoder = (r: BinReader) => any;

/**
 * Builds the binary decoder registry for a set of models. `MODELS` maps each model name to its ordered
 * field descriptors (emitted by codegen). Returns `getDecoder` (shape → cached decoder, or `null` when the
 * shape isn't binary-decodable → caller falls back to JSON) and `decodeBuffer` (run a decoder over a buffer).
 */
export function createDecoderRegistry(MODELS: Record<string, FieldDesc[]>) {
  const cache = new Map<string, Decoder | null>();

  const isSel = (v: any) => v !== undefined && v !== false;

  // `rel: true` selects all scalar fields of the relation (mirrors the engine's QueryOp::all).
  function allSelect(model: string): Record<string, any> {
    const out: Record<string, any> = {};
    for (const f of MODELS[model] ?? []) {
      if (f.k === "key" || f.k === "body") out[f.n] = true;
    }
    return out;
  }

  // A stable key for the result *shape* (projection + nesting), independent of select key order and of
  // shape-irrelevant keys ($where/$order/…). Derived by walking the model's fields, so it matches the
  // engine's slot-order encoding.
  function projectionKey(model: string, select: Record<string, any>): string {
    const fields = MODELS[model];
    if (!fields) return "?";
    const known = new Set(fields.map((f) => f.n));
    let key = model + "(";
    for (const f of fields) {
      const sel = select[f.n];
      if (!isSel(sel)) continue;
      if (f.k === "one" || f.k === "many") {
        const sub = sel === true ? allSelect(f.m!) : sel;
        key += f.n + "{" + projectionKey(f.m!, sub) + "}";
      } else {
        key += f.n + ",";
      }
    }
    // A selected field this client doesn't know about forces a distinct (null) decoder — see `build`. It
    // must be in the key, else `{name}` and `{name, unknown}` would collide on the same cache entry.
    const unknown = Object.keys(select).filter((k) => k[0] !== "$" && !known.has(k) && isSel(select[k])).sort();
    if (unknown.length) key += "!" + unknown.join("!");
    return key + ")";
  }

  function build(model: string, select: Record<string, any>): Decoder | null {
    const fields = MODELS[model];
    if (!fields) return null;

    // If the select references a field outside this client's schema (e.g. one added by a runtime migration
    // after the client was generated), the binary layout can't be followed safely — the engine encodes that
    // field but the decoder, driven by these descriptors, wouldn't read it, desyncing the cursor. Fall back
    // to JSON, which is self-describing. (The fix in normal use is to regenerate the client after a schema
    // change; this guard keeps a stale client *correct*, just not on the fast path.)
    const known = new Set(fields.map((f) => f.n));
    for (const k in select) {
      if (k[0] === "$") continue;
      if (!known.has(k) && isSel(select[k])) return null;
    }

    const steps: { name: string; key: boolean; t: string }[] = [];
    const rels: { name: string; many: boolean; fn: Decoder }[] = [];

    for (const f of fields) {
      const sel = select[f.n];
      if (!isSel(sel)) continue;
      if (f.k === "one" || f.k === "many") {
        // Aggregate over a relation ($count/$sum/…) is formatted JSON-side — fall back.
        if (f.k === "many" && sel && typeof sel === "object" && AGG_KEYS.some((k) => k in sel)) return null;
        const sub = sel === true ? allSelect(f.m!) : sel;
        const fn = build(f.m!, sub);
        if (!fn) return null;
        rels.push({ name: f.n, many: f.k === "many", fn });
      } else {
        if (f.t == null) return null; // selected, but not a binary-decodable scalar → fall back
        steps.push({ name: f.n, key: f.k === "key", t: f.t });
      }
    }

    return (r: BinReader) => {
      const obj: Record<string, any> = {};
      for (let i = 0; i < steps.length; i++) {
        const s = steps[i]!;
        if (s.key) {
          obj[s.name] = readScalar(r, s.t);
        } else {
          obj[s.name] = r.u8() === 0 ? null : readScalar(r, s.t);
        }
      }
      for (let i = 0; i < rels.length; i++) {
        const rel = rels[i]!;
        if (rel.many) {
          const n = r.u32();
          const arr = new Array(n);
          for (let j = 0; j < n; j++) arr[j] = rel.fn(r);
          obj[rel.name] = arr;
        } else {
          obj[rel.name] = r.u8() === 0 ? null : rel.fn(r);
        }
      }
      return obj;
    };
  }

  function getDecoder(model: string, select: Record<string, any> | undefined): Decoder | null {
    const sel = select ?? {};
    const key = projectionKey(model, sel);
    if (cache.has(key)) return cache.get(key)!;
    const fn = build(model, sel);
    cache.set(key, fn);
    return fn;
  }

  function decodeBuffer(decoder: Decoder, bytes: Uint8Array, many: boolean): any {
    const r = new BinReader(bytes);
    const version = r.u8();
    if (version !== BINARY_VERSION) throw new Error(`marcidb: unsupported binary result version ${version}`);
    const count = r.u32();
    if (many) {
      const out = new Array(count);
      for (let i = 0; i < count; i++) out[i] = decoder(r);
      return out;
    }
    return count === 0 ? null : decoder(r);
  }

  return { getDecoder, decodeBuffer };
}

export function encodeId(id: any) {
  if (typeof id === "object" && id !== null) {
    // Composite key: nested objects (a relation in @id) are expanded into a dot-path,
    // as the server expects — `chat.id=1&id=1`, not `chat=[object Object]&id=1`
    const parts: string[] = [];
    const walk = (obj: Record<string, any>, prefix: string) => {
      for (const [k, v] of Object.entries(obj)) {
        const key = prefix ? `${prefix}.${k}` : k;
        if (typeof v === "object" && v !== null) {
          walk(v, key);
        } else {
          parts.push(`${key}=${encodeURIComponent(String(v))}`);
        }
      }
    };
    walk(id, "");
    return parts.join("&");
  }
  return String(id);
}
