import { request, requestBinary, encodeId, createDecoderRegistry, createQueryLayer } from 'marcidb-client/runtime'

// Reference to the result of a previous operation inside $transaction
export const ref = (path) => ({ $ref: path })

// Per-model field descriptors (names, kinds, type codes, relation targets) the binary decoder-compiler reads.
// Generated from the schema; static. See marcidb-client/runtime `createDecoderRegistry`.
const MODELS = /* generated_models */

// Fingerprint of the schema this client was generated from, as the binary decoder sees it (field names, kinds,
// type codes, relation targets — not slots or order). Sent with every binary read (`X-Marci-Schema` over HTTP,
// the op's `schema` over the embedded FFI); the engine returns binary only when it matches the target DB's
// current schema, else JSON — the handshake that keeps a stale client correct (never wrong bytes). See
// `wire_fingerprint` (Rust, marcidb-schema).
const SCHEMA_HASH = /* generated_schema_hash */

// HTTP transport: maps a transport-neutral op descriptor `{ model, action, query/data/id }` onto the
// server's REST routes. This is the default when `marcidb()` is given a URL string. Behavior is identical
// to the original per-action helpers — the embedded transport (marcidb-embedded) implements the same
// `{ exec, batch }` interface against the native FFI instead, plus an optional `queryBinary` fast path.
function httpTransport(url, options) {
  // Every request carries the transport's options (a bearer token) — see `HttpOptions` in the runtime.
  const send = (method, route, body) => request(method, route, body, options);
  return {
    exec(op) {
      switch (op.action) {
        case "findMany":  return send("POST", `${url}/${op.model}/findMany`, op.query);
        case "findFirst": return send("POST", `${url}/${op.model}/findFirst`, op.query);
        case "insert":    return send("POST", `${url}/${op.model}/insert`, op.data);
        case "update":    return send("POST", `${url}/${op.model}/update/${encodeId(op.id)}`, op.data);
        case "upsert":    return send("POST", `${url}/${op.model}/upsert`, { key: op.key, data: op.data, ...(op.onInsert !== undefined ? { onInsert: op.onInsert } : {}) });
        // No id in the path — the filter and the payload travel together in the body
        case "updateMany": return send("POST", `${url}/${op.model}/updateMany`, { ...(op.query ?? {}), data: op.data });
        case "delete":    return send("POST", `${url}/${op.model}/delete/${encodeId(op.id)}`);
        case "deleteMany": return send("POST", `${url}/${op.model}/deleteMany`, op.query ?? {});
        case "count":     return send("POST", `${url}/${op.model}/count`, op.query ?? {});
        case "aggregate": return send("POST", `${url}/${op.model}/aggregate`, op.query);
        case "$reindex":  return send("POST", `${url}/${op.model}/$reindex`);
        // A journal of the model's changes: create, read (`after` confirms), drop
        case "$journalOpen": return send("POST", `${url}/${op.model}/$journal/${op.journal.name}`, { on: op.journal.on });
        case "$journalRead": {
          const { name, after, limit, wait } = op.journal;
          const params = [after !== undefined && `after=${after}`, limit !== undefined && `limit=${limit}`, wait && `wait=${wait}`].filter(Boolean).join("&");
          return send("GET", `${url}/${op.model}/$journal/${name}${params ? "?" + params : ""}`);
        }
        case "$journalDrop": return send("DELETE", `${url}/${op.model}/$journal/${op.journal.name}`);
        default: throw new Error(`marcidb: unknown action '${op.action}'`);
      }
    },
    batch(ops) {
      return send("POST", `${url}/$transaction`, ops);
    },
    // Binary read fast path over HTTP: one request that advertises `Accept: <binary>, json` + the schema
    // fingerprint. The server replies binary (→ Uint8Array) when it agrees on schema + shape, else JSON
    // (→ `{ json }`, already parsed). No fallback round-trip; `run()` handles both. Reads only.
    queryBinary(op) {
      const path = op.action === "findFirst" ? "findFirst" : "findMany";
      return requestBinary(`${url}/${op.model}/${path}`, op.query, SCHEMA_HASH, options);
    },
  };
}

export function marcidb(transport, options) {
  // A URL string selects the HTTP transport, configured by `options` ({ token }). An object is used as-is and
  // must implement { exec(op), batch(ops) } — this is how marcidb-embedded plugs in the in-process FFI; it
  // carries its own configuration, so options next to it are a mistake.
  if (typeof transport === "string") transport = httpTransport(transport, options);
  else if (options !== undefined) throw new TypeError("marcidb: options configure the HTTP transport — pass them with a URL, not with a transport object");

  // Binary read fast path: a transport that exposes `queryBinary` (the embedded FFI) returns query results
  // as a compact binary buffer, decoded here by a shape-specialized, cached decoder — no JSON tax. The
  // decoder-compiler also gates: shapes it can't decode (and `null` from the engine for shapes it doesn't
  // encode yet) transparently fall back to `transport.exec` (JSON).
  const registry = createDecoderRegistry(MODELS);

  function run(descriptor) {
    if (transport.queryBinary && (descriptor.action === "findMany" || descriptor.action === "findFirst")) {
      const decode = registry.getDecoder(descriptor.model, descriptor.query);
      if (decode) {
        const many = descriptor.action === "findMany";
        // The transport may answer three ways:
        //   Uint8Array → a binary result buffer → decode it on the fast path
        //   { json }   → the transport declined binary (HTTP: schema/shape mismatch) → use the parsed result
        //   null       → no answer (embedded: schema mismatch, or a shape the engine can't encode) → exec/JSON
        // The schema fingerprint goes along so the engine can refuse a decoder compiled for another schema.
        return Promise.resolve(transport.queryBinary(descriptor, SCHEMA_HASH)).then((payload) => {
          if (payload == null) return transport.exec(descriptor);
          if (payload instanceof Uint8Array) return registry.decodeBuffer(decode, payload, many);
          return payload.json;
        });
      }
    }
    return transport.exec(descriptor);
  }

  // The query builder (`db.<model>` — where/order/limit/select/…, sub-queries, the empty-select rule) and
  // the lazy `op` come from the shared query layer; `run` is the only transport-specific piece.
  const { collection } = createQueryLayer({ models: MODELS, run });

  // Atomic batch transaction: array of operation descriptors → one batch call to the transport
  const $transaction = (ops) => transport.batch(ops.map((o) => o.__op));

  return {
    $transaction,
    /* generated_data */
  };
}
