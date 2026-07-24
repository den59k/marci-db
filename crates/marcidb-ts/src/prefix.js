import { request, requestBinary, encodeId, createDecoderRegistry } from 'marcidb-client/runtime'

// Reference to the result of a previous operation inside $transaction
export const ref = (path) => ({ $ref: path })

// Per-model field descriptors (slot order, type codes, relation targets) the binary decoder-compiler reads.
// Generated from the schema; static. See marcidb-client/runtime `createDecoderRegistry`.
const MODELS = /* generated_models */

// Fingerprint of the schema this client was generated from. Sent on binary HTTP reads (`X-Marci-Schema`); the
// server returns binary only when it matches the target DB's current schema, else JSON — the wire-format
// handshake that keeps a stale client correct (never wrong bytes). See `schema_fingerprint` (Rust).
const SCHEMA_HASH = /* generated_schema_hash */

// HTTP transport: maps a transport-neutral op descriptor `{ model, action, query/data/id }` onto the
// server's REST routes. This is the default when `marcidb()` is given a URL string. Behavior is identical
// to the original per-action helpers — the embedded transport (marcidb-embedded) implements the same
// `{ exec, batch }` interface against the native FFI instead, plus an optional `queryBinary` fast path.
function httpTransport(url) {
  return {
    exec(op) {
      switch (op.action) {
        case "findMany":  return request("POST", `${url}/${op.model}/findMany`, op.query);
        case "findFirst": return request("POST", `${url}/${op.model}/findFirst`, op.query);
        case "insert":    return request("POST", `${url}/${op.model}/insert`, op.data);
        case "update":    return request("POST", `${url}/${op.model}/update/${encodeId(op.id)}`, op.data);
        // No id in the path — the filter and the payload travel together in the body
        case "updateMany": return request("POST", `${url}/${op.model}/updateMany`, { ...(op.query ?? {}), data: op.data });
        case "delete":    return request("POST", `${url}/${op.model}/delete/${encodeId(op.id)}`);
        case "count":     return request("POST", `${url}/${op.model}/count`, op.query ?? {});
        case "aggregate": return request("POST", `${url}/${op.model}/aggregate`, op.query);
        case "$reindex":  return request("POST", `${url}/${op.model}/$reindex`);
        default: throw new Error(`marcidb: unknown action '${op.action}'`);
      }
    },
    batch(ops) {
      return request("POST", `${url}/$transaction`, ops);
    },
    // Binary read fast path over HTTP: one request that advertises `Accept: <binary>, json` + the schema
    // fingerprint. The server replies binary (→ Uint8Array) when it agrees on schema + shape, else JSON
    // (→ `{ json }`, already parsed). No fallback round-trip; `run()` handles both. Reads only.
    queryBinary(op) {
      const path = op.action === "findFirst" ? "findFirst" : "findMany";
      return requestBinary(`${url}/${op.model}/${path}`, op.query, SCHEMA_HASH);
    },
  };
}

export function marcidb(transport) {
  // Backward compatible: a URL string selects the HTTP transport. An object is used as-is and must
  // implement { exec(op), batch(ops) } — this is how marcidb-embedded plugs in the in-process FFI.
  if (typeof transport === "string") transport = httpTransport(transport);

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
        //   null       → no answer (embedded: engine can't encode this shape) → fall back to exec/JSON
        return Promise.resolve(transport.queryBinary(descriptor)).then((payload) => {
          if (payload == null) return transport.exec(descriptor);
          if (payload instanceof Uint8Array) return registry.decodeBuffer(decode, payload, many);
          return payload.json;
        });
      }
    }
    return transport.exec(descriptor);
  }

  // Lazily-executed operation: `await` runs it through the transport as a single op,
  // while `$transaction` takes only the `__op` descriptor and sends them as one batch.
  const op = (descriptor) => ({
    __op: descriptor,
    then: (onFulfilled, onRejected) => run(descriptor).then(onFulfilled, onRejected),
    catch: (onRejected) => run(descriptor).catch(onRejected),
    finally: (onFinally) => run(descriptor).finally(onFinally),
  });

  // Atomic batch transaction: array of operation descriptors → one batch call to the transport
  const $transaction = (ops) => transport.batch(ops.map((o) => o.__op));

  return {
    $transaction,
    /* generated_data */
  };
}
