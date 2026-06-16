import { request, encodeId } from 'marcidb-client/runtime'

// Reference to the result of a previous operation inside $transaction
export const ref = (path) => ({ $ref: path })

// HTTP transport: maps a transport-neutral op descriptor `{ model, action, query/data/id }` onto the
// server's REST routes. This is the default when `marcidb()` is given a URL string. Behavior is identical
// to the original per-action helpers — the embedded transport (marcidb-embedded) implements the same
// `{ exec, batch }` interface against the native FFI instead.
function httpTransport(url) {
  return {
    exec(op) {
      switch (op.action) {
        case "findMany":  return request("POST", `${url}/${op.model}/findMany`, op.query);
        case "findFirst": return request("POST", `${url}/${op.model}/findFirst`, op.query);
        case "insert":    return request("POST", `${url}/${op.model}/insert`, op.data);
        case "update":    return request("POST", `${url}/${op.model}/update/${encodeId(op.id)}`, op.data);
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
  };
}

export function marcidb(transport) {
  // Backward compatible: a URL string selects the HTTP transport. An object is used as-is and must
  // implement { exec(op), batch(ops) } — this is how marcidb-embedded plugs in the in-process FFI.
  if (typeof transport === "string") transport = httpTransport(transport);

  // Lazily-executed operation: `await` runs it through the transport as a single op,
  // while `$transaction` takes only the `__op` descriptor and sends them as one batch.
  const op = (descriptor) => ({
    __op: descriptor,
    then: (onFulfilled, onRejected) => transport.exec(descriptor).then(onFulfilled, onRejected),
    catch: (onRejected) => transport.exec(descriptor).catch(onRejected),
    finally: (onFinally) => transport.exec(descriptor).finally(onFinally),
  });

  // Atomic batch transaction: array of operation descriptors → one batch call to the transport
  const $transaction = (ops) => transport.batch(ops.map((o) => o.__op));

  return {
    $transaction,
    /* generated_data */
  };
}
