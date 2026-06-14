import { request, encodeId } from 'marcidb-client/runtime'

// Reference to the result of a previous operation inside $transaction
export const ref = (path) => ({ $ref: path })

export function marcidb(url) {

  const findMany = (model, select) => {
    return request("POST", `${url}/${model}/findMany`, select);
  }

  const findFirst = (model, select) => {
    return request("POST", `${url}/${model}/findFirst`, select);
  }

  const insert = (model, data) => {
    return request("POST", `${url}/${model}/insert`, data);
  }

  const update = (model, id, data) => {
    return request("POST", `${url}/${model}/update/${encodeId(id)}`, data);
  }

  const runDelete = (model, id) => {
    return request("POST", `${url}/${model}/delete/${encodeId(id)}`);
  }

  const count = (model, query) => {
    return request("POST", `${url}/${model}/count`, query ?? {});
  }

  const aggregate = (model, query) => {
    return request("POST", `${url}/${model}/aggregate`, query);
  }

  // Lazily-executed operation: then() runs a single request,
  // while $transaction takes only the __op descriptor
  const op = (descriptor, run) => ({
    __op: descriptor,
    then: (onFulfilled, onRejected) => run().then(onFulfilled, onRejected),
    catch: (onRejected) => run().catch(onRejected),
    finally: (onFinally) => run().finally(onFinally),
  });

  // Atomic batch transaction: array of operations → one request to /$transaction
  const $transaction = (ops) => request("POST", `${url}/$transaction`, ops.map((o) => o.__op));

  return {
    $transaction,
    /* generated_data */
  };
}
