import { request, encodeId } from 'marcidb-client/runtime'

// Ссылка на результат предыдущей операции внутри $transaction
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

  // Лениво-исполняемая операция: then() выполняет одиночный запрос,
  // а в $transaction берётся только дескриптор __op
  const op = (descriptor, run) => ({
    __op: descriptor,
    then: (onFulfilled, onRejected) => run().then(onFulfilled, onRejected),
    catch: (onRejected) => run().catch(onRejected),
    finally: (onFinally) => run().finally(onFinally),
  });

  // Атомарная batch-транзакция: массив операций → один запрос на /$transaction
  const $transaction = (ops) => request("POST", `${url}/$transaction`, ops.map((o) => o.__op));

  return {
    $transaction,
    /* generated_data */
  };
}
