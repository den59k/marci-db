import { request, encodeId } from 'marcidb-client/runtime'

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

  return {
    /* generated_data */
  };
}
