import { request, encodeId } from 'marcidb/runtime'

export function marci(url) {

  const findMany = (model, select) => {
    return request("POST", `${url}/${model}/findMany`, select);
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

  return {
    /* generated_data */
  };
}
