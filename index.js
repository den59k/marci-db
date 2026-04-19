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

  return {
    user: {
      findMany: (select) => findMany("User", select),
      findFirst: (select) => findFirst("User", select),
      insert: (data) => insert("User", data),
      update: (id,data) => update("User", id, data),
      delete: (id) => runDelete("User", id)
    },
    post: {
      findMany: (select) => findMany("Post", select),
      findFirst: (select) => findFirst("Post", select),
      insert: (data) => insert("Post", data),
      update: (id,data) => update("Post", id, data),
      delete: (id) => runDelete("Post", id)
    },
    project: {
      findMany: (select) => findMany("Project", select),
      findFirst: (select) => findFirst("Project", select),
      insert: (data) => insert("Project", data),
      update: (id,data) => update("Project", id, data),
      delete: (id) => runDelete("Project", id)
    },
  };
}
