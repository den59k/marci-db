// src/client.ts
class MarciClient {
  baseUrl;
  constructor(url) {
    this.baseUrl = url.replace(/\/$/, "");
  }
  async#request(method, path, body) {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body !== undefined ? JSON.stringify(body) : undefined
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`MarciDB [${res.status}]: ${text}`);
    }
    return res.json();
  }
  findMany(model, select) {
    return this.#request("POST", `/${model}/findMany`, select);
  }
  insert(model, data) {
    return this.#request("POST", `/${model}/insert`, data);
  }
  update(model, id, data) {
    return this.#request("POST", `/${model}/update/${encodeId(id)}`, data);
  }
  delete(model, id) {
    return this.#request("POST", `/${model}/delete/${encodeId(id)}`);
  }
}
function encodeId(id) {
  if (typeof id === "object" && id !== null) {
    return Object.entries(id).map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`).join("&");
  }
  return String(id);
}

// src/index.ts
import { connect } from ".marci/client";
var marci = (url) => {
  const client = new MarciClient(url);
  return connect(client);
};
export {
  marci
};
