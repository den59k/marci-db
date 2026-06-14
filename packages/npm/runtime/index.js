// src/index.ts
async function request(method, url, body) {
  const res = await fetch(url, {
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
function encodeId(id) {
  if (typeof id === "object" && id !== null) {
    const parts = [];
    const walk = (obj, prefix) => {
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
export {
  request,
  encodeId
};
