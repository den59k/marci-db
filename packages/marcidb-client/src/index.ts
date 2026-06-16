export async function request(method: string, url: string, body?: any) {
  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`MarciDB [${res.status}]: ${text}`);
  }

  // 204 No Content — delete/update may not return a body
  // const contentType = res.headers.get("content-type");
  // if (res.status === 204 || !contentType?.includes("application/json")) {
  //   return undefined;
  // }

  return res.json();
}


export function encodeId(id: any) {
  if (typeof id === "object" && id !== null) {
    // Composite key: nested objects (a relation in @id) are expanded into a dot-path,
    // as the server expects — `chat.id=1&id=1`, not `chat=[object Object]&id=1`
    const parts: string[] = [];
    const walk = (obj: Record<string, any>, prefix: string) => {
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
