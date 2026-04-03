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

  // 204 No Content — delete/update могут не возвращать тело
  // const contentType = res.headers.get("content-type");
  // if (res.status === 204 || !contentType?.includes("application/json")) {
  //   return undefined;
  // }

  return res.json();
}


export function encodeId(id: any) {
  if (typeof id === "object" && id !== null) {
    return Object.entries(id)
      .map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`)
      .join("&");
  }
  return String(id);
}
