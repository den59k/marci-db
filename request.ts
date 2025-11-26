
// @ts-ignore
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

declare var Bun: any

const connect = async () => {
  const resp = await fetch("https://ngw.devices.sberbank.ru:9443/api/v2/oauth", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "RqUID": Bun.env.API_ID,
      "Authorization": `Basic ${Bun.env.API_SECRET}`,
    },
    body: "scope=GIGACHAT_API_PERS"
  })
  const { access_token } = await resp.json()

  while (true) {
    const input = prompt("Введите ваш запрос:");
    
    const resp = await fetch("https://gigachat.devices.sberbank.ru/api/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${access_token}`
      },
      body: JSON.stringify({
        model: "EmbeddingsGigaR",
        input: input
      })
    })

    const body = await resp.json()
    const embeddings = body.data[0].embedding

    const landmarksResp = await fetch("http://localhost:3000/Landmark/findMany", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        "id": true,
        "name": true,
        "$where": {
          "embeddings": {
            "$close": embeddings,
            "$take": 10,
            "$threshold": 0.02
          }
        }
      })
    })

    if (landmarksResp.status >= 400) {
      console.log(await landmarksResp.text())
      break
    }

    const arr = await landmarksResp.json()
    for (let item of arr.slice(0, 5)) {
      console.log(item)
    }

  }
}

connect()