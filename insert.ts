import landmarkTags from './mock/landmarkTags.json'
import landmarks from './mock/landmarks.json'

declare var Bun: any

const host = "http://localhost:3000"

type Model = "Post" | "User" | "Project" | "File" | "LandmarkTag" | "Landmark"

// @ts-ignore
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const insert = async (model: Model, obj: any) => {
  const resp = await fetch(`${host}/${model}/insert`, {
    method: "POST",
    headers: {
      "content-data": "application/json"
    },
    body: JSON.stringify(obj)
  })
  const resp2 = await resp.json()
  console.log(`${model} inserted: ${JSON.stringify(resp2)}`)
}

const insertData = async () => {

  await insert("User", { name: "Alice", email: "alice@mail.ru" })
  await insert("User", { name: "Bob", email: "bob@mail.ru" })

  await insert("File", { name: "cat.jpg" })
  await insert("File", { name: "cats.jpg" })
  await insert("File", { name: "cats2.jpg" })
  await insert("File", { name: "birds.jpg" })

  await insert("Post", {
    "title": "First post",
    "author": { "id": 1 },
    "images": [
      { "id": 1 },
      { "id": 2 }
    ]
  })

  await insert("Post", {
    "title": "Second post",
    "author": { "id": 1 },
    "images": []
  })

  await insert("Post", {
    "title": "Unnamed post",
    "author": { "id": 2 },
    "images": [
      { id: 3 }
    ]
  })

  await insert("Post", {
    "author": { "id": 2 },
    "images": [
      { "id": 1 }
    ]
  })

  for (let tag of landmarkTags) {
    await insert("LandmarkTag", {
      name: tag.name
    })
  }
  
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
  
  const toWrite: string[] = []

  const file = Bun.file("mock/embeddings.txt")
  if (await file.exists()) {
    const text = await file.text()
    toWrite.push(...text.split("\n"))
    console.log(`Load ${toWrite.length} entries`)
  }
  
  let i = 0
  for (let landmark of landmarks) {
    let embeddings
    if (toWrite[i]) {
      embeddings = JSON.parse(toWrite[i])
    } else {
      const resp = await fetch("https://gigachat.devices.sberbank.ru/api/v1/embeddings", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${access_token}`
        },
        body: JSON.stringify({
          model: "EmbeddingsGigaR",
          input: landmark.description
        })
      })
  
      if (resp.status >= 400) {
        console.warn(await resp.text())
        break
      }
  
      const body = await resp.json()
      embeddings = body.data[0].embedding
  
      toWrite.push(JSON.stringify(embeddings))
      await Bun.write("mock/embeddings.txt", toWrite.join("\n"));
      console.log("Success fetched embedding")
    }
    
    const loc = landmark.location.split(",")
    await insert("Landmark", {
      name: landmark.name,
      description: landmark.description,
      location: [ parseFloat(loc[1]), parseFloat(loc[0]) ],
      tags: landmark.tags,
      embeddings
    })

    console.log(`write landmark ${toWrite.length} ${landmark.name}`)
    i++
  }
}

insertData()