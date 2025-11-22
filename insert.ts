const host = "http://localhost:3000"

type Model = "Post" | "User" | "Project" | "File"

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

  await insert("User", { name: "Alice" })
  await insert("User", { name: "Bob" })

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

}

insertData()