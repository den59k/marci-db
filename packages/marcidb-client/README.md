# marci-db

TypeScript client for [MarciDB](https://github.com/den59k/marci-db.git) — a NoSQL database with a schema-first approach.

## Installation

```bash
npm install marcidb-client
# or
bun add marcidb-client
```

## Setup

1. Create a `schema.marci` file in your project root:

```
model User {
    name    String
    posts   Post[]  @bind(Post.author)
}

model Post {
    title   String
    author  User?
}
```

2. Generate the client:

```bash
marcidb generate
# or with custom paths
marcidb generate schema.marci node_modules/.marcidb/client
```

3. Start your MarciDB server and connect — the database name is the last segment of the URL:

```typescript
import { marcidb } from "marcidb-client";

const db = marcidb("http://localhost:3000/myapp");

// a server started with --token (or an embedded app's maintenance port opened with one)
const secured = marcidb("http://localhost:3000/myapp", { token: process.env.MARCI_TOKEN });
```

## Usage

```typescript
import { marcidb } from "marcidb-client";

const db = marcidb("http://localhost:3000/myapp");

// Find — a chainable query; no select = id + every scalar
const users = await db.user.where({ name: { $startsWith: "A" } }).order("id", "desc").limit(20);
const named = await db.user.select({ id: true, name: true });
const one = await db.user.where({ id: 1 }).first();

// Insert
const id = await db.user.insert({
  name: "Alice",
});

// Update
await db.user.update({ id: 1 }, {
  name: "Bob",
});

// Delete
await db.user.delete({ id: 1 });
```

## Requirements

- Node.js 18+ or Bun
- MarciDB server running

## Re-generating types

After every change to `schema.marci`, re-run:

```bash
marcidb generate
```

If types don't update in VSCode, run **TypeScript: Restart TS Server** (`Ctrl+Shift+P`).