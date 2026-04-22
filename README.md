# graphql-multi-db-agent-server

TypeScript GraphQL server for agent-scoped schema registration and strict JSON Schema data operations over MongoDB or Postgres.

## Stack
- Node.js + TypeScript
- Apollo Server + Express
- MongoDB (`mongodb`)
- Postgres (`pg`)
- AJV for JSON Schema validation

## Conventions
- camelCase identifiers
- dot-notation file names
- modular structure
- object-oriented service/repository classes

## Setup
1. Install dependencies
   - `npm install`
2. Copy env
   - `copy .env.example .env`
3. Set your DB URLs in `.env`
4. Start in dev mode
   - `npm run dev`
   - Auto-reloads on file changes

## Auth and tenancy
- Send header `x-agent-id` in every request.
- Server sets `tenantId = agentId`.
- All data reads/writes are scoped by `tenantId`.
- New typed endpoint: `POST /graphql/agent/:agentId` (header must match URL agent id).

## GraphQL operations

### 1) Register agent schema
```graphql
mutation CreateSchema($input: CreateAgentSchemaInput!) {
  createAgentSchema(input: $input) {
    tenantId
    agentId
    entityName
    dbChoice
    schemaVersion
  }
}
```

## Typed per-agent GraphQL endpoint

After registering schema(s) with the generic endpoint, query typed fields via:

- `POST /graphql/agent/:agentId`

Example request (agent endpoint):

```graphql
query {
  listLead(limit: 10, offset: 0) {
    id
    name
    email
    score
    source
    createdAt
  }
}
```

You can also fetch a record by id:

```graphql
query {
  getLeadById(id: "<record-id>") {
    id
    name
    email
    score
  }
}
```

And create typed records:

```graphql
mutation {
  createLead(input: { name: "Riya", email: "riya@example.com", score: 87 }) {
    id
    name
    email
    score
  }
}
```

Variables:
```json
{
  "input": {
    "entityName": "lead",
    "dbChoice": "POSTGRES",
    "jsonSchema": {
      "type": "object",
      "required": ["name", "email", "score"],
      "properties": {
        "name": { "type": "string" },
        "email": { "type": "string", "format": "email" },
        "score": { "type": "number" }
      },
      "additionalProperties": false
    }
  }
}
```

### 2) Insert data
```graphql
mutation Insert($input: InsertRecordInput!) {
  insertRecord(input: $input) {
    id
    entityName
    source
    schemaVersion
    data
  }
}
```

Variables:
```json
{
  "input": {
    "entityName": "lead",
    "data": {
      "name": "Riya",
      "email": "riya@example.com",
      "score": 87
    }
  }
}
```

### 3) Query data
```graphql
query QueryRecords($input: QueryRecordsInput!) {
  queryRecords(input: $input) {
    id
    entityName
    source
    data
    createdAt
  }
}
```

Variables:
```json
{
  "input": {
    "entityName": "lead",
    "where": {
      "score": { "$gte": 80 }
    },
    "limit": 20,
    "offset": 0
  }
}
```
