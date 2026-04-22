export const typeDefs = `#graphql
  scalar JSON

  enum DataSource {
    MONGO
    POSTGRES
  }

  type CollectionResult {
    dbChoice: DataSource!
    status: String!
  }

  type RecordResult {
    id: String!
    source: DataSource!
    data: JSON!
    createdAt: String!
    updatedAt: String!
  }

  input InsertRecordInput {
    data: JSON!
  }

  input QueryRecordsInput {
    where: JSON
    limit: Int = 20
    offset: Int = 0
  }

  type Query {
    queryRecords(input: QueryRecordsInput!): [RecordResult!]!
    getRecordById(id: ID!): RecordResult
  }

  type Mutation {
    createCollection(dbChoice: DataSource!, jsonSchema: JSON!): CollectionResult!
    insertRecord(input: InsertRecordInput!): RecordResult!
  }
`;
