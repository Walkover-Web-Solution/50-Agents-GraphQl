export const typeDefs = `#graphql
  scalar JSON

  enum DataSource {
    MONGO
    POSTGRES
  }

  enum SortDir {
    ASC
    DESC
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

  input OrderByInput {
    field: String!
    dir: SortDir = DESC
  }

  input QueryRecordsInput {
    where: JSON
    limit: Int = 20
    offset: Int = 0
    orderBy: [OrderByInput!]
  }

  input UpdateRecordInput {
    id: ID!
    data: JSON!
  }

  enum AggOp {
    COUNT
    SUM
    AVG
    MIN
    MAX
  }

  enum TimeUnit {
    DAY
    WEEK
    MONTH
    YEAR
  }

  input GroupKeyInput {
    """The record field to group by."""
    field: String!
    """Optional time truncation for date/datetime fields."""
    truncate: TimeUnit
    """Output alias used in the result key (defaults to field)."""
    alias: String
  }

  input MetricInput {
    """Output key in the result metrics object."""
    alias: String!
    """Aggregate operation."""
    op: AggOp!
    """Field to aggregate. Required for SUM, AVG, MIN, MAX. Ignored for COUNT."""
    field: String
  }

  input AggregateOrderByInput {
    """Either a metric alias or a group-key alias."""
    alias: String!
    dir: SortDir = DESC
  }

  input AggregateRecordsInput {
    """Where filter applied BEFORE grouping. Same operators as queryRecords."""
    where: JSON
    """Zero-or-more group keys. Empty = a single global bucket."""
    groupBy: [GroupKeyInput!]
    """One-or-more aggregate metrics."""
    metrics: [MetricInput!]!
    """Optional filter applied AFTER grouping. Maps metric alias -> operator object,
       e.g. { "total": { "$gt": 5 } }."""
    having: JSON
    """Optional ordering on metric or group-key aliases."""
    orderBy: [AggregateOrderByInput!]
    """Maximum buckets returned (1-100, default 50)."""
    limit: Int = 50
  }

  type AggregateBucket {
    """Group key values keyed by group alias. Empty object when groupBy was empty."""
    key: JSON!
    """Metric values keyed by metric alias."""
    metrics: JSON!
  }

  type Query {
    queryRecords(input: QueryRecordsInput!): [RecordResult!]!
    getRecordById(id: ID!): RecordResult
    countRecords(where: JSON): Int!
    aggregateRecords(input: AggregateRecordsInput!): [AggregateBucket!]!
  }

  type Mutation {
    createCollection(dbChoice: DataSource!, jsonSchema: JSON!): CollectionResult!
    insertRecord(input: InsertRecordInput!): RecordResult!
    updateRecord(input: UpdateRecordInput!): RecordResult!
    deleteRecord(id: ID!): Boolean!
  }
`;
