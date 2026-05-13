import { DataSource, RecordItem } from "../common/types";
import { MongoRecordRepository } from "./mongo-record.repository";
import { PostgresRecordRepository } from "./postgres-record.repository";

interface CreateCollectionInput {
  source: DataSource;
  agentId: string;
  jsonSchema: Record<string, unknown>;
}

interface InsertRecordInput {
  source: DataSource;
  id: string;
  agentId: string;
  schemaVersion: number;
  data: Record<string, unknown>;
}

export interface OrderBy {
  field: string;
  dir?: "ASC" | "DESC";
}

interface QueryRecordsInput {
  source: DataSource;
  agentId: string;
  where?: Record<string, unknown>;
  limit: number;
  offset: number;
  orderBy?: OrderBy[];
}

interface GetRecordByIdInput {
  source: DataSource;
  agentId: string;
  id: string;
}

interface CountRecordsInput {
  source: DataSource;
  agentId: string;
  where?: Record<string, unknown>;
}

interface UpdateRecordInput {
  source: DataSource;
  agentId: string;
  id: string;
  data: Record<string, unknown>;
}

interface DeleteRecordInput {
  source: DataSource;
  agentId: string;
  id: string;
}

export type AggOp = "COUNT" | "SUM" | "AVG" | "MIN" | "MAX";
export type TimeUnit = "DAY" | "WEEK" | "MONTH" | "YEAR";

export interface AggregateGroupKey {
  field: string;
  alias: string;
  truncate?: TimeUnit;
}

export interface AggregateMetric {
  alias: string;
  op: AggOp;
  field?: string;
}

export interface AggregateOrderBy {
  alias: string;
  dir: "ASC" | "DESC";
}

export interface AggregateBucket {
  key: Record<string, unknown>;
  metrics: Record<string, unknown>;
}

export interface AggregateRecordsInput {
  source: DataSource;
  agentId: string;
  where?: Record<string, unknown>;
  groupBy: AggregateGroupKey[];
  metrics: AggregateMetric[];
  having?: Record<string, unknown>;
  orderBy: AggregateOrderBy[];
  limit: number;
}

export class DataSourceRouterService {
  constructor(
    private readonly mongoRepository: MongoRecordRepository,
    private readonly postgresRepository: PostgresRecordRepository,
  ) {}

  public async createCollection(input: CreateCollectionInput): Promise<void> {
    if (input.source === "MONGO") {
      return this.mongoRepository.createCollection(input.agentId, input.jsonSchema);
    }

    return this.postgresRepository.createTable(input.agentId, input.jsonSchema);
  }

  public async insertRecord(input: InsertRecordInput): Promise<RecordItem> {
    if (input.source === "MONGO") {
      return this.mongoRepository.insertRecord(input);
    }

    return this.postgresRepository.insertRecord(input);
  }

  public async queryRecords(input: QueryRecordsInput): Promise<RecordItem[]> {
    if (input.source === "MONGO") {
      return this.mongoRepository.queryRecords(input);
    }

    return this.postgresRepository.queryRecords(input);
  }

  public async getRecordById(input: GetRecordByIdInput): Promise<RecordItem | null> {
    if (input.source === "MONGO") {
      return this.mongoRepository.getRecordById(input.agentId, input.id);
    }

    return this.postgresRepository.getRecordById(input.agentId, input.id);
  }

  public async countRecords(input: CountRecordsInput): Promise<number> {
    if (input.source === "MONGO") {
      return this.mongoRepository.countRecords(input.agentId, input.where);
    }

    return this.postgresRepository.countRecords(input.agentId, input.where);
  }

  public async updateRecord(input: UpdateRecordInput): Promise<RecordItem> {
    if (input.source === "MONGO") {
      return this.mongoRepository.updateRecord(input.agentId, input.id, input.data);
    }

    return this.postgresRepository.updateRecord(input.agentId, input.id, input.data);
  }

  public async deleteRecord(input: DeleteRecordInput): Promise<boolean> {
    if (input.source === "MONGO") {
      return this.mongoRepository.deleteRecord(input.agentId, input.id);
    }

    return this.postgresRepository.deleteRecord(input.agentId, input.id);
  }

  public async aggregateRecords(input: AggregateRecordsInput): Promise<AggregateBucket[]> {
    const { source, ...rest } = input;
    if (source === "MONGO") {
      return this.mongoRepository.aggregateRecords(rest);
    }
    return this.postgresRepository.aggregateRecords(rest);
  }
}
