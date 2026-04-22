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

interface QueryRecordsInput {
  source: DataSource;
  agentId: string;
  where?: Record<string, unknown>;
  limit: number;
  offset: number;
}

interface GetRecordByIdInput {
  source: DataSource;
  agentId: string;
  id: string;
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
}
