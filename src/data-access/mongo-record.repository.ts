import { Db } from "mongodb";
import { RecordItem } from "../common/types";

interface InsertMongoRecordInput {
  id: string;
  agentId: string;
  schemaVersion: number;
  data: Record<string, unknown>;
}

interface QueryMongoRecordsInput {
  agentId: string;
  where?: Record<string, unknown>;
  limit: number;
  offset: number;
}

export class MongoRecordRepository {
  constructor(private readonly mongoDb: Db) {}

  public async createCollection(
    agentId: string,
    jsonSchema: Record<string, unknown>,
  ): Promise<void> {
    const name = this.collectionName(agentId);
    const existing = await this.mongoDb.listCollections({ name }).toArray();

    const validator = this.buildMongoValidator(jsonSchema);

    if (existing.length === 0) {
      await this.mongoDb.createCollection(name, {
        validator: { $jsonSchema: validator },
      });
    } else {
      await this.mongoDb.command({
        collMod: name,
        validator: { $jsonSchema: validator },
      });
    }

    await this.mongoDb.collection(name).createIndex(
      { createdAt: -1 },
      { name: "created_desc" },
    );
  }

  public async insertRecord(input: InsertMongoRecordInput): Promise<RecordItem> {
    const now = new Date().toISOString();
    const collection = this.collectionName(input.agentId);

    const doc = {
      id: input.id,
      schemaVersion: input.schemaVersion,
      ...input.data,
      createdAt: now,
      updatedAt: now,
    };

    await this.mongoDb.collection(collection).insertOne(doc);

    return {
      id: input.id,
      agentId: input.agentId,
      schemaVersion: input.schemaVersion,
      source: "MONGO",
      data: input.data,
      createdAt: now,
      updatedAt: now,
    };
  }

  public async queryRecords(input: QueryMongoRecordsInput): Promise<RecordItem[]> {
    const collection = this.collectionName(input.agentId);
    const filter = input.where ? this.buildFilter(input.where) : {};

    const docs = await this.mongoDb
      .collection(collection)
      .find(filter)
      .skip(input.offset)
      .limit(input.limit)
      .sort({ createdAt: -1 })
      .toArray();

    return docs.map((doc) => {
      const data: Record<string, unknown> = { ...doc };
      delete data._id;
      delete data.id;
      delete data.schemaVersion;
      delete data.createdAt;
      delete data.updatedAt;

      return {
        id: String(doc.id),
        agentId: input.agentId,
        schemaVersion: Number(doc.schemaVersion ?? 1),
        source: "MONGO",
        data,
        createdAt: String(doc.createdAt),
        updatedAt: String(doc.updatedAt),
      };
    });
  }

  public async getRecordById(agentId: string, id: string): Promise<RecordItem | null> {
    const collection = this.collectionName(agentId);
    const doc = await this.mongoDb.collection(collection).findOne({ id });

    if (!doc) {
      return null;
    }

    const data: Record<string, unknown> = { ...doc };
    delete data._id;
    delete data.id;
    delete data.schemaVersion;
    delete data.createdAt;
    delete data.updatedAt;

    return {
      id: String(doc.id),
      agentId,
      schemaVersion: Number(doc.schemaVersion ?? 1),
      source: "MONGO",
      data,
      createdAt: String(doc.createdAt),
      updatedAt: String(doc.updatedAt),
    };
  }

  private collectionName(agentId: string): string {
    return `agent_${agentId}`;
  }

  private buildMongoValidator(jsonSchema: Record<string, unknown>): Record<string, unknown> {
    const properties = (jsonSchema.properties ?? {}) as Record<string, Record<string, unknown>>;
    const required = (jsonSchema.required ?? []) as string[];

    const bsonProperties: Record<string, unknown> = {
      id: { bsonType: "string" },
      schemaVersion: { bsonType: "int" },
      createdAt: { bsonType: "string" },
      updatedAt: { bsonType: "string" },
    };

    for (const [name, fieldSchema] of Object.entries(properties)) {
      bsonProperties[name] = { bsonType: this.jsonTypeToBsonType(fieldSchema) };
    }

    return {
      bsonType: "object",
      required: ["id", ...required],
      properties: bsonProperties,
    };
  }

  private jsonTypeToBsonType(fieldSchema: Record<string, unknown>): string {
    const type = fieldSchema.type;

    if (type === "integer") return "int";
    if (type === "number") return "double";
    if (type === "boolean") return "bool";
    if (type === "array") return "array";
    if (type === "object") return "object";

    return "string";
  }

  private buildFilter(where: Record<string, unknown>): Record<string, unknown> {
    const filter: Record<string, unknown> = {};

    for (const [field, value] of Object.entries(where)) {
      if (value && typeof value === "object" && !Array.isArray(value)) {
        const operatorObject = value as Record<string, unknown>;
        const converted: Record<string, unknown> = {};

        for (const [operator, operatorValue] of Object.entries(operatorObject)) {
          if (
            operator === "$eq" ||
            operator === "$ne" ||
            operator === "$gt" ||
            operator === "$lt" ||
            operator === "$gte" ||
            operator === "$lte" ||
            operator === "$in"
          ) {
            converted[operator] = operatorValue;
          }
        }

        filter[field] = converted;
        continue;
      }

      filter[field] = value;
    }

    return filter;
  }
}
