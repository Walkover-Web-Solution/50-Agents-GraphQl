import { Db } from "mongodb";
import { RecordItem } from "../common/types";

interface InsertMongoRecordInput {
  id: string;
  agentId: string;
  schemaVersion: number;
  data: Record<string, unknown>;
}

interface OrderByMongo {
  field: string;
  dir?: "ASC" | "DESC";
}

interface QueryMongoRecordsInput {
  agentId: string;
  where?: Record<string, unknown>;
  limit: number;
  offset: number;
  orderBy?: OrderByMongo[];
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
      // Best-effort validator refresh. Some hosted Mongo tiers (Atlas
      // free/shared) deny `collMod` to non-dbAdmin users. The validator is
      // not load-bearing here — application-level Ajv already validates
      // payloads before insert — so we log and continue instead of failing
      // the whole call.
      try {
        await this.mongoDb.command({
          collMod: name,
          validator: { $jsonSchema: validator },
        });
      } catch (err) {
        const code = (err as { code?: number; codeName?: string })?.code;
        const codeName = (err as { codeName?: string })?.codeName;
        if (code === 13 || codeName === "Unauthorized") {
          // eslint-disable-next-line no-console
          console.warn(
            `[mongo] skipping collMod validator update for ${name}: ` +
              `user lacks dbAdmin. App-level validation still enforced.`,
          );
        } else {
          throw err;
        }
      }
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

    // Build sort spec. Default to createdAt desc for backwards compat when no
    // orderBy is supplied. Each orderBy entry is converted to { field: 1|-1 }.
    const sortSpec: Record<string, 1 | -1> =
      input.orderBy && input.orderBy.length > 0
        ? input.orderBy.reduce<Record<string, 1 | -1>>((acc, ob) => {
            acc[ob.field] = ob.dir === "ASC" ? 1 : -1;
            return acc;
          }, {})
        : { createdAt: -1 };

    const docs = await this.mongoDb
      .collection(collection)
      .find(filter)
      .skip(input.offset)
      .limit(input.limit)
      .sort(sortSpec)
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
      // Logical: $and / $or take an array of sub-where objects.
      if (field === "$and" || field === "$or") {
        if (!Array.isArray(value)) continue;
        filter[field] = (value as Array<Record<string, unknown>>).map((entry) =>
          this.buildFilter(entry),
        );
        continue;
      }

      // Logical: $not takes a single sub-where object.
      if (field === "$not") {
        if (!value || typeof value !== "object" || Array.isArray(value)) continue;
        filter["$nor"] = [this.buildFilter(value as Record<string, unknown>)];
        continue;
      }

      // Field-level operator object: { $eq: ..., $contains: ..., ... }
      if (value && typeof value === "object" && !Array.isArray(value)) {
        const operatorObject = value as Record<string, unknown>;
        const converted: Record<string, unknown> = {};

        for (const [operator, operatorValue] of Object.entries(operatorObject)) {
          switch (operator) {
            case "$eq":
            case "$ne":
            case "$gt":
            case "$lt":
            case "$gte":
            case "$lte":
            case "$in":
            case "$nin":
              converted[operator] = operatorValue;
              break;
            case "$contains":
              converted["$regex"] = escapeRegex(String(operatorValue));
              converted["$options"] = "i";
              break;
            case "$startsWith":
              converted["$regex"] = "^" + escapeRegex(String(operatorValue));
              converted["$options"] = "i";
              break;
            case "$endsWith":
              converted["$regex"] = escapeRegex(String(operatorValue)) + "$";
              converted["$options"] = "i";
              break;
            case "$exists":
              converted["$exists"] = Boolean(operatorValue);
              break;
            // unknown operators are dropped silently — they're rejected
            // upstream in validateWhereOperators so we shouldn't see them.
          }
        }

        filter[field] = converted;
        continue;
      }

      filter[field] = value;
    }

    return filter;
  }

  public async countRecords(
    agentId: string,
    where?: Record<string, unknown>,
  ): Promise<number> {
    const collection = this.collectionName(agentId);
    const filter = where ? this.buildFilter(where) : {};
    return this.mongoDb.collection(collection).countDocuments(filter);
  }

  public async updateRecord(
    agentId: string,
    id: string,
    data: Record<string, unknown>,
  ): Promise<RecordItem> {
    const collection = this.collectionName(agentId);
    const now = new Date().toISOString();

    // Patch update: only set provided fields. Never overwrite system keys.
    const $set: Record<string, unknown> = { ...data, updatedAt: now };
    delete ($set as any).id;
    delete ($set as any)._id;
    delete ($set as any).createdAt;
    delete ($set as any).schemaVersion;

    const result = await this.mongoDb
      .collection(collection)
      .findOneAndUpdate(
        { id },
        { $set },
        { returnDocument: "after" },
      );

    const doc = (result as any)?.value ?? result;
    if (!doc) {
      throw new Error(`Record ${id} not found`);
    }

    const dataOut: Record<string, unknown> = { ...doc };
    delete dataOut._id;
    delete dataOut.id;
    delete dataOut.schemaVersion;
    delete dataOut.createdAt;
    delete dataOut.updatedAt;

    return {
      id: String(doc.id),
      agentId,
      schemaVersion: Number(doc.schemaVersion ?? 1),
      source: "MONGO",
      data: dataOut,
      createdAt: String(doc.createdAt),
      updatedAt: String(doc.updatedAt),
    };
  }

  public async deleteRecord(agentId: string, id: string): Promise<boolean> {
    const collection = this.collectionName(agentId);
    const result = await this.mongoDb.collection(collection).deleteOne({ id });
    return (result.deletedCount ?? 0) > 0;
  }

  public async aggregateRecords(input: AggregateMongoInput): Promise<AggregateBucketOut[]> {
    const collection = this.collectionName(input.agentId);
    const pipeline: Record<string, unknown>[] = [];

    // 1. Pre-group filter ($match).
    if (input.where && Object.keys(input.where).length > 0) {
      pipeline.push({ $match: this.buildFilter(input.where) });
    }

    // 2. Build the $group _id (key) and metric accumulators.
    //    _id is an object whose keys are alias names (or `null` for global agg).
    const groupId: Record<string, unknown> = {};
    for (const gk of input.groupBy) {
      groupId[gk.alias] = gk.truncate
        ? {
            $dateTrunc: {
              date: this.dateExpr(gk.field),
              unit: this.mongoDateUnit(gk.truncate),
            },
          }
        : `$${gk.field}`;
    }

    const groupStage: Record<string, unknown> = {
      _id: input.groupBy.length === 0 ? null : groupId,
    };
    for (const m of input.metrics) {
      groupStage[m.alias] = this.metricAccumulator(m);
    }
    pipeline.push({ $group: groupStage });

    // 3. Post-group filter ($match using having). Aliases are top-level fields
    //    after $group, so we can reuse buildFilter directly with operator
    //    objects keyed by alias.
    if (input.having && Object.keys(input.having).length > 0) {
      pipeline.push({ $match: this.buildFilter(input.having) });
    }

    // 4. Sort.
    if (input.orderBy.length > 0) {
      const sort: Record<string, 1 | -1> = {};
      for (const ob of input.orderBy) {
        // Group keys are nested under _id after $group; metric aliases are at top level.
        const path = input.groupBy.some((g) => g.alias === ob.alias)
          ? `_id.${ob.alias}`
          : ob.alias;
        sort[path] = ob.dir === "ASC" ? 1 : -1;
      }
      pipeline.push({ $sort: sort });
    }

    // 5. Limit.
    pipeline.push({ $limit: input.limit });

    const docs = await this.mongoDb.collection(collection).aggregate(pipeline).toArray();

    return docs.map((doc) => {
      const key: Record<string, unknown> = {};
      if (input.groupBy.length > 0 && doc._id && typeof doc._id === "object") {
        for (const gk of input.groupBy) {
          key[gk.alias] = (doc._id as Record<string, unknown>)[gk.alias];
        }
      }
      const metrics: Record<string, unknown> = {};
      for (const m of input.metrics) {
        metrics[m.alias] = (doc as Record<string, unknown>)[m.alias];
      }
      return { key, metrics };
    });
  }

  /** Map a TimeUnit to its Mongo $dateTrunc unit string. */
  private mongoDateUnit(t: "DAY" | "WEEK" | "MONTH" | "YEAR"): string {
    return t === "DAY" ? "day" : t === "WEEK" ? "week" : t === "MONTH" ? "month" : "year";
  }

  /**
   * Coerce a date-like field into a Date for $dateTrunc. Records may store
   * timestamps as ISO strings (createdAt/updatedAt do); $dateTrunc requires a
   * Date so we $toDate them on the fly.
   */
  private dateExpr(field: string): Record<string, unknown> {
    return { $toDate: `$${field}` };
  }

  private metricAccumulator(m: { op: string; field?: string }): Record<string, unknown> {
    if (m.op === "COUNT") return { $sum: 1 };
    const ref = `$${m.field}`;
    if (m.op === "SUM") return { $sum: ref };
    if (m.op === "AVG") return { $avg: ref };
    if (m.op === "MIN") return { $min: ref };
    return { $max: ref };
  }
}

interface AggregateMongoInput {
  agentId: string;
  where?: Record<string, unknown>;
  groupBy: Array<{ field: string; alias: string; truncate?: "DAY" | "WEEK" | "MONTH" | "YEAR" }>;
  metrics: Array<{ alias: string; op: "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"; field?: string }>;
  having?: Record<string, unknown>;
  orderBy: Array<{ alias: string; dir: "ASC" | "DESC" }>;
  limit: number;
}

interface AggregateBucketOut {
  key: Record<string, unknown>;
  metrics: Record<string, unknown>;
}

// Escape a string for safe inclusion in a Mongo regex pattern.
function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
