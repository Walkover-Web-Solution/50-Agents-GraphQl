import { Pool } from "pg";
import { RecordItem } from "../common/types";

interface InsertPostgresRecordInput {
  id: string;
  agentId: string;
  schemaVersion: number;
  data: Record<string, unknown>;
}

interface QueryPostgresRecordsInput {
  agentId: string;
  where?: Record<string, unknown>;
  limit: number;
  offset: number;
}

export class PostgresRecordRepository {
  constructor(private readonly postgresPool: Pool) {}

  public async createTable(
    agentId: string,
    jsonSchema: Record<string, unknown>,
  ): Promise<void> {
    const table = this.tableName(agentId);
    const columns = this.buildColumnsFromSchema(jsonSchema);

    const columnDefinitions = [
      "id TEXT PRIMARY KEY",
      ...columns.map((col) => `"${col.name}" ${col.sqlType}`),
      "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
      "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    ];

    await this.postgresPool.query(`
      CREATE TABLE IF NOT EXISTS "${table}" (
        ${columnDefinitions.join(",\n        ")}
      );
    `);

    await this.postgresPool.query(`
      CREATE INDEX IF NOT EXISTS "idx_${table}_created"
      ON "${table}" (created_at DESC);
    `);
  }

  public async insertRecord(input: InsertPostgresRecordInput): Promise<RecordItem> {
    const table = this.tableName(input.agentId);
    const data = input.data;
    const fields = Object.keys(data);

    const columnNames = ["id", ...fields.map((f) => `"${f}"`)];
    const placeholders = columnNames.map((_, i) => `$${i + 1}`);
    const values = [
      input.id,
      ...fields.map((f) => {
        const val = data[f];
        return typeof val === "object" && val !== null ? JSON.stringify(val) : val;
      }),
    ];

    const returningFields = ["id", ...fields.map((f) => `"${f}"`), "created_at", "updated_at"];

    const query = `
      INSERT INTO "${table}" (${columnNames.join(", ")})
      VALUES (${placeholders.join(", ")})
      RETURNING ${returningFields.join(", ")};
    `;

    const result = await this.postgresPool.query(query, values);
    const row = result.rows[0];

    const resultData: Record<string, unknown> = {};
    for (const field of fields) {
      resultData[field] = row[field];
    }

    return {
      id: row.id,
      agentId: input.agentId,
      schemaVersion: input.schemaVersion,
      source: "POSTGRES",
      data: resultData,
      createdAt: new Date(row.created_at).toISOString(),
      updatedAt: new Date(row.updated_at).toISOString(),
    };
  }

  public async queryRecords(input: QueryPostgresRecordsInput): Promise<RecordItem[]> {
    const table = this.tableName(input.agentId);
    const { whereClause, values } = this.buildWhereClause(input.where);

    const query = `
      SELECT *
      FROM "${table}"
      ${whereClause}
      ORDER BY created_at DESC
      LIMIT $${values.length + 1} OFFSET $${values.length + 2};
    `;

    const result = await this.postgresPool.query(query, [...values, input.limit, input.offset]);

    return result.rows.map((row) => {
      const data: Record<string, unknown> = { ...row };
      delete data.id;
      delete data.created_at;
      delete data.updated_at;

      return {
        id: row.id,
        agentId: input.agentId,
        schemaVersion: 1,
        source: "POSTGRES",
        data,
        createdAt: new Date(row.created_at).toISOString(),
        updatedAt: new Date(row.updated_at).toISOString(),
      };
    });
  }

  public async getRecordById(agentId: string, id: string): Promise<RecordItem | null> {
    const table = this.tableName(agentId);

    const query = `SELECT * FROM "${table}" WHERE id = $1;`;
    const result = await this.postgresPool.query(query, [id]);
    const row = result.rows[0];

    if (!row) {
      return null;
    }

    const data: Record<string, unknown> = { ...row };
    delete data.id;
    delete data.created_at;
    delete data.updated_at;

    return {
      id: row.id,
      agentId,
      schemaVersion: 1,
      source: "POSTGRES",
      data,
      createdAt: new Date(row.created_at).toISOString(),
      updatedAt: new Date(row.updated_at).toISOString(),
    };
  }

  private tableName(agentId: string): string {
    return `agent_${agentId}`;
  }

  private buildColumnsFromSchema(
    jsonSchema: Record<string, unknown>,
  ): Array<{ name: string; sqlType: string }> {
    const properties = (jsonSchema.properties ?? {}) as Record<string, Record<string, unknown>>;
    const required = new Set((jsonSchema.required ?? []) as string[]);

    return Object.entries(properties).map(([name, fieldSchema]) => {
      const sqlType = this.jsonTypeToSqlType(fieldSchema);
      const nullable = required.has(name) ? " NOT NULL" : "";
      return { name, sqlType: `${sqlType}${nullable}` };
    });
  }

  private jsonTypeToSqlType(fieldSchema: Record<string, unknown>): string {
    const type = fieldSchema.type;

    if (type === "integer") return "INTEGER";
    if (type === "number") return "NUMERIC";
    if (type === "boolean") return "BOOLEAN";
    if (type === "array") return "JSONB";
    if (type === "object") return "JSONB";

    if (fieldSchema.format === "date-time") return "TIMESTAMPTZ";
    if (fieldSchema.format === "date") return "DATE";

    return "TEXT";
  }

  private buildWhereClause(
    where?: Record<string, unknown>,
  ): { whereClause: string; values: unknown[] } {
    const conditions: string[] = [];
    const values: unknown[] = [];

    if (where) {
      for (const [field, rawValue] of Object.entries(where)) {
        if (rawValue && typeof rawValue === "object" && !Array.isArray(rawValue)) {
          const operatorObject = rawValue as Record<string, unknown>;

          for (const [operator, operatorValue] of Object.entries(operatorObject)) {
            values.push(operatorValue);
            const paramIndex = values.length;

            if (operator === "$eq") {
              conditions.push(`"${field}" = $${paramIndex}`);
            } else if (operator === "$ne") {
              conditions.push(`"${field}" <> $${paramIndex}`);
            } else if (operator === "$gt") {
              conditions.push(`"${field}" > $${paramIndex}`);
            } else if (operator === "$lt") {
              conditions.push(`"${field}" < $${paramIndex}`);
            } else if (operator === "$gte") {
              conditions.push(`"${field}" >= $${paramIndex}`);
            } else if (operator === "$lte") {
              conditions.push(`"${field}" <= $${paramIndex}`);
            }
          }

          continue;
        }

        values.push(rawValue);
        conditions.push(`"${field}" = $${values.length}`);
      }
    }

    return {
      whereClause: conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "",
      values,
    };
  }
}
