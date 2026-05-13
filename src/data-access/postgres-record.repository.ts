import { Pool } from "pg";
import { RecordItem } from "../common/types";

interface InsertPostgresRecordInput {
  id: string;
  agentId: string;
  schemaVersion: number;
  data: Record<string, unknown>;
}

interface OrderByPostgres {
  field: string;
  dir?: "ASC" | "DESC";
}

interface QueryPostgresRecordsInput {
  agentId: string;
  where?: Record<string, unknown>;
  limit: number;
  offset: number;
  orderBy?: OrderByPostgres[];
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

    const orderClause = this.buildOrderByClause(input.orderBy);

    const query = `
      SELECT *
      FROM "${table}"
      ${whereClause}
      ${orderClause}
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

  public async countRecords(
    agentId: string,
    where?: Record<string, unknown>,
  ): Promise<number> {
    const table = this.tableName(agentId);
    const { whereClause, values } = this.buildWhereClause(where);
    const query = `SELECT COUNT(*)::int AS count FROM "${table}" ${whereClause};`;
    const result = await this.postgresPool.query(query, values);
    return Number(result.rows[0]?.count ?? 0);
  }

  public async updateRecord(
    agentId: string,
    id: string,
    data: Record<string, unknown>,
  ): Promise<RecordItem> {
    const table = this.tableName(agentId);
    const fields = Object.keys(data).filter(
      (k) => k !== "id" && k !== "created_at" && k !== "updated_at",
    );
    if (fields.length === 0) {
      const existing = await this.getRecordById(agentId, id);
      if (!existing) throw new Error(`Record ${id} not found`);
      return existing;
    }

    const setClauses = fields.map((f, i) => `"${f}" = $${i + 1}`);
    const values = fields.map((f) => {
      const val = data[f];
      return typeof val === "object" && val !== null ? JSON.stringify(val) : val;
    });
    setClauses.push(`updated_at = NOW()`);

    const idIndex = values.length + 1;
    const returning = ["id", ...fields.map((f) => `"${f}"`), "created_at", "updated_at"];

    const query = `
      UPDATE "${table}"
      SET ${setClauses.join(", ")}
      WHERE id = $${idIndex}
      RETURNING ${returning.join(", ")};
    `;

    const result = await this.postgresPool.query(query, [...values, id]);
    const row = result.rows[0];
    if (!row) throw new Error(`Record ${id} not found`);

    const dataOut: Record<string, unknown> = {};
    for (const field of fields) dataOut[field] = row[field];

    return {
      id: row.id,
      agentId,
      schemaVersion: 1,
      source: "POSTGRES",
      data: dataOut,
      createdAt: new Date(row.created_at).toISOString(),
      updatedAt: new Date(row.updated_at).toISOString(),
    };
  }

  public async deleteRecord(agentId: string, id: string): Promise<boolean> {
    const table = this.tableName(agentId);
    const result = await this.postgresPool.query(
      `DELETE FROM "${table}" WHERE id = $1;`,
      [id],
    );
    return (result.rowCount ?? 0) > 0;
  }

  public async aggregateRecords(input: AggregatePostgresInput): Promise<AggregateBucketOut[]> {
    const table = this.tableName(input.agentId);
    const values: unknown[] = [];

    // 1. WHERE — reuse existing builder, sharing the params array.
    const whereSql = input.where
      ? this.buildCondition(input.where, values)
      : "";

    // 2. SELECT list: group expressions (aliased) + metric expressions (aliased).
    const groupExprs = input.groupBy.map((gk) => this.groupExpr(gk));
    const selectParts = [
      ...input.groupBy.map((gk) => `${this.groupExpr(gk)} AS "${gk.alias}"`),
      ...input.metrics.map((m) => `${this.metricExpr(m)} AS "${m.alias}"`),
    ];
    if (selectParts.length === 0) {
      // metrics is non-empty per resolver validation, so this is unreachable.
      throw new Error("aggregateRecords: empty select");
    }

    // 3. GROUP BY — by ordinal positions to keep the expression in one place.
    const groupByClause =
      groupExprs.length > 0
        ? `GROUP BY ${groupExprs.map((_, i) => i + 1).join(", ")}`
        : "";

    // 4. HAVING — alias references resolve to the metric expression. Use the
    //    expanded expression directly (Postgres supports HAVING <expr> <op> <val>).
    const metricByAlias = new Map(input.metrics.map((m) => [m.alias, m]));
    const havingParts: string[] = [];
    if (input.having) {
      for (const [alias, opObj] of Object.entries(input.having)) {
        const m = metricByAlias.get(alias);
        if (!m) continue; // resolver already validated, but be defensive
        const expr = this.metricExpr(m);
        for (const [op, val] of Object.entries(opObj as Record<string, unknown>)) {
          const sqlOp =
            op === "$eq" ? "=" :
            op === "$ne" ? "<>" :
            op === "$gt" ? ">" :
            op === "$lt" ? "<" :
            op === "$gte" ? ">=" :
            op === "$lte" ? "<=" : null;
          if (!sqlOp) continue;
          values.push(val);
          havingParts.push(`${expr} ${sqlOp} $${values.length}`);
        }
      }
    }
    const havingClause = havingParts.length > 0 ? `HAVING ${havingParts.join(" AND ")}` : "";

    // 5. ORDER BY — alias references resolve to either the group alias column
    //    or the metric expression. Use double-quoted aliases — Postgres
    //    accepts these in ORDER BY for the SELECT list.
    const orderParts = input.orderBy.map((ob) => {
      const dir = ob.dir === "ASC" ? "ASC" : "DESC";
      return `"${ob.alias}" ${dir}`;
    });
    const orderClause = orderParts.length > 0 ? `ORDER BY ${orderParts.join(", ")}` : "";

    // 6. LIMIT.
    values.push(input.limit);
    const limitClause = `LIMIT $${values.length}`;

    const sql = `
      SELECT ${selectParts.join(", ")}
      FROM "${table}"
      ${whereSql ? `WHERE ${whereSql}` : ""}
      ${groupByClause}
      ${havingClause}
      ${orderClause}
      ${limitClause};
    `;

    const result = await this.postgresPool.query(sql, values);

    return result.rows.map((row) => {
      const key: Record<string, unknown> = {};
      for (const gk of input.groupBy) key[gk.alias] = row[gk.alias];
      const metrics: Record<string, unknown> = {};
      for (const m of input.metrics) {
        const v = row[m.alias];
        // pg returns NUMERIC as string for SUM/AVG; coerce to number.
        metrics[m.alias] = typeof v === "string" && v !== "" && !isNaN(Number(v))
          ? Number(v)
          : v;
      }
      return { key, metrics };
    });
  }

  /** Build the SQL expression for a group key (handles time truncation). */
  private groupExpr(gk: { field: string; truncate?: "DAY" | "WEEK" | "MONTH" | "YEAR" }): string {
    const col = `"${this.physicalColumn(gk.field)}"`;
    if (!gk.truncate) return col;
    const unit =
      gk.truncate === "DAY" ? "day" :
      gk.truncate === "WEEK" ? "week" :
      gk.truncate === "MONTH" ? "month" : "year";
    // date_trunc requires timestamp; cast for safety. Works for TIMESTAMPTZ
    // and DATE columns (Postgres auto-promotes DATE).
    return `date_trunc('${unit}', ${col}::timestamptz)`;
  }

  /** Build the SQL expression for an aggregate metric. */
  private metricExpr(m: { op: string; field?: string }): string {
    if (m.op === "COUNT") return "COUNT(*)::bigint";
    const col = `"${this.physicalColumn(m.field as string)}"`;
    if (m.op === "SUM") return `SUM(${col})`;
    if (m.op === "AVG") return `AVG(${col})`;
    if (m.op === "MIN") return `MIN(${col})`;
    return `MAX(${col})`;
  }

  /** Map logical field name to its physical column (snake_case for system fields). */
  private physicalColumn(field: string): string {
    if (field === "createdAt") return "created_at";
    if (field === "updatedAt") return "updated_at";
    return field;
  }

  private buildOrderByClause(orderBy?: OrderByPostgres[]): string {
    if (!orderBy || orderBy.length === 0) {
      return "ORDER BY created_at DESC";
    }
    const parts = orderBy.map((ob) => {
      // Map common synonyms
      const fieldRaw =
        ob.field === "createdAt" ? "created_at" :
        ob.field === "updatedAt" ? "updated_at" :
        ob.field;
      const dir = ob.dir === "ASC" ? "ASC" : "DESC";
      return `"${fieldRaw}" ${dir}`;
    });
    return `ORDER BY ${parts.join(", ")}`;
  }

  private buildWhereClause(
    where?: Record<string, unknown>,
  ): { whereClause: string; values: unknown[] } {
    const values: unknown[] = [];
    const root = where ? this.buildCondition(where, values) : "";
    return {
      whereClause: root ? `WHERE ${root}` : "",
      values,
    };
  }

  // Recursively builds a SQL boolean expression for a where object. Pushes
  // parameter values onto the shared `values` array (1-indexed in the SQL).
  private buildCondition(
    where: Record<string, unknown>,
    values: unknown[],
  ): string {
    const parts: string[] = [];

    for (const [field, rawValue] of Object.entries(where)) {
      if (field === "$and" || field === "$or") {
        if (!Array.isArray(rawValue)) continue;
        const subParts = (rawValue as Array<Record<string, unknown>>)
          .map((entry) => this.buildCondition(entry, values))
          .filter(Boolean);
        if (subParts.length === 0) continue;
        const joiner = field === "$and" ? " AND " : " OR ";
        parts.push(`(${subParts.join(joiner)})`);
        continue;
      }

      if (field === "$not") {
        if (!rawValue || typeof rawValue !== "object" || Array.isArray(rawValue)) continue;
        const inner = this.buildCondition(rawValue as Record<string, unknown>, values);
        if (inner) parts.push(`NOT (${inner})`);
        continue;
      }

      // Field-level operator object
      if (rawValue && typeof rawValue === "object" && !Array.isArray(rawValue)) {
        const operatorObject = rawValue as Record<string, unknown>;
        for (const [operator, operatorValue] of Object.entries(operatorObject)) {
          const sql = this.buildFieldOp(field, operator, operatorValue, values);
          if (sql) parts.push(sql);
        }
        continue;
      }

      // Bare value: equality
      values.push(rawValue);
      parts.push(`"${field}" = $${values.length}`);
    }

    return parts.join(" AND ");
  }

  private buildFieldOp(
    field: string,
    operator: string,
    operatorValue: unknown,
    values: unknown[],
  ): string {
    const col = `"${field}"`;
    switch (operator) {
      case "$eq":
        values.push(operatorValue);
        return `${col} = $${values.length}`;
      case "$ne":
        values.push(operatorValue);
        return `${col} <> $${values.length}`;
      case "$gt":
        values.push(operatorValue);
        return `${col} > $${values.length}`;
      case "$lt":
        values.push(operatorValue);
        return `${col} < $${values.length}`;
      case "$gte":
        values.push(operatorValue);
        return `${col} >= $${values.length}`;
      case "$lte":
        values.push(operatorValue);
        return `${col} <= $${values.length}`;
      case "$in":
        if (!Array.isArray(operatorValue) || operatorValue.length === 0) return "";
        values.push(operatorValue);
        return `${col} = ANY($${values.length})`;
      case "$nin":
        if (!Array.isArray(operatorValue) || operatorValue.length === 0) return "";
        values.push(operatorValue);
        return `${col} <> ALL($${values.length})`;
      case "$contains":
        values.push(`%${String(operatorValue)}%`);
        return `${col}::text ILIKE $${values.length}`;
      case "$startsWith":
        values.push(`${String(operatorValue)}%`);
        return `${col}::text ILIKE $${values.length}`;
      case "$endsWith":
        values.push(`%${String(operatorValue)}`);
        return `${col}::text ILIKE $${values.length}`;
      case "$exists":
        return operatorValue ? `${col} IS NOT NULL` : `${col} IS NULL`;
      default:
        return "";
    }
  }
}

interface AggregatePostgresInput {
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
