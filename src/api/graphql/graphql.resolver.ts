import { v4 as uuidV4 } from "uuid";
import { AgentRegistryService } from "../../agent-registry/agent-registry.service";
import { AppError, ValidationError } from "../../common/errors";
import { DataSource, RequestContext } from "../../common/types";
import { DataSourceRouterService } from "../../data-access/data-source.router.service";
import { SchemaValidatorService } from "../../validation/schema-validator.service";
import { jsonScalar } from "./json.scalar";

interface CreateCollectionArgs {
  dbChoice: DataSource;
  jsonSchema: Record<string, unknown>;
}

interface InsertRecordArgs {
  input: {
    data: Record<string, unknown>;
  };
}

interface OrderByInput {
  field: string;
  dir?: "ASC" | "DESC";
}

interface QueryRecordsArgs {
  input: {
    where?: Record<string, unknown>;
    limit?: number;
    offset?: number;
    orderBy?: OrderByInput[];
  };
}

interface GetRecordByIdArgs {
  id: string;
}

interface CountRecordsArgs {
  where?: Record<string, unknown>;
}

type AggOp = "COUNT" | "SUM" | "AVG" | "MIN" | "MAX";
type TimeUnit = "DAY" | "WEEK" | "MONTH" | "YEAR";

interface AggregateGroupKey {
  field: string;
  truncate?: TimeUnit;
  alias?: string;
}

interface AggregateMetric {
  alias: string;
  op: AggOp;
  field?: string;
}

interface AggregateOrderBy {
  alias: string;
  dir?: "ASC" | "DESC";
}

interface AggregateRecordsArgs {
  input: {
    where?: Record<string, unknown>;
    groupBy?: AggregateGroupKey[];
    metrics: AggregateMetric[];
    having?: Record<string, unknown>;
    orderBy?: AggregateOrderBy[];
    limit?: number;
  };
}

interface UpdateRecordArgs {
  input: {
    id: string;
    data: Record<string, unknown>;
  };
}

interface DeleteRecordArgs {
  id: string;
}

interface BuildResolverInput {
  agentRegistryService: AgentRegistryService;
  validatorService: SchemaValidatorService;
  dataSourceRouterService: DataSourceRouterService;
}

function ensurePlainObject(value: unknown, fieldName: string): Record<string, unknown> {
  let parsedValue = value;
  
  if (typeof value === "string") {
    try {
      parsedValue = JSON.parse(value);
    } catch (e) {
      throw new ValidationError(`${fieldName} contains an invalid JSON string`);
    }
  }

  if (!parsedValue || typeof parsedValue !== "object" || Array.isArray(parsedValue)) {
    throw new ValidationError(`${fieldName} must be a JSON object`);
  }

  return parsedValue as Record<string, unknown>;
}

function validatePagination(limit?: number, offset?: number): { limit: number; offset: number } {
  const safeLimit = limit ?? 20;
  const safeOffset = offset ?? 0;

  if (!Number.isInteger(safeLimit) || safeLimit <= 0 || safeLimit > 100) {
    throw new ValidationError("limit must be an integer between 1 and 100");
  }

  if (!Number.isInteger(safeOffset) || safeOffset < 0) {
    throw new ValidationError("offset must be an integer >= 0");
  }

  return {
    limit: safeLimit,
    offset: safeOffset,
  };
}

export function buildResolvers(input: BuildResolverInput) {
  return {
    JSON: jsonScalar,
    Query: {
      queryRecords: async (
        _parent: unknown,
        args: QueryRecordsArgs,
        context: RequestContext,
      ) => {
        try {
          const { where, limit, offset, orderBy } = args.input;
          const { limit: safeLimit, offset: safeOffset } = validatePagination(limit, offset);

          const schema = await input.agentRegistryService.getSchemaOrThrow(context.agentId);

          if (where) {
            const whereFilter = ensurePlainObject(where, "where");
            validateWhereOperators(whereFilter);
          }

          return input.dataSourceRouterService.queryRecords({
            source: schema.dbChoice,
            agentId: context.agentId,
            ...(where ? { where } : {}),
            limit: safeLimit,
            offset: safeOffset,
            ...(orderBy && orderBy.length ? { orderBy } : {}),
          });
        } catch (error) {
          throw normalizeGraphqlError(error);
        }
      },
      getRecordById: async (
        _parent: unknown,
        args: GetRecordByIdArgs,
        context: RequestContext,
      ) => {
        try {
          const schema = await input.agentRegistryService.getSchemaOrThrow(context.agentId);

          return input.dataSourceRouterService.getRecordById({
            source: schema.dbChoice,
            agentId: context.agentId,
            id: args.id,
          });
        } catch (error) {
          throw normalizeGraphqlError(error);
        }
      },
      countRecords: async (
        _parent: unknown,
        args: CountRecordsArgs,
        context: RequestContext,
      ) => {
        try {
          const schema = await input.agentRegistryService.getSchemaOrThrow(context.agentId);

          if (args.where) {
            const whereFilter = ensurePlainObject(args.where, "where");
            validateWhereOperators(whereFilter);
          }

          return input.dataSourceRouterService.countRecords({
            source: schema.dbChoice,
            agentId: context.agentId,
            ...(args.where ? { where: args.where } : {}),
          });
        } catch (error) {
          throw normalizeGraphqlError(error);
        }
      },
      aggregateRecords: async (
        _parent: unknown,
        args: AggregateRecordsArgs,
        context: RequestContext,
      ) => {
        try {
          const { where, groupBy, metrics, having, orderBy, limit } = args.input;

          if (!Array.isArray(metrics) || metrics.length === 0) {
            throw new ValidationError("metrics must be a non-empty array");
          }
          const safeLimit = limit ?? 50;
          if (!Number.isInteger(safeLimit) || safeLimit <= 0 || safeLimit > 100) {
            throw new ValidationError("limit must be an integer between 1 and 100");
          }

          if (where) {
            const whereFilter = ensurePlainObject(where, "where");
            validateWhereOperators(whereFilter);
          }

          // Validate group keys (identifier safety + alias collision detection).
          const aliases = new Set<string>();
          const safeGroupBy = (groupBy ?? []).map((gk) => {
            assertSafeIdentifier(gk.field, "groupBy.field");
            const alias = gk.alias ?? gk.field;
            assertSafeIdentifier(alias, "groupBy.alias");
            if (aliases.has(alias)) {
              throw new ValidationError(`Duplicate alias in groupBy/metrics: ${alias}`);
            }
            aliases.add(alias);
            if (gk.truncate && !["DAY", "WEEK", "MONTH", "YEAR"].includes(gk.truncate)) {
              throw new ValidationError(`Unsupported truncate unit: ${gk.truncate}`);
            }
            return { field: gk.field, alias, ...(gk.truncate ? { truncate: gk.truncate } : {}) };
          });

          // Validate metrics (op + field pairing, alias safety/uniqueness).
          const safeMetrics = metrics.map((m) => {
            assertSafeIdentifier(m.alias, "metrics.alias");
            if (aliases.has(m.alias)) {
              throw new ValidationError(`Duplicate alias in groupBy/metrics: ${m.alias}`);
            }
            aliases.add(m.alias);
            if (!["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(m.op)) {
              throw new ValidationError(`Unsupported aggregate op: ${m.op}`);
            }
            if (m.op !== "COUNT") {
              if (!m.field) {
                throw new ValidationError(`metric '${m.alias}' op=${m.op} requires a field`);
              }
              assertSafeIdentifier(m.field, "metrics.field");
            }
            return { alias: m.alias, op: m.op, ...(m.field ? { field: m.field } : {}) };
          });

          // Validate having (alias-keyed operator object). Only metric-style
          // operators are allowed; logical combinators are explicitly NOT
          // allowed in having to keep the surface tight.
          let safeHaving: Record<string, unknown> | undefined;
          if (having) {
            safeHaving = ensurePlainObject(having, "having");
            for (const [alias, opObj] of Object.entries(safeHaving)) {
              if (!aliases.has(alias)) {
                throw new ValidationError(
                  `having references unknown alias: ${alias}`,
                );
              }
              if (!opObj || typeof opObj !== "object" || Array.isArray(opObj)) {
                throw new ValidationError(
                  `having[${alias}] must be an operator object`,
                );
              }
              for (const op of Object.keys(opObj as Record<string, unknown>)) {
                if (!["$eq", "$ne", "$gt", "$lt", "$gte", "$lte"].includes(op)) {
                  throw new ValidationError(
                    `having operator '${op}' not allowed (use $eq/$ne/$gt/$lt/$gte/$lte)`,
                  );
                }
              }
            }
          }

          // Validate orderBy aliases.
          const safeOrderBy = (orderBy ?? []).map((ob) => {
            if (!aliases.has(ob.alias)) {
              throw new ValidationError(
                `orderBy references unknown alias: ${ob.alias}`,
              );
            }
            return { alias: ob.alias, dir: ob.dir === "ASC" ? "ASC" : "DESC" } as const;
          });

          const schema = await input.agentRegistryService.getSchemaOrThrow(context.agentId);

          return input.dataSourceRouterService.aggregateRecords({
            source: schema.dbChoice,
            agentId: context.agentId,
            ...(where ? { where } : {}),
            groupBy: safeGroupBy,
            metrics: safeMetrics,
            ...(safeHaving ? { having: safeHaving } : {}),
            orderBy: safeOrderBy,
            limit: safeLimit,
          });
        } catch (error) {
          throw normalizeGraphqlError(error);
        }
      },
    },
    Mutation: {
      createCollection: async (
        _parent: unknown,
        args: CreateCollectionArgs,
        context: RequestContext,
      ) => {
        try {
          const { dbChoice, jsonSchema } = args;
          const schemaObject = ensurePlainObject(jsonSchema, "jsonSchema");

          await input.dataSourceRouterService.createCollection({
            source: dbChoice,
            agentId: context.agentId,
            jsonSchema: schemaObject,
          });

          return {
            dbChoice,
            status: "CREATED",
          };
        } catch (error) {
          throw normalizeGraphqlError(error);
        }
      },
      insertRecord: async (
        _parent: unknown,
        args: InsertRecordArgs,
        context: RequestContext,
      ) => {
        try {
          const { data } = args.input;
          const safeData = ensurePlainObject(data, "data");

          const schema = await input.agentRegistryService.getSchemaOrThrow(context.agentId);

          input.validatorService.validateData(
            context.agentId,
            schema.agentId,
            schema.schemaVersion,
            schema.jsonSchema,
            safeData,
          );

          return input.dataSourceRouterService.insertRecord({
            source: schema.dbChoice,
            id: uuidV4(),
            agentId: context.agentId,
            schemaVersion: schema.schemaVersion,
            data: safeData,
          });
        } catch (error) {
          throw normalizeGraphqlError(error);
        }
      },
      updateRecord: async (
        _parent: unknown,
        args: UpdateRecordArgs,
        context: RequestContext,
      ) => {
        try {
          const { id, data } = args.input;
          const safeData = ensurePlainObject(data, "data");

          const schema = await input.agentRegistryService.getSchemaOrThrow(context.agentId);

          // Validate against the agent's JSON Schema. We treat update as a
          // patch: only provided fields are validated as a partial document.
          input.validatorService.validateData(
            context.agentId,
            schema.agentId,
            schema.schemaVersion,
            schema.jsonSchema,
            safeData,
          );

          return input.dataSourceRouterService.updateRecord({
            source: schema.dbChoice,
            agentId: context.agentId,
            id,
            data: safeData,
          });
        } catch (error) {
          throw normalizeGraphqlError(error);
        }
      },
      deleteRecord: async (
        _parent: unknown,
        args: DeleteRecordArgs,
        context: RequestContext,
      ) => {
        try {
          const schema = await input.agentRegistryService.getSchemaOrThrow(context.agentId);

          return input.dataSourceRouterService.deleteRecord({
            source: schema.dbChoice,
            agentId: context.agentId,
            id: args.id,
          });
        } catch (error) {
          throw normalizeGraphqlError(error);
        }
      },
    },
  };
}

const ALLOWED_FIELD_OPERATORS = new Set([
  "$eq",
  "$ne",
  "$gt",
  "$lt",
  "$gte",
  "$lte",
  "$in",
  "$nin",
  "$contains",
  "$startsWith",
  "$endsWith",
  "$exists",
]);

const ALLOWED_LOGICAL_OPERATORS = new Set(["$and", "$or", "$not"]);

function validateWhereOperators(where: Record<string, unknown>): void {
  for (const [key, value] of Object.entries(where)) {
    // Top-level logical operators: array of sub-where objects ($and/$or) or
    // a single sub-where object ($not).
    if (ALLOWED_LOGICAL_OPERATORS.has(key)) {
      if (key === "$not") {
        if (!value || typeof value !== "object" || Array.isArray(value)) {
          throw new ValidationError("$not must be an object");
        }
        validateWhereOperators(value as Record<string, unknown>);
        continue;
      }
      if (!Array.isArray(value)) {
        throw new ValidationError(`${key} must be an array of conditions`);
      }
      for (const item of value) {
        if (!item || typeof item !== "object" || Array.isArray(item)) {
          throw new ValidationError(`${key} entries must be objects`);
        }
        validateWhereOperators(item as Record<string, unknown>);
      }
      continue;
    }

    // Field-level operator object, e.g. { field: { $eq: "..." } }.
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      continue;
    }

    for (const operator of Object.keys(value as Record<string, unknown>)) {
      if (!ALLOWED_FIELD_OPERATORS.has(operator)) {
        throw new ValidationError(`Unsupported operator: ${operator}`);
      }
    }
  }
}

/**
 * Reject anything that doesn't look like a safe field/alias identifier. We
 * inject these strings directly into SQL column references and Mongo
 * aggregation pipeline keys, so they MUST be tightly constrained to
 * `[A-Za-z_][A-Za-z0-9_]*` (with an optional dotted nested path for JSONB).
 */
function assertSafeIdentifier(value: unknown, label: string): void {
  if (typeof value !== "string" || value.length === 0 || value.length > 64) {
    throw new ValidationError(`${label} must be a non-empty identifier <= 64 chars`);
  }
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new ValidationError(
      `${label} '${value}' must match [A-Za-z_][A-Za-z0-9_]*`,
    );
  }
}

function normalizeGraphqlError(error: unknown): Error {
  if (error instanceof AppError) {
    return new Error(`${error.code}: ${error.message}`);
  }

  if (error instanceof Error) {
    return error;
  }

  return new Error("Unknown error");
}
