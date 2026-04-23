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

interface QueryRecordsArgs {
  input: {
    where?: Record<string, unknown>;
    limit?: number;
    offset?: number;
  };
}

interface GetRecordByIdArgs {
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
          const { where, limit, offset } = args.input;
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
    },
  };
}

function validateWhereOperators(where: Record<string, unknown>): void {
  const allowedOperators = new Set(["$eq", "$ne", "$gt", "$lt", "$gte", "$lte", "$in"]);

  for (const value of Object.values(where)) {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      continue;
    }

    for (const operator of Object.keys(value as Record<string, unknown>)) {
      if (!allowedOperators.has(operator)) {
        throw new ValidationError(`Unsupported operator: ${operator}`);
      }
    }
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
