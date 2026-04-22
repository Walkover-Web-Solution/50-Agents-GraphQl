import cors from "cors";
import express from "express";
import { json } from "body-parser";
import { ApolloServer } from "@apollo/server";
import { expressMiddleware } from "@as-integrations/express5";
import { typeDefs } from "./api/graphql/graphql.schema";
import { buildResolvers } from "./api/graphql/graphql.resolver";
import { AgentSchemaRepository } from "./agent-registry/agent-schema.repository";
import { AgentRegistryService } from "./agent-registry/agent-registry.service";
import { env } from "./config/env";
import { DataSourceRouterService } from "./data-access/data-source.router.service";
import { MongoRecordRepository } from "./data-access/mongo-record.repository";
import { PostgresRecordRepository } from "./data-access/postgres-record.repository";
import { DbBootstrapService } from "./database/db.bootstrap.service";
import { MongoDbClient } from "./database/mongo.client";
import { PostgresClient } from "./database/postgres.client";
import { RequestContext } from "./common/types";
import { RequestContextService } from "./security/request-context.service";
import { SchemaValidatorService } from "./validation/schema-validator.service";

export async function buildApp() {
  const postgresClient = new PostgresClient(env.postgresUrl);
  const registryMongoClient = new MongoDbClient(env.mongoRegistryUrl, env.mongoRegistryDbName);
  const dataMongoClient = new MongoDbClient(env.mongoUrl, env.mongoDbName);

  const dbBootstrapService = new DbBootstrapService(postgresClient, registryMongoClient, dataMongoClient);
  await dbBootstrapService.initialize();

  const agentSchemaRepository = new AgentSchemaRepository(registryMongoClient.getDb());
  const agentRegistryService = new AgentRegistryService(agentSchemaRepository);
  const schemaValidatorService = new SchemaValidatorService();

  const mongoRecordRepository = new MongoRecordRepository(dataMongoClient.getDb());
  const postgresRecordRepository = new PostgresRecordRepository(postgresClient.getPool());
  const dataSourceRouterService = new DataSourceRouterService(mongoRecordRepository, postgresRecordRepository);
  const requestContextService = new RequestContextService();

  const resolvers = buildResolvers({
    agentRegistryService,
    validatorService: schemaValidatorService,
    dataSourceRouterService,
  });

  const server = new ApolloServer<RequestContext>({
    typeDefs,
    resolvers,
  });

  await server.start();

  const app = express();
  app.use(cors());
  app.use(json());

  app.get("/health", (_request, response) => {
    response.status(200).json({ status: "ok" });
  });

  app.use(
    "/graphql",
    expressMiddleware(server, {
      context: async ({ req }) => {
        const headerValue = req.headers["x-agent-id"];
        const agentId = Array.isArray(headerValue) ? headerValue[0] : headerValue;
        return requestContextService.createContext(agentId);
      },
    }),
  );

  return {
    app,
    close: async () => {
      await server.stop();
      await registryMongoClient.close();
      await dataMongoClient.close();
      await postgresClient.close();
    },
  };
}
