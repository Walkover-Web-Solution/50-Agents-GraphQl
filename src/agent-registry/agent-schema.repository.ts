import { Db, ObjectId } from "mongodb";
import { AgentSchemaRecord, DataSource } from "../common/types";

export class AgentSchemaRepository {
  constructor(private readonly registryDb: Db) {}

  public async getSchema(agentId: string): Promise<AgentSchemaRecord | null> {
    const doc = await this.registryDb.collection("microapps").findOne({
      agent_id: new ObjectId(agentId),
      active_status: true,
      deleted_at: null,
    });

    if (!doc) {
      return null;
    }

    return this.mapDocToSchemaRecord(doc);
  }

  private mapDocToSchemaRecord(doc: Record<string, unknown>): AgentSchemaRecord {
    const rawSchema = doc.response_schema;
    const jsonSchema =
      typeof rawSchema === "string" ? JSON.parse(rawSchema) : (rawSchema ?? {});

    return {
      agentId: String(doc.agent_id),
      dbChoice: String(doc.db_choice) as DataSource,
      schemaVersion: 1,
      jsonSchema,
      createdAt: doc.created_at ? new Date(doc.created_at as string).toISOString() : new Date().toISOString(),
      updatedAt: doc.updated_at ? new Date(doc.updated_at as string).toISOString() : new Date().toISOString(),
    };
  }
}
