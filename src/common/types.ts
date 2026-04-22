export type DataSource = "MONGO" | "POSTGRES";

export interface AgentSchemaRecord {
  agentId: string;
  dbChoice: DataSource;
  schemaVersion: number;
  jsonSchema: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface RecordItem {
  id: string;
  agentId: string;
  schemaVersion: number;
  source: DataSource;
  data: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface QueryFilter {
  [key: string]: unknown;
}

export interface RequestContext {
  agentId: string;
  tenantId: string;
}
