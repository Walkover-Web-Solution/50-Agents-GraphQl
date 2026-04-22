import { NotFoundError } from "../common/errors";
import { AgentSchemaRecord } from "../common/types";
import { AgentSchemaRepository } from "./agent-schema.repository";

export class AgentRegistryService {
  constructor(private readonly repository: AgentSchemaRepository) {}

  public async getSchemaOrThrow(agentId: string): Promise<AgentSchemaRecord> {
    const schema = await this.repository.getSchema(agentId);
    if (!schema) {
      throw new NotFoundError(`No schema found for agent: ${agentId}`);
    }

    return schema;
  }
}
