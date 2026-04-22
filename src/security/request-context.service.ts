import { UnauthorizedError } from "../common/errors";
import { RequestContext } from "../common/types";

export class RequestContextService {
  public createContext(agentIdHeader?: string): RequestContext {
    const agentId = agentIdHeader?.trim();
    if (!agentId) {
      throw new UnauthorizedError("Missing x-agent-id header");
    }

    return {
      agentId,
      tenantId: agentId,
    };
  }
}
