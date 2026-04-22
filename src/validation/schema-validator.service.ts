import Ajv, { ValidateFunction } from "ajv";
import addFormats from "ajv-formats";
import { ValidationError } from "../common/errors";

export class SchemaValidatorService {
  private readonly ajv: Ajv;
  private readonly validatorCache = new Map<string, ValidateFunction>();

  constructor() {
    this.ajv = new Ajv({ allErrors: true, strict: false });
    addFormats(this.ajv);
  }

  public validateData(
    tenantId: string,
    entityName: string,
    schemaVersion: number,
    jsonSchema: Record<string, unknown>,
    data: Record<string, unknown>,
  ): void {
    const key = `${tenantId}:${entityName}:${schemaVersion}`;
    let validate = this.validatorCache.get(key);

    if (!validate) {
      validate = this.ajv.compile(jsonSchema);
      this.validatorCache.set(key, validate);
    }

    const valid = validate(data);
    if (!valid) {
      const details = (validate.errors ?? []).map((item) => `${item.instancePath} ${item.message}`.trim());
      throw new ValidationError(`Schema validation failed: ${details.join("; ")}`);
    }
  }
}
