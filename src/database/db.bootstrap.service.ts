import { MongoDbClient } from "./mongo.client";
import { PostgresClient } from "./postgres.client";

export class DbBootstrapService {
  constructor(
    private readonly postgresClient: PostgresClient,
    private readonly registryMongoClient: MongoDbClient,
    private readonly dataMongoClient: MongoDbClient,
  ) {}

  public async initialize(): Promise<void> {
    await this.registryMongoClient.connect();
    await this.dataMongoClient.connect();

    // Verify Postgres connection is alive
    await this.postgresClient.getPool().query("SELECT 1");
  }
}
