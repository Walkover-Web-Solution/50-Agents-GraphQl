import { Db, MongoClient } from "mongodb";

export class MongoDbClient {
  private readonly client: MongoClient;
  private readonly dbName: string;

  constructor(url: string, dbName: string) {
    this.client = new MongoClient(url);
    this.dbName = dbName;
  }

  public async connect(): Promise<void> {
    await this.client.connect();
  }

  public getDb(): Db {
    return this.client.db(this.dbName);
  }

  public async close(): Promise<void> {
    await this.client.close();
  }
}
