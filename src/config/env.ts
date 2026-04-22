import dotenv from "dotenv";

dotenv.config();

const requiredKeys = [
  "PORT",
  "POSTGRES_URL",
  "MONGO_URL",
  "MONGO_DB_NAME",
  "MONGO_REGISTRY_URL",
  "MONGO_REGISTRY_DB_NAME",
] as const;

type RequiredKey = (typeof requiredKeys)[number];

function getRequiredValue(key: RequiredKey): string {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Missing required env var: ${key}`);
  }
  return value;
}

export const env = {
  port: Number(getRequiredValue("PORT")),
  postgresUrl: getRequiredValue("POSTGRES_URL"),
  mongoUrl: getRequiredValue("MONGO_URL"),
  mongoDbName: getRequiredValue("MONGO_DB_NAME"),
  mongoRegistryUrl: getRequiredValue("MONGO_REGISTRY_URL"),
  mongoRegistryDbName: getRequiredValue("MONGO_REGISTRY_DB_NAME"),
};
