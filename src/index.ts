import { buildApp } from "./app";
import { env } from "./config/env";

async function main(): Promise<void> {
  const { app } = await buildApp();

  app.listen(env.port, () => {
    // eslint-disable-next-line no-console
    console.log(`Server running on http://localhost:${env.port}/graphql`);
  });
}

main().catch((error) => {
  // eslint-disable-next-line no-console
  console.error("Failed to start server", error);
  process.exit(1);
});
