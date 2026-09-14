import { Pool } from "pg";

import { createApp } from "./api/app.ts";
import { DemoWorker } from "./application/demo-worker.ts";
import { MemoryEventBroker, type EventBroker } from "./application/event-broker.ts";
import type { JobStore } from "./application/job-store.ts";
import { InMemoryJobStore } from "./infrastructure/in-memory-job-store.ts";
import { runMigrations } from "./infrastructure/migrate.ts";
import { OutboxRelay } from "./infrastructure/outbox-relay.ts";
import { PostgresJobStore } from "./infrastructure/postgres-job-store.ts";
import { RedisEventBroker } from "./infrastructure/redis-event-broker.ts";

const port = Number.parseInt(process.env.PORT ?? "3000", 10);

async function bootstrap() {
  let broker: EventBroker;
  if (process.env.REDIS_URL) {
    broker = await RedisEventBroker.connect(process.env.REDIS_URL);
    console.log("Connected to Redis event broker");
  } else {
    broker = new MemoryEventBroker();
    console.log("Using in-memory event broker");
  }

  let store: JobStore;
  let outboxRelay: OutboxRelay | undefined;
  if (process.env.DATABASE_URL && process.env.STORE_DRIVER !== "memory") {
    const pool = new Pool({ connectionString: process.env.DATABASE_URL });
    await runMigrations(pool);
    store = new PostgresJobStore(pool);
    outboxRelay = new OutboxRelay(pool, broker);
    outboxRelay.start();
    console.log("Using PostgreSQL job store");
  } else {
    store = new InMemoryJobStore((event) => broker.publish(event));
    console.log("Using in-memory job store");
  }

  const worker = new DemoWorker(store);
  if (process.env.DEMO_WORKER_AUTO_START !== "false") worker.start();
  const app = createApp({ store, broker, worker });
  const server = app.listen(port, () => {
    console.log(`Distributed job queue API listening on http://localhost:${port}`);
  });

  let shuttingDown = false;
  async function shutdown(signal: string): Promise<void> {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`${signal} received; shutting down`);
    worker.stop();
    outboxRelay?.stop();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await store.close?.();
    await broker.close();
  }

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  return server;
}

export const server = await bootstrap();
