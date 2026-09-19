import assert from "node:assert/strict";
import { test } from "node:test";
import { Pool } from "pg";

import { runMigrations } from "../src/infrastructure/migrate.ts";
import { PostgresJobStore } from "../src/infrastructure/postgres-job-store.ts";

const connectionString = process.env.TEST_DATABASE_URL;

test(
  "PostgreSQL atomically distributes competing claims",
  { skip: !connectionString },
  async () => {
    if (!connectionString) return;
    const pool = new Pool({ connectionString });
    try {
      await runMigrations(pool);
      await pool.query(
        "TRUNCATE outbox_events, job_events, jobs, queues RESTART IDENTITY CASCADE",
      );
      const store = new PostgresJobStore(pool);
      await Promise.all(
        Array.from({ length: 50 }, (_, index) =>
          store.submit({
            queue: "concurrency-test",
            type: "test-job",
            payload: { index },
          }),
        ),
      );

      const claimedIds = await Promise.all(
        Array.from({ length: 10 }, async () => {
          const ids: string[] = [];
          for (let index = 0; index < 5; index += 1) {
            const claim = await store.claim("concurrency-test");
            assert.ok(claim);
            ids.push(claim.job.id);
            await store.complete(claim.job.id, claim.leaseId, {});
          }
          return ids;
        }),
      );

      const flattened = claimedIds.flat();
      assert.equal(flattened.length, 50);
      assert.equal(new Set(flattened).size, 50);
      assert.equal((await store.list({ status: "completed", limit: 100 })).length, 50);
    } finally {
      await pool.end();
    }
  },
);
