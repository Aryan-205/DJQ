import assert from "node:assert/strict";
import { test } from "node:test";

import { QueueError } from "../src/domain/job.ts";
import { InMemoryJobStore } from "../src/infrastructure/in-memory-job-store.ts";

test("runs the core queued -> processing -> completed lifecycle", async () => {
  const store = new InMemoryJobStore();
  const { job } = await store.submit({
    queue: "email",
    type: "send-welcome-email",
    payload: { userId: "user_123" },
  });

  assert.equal(job.status, "queued");
  assert.equal(job.attempts, 0);

  const claim = await store.claim("email");
  assert.ok(claim);
  assert.equal(claim.job.id, job.id);
  assert.equal(claim.job.status, "processing");
  assert.equal(claim.job.attempts, 1);

  const completed = await store.complete(job.id, claim.leaseId, {
    messageId: "msg_123",
  });
  assert.equal(completed.status, "completed");
  assert.deepEqual(completed.result, { messageId: "msg_123" });
  assert.equal(completed.leaseId, undefined);
});

test("returns the existing job for a repeated idempotency key", async () => {
  const store = new InMemoryJobStore();
  const input = {
    queue: "email",
    type: "send-welcome-email",
    payload: { userId: "user_123" },
    idempotencyKey: "welcome-user-123",
  };

  const first = await store.submit(input);
  const second = await store.submit(input);

  assert.equal(first.created, true);
  assert.equal(second.created, false);
  assert.equal(second.job.id, first.job.id);
});

test("rejects completion from a worker with the wrong lease", async () => {
  const store = new InMemoryJobStore();
  const { job } = await store.submit({
    queue: "email",
    type: "send-welcome-email",
    payload: {},
  });
  await store.claim("email");

  await assert.rejects(
    store.complete(job.id, "stale-worker-lease"),
    (error: unknown) => error instanceof QueueError && error.code === "INVALID_LEASE",
  );
});

test("claims higher-priority queued work first", async () => {
  const store = new InMemoryJobStore();
  await store.submit({ queue: "email", type: "low", payload: {}, priority: 1 });
  const high = await store.submit({
    queue: "email",
    type: "high",
    payload: {},
    priority: 10,
  });

  const claim = await store.claim("email");
  assert.equal(claim?.job.id, high.job.id);
});

test("keeps delayed work hidden until it is eligible", async () => {
  const store = new InMemoryJobStore();
  const { job } = await store.submit({
    queue: "reports",
    type: "daily-report",
    payload: {},
    delayMs: 60_000,
  });

  assert.equal(job.status, "delayed");
  assert.equal(await store.claim("reports"), undefined);
});

test("schedules a retry after a failed attempt", async () => {
  const store = new InMemoryJobStore();
  const { job } = await store.submit({
    queue: "email",
    type: "flaky-email",
    payload: {},
    maxRetries: 2,
  });
  const claim = await store.claim("email");
  assert.ok(claim);

  const failed = await store.fail(job.id, claim.leaseId, {
    code: "SMTP_TIMEOUT",
  });
  assert.equal(failed.status, "retrying");
  assert.equal(failed.lastError?.code, "SMTP_TIMEOUT");
  assert.ok(Date.parse(failed.availableAt) > Date.now());
  assert.deepEqual(
    (await store.events(job.id)).map((event) => event.type),
    ["submitted", "claimed", "failed", "retry_scheduled"],
  );
});

test("moves an exhausted job to the dead-letter queue and supports manual retry", async () => {
  const store = new InMemoryJobStore();
  const { job } = await store.submit({
    queue: "payments",
    type: "capture-payment",
    payload: {},
    maxRetries: 0,
  });
  const claim = await store.claim("payments");
  assert.ok(claim);
  const dead = await store.fail(job.id, claim.leaseId, { code: "DECLINED" });
  assert.equal(dead.status, "dead_letter");

  const retried = await store.retry(job.id);
  assert.equal(retried.status, "queued");
  assert.equal(retried.attempts, 0);
  assert.equal(retried.lastError, undefined);
});

test("cooperatively finishes a cancellation requested during processing", async () => {
  const store = new InMemoryJobStore();
  const { job } = await store.submit({
    queue: "video",
    type: "transcode",
    payload: {},
  });
  const claim = await store.claim("video");
  assert.ok(claim);

  const requested = await store.cancel(job.id);
  assert.equal(requested.status, "cancel_requested");
  const cancelled = await store.complete(job.id, claim.leaseId, {});
  assert.equal(cancelled.status, "cancelled");
});
