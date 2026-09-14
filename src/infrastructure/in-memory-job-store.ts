import { randomUUID } from "node:crypto";

import type { JobStore } from "../application/job-store.ts";
import {
  QueueError,
  type ClaimedJob,
  type Job,
  type JsonObject,
  type SubmitJobInput,
} from "../domain/job.ts";

const DEFAULT_VISIBILITY_TIMEOUT_MS = 30_000;

function copyJob(job: Job): Job {
  return structuredClone(job);
}

export class InMemoryJobStore implements JobStore {
  readonly #jobs = new Map<string, Job>();
  readonly #idempotencyKeys = new Map<string, string>();

  async submit(input: SubmitJobInput): Promise<{ job: Job; created: boolean }> {
    if (input.idempotencyKey) {
      const existingId = this.#idempotencyKeys.get(input.idempotencyKey);
      if (existingId) {
        const existing = this.#jobs.get(existingId);
        if (existing) return { job: copyJob(existing), created: false };
      }
    }

    const now = new Date().toISOString();
    const job: Job = {
      id: randomUUID(),
      queue: input.queue,
      type: input.type,
      payload: structuredClone(input.payload),
      priority: input.priority ?? 0,
      status: "queued",
      attempts: 0,
      maxRetries: input.maxRetries ?? 3,
      createdAt: now,
    };

    this.#jobs.set(job.id, job);
    if (input.idempotencyKey) {
      this.#idempotencyKeys.set(input.idempotencyKey, job.id);
    }

    return { job: copyJob(job), created: true };
  }

  async get(jobId: string): Promise<Job | undefined> {
    const job = this.#jobs.get(jobId);
    return job ? copyJob(job) : undefined;
  }

  async claim(
    queue: string,
    visibilityTimeoutMs = DEFAULT_VISIBILITY_TIMEOUT_MS,
  ): Promise<ClaimedJob | undefined> {
    const job = [...this.#jobs.values()]
      .filter((candidate) => candidate.queue === queue && candidate.status === "queued")
      .sort((a, b) => b.priority - a.priority || a.createdAt.localeCompare(b.createdAt))[0];

    if (!job) return undefined;

    const now = new Date();
    const leaseId = randomUUID();
    const leaseExpiresAt = new Date(now.getTime() + visibilityTimeoutMs).toISOString();

    job.status = "processing";
    job.attempts += 1;
    job.startedAt = now.toISOString();
    job.leaseId = leaseId;
    job.leaseExpiresAt = leaseExpiresAt;

    return { job: copyJob(job), leaseId, leaseExpiresAt };
  }

  async complete(jobId: string, leaseId: string, result: JsonObject = {}): Promise<Job> {
    const job = this.#jobs.get(jobId);
    if (!job) {
      throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
    }
    if (job.status !== "processing") {
      throw new QueueError(
        "JOB_NOT_PROCESSING",
        `Cannot complete a job in the ${job.status} state`,
        409,
      );
    }
    if (job.leaseId !== leaseId) {
      throw new QueueError("INVALID_LEASE", "Lease does not own this job", 409);
    }
    if (!job.leaseExpiresAt || Date.parse(job.leaseExpiresAt) <= Date.now()) {
      throw new QueueError("LEASE_EXPIRED", "Lease has expired", 409);
    }

    job.status = "completed";
    job.result = structuredClone(result);
    job.completedAt = new Date().toISOString();
    delete job.leaseId;
    delete job.leaseExpiresAt;

    return copyJob(job);
  }
}
