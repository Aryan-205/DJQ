import { randomUUID } from "node:crypto";

import type { JobStore } from "../application/job-store.ts";
import {
  JOB_STATUSES,
  QueueError,
  type ClaimedJob,
  type EventSink,
  type Job,
  type JobEvent,
  type JobEventType,
  type JobStatus,
  type JsonObject,
  type ListJobsQuery,
  type QueueSummary,
  type SubmitJobInput,
  type SystemSnapshot,
} from "../domain/job.ts";

const DEFAULT_VISIBILITY_TIMEOUT_MS = 30_000;
const DEFAULT_RETRY_DELAY_MS = 1_000;
const MAX_RETRY_DELAY_MS = 30_000;

function copy<T>(value: T): T {
  return structuredClone(value);
}

function emptyCounts(): Record<JobStatus, number> {
  return Object.fromEntries(JOB_STATUSES.map((status) => [status, 0])) as Record<
    JobStatus,
    number
  >;
}

export class InMemoryJobStore implements JobStore {
  readonly #jobs = new Map<string, Job>();
  readonly #idempotencyKeys = new Map<string, string>();
  readonly #events: JobEvent[] = [];
  readonly #eventSink?: EventSink;
  #eventSequence = 0;

  constructor(eventSink?: EventSink) {
    this.#eventSink = eventSink;
  }

  async submit(input: SubmitJobInput): Promise<{ job: Job; created: boolean }> {
    if (input.idempotencyKey) {
      const existingId = this.#idempotencyKeys.get(input.idempotencyKey);
      const existing = existingId ? this.#jobs.get(existingId) : undefined;
      if (existing) return { job: copy(existing), created: false };
    }

    const now = new Date();
    const availableAt = resolveAvailableAt(input, now);
    const status: JobStatus = availableAt.getTime() > now.getTime() ? "delayed" : "queued";
    const timestamp = now.toISOString();
    const job: Job = {
      id: randomUUID(),
      queue: input.queue,
      type: input.type,
      payload: copy(input.payload),
      priority: input.priority ?? 0,
      status,
      attempts: 0,
      maxRetries: input.maxRetries ?? 3,
      availableAt: availableAt.toISOString(),
      createdAt: timestamp,
      updatedAt: timestamp,
    };

    this.#jobs.set(job.id, job);
    if (input.idempotencyKey) this.#idempotencyKeys.set(input.idempotencyKey, job.id);
    this.#record(job, "submitted", undefined, { availableAt: job.availableAt });
    return { job: copy(job), created: true };
  }

  async get(jobId: string): Promise<Job | undefined> {
    const job = this.#jobs.get(jobId);
    return job ? copy(job) : undefined;
  }

  async list(query: ListJobsQuery = {}): Promise<Job[]> {
    const limit = Math.min(Math.max(query.limit ?? 100, 1), 500);
    return [...this.#jobs.values()]
      .filter((job) => !query.queue || job.queue === query.queue)
      .filter((job) => !query.status || job.status === query.status)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
      .slice(0, limit)
      .map(copy);
  }

  async events(jobId: string): Promise<JobEvent[]> {
    return this.#events.filter((event) => event.jobId === jobId).map(copy);
  }

  async snapshot(): Promise<SystemSnapshot> {
    return {
      jobs: await this.list({ limit: 200 }),
      queues: await this.queues(),
      recentEvents: this.#events.slice(-100).reverse().map(copy),
      generatedAt: new Date().toISOString(),
    };
  }

  async queues(): Promise<QueueSummary[]> {
    const summaries = new Map<string, QueueSummary>();
    for (const job of this.#jobs.values()) {
      let summary = summaries.get(job.queue);
      if (!summary) {
        summary = { name: job.queue, counts: emptyCounts(), total: 0 };
        summaries.set(job.queue, summary);
      }
      summary.counts[job.status] += 1;
      summary.total += 1;
    }
    return [...summaries.values()].sort((a, b) => a.name.localeCompare(b.name)).map(copy);
  }

  async claim(
    queue: string,
    visibilityTimeoutMs = DEFAULT_VISIBILITY_TIMEOUT_MS,
  ): Promise<ClaimedJob | undefined> {
    this.#reapExpiredLeases();
    const now = new Date();
    const job = [...this.#jobs.values()]
      .filter(
        (candidate) =>
          candidate.queue === queue &&
          ["queued", "delayed", "retrying"].includes(candidate.status) &&
          Date.parse(candidate.availableAt) <= now.getTime(),
      )
      .sort((a, b) => b.priority - a.priority || a.createdAt.localeCompare(b.createdAt))[0];

    if (!job) return undefined;
    const fromStatus = job.status;
    const leaseId = randomUUID();
    const leaseExpiresAt = new Date(now.getTime() + visibilityTimeoutMs).toISOString();
    job.status = "processing";
    job.attempts += 1;
    job.startedAt = now.toISOString();
    job.updatedAt = now.toISOString();
    job.leaseId = leaseId;
    job.leaseExpiresAt = leaseExpiresAt;
    this.#record(job, "claimed", fromStatus, { leaseId, leaseExpiresAt });
    return { job: copy(job), leaseId, leaseExpiresAt };
  }

  async heartbeat(
    jobId: string,
    leaseId: string,
    extendByMs = DEFAULT_VISIBILITY_TIMEOUT_MS,
  ): Promise<Job> {
    const job = this.#ownedJob(jobId, leaseId);
    job.leaseExpiresAt = new Date(Date.now() + extendByMs).toISOString();
    job.updatedAt = new Date().toISOString();
    this.#record(job, "heartbeat", "processing", { leaseExpiresAt: job.leaseExpiresAt });
    return copy(job);
  }

  async complete(jobId: string, leaseId: string, result: JsonObject = {}): Promise<Job> {
    const job = this.#ownedJob(jobId, leaseId);
    const fromStatus = job.status;
    job.status = "completed";
    job.result = copy(result);
    job.completedAt = new Date().toISOString();
    job.updatedAt = job.completedAt;
    delete job.leaseId;
    delete job.leaseExpiresAt;
    this.#record(job, "completed", fromStatus, { result: job.result });
    return copy(job);
  }

  async fail(jobId: string, leaseId: string, error: JsonObject): Promise<Job> {
    const job = this.#ownedJob(jobId, leaseId);
    job.lastError = copy(error);
    delete job.leaseId;
    delete job.leaseExpiresAt;
    const now = new Date();

    this.#record(job, "failed", "processing", { error: job.lastError, attempt: job.attempts });
    if (job.attempts <= job.maxRetries) {
      const retryDelayMs = Math.min(
        DEFAULT_RETRY_DELAY_MS * 2 ** Math.max(job.attempts - 1, 0),
        MAX_RETRY_DELAY_MS,
      );
      job.status = "retrying";
      job.availableAt = new Date(now.getTime() + retryDelayMs).toISOString();
      job.updatedAt = now.toISOString();
      this.#record(job, "retry_scheduled", "processing", {
        retryDelayMs,
        availableAt: job.availableAt,
      });
    } else {
      job.status = "dead_letter";
      job.updatedAt = now.toISOString();
      this.#record(job, "dead_lettered", "processing", { attempts: job.attempts });
    }
    return copy(job);
  }

  async cancel(jobId: string): Promise<Job> {
    const job = this.#requiredJob(jobId);
    if (job.status === "cancelled") return copy(job);
    if (["completed", "dead_letter"].includes(job.status)) {
      throw new QueueError("JOB_TERMINAL", `Cannot cancel a ${job.status} job`, 409);
    }

    const fromStatus = job.status;
    job.updatedAt = new Date().toISOString();
    if (job.status === "processing") {
      job.status = "cancel_requested";
      this.#record(job, "cancel_requested", fromStatus, {});
    } else {
      job.status = "cancelled";
      job.cancelledAt = job.updatedAt;
      this.#record(job, "cancelled", fromStatus, {});
    }
    return copy(job);
  }

  async retry(jobId: string): Promise<Job> {
    const job = this.#requiredJob(jobId);
    if (job.status !== "dead_letter") {
      throw new QueueError("JOB_NOT_RETRYABLE", "Only dead-letter jobs can be retried", 409);
    }
    job.status = "queued";
    job.attempts = 0;
    job.availableAt = new Date().toISOString();
    job.updatedAt = job.availableAt;
    delete job.lastError;
    this.#record(job, "manual_retry", "dead_letter", {});
    return copy(job);
  }

  #requiredJob(jobId: string): Job {
    const job = this.#jobs.get(jobId);
    if (!job) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
    return job;
  }

  #ownedJob(jobId: string, leaseId: string): Job {
    const job = this.#requiredJob(jobId);
    if (job.status !== "processing") {
      throw new QueueError(
        "JOB_NOT_PROCESSING",
        `Cannot acknowledge a job in the ${job.status} state`,
        409,
      );
    }
    if (job.leaseId !== leaseId) {
      throw new QueueError("INVALID_LEASE", "Lease does not own this job", 409);
    }
    if (!job.leaseExpiresAt || Date.parse(job.leaseExpiresAt) <= Date.now()) {
      throw new QueueError("LEASE_EXPIRED", "Lease has expired", 409);
    }
    return job;
  }

  #reapExpiredLeases(): void {
    const now = Date.now();
    for (const job of this.#jobs.values()) {
      if (
        job.status === "processing" &&
        job.leaseExpiresAt &&
        Date.parse(job.leaseExpiresAt) <= now
      ) {
        delete job.leaseId;
        delete job.leaseExpiresAt;
        job.status = "queued";
        job.availableAt = new Date(now).toISOString();
        job.updatedAt = job.availableAt;
        this.#record(job, "lease_expired", "processing", {});
      }
    }
  }

  #record(
    job: Job,
    type: JobEventType,
    fromStatus: JobStatus | undefined,
    data: JsonObject,
  ): void {
    const event: JobEvent = {
      id: String(++this.#eventSequence),
      jobId: job.id,
      queue: job.queue,
      type,
      toStatus: job.status,
      data: copy(data),
      createdAt: new Date().toISOString(),
    };
    if (fromStatus) event.fromStatus = fromStatus;
    this.#events.push(event);
    if (this.#eventSink) {
      Promise.resolve(this.#eventSink(copy(event))).catch((error: unknown) => {
        console.error("Failed to publish in-memory job event", error);
      });
    }
  }
}

function resolveAvailableAt(input: SubmitJobInput, now: Date): Date {
  if (input.scheduleAt) {
    const timestamp = Date.parse(input.scheduleAt);
    if (Number.isNaN(timestamp)) {
      throw new QueueError("VALIDATION_ERROR", "scheduleAt must be an ISO timestamp", 400);
    }
    return new Date(timestamp);
  }
  const delayMs = input.delayMs ?? 0;
  return new Date(now.getTime() + Math.max(delayMs, 0));
}
