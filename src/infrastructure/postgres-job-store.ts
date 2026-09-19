import { randomUUID } from "node:crypto";
import type { Pool, PoolClient, QueryResultRow } from "pg";

import type { JobStore } from "../application/job-store.ts";
import {
  JOB_STATUSES,
  QueueError,
  type ClaimedJob,
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

export class PostgresJobStore implements JobStore {
  readonly #pool: Pool;

  constructor(pool: Pool) {
    this.#pool = pool;
  }

  async submit(input: SubmitJobInput): Promise<{ job: Job; created: boolean }> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `INSERT INTO queues (name, max_retries)
         VALUES ($1, $2)
         ON CONFLICT (name) DO NOTHING`,
        [input.queue, input.maxRetries ?? 3],
      );

      const now = new Date();
      const availableAt = resolveAvailableAt(input, now);
      const status: JobStatus = availableAt.getTime() > now.getTime() ? "delayed" : "queued";
      const id = randomUUID();
      const inserted = await client.query(
        `INSERT INTO jobs (
           id, queue_name, type, payload, priority, status, max_retries,
           available_at, idempotency_key
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
         RETURNING *`,
        [
          id,
          input.queue,
          input.type,
          input.payload,
          input.priority ?? 0,
          status,
          input.maxRetries ?? 3,
          availableAt,
          input.idempotencyKey ?? null,
        ],
      );

      if (inserted.rowCount === 0 && input.idempotencyKey) {
        const existing = await client.query(
          "SELECT * FROM jobs WHERE idempotency_key = $1",
          [input.idempotencyKey],
        );
        await client.query("COMMIT");
        return { job: mapJob(existing.rows[0]), created: false };
      }

      const job = mapJob(inserted.rows[0]);
      await insertEvent(client, job, "submitted", undefined, { availableAt: job.availableAt });
      await client.query("COMMIT");
      return { job, created: true };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async get(jobId: string): Promise<Job | undefined> {
    const result = await this.#pool.query("SELECT * FROM jobs WHERE id = $1", [jobId]);
    return result.rows[0] ? mapJob(result.rows[0]) : undefined;
  }

  async list(query: ListJobsQuery = {}): Promise<Job[]> {
    const conditions: string[] = [];
    const values: unknown[] = [];
    if (query.queue) {
      values.push(query.queue);
      conditions.push(`queue_name = $${values.length}`);
    }
    if (query.status) {
      values.push(query.status);
      conditions.push(`status = $${values.length}`);
    }
    values.push(Math.min(Math.max(query.limit ?? 100, 1), 500));
    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const result = await this.#pool.query(
      `SELECT * FROM jobs ${where} ORDER BY created_at DESC LIMIT $${values.length}`,
      values,
    );
    return result.rows.map(mapJob);
  }

  async events(jobId: string): Promise<JobEvent[]> {
    const result = await this.#pool.query(
      "SELECT * FROM job_events WHERE job_id = $1 ORDER BY id",
      [jobId],
    );
    return result.rows.map(mapEvent);
  }

  async snapshot(): Promise<SystemSnapshot> {
    const [jobs, queues, events] = await Promise.all([
      this.list({ limit: 200 }),
      this.queues(),
      this.#pool.query("SELECT * FROM job_events ORDER BY id DESC LIMIT 100"),
    ]);
    return {
      jobs,
      queues,
      recentEvents: events.rows.map(mapEvent),
      generatedAt: new Date().toISOString(),
    };
  }

  async queues(): Promise<QueueSummary[]> {
    const result = await this.#pool.query(
      `SELECT q.name, j.status, count(j.id)::int AS count
       FROM queues q
       LEFT JOIN jobs j ON j.queue_name = q.name
       GROUP BY q.name, j.status
       ORDER BY q.name`,
    );
    const summaries = new Map<string, QueueSummary>();
    for (const row of result.rows) {
      let summary = summaries.get(String(row.name));
      if (!summary) {
        summary = { name: String(row.name), counts: emptyCounts(), total: 0 };
        summaries.set(summary.name, summary);
      }
      if (row.status) {
        const status = row.status as JobStatus;
        const count = Number(row.count);
        summary.counts[status] = count;
        summary.total += count;
      }
    }
    return [...summaries.values()];
  }

  async claim(
    queue: string,
    visibilityTimeoutMs = DEFAULT_VISIBILITY_TIMEOUT_MS,
  ): Promise<ClaimedJob | undefined> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const expired = await client.query(
        `UPDATE jobs
         SET status = CASE WHEN status = 'cancel_requested' THEN 'cancelled' ELSE 'queued' END,
             cancelled_at = CASE WHEN status = 'cancel_requested' THEN now() ELSE cancelled_at END,
             lease_id = NULL, lease_expires_at = NULL, available_at = now(), updated_at = now()
         WHERE status IN ('processing', 'cancel_requested') AND lease_expires_at <= now()
         RETURNING *`,
      );
      for (const row of expired.rows) {
        const expiredJob = mapJob(row);
        await insertEvent(
          client,
          expiredJob,
          expiredJob.status === "cancelled" ? "cancelled" : "lease_expired",
          expiredJob.status === "cancelled" ? "cancel_requested" : "processing",
          {},
        );
      }

      const candidate = await client.query(
        `SELECT j.*
         FROM jobs j
         JOIN queues q ON q.name = j.queue_name
         WHERE j.queue_name = $1
           AND q.status = 'active'
           AND j.status IN ('queued', 'delayed', 'retrying')
           AND j.available_at <= now()
         ORDER BY j.priority DESC, j.created_at ASC
         FOR UPDATE OF j SKIP LOCKED
         LIMIT 1`,
        [queue],
      );
      if (!candidate.rows[0]) {
        await client.query("COMMIT");
        return undefined;
      }

      const fromStatus = candidate.rows[0].status as JobStatus;
      const leaseId = randomUUID();
      const updated = await client.query(
        `UPDATE jobs
         SET status = 'processing', attempts = attempts + 1, lease_id = $2,
             lease_expires_at = now() + ($3 * interval '1 millisecond'),
             started_at = COALESCE(started_at, now()), updated_at = now()
         WHERE id = $1
         RETURNING *`,
        [candidate.rows[0].id, leaseId, visibilityTimeoutMs],
      );
      const job = mapJob(updated.rows[0]);
      await insertEvent(client, job, "claimed", fromStatus, {
        leaseId,
        leaseExpiresAt: job.leaseExpiresAt,
      });
      await client.query("COMMIT");
      return { job, leaseId, leaseExpiresAt: job.leaseExpiresAt! };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async heartbeat(
    jobId: string,
    leaseId: string,
    extendByMs = DEFAULT_VISIBILITY_TIMEOUT_MS,
  ): Promise<Job> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        `UPDATE jobs
         SET lease_expires_at = now() + ($3 * interval '1 millisecond'), updated_at = now()
         WHERE id = $1 AND lease_id = $2 AND status = 'processing' AND lease_expires_at > now()
         RETURNING *`,
        [jobId, leaseId, extendByMs],
      );
      if (!result.rows[0]) await throwLeaseError(client, jobId, leaseId);
      const job = mapJob(result.rows[0]);
      await insertEvent(client, job, "heartbeat", "processing", {
        leaseExpiresAt: job.leaseExpiresAt,
      });
      await client.query("COMMIT");
      return job;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async complete(jobId: string, leaseId: string, result: JsonObject = {}): Promise<Job> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const selected = await client.query("SELECT * FROM jobs WHERE id = $1 FOR UPDATE", [jobId]);
      if (!selected.rows[0]) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
      const current = mapJob(selected.rows[0]);
      assertOwned(current, leaseId, true);
      const cancelled = current.status === "cancel_requested";
      const updated = await client.query(
        `UPDATE jobs
         SET status = $2, result = CASE WHEN $2 = 'completed' THEN $3 ELSE result END,
             completed_at = CASE WHEN $2 = 'completed' THEN now() ELSE completed_at END,
             cancelled_at = CASE WHEN $2 = 'cancelled' THEN now() ELSE cancelled_at END,
             updated_at = now(),
             lease_id = NULL, lease_expires_at = NULL
         WHERE id = $1
         RETURNING *`,
        [jobId, cancelled ? "cancelled" : "completed", result],
      );
      const job = mapJob(updated.rows[0]);
      await insertEvent(
        client,
        job,
        cancelled ? "cancelled" : "completed",
        current.status,
        cancelled ? { acknowledgedByWorker: true } : { result },
      );
      await client.query("COMMIT");
      return job;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async fail(jobId: string, leaseId: string, error: JsonObject): Promise<Job> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const selected = await client.query("SELECT * FROM jobs WHERE id = $1 FOR UPDATE", [jobId]);
      if (!selected.rows[0]) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
      const current = mapJob(selected.rows[0]);
      assertOwned(current, leaseId, true);
      if (current.status === "cancel_requested") {
        const cancelled = await client.query(
          `UPDATE jobs SET status = 'cancelled', cancelled_at = now(), updated_at = now(),
             lease_id = NULL, lease_expires_at = NULL WHERE id = $1 RETURNING *`,
          [jobId],
        );
        const job = mapJob(cancelled.rows[0]);
        await insertEvent(client, job, "cancelled", "cancel_requested", {
          acknowledgedByWorker: true,
        });
        await client.query("COMMIT");
        return job;
      }
      const retrying = current.attempts <= current.maxRetries;
      const delayMs = Math.min(1_000 * 2 ** Math.max(current.attempts - 1, 0), 30_000);
      const status: JobStatus = retrying ? "retrying" : "dead_letter";
      const updated = await client.query(
        `UPDATE jobs
         SET status = $2, last_error = $3,
             available_at = CASE WHEN $2 = 'retrying'
               THEN now() + ($4 * interval '1 millisecond') ELSE available_at END,
             lease_id = NULL, lease_expires_at = NULL, updated_at = now()
         WHERE id = $1
         RETURNING *`,
        [jobId, status, error, delayMs],
      );
      const job = mapJob(updated.rows[0]);
      await insertEvent(client, job, "failed", "processing", { error, attempt: job.attempts });
      await insertEvent(
        client,
        job,
        retrying ? "retry_scheduled" : "dead_lettered",
        "processing",
        retrying ? { retryDelayMs: delayMs, availableAt: job.availableAt } : { attempts: job.attempts },
      );
      await client.query("COMMIT");
      return job;
    } catch (caught) {
      await client.query("ROLLBACK");
      throw caught;
    } finally {
      client.release();
    }
  }

  async cancel(jobId: string): Promise<Job> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const selected = await client.query("SELECT * FROM jobs WHERE id = $1 FOR UPDATE", [jobId]);
      if (!selected.rows[0]) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
      const current = mapJob(selected.rows[0]);
      if (current.status === "cancelled") {
        await client.query("COMMIT");
        return current;
      }
      if (["completed", "dead_letter"].includes(current.status)) {
        throw new QueueError("JOB_TERMINAL", `Cannot cancel a ${current.status} job`, 409);
      }
      const nextStatus: JobStatus = current.status === "processing" ? "cancel_requested" : "cancelled";
      const updated = await client.query(
        `UPDATE jobs
         SET status = $2, updated_at = now(),
             cancelled_at = CASE WHEN $2 = 'cancelled' THEN now() ELSE cancelled_at END
         WHERE id = $1 RETURNING *`,
        [jobId, nextStatus],
      );
      const job = mapJob(updated.rows[0]);
      await insertEvent(
        client,
        job,
        nextStatus === "cancelled" ? "cancelled" : "cancel_requested",
        current.status,
        {},
      );
      await client.query("COMMIT");
      return job;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async retry(jobId: string): Promise<Job> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const updated = await client.query(
        `UPDATE jobs
         SET status = 'queued', attempts = 0, available_at = now(), last_error = NULL,
             updated_at = now()
         WHERE id = $1 AND status = 'dead_letter'
         RETURNING *`,
        [jobId],
      );
      if (!updated.rows[0]) {
        const existing = await client.query("SELECT status FROM jobs WHERE id = $1", [jobId]);
        if (!existing.rows[0]) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
        throw new QueueError("JOB_NOT_RETRYABLE", "Only dead-letter jobs can be retried", 409);
      }
      const job = mapJob(updated.rows[0]);
      await insertEvent(client, job, "manual_retry", "dead_letter", {});
      await client.query("COMMIT");
      return job;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.#pool.end();
  }
}

async function insertEvent(
  client: PoolClient,
  job: Job,
  type: JobEventType,
  fromStatus: JobStatus | undefined,
  data: JsonObject,
): Promise<JobEvent> {
  const inserted = await client.query(
    `INSERT INTO job_events (job_id, queue_name, event_type, from_status, to_status, data)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING id::text, created_at`,
    [job.id, job.queue, type, fromStatus ?? null, job.status, data],
  );
  const event: JobEvent = {
    id: String(inserted.rows[0].id),
    jobId: job.id,
    queue: job.queue,
    type,
    toStatus: job.status,
    data,
    createdAt: toIso(inserted.rows[0].created_at),
  };
  if (fromStatus) event.fromStatus = fromStatus;
  await client.query(
    `INSERT INTO outbox_events (topic, event_type, aggregate_id, payload)
     VALUES ('job-events', $1, $2, $3)`,
    [type, job.id, event],
  );
  return event;
}

async function throwLeaseError(client: PoolClient, jobId: string, leaseId: string): Promise<never> {
  const result = await client.query("SELECT * FROM jobs WHERE id = $1", [jobId]);
  if (!result.rows[0]) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
  assertOwned(mapJob(result.rows[0]), leaseId);
  throw new QueueError("LEASE_CONFLICT", "The lease could not be updated", 409);
}

function assertOwned(job: Job, leaseId: string, allowCancelRequested = false): void {
  if (job.status !== "processing" && !(allowCancelRequested && job.status === "cancel_requested")) {
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
}

function mapJob(row: QueryResultRow | undefined): Job {
  if (!row) throw new Error("Expected a job row");
  const job: Job = {
    id: String(row.id),
    queue: String(row.queue_name),
    type: String(row.type),
    payload: asObject(row.payload),
    priority: Number(row.priority),
    status: row.status as JobStatus,
    attempts: Number(row.attempts),
    maxRetries: Number(row.max_retries),
    availableAt: toIso(row.available_at),
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
  };
  assignDate(job, "leaseExpiresAt", row.lease_expires_at);
  assignDate(job, "startedAt", row.started_at);
  assignDate(job, "completedAt", row.completed_at);
  assignDate(job, "cancelledAt", row.cancelled_at);
  if (row.lease_id) job.leaseId = String(row.lease_id);
  if (row.result) job.result = asObject(row.result);
  if (row.last_error) job.lastError = asObject(row.last_error);
  return job;
}

function mapEvent(row: QueryResultRow): JobEvent {
  const event: JobEvent = {
    id: String(row.id),
    jobId: String(row.job_id),
    queue: String(row.queue_name),
    type: row.event_type as JobEventType,
    toStatus: row.to_status as JobStatus,
    data: asObject(row.data),
    createdAt: toIso(row.created_at),
  };
  if (row.from_status) event.fromStatus = row.from_status as JobStatus;
  return event;
}

function emptyCounts(): Record<JobStatus, number> {
  return Object.fromEntries(JOB_STATUSES.map((status) => [status, 0])) as Record<
    JobStatus,
    number
  >;
}

function asObject(value: unknown): JsonObject {
  if (value && typeof value === "object" && !Array.isArray(value)) return value as JsonObject;
  return {};
}

function assignDate(job: Job, key: keyof Job, value: unknown): void {
  if (value) Object.assign(job, { [key]: toIso(value) });
}

function toIso(value: unknown): string {
  return value instanceof Date ? value.toISOString() : new Date(String(value)).toISOString();
}

function resolveAvailableAt(input: SubmitJobInput, now: Date): Date {
  if (input.scheduleAt) {
    const timestamp = Date.parse(input.scheduleAt);
    if (Number.isNaN(timestamp)) {
      throw new QueueError("VALIDATION_ERROR", "scheduleAt must be an ISO timestamp", 400);
    }
    return new Date(timestamp);
  }
  return new Date(now.getTime() + Math.max(input.delayMs ?? 0, 0));
}
