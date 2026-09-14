export const JOB_STATUSES = [
  "delayed",
  "queued",
  "processing",
  "retrying",
  "cancel_requested",
  "completed",
  "cancelled",
  "dead_letter",
] as const;

export type JobStatus = (typeof JOB_STATUSES)[number];
export type JsonObject = Record<string, unknown>;

export interface Job {
  id: string;
  queue: string;
  type: string;
  payload: JsonObject;
  priority: number;
  status: JobStatus;
  attempts: number;
  maxRetries: number;
  availableAt: string;
  leaseId?: string;
  leaseExpiresAt?: string;
  result?: JsonObject;
  lastError?: JsonObject;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  completedAt?: string;
  cancelledAt?: string;
}

export interface SubmitJobInput {
  queue: string;
  type: string;
  payload: JsonObject;
  priority?: number;
  maxRetries?: number;
  idempotencyKey?: string;
  scheduleAt?: string;
  delayMs?: number;
}

export interface ClaimedJob {
  job: Job;
  leaseId: string;
  leaseExpiresAt: string;
}

export const JOB_EVENT_TYPES = [
  "submitted",
  "claimed",
  "heartbeat",
  "completed",
  "failed",
  "retry_scheduled",
  "dead_lettered",
  "cancel_requested",
  "cancelled",
  "manual_retry",
  "lease_expired",
] as const;

export type JobEventType = (typeof JOB_EVENT_TYPES)[number];

export interface JobEvent {
  id: string;
  jobId: string;
  queue: string;
  type: JobEventType;
  fromStatus?: JobStatus;
  toStatus: JobStatus;
  data: JsonObject;
  createdAt: string;
}

export interface QueueSummary {
  name: string;
  counts: Record<JobStatus, number>;
  total: number;
}

export interface SystemSnapshot {
  jobs: Job[];
  queues: QueueSummary[];
  recentEvents: JobEvent[];
  generatedAt: string;
}

export interface ListJobsQuery {
  queue?: string;
  status?: JobStatus;
  limit?: number;
}

export type EventSink = (event: JobEvent) => void | Promise<void>;

export class QueueError extends Error {
  readonly code: string;
  readonly statusCode: number;

  constructor(code: string, message: string, statusCode: number) {
    super(message);
    this.name = "QueueError";
    this.code = code;
    this.statusCode = statusCode;
  }
}
