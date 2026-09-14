export const JOB_STATUSES = [
  "queued",
  "processing",
  "completed",
  "cancelled",
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
  leaseId?: string;
  leaseExpiresAt?: string;
  result?: JsonObject;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
}

export interface SubmitJobInput {
  queue: string;
  type: string;
  payload: JsonObject;
  priority?: number;
  maxRetries?: number;
  idempotencyKey?: string;
}

export interface ClaimedJob {
  job: Job;
  leaseId: string;
  leaseExpiresAt: string;
}

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
