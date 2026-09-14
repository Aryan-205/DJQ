export type JobStatus =
  | "delayed"
  | "queued"
  | "processing"
  | "retrying"
  | "cancel_requested"
  | "completed"
  | "cancelled"
  | "dead_letter";

export interface Job {
  id: string;
  queue: string;
  type: string;
  payload: Record<string, unknown>;
  priority: number;
  status: JobStatus;
  attempts: number;
  maxRetries: number;
  availableAt: string;
  leaseId?: string;
  leaseExpiresAt?: string;
  result?: Record<string, unknown>;
  lastError?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  completedAt?: string;
  cancelledAt?: string;
}

export interface JobEvent {
  id: string;
  jobId: string;
  queue: string;
  type: string;
  fromStatus?: JobStatus;
  toStatus: JobStatus;
  data: Record<string, unknown>;
  createdAt: string;
}

export interface QueueSummary {
  name: string;
  counts: Record<JobStatus, number>;
  total: number;
}

export interface WorkerStatus {
  id: string;
  running: boolean;
  state: "stopped" | "idle" | "processing";
  activeJobId?: string;
  processingTimeMs: number;
  completed: number;
  failed: number;
  failNext: boolean;
  lastSeenAt: string;
}

export interface SystemSnapshot {
  jobs: Job[];
  queues: QueueSummary[];
  recentEvents: JobEvent[];
  generatedAt: string;
  worker: WorkerStatus;
}

export interface CreateJobRequest {
  queue: string;
  type: string;
  payload: Record<string, unknown>;
  priority: number;
  maxRetries: number;
  delayMs: number;
}
