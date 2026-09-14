import type {
  ClaimedJob,
  Job,
  JobEvent,
  JsonObject,
  ListJobsQuery,
  QueueSummary,
  SubmitJobInput,
  SystemSnapshot,
} from "../domain/job.ts";

export interface JobStore {
  submit(input: SubmitJobInput): Promise<{ job: Job; created: boolean }>;
  get(jobId: string): Promise<Job | undefined>;
  list(query?: ListJobsQuery): Promise<Job[]>;
  events(jobId: string): Promise<JobEvent[]>;
  snapshot(): Promise<SystemSnapshot>;
  queues(): Promise<QueueSummary[]>;
  claim(queue: string, visibilityTimeoutMs?: number): Promise<ClaimedJob | undefined>;
  heartbeat(jobId: string, leaseId: string, extendByMs?: number): Promise<Job>;
  complete(jobId: string, leaseId: string, result?: JsonObject): Promise<Job>;
  fail(jobId: string, leaseId: string, error: JsonObject): Promise<Job>;
  cancel(jobId: string): Promise<Job>;
  retry(jobId: string): Promise<Job>;
  close?(): Promise<void>;
}
