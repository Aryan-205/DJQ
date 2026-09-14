import type {
  ClaimedJob,
  Job,
  JsonObject,
  SubmitJobInput,
} from "../domain/job.ts";

export interface JobStore {
  submit(input: SubmitJobInput): Promise<{ job: Job; created: boolean }>;
  get(jobId: string): Promise<Job | undefined>;
  claim(queue: string, visibilityTimeoutMs?: number): Promise<ClaimedJob | undefined>;
  complete(jobId: string, leaseId: string, result?: JsonObject): Promise<Job>;
}
