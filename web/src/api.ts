import type {
  CreateJobRequest,
  Job,
  JobEvent,
  SystemSnapshot,
  WorkerStatus,
} from "../../shared/contracts/index.ts";

const API_BASE = import.meta.env.VITE_API_URL ?? "";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: { "content-type": "application/json", ...options?.headers },
  });
  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as
      | { error?: { message?: string } }
      | null;
    throw new Error(body?.error?.message ?? `Request failed with status ${response.status}`);
  }
  return (await response.json()) as T;
}

export function getSnapshot(): Promise<SystemSnapshot> {
  return request("/v1/system/snapshot");
}

export function createJob(input: CreateJobRequest, idempotencyKey?: string): Promise<Job> {
  return request("/v1/jobs", {
    method: "POST",
    headers: idempotencyKey ? { "idempotency-key": idempotencyKey } : undefined,
    body: JSON.stringify(input),
  });
}

export async function getJobEvents(jobId: string): Promise<JobEvent[]> {
  const response = await request<{ events: JobEvent[] }>(`/v1/jobs/${jobId}/events`);
  return response.events;
}

export function cancelJob(jobId: string): Promise<Job> {
  return request(`/v1/jobs/${jobId}/cancel`, { method: "POST", body: "{}" });
}

export function retryJob(jobId: string): Promise<Job> {
  return request(`/v1/jobs/${jobId}/retry`, { method: "POST", body: "{}" });
}

export function workerCommand(
  command: "start" | "stop" | "fail-next",
): Promise<WorkerStatus> {
  return request(`/v1/demo/worker/${command}`, { method: "POST", body: "{}" });
}

export function configureWorker(processingTimeMs: number): Promise<WorkerStatus> {
  return request("/v1/demo/worker/configure", {
    method: "POST",
    body: JSON.stringify({ processingTimeMs }),
  });
}

export function subscribeToEvents(
  onEvent: (event: JobEvent) => void,
  onConnection: (state: "live" | "reconnecting") => void,
): () => void {
  const source = new EventSource(`${API_BASE}/v1/events/stream`);
  source.addEventListener("connected", () => onConnection("live"));
  source.addEventListener("job-event", (message) => {
    onConnection("live");
    onEvent(JSON.parse((message as MessageEvent<string>).data) as JobEvent);
  });
  source.onerror = () => onConnection("reconnecting");
  return () => source.close();
}
