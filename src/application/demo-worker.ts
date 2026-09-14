import type { JobStore } from "./job-store.ts";

export interface DemoWorkerStatus {
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

export class DemoWorker {
  readonly #store: JobStore;
  readonly #id: string;
  #running = false;
  #busy = false;
  #timer?: NodeJS.Timeout;
  #activeJobId?: string;
  #processingTimeMs = 1_800;
  #completed = 0;
  #failed = 0;
  #failNext = false;
  #lastSeenAt = new Date().toISOString();

  constructor(store: JobStore, id = "demo-worker-1") {
    this.#store = store;
    this.#id = id;
  }

  start(): void {
    if (this.#running) return;
    this.#running = true;
    this.#schedule(0);
  }

  stop(): void {
    this.#running = false;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  failNext(): void {
    this.#failNext = true;
  }

  configure(processingTimeMs: number): void {
    this.#processingTimeMs = Math.min(Math.max(processingTimeMs, 100), 30_000);
  }

  status(): DemoWorkerStatus {
    const status: DemoWorkerStatus = {
      id: this.#id,
      running: this.#running,
      state: !this.#running ? "stopped" : this.#busy ? "processing" : "idle",
      processingTimeMs: this.#processingTimeMs,
      completed: this.#completed,
      failed: this.#failed,
      failNext: this.#failNext,
      lastSeenAt: this.#lastSeenAt,
    };
    if (this.#activeJobId) status.activeJobId = this.#activeJobId;
    return status;
  }

  #schedule(delayMs = 400): void {
    if (!this.#running) return;
    this.#timer = setTimeout(() => void this.#tick(), delayMs);
  }

  async #tick(): Promise<void> {
    if (!this.#running || this.#busy) return;
    this.#busy = true;
    this.#lastSeenAt = new Date().toISOString();
    try {
      const queues = await this.#store.queues();
      for (const queue of queues) {
        const claim = await this.#store.claim(queue.name);
        if (!claim) continue;
        this.#activeJobId = claim.job.id;
        await wait(this.#processingTimeMs);
        if (this.#failNext) {
          this.#failNext = false;
          await this.#store.fail(claim.job.id, claim.leaseId, {
            code: "DEMO_FAILURE",
            message: "The demo worker was instructed to fail this attempt",
          });
          this.#failed += 1;
        } else {
          await this.#store.complete(claim.job.id, claim.leaseId, {
            workerId: this.#id,
            message: "Demo job completed successfully",
          });
          this.#completed += 1;
        }
        break;
      }
    } catch (error) {
      console.error("Demo worker tick failed", error);
    } finally {
      this.#activeJobId = undefined;
      this.#busy = false;
      this.#lastSeenAt = new Date().toISOString();
      this.#schedule();
    }
  }
}

function wait(delayMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}
