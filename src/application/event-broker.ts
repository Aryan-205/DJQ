import type { JobEvent } from "../domain/job.ts";

export type EventListener = (event: JobEvent) => void;

export interface EventBroker {
  publish(event: JobEvent): Promise<void>;
  subscribe(listener: EventListener): () => void;
  close(): Promise<void>;
}

export class MemoryEventBroker implements EventBroker {
  readonly #listeners = new Set<EventListener>();

  async publish(event: JobEvent): Promise<void> {
    for (const listener of this.#listeners) listener(structuredClone(event));
  }

  subscribe(listener: EventListener): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  async close(): Promise<void> {
    this.#listeners.clear();
  }
}
