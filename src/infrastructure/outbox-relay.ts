import type { Pool } from "pg";

import type { EventBroker } from "../application/event-broker.ts";
import type { JobEvent } from "../domain/job.ts";

interface OutboxRow {
  id: string;
  payload: JobEvent;
}

export class OutboxRelay {
  readonly #pool: Pool;
  readonly #broker: EventBroker;
  #timer?: NodeJS.Timeout;
  #running = false;
  #working = false;

  constructor(pool: Pool, broker: EventBroker) {
    this.#pool = pool;
    this.#broker = broker;
  }

  start(): void {
    if (this.#running) return;
    this.#running = true;
    this.#schedule(0);
  }

  stop(): void {
    this.#running = false;
    if (this.#timer) clearTimeout(this.#timer);
  }

  #schedule(delayMs = 250): void {
    if (!this.#running) return;
    this.#timer = setTimeout(() => void this.#flush(), delayMs);
  }

  async #flush(): Promise<void> {
    if (!this.#running || this.#working) return;
    this.#working = true;
    try {
      const result = await this.#pool.query<OutboxRow>(
        `SELECT id::text, payload
         FROM outbox_events
         WHERE published_at IS NULL
         ORDER BY id
         LIMIT 100`,
      );
      for (const row of result.rows) {
        try {
          await this.#broker.publish(row.payload);
          await this.#pool.query(
            `UPDATE outbox_events
             SET published_at = now(), attempts = attempts + 1, last_error = NULL
             WHERE id = $1`,
            [row.id],
          );
        } catch (error) {
          await this.#pool.query(
            `UPDATE outbox_events
             SET attempts = attempts + 1, last_error = $2
             WHERE id = $1`,
            [row.id, error instanceof Error ? error.message : String(error)],
          );
          break;
        }
      }
    } catch (error) {
      console.error("Outbox relay failed", error);
    } finally {
      this.#working = false;
      this.#schedule();
    }
  }
}
