import { createClient, type RedisClientType } from "redis";

import type { EventBroker, EventListener } from "../application/event-broker.ts";
import type { JobEvent } from "../domain/job.ts";

const CHANNEL = "djqueue:job-events";

export class RedisEventBroker implements EventBroker {
  readonly #publisher: RedisClientType;
  readonly #subscriber: RedisClientType;
  readonly #listeners = new Set<EventListener>();

  private constructor(publisher: RedisClientType, subscriber: RedisClientType) {
    this.#publisher = publisher;
    this.#subscriber = subscriber;
  }

  static async connect(url: string): Promise<RedisEventBroker> {
    const publisher = createClient({ url });
    const subscriber = publisher.duplicate();
    publisher.on("error", (error) => console.error("Redis publisher error", error));
    subscriber.on("error", (error) => console.error("Redis subscriber error", error));
    await Promise.all([publisher.connect(), subscriber.connect()]);
    const broker = new RedisEventBroker(publisher, subscriber);
    await subscriber.subscribe(CHANNEL, (message) => broker.#receive(message));
    return broker;
  }

  async publish(event: JobEvent): Promise<void> {
    await this.#publisher.publish(CHANNEL, JSON.stringify(event));
  }

  subscribe(listener: EventListener): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  async close(): Promise<void> {
    this.#listeners.clear();
    await Promise.all([this.#subscriber.quit(), this.#publisher.quit()]);
  }

  #receive(message: string): void {
    try {
      const event = JSON.parse(message) as JobEvent;
      for (const listener of this.#listeners) listener(event);
    } catch (error) {
      console.error("Ignoring malformed Redis job event", error);
    }
  }
}
