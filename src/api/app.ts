import express, { type ErrorRequestHandler } from "express";

import type { DemoWorker } from "../application/demo-worker.ts";
import type { EventBroker } from "../application/event-broker.ts";
import type { JobStore } from "../application/job-store.ts";
import { QueueError } from "../domain/job.ts";
import { isBodyParserError } from "./routes/helpers.ts";
import { demoRouter } from "./routes/demo.ts";
import { jobsRouter } from "./routes/jobs.ts";
import { systemRouter } from "./routes/system.ts";

export interface AppDependencies {
  store: JobStore;
  broker: EventBroker;
  worker: DemoWorker;
}

export function createApp({ store, broker, worker }: AppDependencies) {
  const app = express();
  app.disable("x-powered-by");
  app.use((request, response, next) => {
    const allowedOrigin = process.env.WEB_ORIGIN ?? "http://localhost:5173";
    response.setHeader("access-control-allow-origin", allowedOrigin);
    response.setHeader("access-control-allow-headers", "content-type,idempotency-key,last-event-id");
    response.setHeader("access-control-allow-methods", "GET,POST,OPTIONS");
    if (request.method === "OPTIONS") {
      response.status(204).end();
      return;
    }
    next();
  });
  app.use(express.json({ limit: "1mb" }));

  app.use(systemRouter(store, broker, worker));
  app.use(jobsRouter(store));
  app.use(demoRouter(worker));

  app.use((_request, _response, next) => {
    next(new QueueError("ROUTE_NOT_FOUND", "Route not found", 404));
  });

  const errorHandler: ErrorRequestHandler = (error: unknown, _request, response, _next) => {
    if (error instanceof QueueError) {
      response.status(error.statusCode).json({ error: { code: error.code, message: error.message } });
      return;
    }
    if (isBodyParserError(error)) {
      const tooLarge = error.type === "entity.too.large";
      response.status(tooLarge ? 413 : 400).json({
        error: {
          code: tooLarge ? "PAYLOAD_TOO_LARGE" : "INVALID_JSON",
          message: tooLarge ? "Request body exceeds 1 MB" : "Request body must be valid JSON",
        },
      });
      return;
    }
    console.error(error);
    response.status(500).json({
      error: { code: "INTERNAL_ERROR", message: "An unexpected error occurred" },
    });
  };
  app.use(errorHandler);
  return app;
}
