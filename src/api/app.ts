import express, {
  type ErrorRequestHandler,
  type NextFunction,
  type Request,
  type Response,
} from "express";

import type { DemoWorker } from "../application/demo-worker.ts";
import type { EventBroker } from "../application/event-broker.ts";
import type { JobStore } from "../application/job-store.ts";
import {
  JOB_STATUSES,
  QueueError,
  type JobStatus,
  type JsonObject,
  type SubmitJobInput,
} from "../domain/job.ts";

export interface AppDependencies {
  store: JobStore;
  broker: EventBroker;
  worker: DemoWorker;
}

function objectBody(request: Request): JsonObject {
  const body: unknown = request.body;
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new QueueError("VALIDATION_ERROR", "Request body must be a JSON object", 400);
  }
  return body as JsonObject;
}

function requiredString(body: JsonObject, field: string): string {
  const value = body[field];
  if (typeof value !== "string" || value.trim() === "") {
    throw new QueueError("VALIDATION_ERROR", `${field} must be a non-empty string`, 400);
  }
  return value.trim();
}

function routeParam(request: Request, field: string): string {
  const value = request.params[field];
  if (typeof value !== "string" || value === "") {
    throw new QueueError("VALIDATION_ERROR", `${field} path parameter is required`, 400);
  }
  return value;
}

function optionalNumber(value: unknown, field: string, minimum: number, maximum: number) {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value) || value < minimum || value > maximum) {
    throw new QueueError(
      "VALIDATION_ERROR",
      `${field} must be a number between ${minimum} and ${maximum}`,
      400,
    );
  }
  return value;
}

function asyncRoute(
  handler: (request: Request, response: Response) => Promise<void>,
): (request: Request, response: Response, next: NextFunction) => void {
  return (request, response, next) => handler(request, response).catch(next);
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

  app.get("/health/live", (_request, response) => {
    response.status(200).json({ status: "ok" });
  });

  app.get(
    "/v1/system/snapshot",
    asyncRoute(async (_request, response) => {
      response.status(200).json({ ...(await store.snapshot()), worker: worker.status() });
    }),
  );

  app.get("/v1/events/stream", (request, response) => {
    response.status(200);
    response.setHeader("content-type", "text/event-stream");
    response.setHeader("cache-control", "no-cache, no-transform");
    response.setHeader("connection", "keep-alive");
    response.flushHeaders();
    response.write(`event: connected\ndata: ${JSON.stringify({ connectedAt: new Date() })}\n\n`);

    const unsubscribe = broker.subscribe((event) => {
      response.write(`id: ${event.id}\nevent: job-event\ndata: ${JSON.stringify(event)}\n\n`);
    });
    const heartbeat = setInterval(() => response.write(": keep-alive\n\n"), 15_000);
    request.on("close", () => {
      clearInterval(heartbeat);
      unsubscribe();
    });
  });

  app.post(
    "/v1/jobs",
    asyncRoute(async (request, response) => {
      const body = objectBody(request);
      const payload = body.payload ?? {};
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
        throw new QueueError("VALIDATION_ERROR", "payload must be an object", 400);
      }
      const input: SubmitJobInput = {
        queue: requiredString(body, "queue"),
        type: requiredString(body, "type"),
        payload: payload as JsonObject,
      };
      const idempotencyKey = request.header("idempotency-key");
      if (idempotencyKey) input.idempotencyKey = idempotencyKey;
      const priority = optionalNumber(body.priority, "priority", -100, 100);
      const maxRetries = optionalNumber(body.maxRetries, "maxRetries", 0, 20);
      const delayMs = optionalNumber(body.delayMs, "delayMs", 0, 86_400_000);
      if (priority !== undefined) input.priority = priority;
      if (maxRetries !== undefined) input.maxRetries = maxRetries;
      if (delayMs !== undefined) input.delayMs = delayMs;
      if (typeof body.scheduleAt === "string") input.scheduleAt = body.scheduleAt;

      const submitted = await store.submit(input);
      response.status(submitted.created ? 201 : 200).json(submitted.job);
    }),
  );

  app.get(
    "/v1/jobs",
    asyncRoute(async (request, response) => {
      const queue = typeof request.query.queue === "string" ? request.query.queue : undefined;
      const rawStatus = typeof request.query.status === "string" ? request.query.status : undefined;
      if (rawStatus && !JOB_STATUSES.includes(rawStatus as JobStatus)) {
        throw new QueueError("VALIDATION_ERROR", "Unknown job status", 400);
      }
      const rawLimit = typeof request.query.limit === "string" ? Number(request.query.limit) : undefined;
      response.status(200).json({
        jobs: await store.list({
          ...(queue ? { queue } : {}),
          ...(rawStatus ? { status: rawStatus as JobStatus } : {}),
          ...(Number.isFinite(rawLimit) ? { limit: rawLimit } : {}),
        }),
      });
    }),
  );

  app.get(
    "/v1/jobs/:jobId/events",
    asyncRoute(async (request, response) => {
      const jobId = routeParam(request, "jobId");
      if (!(await store.get(jobId))) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
      response.status(200).json({ events: await store.events(jobId) });
    }),
  );

  app.get(
    "/v1/jobs/:jobId",
    asyncRoute(async (request, response) => {
      const job = await store.get(routeParam(request, "jobId"));
      if (!job) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
      response.status(200).json(job);
    }),
  );

  app.post(
    "/v1/queues/:queue/jobs/claim",
    asyncRoute(async (request, response) => {
      const body = objectBodyOrEmpty(request);
      const visibilityTimeoutMs = optionalNumber(
        body.visibilityTimeoutMs,
        "visibilityTimeoutMs",
        1_000,
        900_000,
      );
      const claimed = await store.claim(routeParam(request, "queue"), visibilityTimeoutMs);
      if (!claimed) {
        response.status(204).end();
        return;
      }
      response.status(200).json(claimed);
    }),
  );

  app.post(
    "/v1/jobs/:jobId/heartbeat",
    asyncRoute(async (request, response) => {
      const body = objectBody(request);
      const extendByMs = optionalNumber(body.extendByMs, "extendByMs", 1_000, 900_000);
      const job = await store.heartbeat(
        routeParam(request, "jobId"),
        requiredString(body, "leaseId"),
        extendByMs,
      );
      response.status(200).json(job);
    }),
  );

  app.post(
    "/v1/jobs/:jobId/complete",
    asyncRoute(async (request, response) => {
      const body = objectBody(request);
      const result = body.result ?? {};
      if (!result || typeof result !== "object" || Array.isArray(result)) {
        throw new QueueError("VALIDATION_ERROR", "result must be an object", 400);
      }
      response.status(200).json(
        await store.complete(
          routeParam(request, "jobId"),
          requiredString(body, "leaseId"),
          result as JsonObject,
        ),
      );
    }),
  );

  app.post(
    "/v1/jobs/:jobId/fail",
    asyncRoute(async (request, response) => {
      const body = objectBody(request);
      const error = body.error;
      if (!error || typeof error !== "object" || Array.isArray(error)) {
        throw new QueueError("VALIDATION_ERROR", "error must be an object", 400);
      }
      response.status(200).json(
        await store.fail(
          routeParam(request, "jobId"),
          requiredString(body, "leaseId"),
          error as JsonObject,
        ),
      );
    }),
  );

  app.post(
    "/v1/jobs/:jobId/cancel",
    asyncRoute(async (request, response) => {
      response.status(200).json(await store.cancel(routeParam(request, "jobId")));
    }),
  );

  app.post(
    "/v1/jobs/:jobId/retry",
    asyncRoute(async (request, response) => {
      response.status(200).json(await store.retry(routeParam(request, "jobId")));
    }),
  );

  app.get(
    "/v1/queues",
    asyncRoute(async (_request, response) => {
      response.status(200).json({ queues: await store.queues() });
    }),
  );

  app.get("/v1/demo/worker", (_request, response) => response.json(worker.status()));
  app.post("/v1/demo/worker/start", (_request, response) => {
    worker.start();
    response.json(worker.status());
  });
  app.post("/v1/demo/worker/stop", (_request, response) => {
    worker.stop();
    response.json(worker.status());
  });
  app.post("/v1/demo/worker/fail-next", (_request, response) => {
    worker.failNext();
    response.json(worker.status());
  });
  app.post(
    "/v1/demo/worker/configure",
    asyncRoute(async (request, response) => {
      const processingTimeMs = optionalNumber(
        objectBody(request).processingTimeMs,
        "processingTimeMs",
        100,
        30_000,
      );
      if (processingTimeMs === undefined) {
        throw new QueueError("VALIDATION_ERROR", "processingTimeMs is required", 400);
      }
      worker.configure(processingTimeMs);
      response.json(worker.status());
    }),
  );

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

function objectBodyOrEmpty(request: Request): JsonObject {
  return request.body && typeof request.body === "object" && !Array.isArray(request.body)
    ? (request.body as JsonObject)
    : {};
}

function isBodyParserError(error: unknown): error is { type: string } {
  return (
    error instanceof Error &&
    "type" in error &&
    typeof (error as { type?: unknown }).type === "string"
  );
}
