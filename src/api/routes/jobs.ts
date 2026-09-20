import { Router } from "express";

import type { JobStore } from "../../application/job-store.ts";
import {
  JOB_STATUSES,
  QueueError,
  type JobStatus,
  type JsonObject,
  type SubmitJobInput,
} from "../../domain/job.ts";
import {
  asyncRoute,
  objectBody,
  objectBodyOrEmpty,
  optionalNumber,
  requiredString,
  routeParam,
} from "./helpers.ts";

export function jobsRouter(store: JobStore) {
  const r = Router();

  r.post(
    "/v1/jobs",
    asyncRoute(async (req, res) => {
      const body = objectBody(req);
      const payload = body.payload ?? {};
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
        throw new QueueError("VALIDATION_ERROR", "payload must be an object", 400);
      }
      const input: SubmitJobInput = {
        queue: requiredString(body, "queue"),
        type: requiredString(body, "type"),
        payload: payload as JsonObject,
      };
      const idempotencyKey = req.header("idempotency-key");
      if (idempotencyKey) input.idempotencyKey = idempotencyKey;
      const priority = optionalNumber(body.priority, "priority", -100, 100);
      const maxRetries = optionalNumber(body.maxRetries, "maxRetries", 0, 20);
      const delayMs = optionalNumber(body.delayMs, "delayMs", 0, 86_400_000);
      if (priority !== undefined) input.priority = priority;
      if (maxRetries !== undefined) input.maxRetries = maxRetries;
      if (delayMs !== undefined) input.delayMs = delayMs;
      if (typeof body.scheduleAt === "string") input.scheduleAt = body.scheduleAt;

      const submitted = await store.submit(input);
      res.status(submitted.created ? 201 : 200).json(submitted.job);
    }),
  );

  r.get(
    "/v1/jobs",
    asyncRoute(async (req, res) => {
      const queue = typeof req.query.queue === "string" ? req.query.queue : undefined;
      const rawStatus = typeof req.query.status === "string" ? req.query.status : undefined;
      if (rawStatus && !JOB_STATUSES.includes(rawStatus as JobStatus)) {
        throw new QueueError("VALIDATION_ERROR", "Unknown job status", 400);
      }
      const rawLimit = typeof req.query.limit === "string" ? Number(req.query.limit) : undefined;
      res.status(200).json({
        jobs: await store.list({
          ...(queue ? { queue } : {}),
          ...(rawStatus ? { status: rawStatus as JobStatus } : {}),
          ...(Number.isFinite(rawLimit) ? { limit: rawLimit } : {}),
        }),
      });
    }),
  );

  r.get(
    "/v1/jobs/:jobId/events",
    asyncRoute(async (req, res) => {
      const jobId = routeParam(req, "jobId");
      if (!(await store.get(jobId))) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
      res.status(200).json({ events: await store.events(jobId) });
    }),
  );

  r.get(
    "/v1/jobs/:jobId",
    asyncRoute(async (req, res) => {
      const job = await store.get(routeParam(req, "jobId"));
      if (!job) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
      res.status(200).json(job);
    }),
  );

  r.post(
    "/v1/queues/:queue/jobs/claim",
    asyncRoute(async (req, res) => {
      const body = objectBodyOrEmpty(req);
      const visibilityTimeoutMs = optionalNumber(
        body.visibilityTimeoutMs,
        "visibilityTimeoutMs",
        1_000,
        900_000,
      );
      const claimed = await store.claim(routeParam(req, "queue"), visibilityTimeoutMs);
      if (!claimed) {
        res.status(204).end();
        return;
      }
      res.status(200).json(claimed);
    }),
  );

  r.post(
    "/v1/jobs/:jobId/heartbeat",
    asyncRoute(async (req, res) => {
      const body = objectBody(req);
      const extendByMs = optionalNumber(body.extendByMs, "extendByMs", 1_000, 900_000);
      const job = await store.heartbeat(
        routeParam(req, "jobId"),
        requiredString(body, "leaseId"),
        extendByMs,
      );
      res.status(200).json(job);
    }),
  );

  r.post(
    "/v1/jobs/:jobId/complete",
    asyncRoute(async (req, res) => {
      const body = objectBody(req);
      const result = body.result ?? {};
      if (!result || typeof result !== "object" || Array.isArray(result)) {
        throw new QueueError("VALIDATION_ERROR", "result must be an object", 400);
      }
      res.status(200).json(
        await store.complete(
          routeParam(req, "jobId"),
          requiredString(body, "leaseId"),
          result as JsonObject,
        ),
      );
    }),
  );

  r.post(
    "/v1/jobs/:jobId/fail",
    asyncRoute(async (req, res) => {
      const body = objectBody(req);
      const error = body.error;
      if (!error || typeof error !== "object" || Array.isArray(error)) {
        throw new QueueError("VALIDATION_ERROR", "error must be an object", 400);
      }
      res.status(200).json(
        await store.fail(
          routeParam(req, "jobId"),
          requiredString(body, "leaseId"),
          error as JsonObject,
        ),
      );
    }),
  );

  r.post(
    "/v1/jobs/:jobId/cancel",
    asyncRoute(async (req, res) => {
      res.status(200).json(await store.cancel(routeParam(req, "jobId")));
    }),
  );

  r.post(
    "/v1/jobs/:jobId/retry",
    asyncRoute(async (req, res) => {
      res.status(200).json(await store.retry(routeParam(req, "jobId")));
    }),
  );

  return r;
}
