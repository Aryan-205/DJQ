import { Router } from "express";

import type { DemoWorker } from "../../application/demo-worker.ts";
import { QueueError } from "../../domain/job.ts";
import { asyncRoute, objectBody, optionalNumber } from "./helpers.ts";

export function demoRouter(worker: DemoWorker) {
  const r = Router();

  r.get("/v1/demo/worker", (_req, res) => res.json(worker.status()));
  r.post("/v1/demo/worker/start", (_req, res) => {
    worker.start();
    res.json(worker.status());
  });
  r.post("/v1/demo/worker/stop", (_req, res) => {
    worker.stop();
    res.json(worker.status());
  });
  r.post("/v1/demo/worker/fail-next", (_req, res) => {
    worker.failNext();
    res.json(worker.status());
  });
  r.post(
    "/v1/demo/worker/configure",
    asyncRoute(async (req, res) => {
      const processingTimeMs = optionalNumber(
        objectBody(req).processingTimeMs,
        "processingTimeMs",
        100,
        30_000,
      );
      if (processingTimeMs === undefined) {
        throw new QueueError("VALIDATION_ERROR", "processingTimeMs is required", 400);
      }
      worker.configure(processingTimeMs);
      res.json(worker.status());
    }),
  );

  return r;
}
