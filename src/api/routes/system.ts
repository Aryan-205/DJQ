import { Router } from "express";

import type { DemoWorker } from "../../application/demo-worker.ts";
import type { EventBroker } from "../../application/event-broker.ts";
import type { JobStore } from "../../application/job-store.ts";
import { asyncRoute } from "./helpers.ts";

export function systemRouter(store: JobStore, broker: EventBroker, worker: DemoWorker) {
  const r = Router();

  r.get("/health/live", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  r.get("/health/ready", (_req, res) => {
    res.status(200).json({ status: "ready" });
  });

  r.get(
    "/metrics",
    asyncRoute(async (_req, res) => {
      const queues = await store.queues();
      const workerStatus = worker.status();
      const totals = queues.reduce(
        (agg, queue) => {
          agg.queued += queue.counts.queued;
          agg.processing += queue.counts.processing;
          agg.completed += queue.counts.completed;
          agg.deadLetter += queue.counts.dead_letter;
          return agg;
        },
        { queued: 0, processing: 0, completed: 0, deadLetter: 0 },
      );
      res.type("text/plain; version=0.0.4").send(
        [
          "# HELP djqueue_jobs Number of jobs by lifecycle state.",
          "# TYPE djqueue_jobs gauge",
          `djqueue_jobs{status="queued"} ${totals.queued}`,
          `djqueue_jobs{status="processing"} ${totals.processing}`,
          `djqueue_jobs{status="completed"} ${totals.completed}`,
          `djqueue_jobs{status="dead_letter"} ${totals.deadLetter}`,
          "# HELP djqueue_demo_worker_running Whether the demo worker is running.",
          "# TYPE djqueue_demo_worker_running gauge",
          `djqueue_demo_worker_running ${workerStatus.running ? 1 : 0}`,
          `djqueue_demo_worker_completed_total ${workerStatus.completed}`,
          `djqueue_demo_worker_failed_total ${workerStatus.failed}`,
          "",
        ].join("\n"),
      );
    }),
  );

  r.get(
    "/v1/system/snapshot",
    asyncRoute(async (_req, res) => {
      res.status(200).json({ ...(await store.snapshot()), worker: worker.status() });
    }),
  );

  r.get("/v1/events/stream", (req, res) => {
    res.status(200);
    res.setHeader("content-type", "text/event-stream");
    res.setHeader("cache-control", "no-cache, no-transform");
    res.setHeader("connection", "keep-alive");
    res.flushHeaders();
    res.write(`event: connected\ndata: ${JSON.stringify({ connectedAt: new Date() })}\n\n`);

    const unsubscribe = broker.subscribe((event) => {
      res.write(`id: ${event.id}\nevent: job-event\ndata: ${JSON.stringify(event)}\n\n`);
    });
    const heartbeat = setInterval(() => res.write(": keep-alive\n\n"), 15_000);
    req.on("close", () => {
      clearInterval(heartbeat);
      unsubscribe();
    });
  });

  r.get(
    "/v1/queues",
    asyncRoute(async (_req, res) => {
      res.status(200).json({ queues: await store.queues() });
    }),
  );

  r.get("/v1/workers", (_req, res) => {
    res.status(200).json({ workers: [worker.status()] });
  });

  return r;
}
