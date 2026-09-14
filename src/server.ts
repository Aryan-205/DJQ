import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import { QueueError, type JsonObject, type SubmitJobInput } from "./domain/job.ts";
import { InMemoryJobStore } from "./infrastructure/in-memory-job-store.ts";

const store = new InMemoryJobStore();
const port = Number.parseInt(process.env.PORT ?? "3000", 10);

function sendJson(response: ServerResponse, statusCode: number, body: unknown): void {
  response.writeHead(statusCode, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(body));
}

async function readJson(request: IncomingMessage): Promise<JsonObject> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.from(chunk);
    size += buffer.length;
    if (size > 1_000_000) {
      throw new QueueError("PAYLOAD_TOO_LARGE", "Request body exceeds 1 MB", 413);
    }
    chunks.push(buffer);
  }

  if (chunks.length === 0) return {};
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new Error("body must be an object");
    }
    return value as JsonObject;
  } catch {
    throw new QueueError("INVALID_JSON", "Request body must be valid JSON", 400);
  }
}

function requiredString(body: JsonObject, field: string): string {
  const value = body[field];
  if (typeof value !== "string" || value.trim() === "") {
    throw new QueueError("VALIDATION_ERROR", `${field} must be a non-empty string`, 400);
  }
  return value.trim();
}

async function route(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const method = request.method ?? "GET";
  const url = new URL(request.url ?? "/", "http://localhost");

  if (method === "GET" && url.pathname === "/health/live") {
    sendJson(response, 200, { status: "ok" });
    return;
  }

  if (method === "POST" && url.pathname === "/v1/jobs") {
    const body = await readJson(request);
    const payload = body.payload ?? {};
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
      throw new QueueError("VALIDATION_ERROR", "payload must be an object", 400);
    }

    const input: SubmitJobInput = {
      queue: requiredString(body, "queue"),
      type: requiredString(body, "type"),
      payload: payload as JsonObject,
      idempotencyKey:
        typeof request.headers["idempotency-key"] === "string"
          ? request.headers["idempotency-key"]
          : undefined,
    };
    if (typeof body.priority === "number") input.priority = body.priority;
    if (typeof body.maxRetries === "number") input.maxRetries = body.maxRetries;

    const submitted = await store.submit(input);
    sendJson(response, submitted.created ? 201 : 200, submitted.job);
    return;
  }

  const claimMatch = url.pathname.match(/^\/v1\/queues\/([^/]+)\/jobs\/claim$/);
  if (method === "POST" && claimMatch) {
    const claimed = await store.claim(decodeURIComponent(claimMatch[1]));
    if (!claimed) {
      response.writeHead(204);
      response.end();
      return;
    }
    sendJson(response, 200, claimed);
    return;
  }

  const completeMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)\/complete$/);
  if (method === "POST" && completeMatch) {
    const body = await readJson(request);
    const leaseId = requiredString(body, "leaseId");
    const result = body.result ?? {};
    if (!result || typeof result !== "object" || Array.isArray(result)) {
      throw new QueueError("VALIDATION_ERROR", "result must be an object", 400);
    }
    const job = await store.complete(
      decodeURIComponent(completeMatch[1]),
      leaseId,
      result as JsonObject,
    );
    sendJson(response, 200, job);
    return;
  }

  const jobMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)$/);
  if (method === "GET" && jobMatch) {
    const job = await store.get(decodeURIComponent(jobMatch[1]));
    if (!job) throw new QueueError("JOB_NOT_FOUND", "Job not found", 404);
    sendJson(response, 200, job);
    return;
  }

  throw new QueueError("ROUTE_NOT_FOUND", "Route not found", 404);
}

export const server = createServer((request, response) => {
  route(request, response).catch((error: unknown) => {
    if (error instanceof QueueError) {
      sendJson(response, error.statusCode, {
        error: { code: error.code, message: error.message },
      });
      return;
    }

    console.error(error);
    sendJson(response, 500, {
      error: { code: "INTERNAL_ERROR", message: "An unexpected error occurred" },
    });
  });
});

if (process.env.NODE_ENV !== "test") {
  server.listen(port, () => {
    console.log(`Distributed job queue API listening on http://localhost:${port}`);
  });
}
