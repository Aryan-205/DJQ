import type { NextFunction, Request, Response } from "express";

import { QueueError, type JsonObject } from "../../domain/job.ts";

export function objectBody(request: Request): JsonObject {
  const body: unknown = request.body;
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new QueueError("VALIDATION_ERROR", "Request body must be a JSON object", 400);
  }
  return body as JsonObject;
}

export function objectBodyOrEmpty(request: Request): JsonObject {
  return request.body && typeof request.body === "object" && !Array.isArray(request.body)
    ? (request.body as JsonObject)
    : {};
}

export function requiredString(body: JsonObject, field: string): string {
  const value = body[field];
  if (typeof value !== "string" || value.trim() === "") {
    throw new QueueError("VALIDATION_ERROR", `${field} must be a non-empty string`, 400);
  }
  return value.trim();
}

export function routeParam(request: Request, field: string): string {
  const value = request.params[field];
  if (typeof value !== "string" || value === "") {
    throw new QueueError("VALIDATION_ERROR", `${field} path parameter is required`, 400);
  }
  return value;
}

export function optionalNumber(value: unknown, field: string, minimum: number, maximum: number) {
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

export function asyncRoute(
  handler: (request: Request, response: Response) => Promise<void>,
): (request: Request, response: Response, next: NextFunction) => void {
  return (request, response, next) => handler(request, response).catch(next);
}

export function isBodyParserError(error: unknown): error is { type: string } {
  return (
    error instanceof Error &&
    "type" in error &&
    typeof (error as { type?: unknown }).type === "string"
  );
}
