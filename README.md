# Distributed Job Queue

A job queue built from first principles to explore leases, at-least-once delivery, retries, idempotency, scheduling, and horizontal scaling.

The current checkpoint is an Express/TypeScript API with an in-memory vertical slice. It supports job submission, idempotent requests, job lookup, leased claiming, and lease-validated completion. PostgreSQL durability and the Redis ready path are the next backend milestone; Kafka is planned later for durable event distribution.

Read [PLAN.md](./PLAN.md) for the architecture, learning goals, milestones, and acceptance tests.

## Requirements

- Node.js 22.6 or newer (Node.js 24 is recommended)
- Docker with Compose for the PostgreSQL and Redis development services

## Run

```bash
npm start
```

The server listens on `http://localhost:3000` by default. Set `PORT` to use another port.

To start the API, PostgreSQL, and Redis together:

```bash
docker compose up --build
```

## Test

```bash
npm test
```

## Try the first lifecycle

Create a job:

```bash
curl -i http://localhost:3000/v1/jobs \
  -X POST \
  -H 'content-type: application/json' \
  -H 'idempotency-key: welcome-user-123' \
  -d '{"queue":"email","type":"send-welcome-email","payload":{"userId":"user_123"}}'
```

Claim the next email job:

```bash
curl -i http://localhost:3000/v1/queues/email/jobs/claim -X POST
```

Use the returned job ID and lease ID to complete it:

```bash
curl -i http://localhost:3000/v1/jobs/JOB_ID/complete \
  -X POST \
  -H 'content-type: application/json' \
  -d '{"leaseId":"LEASE_ID","result":{"messageId":"demo-message"}}'
```

This version intentionally loses its data when the process stops. That limitation is visible and temporary: the next milestone replaces the in-memory adapter with PostgreSQL while preserving the same application contract.
# DJQ
