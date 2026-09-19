# DJQ — Distributed Job Queue

A full-stack distributed job queue built to make normally invisible system behavior visible. Create a request in the React control plane and watch it move through scheduling, queueing, a leased worker claim, retry backoff, completion, cancellation, or the dead-letter queue.

![Node.js](https://img.shields.io/badge/Node.js-24-5FA04E)
![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6)
![Express](https://img.shields.io/badge/Express-5-111111)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1)
![Redis](https://img.shields.io/badge/Redis-7-DC382D)
![React](https://img.shields.io/badge/React-19-61DAFB)

## What is included

- Express 5 and strict TypeScript API;
- PostgreSQL-backed jobs, leases, idempotency keys, and immutable event history;
- atomic competing-worker claims using `FOR UPDATE SKIP LOCKED`;
- Redis Pub/Sub for cross-process live lifecycle events;
- transactional PostgreSQL outbox so committed jobs do not disappear during a Redis failure;
- delayed jobs, priority ordering, visibility timeouts, heartbeats, retry backoff, cancellation, manual retry, and dead-lettering;
- Server-Sent Events (SSE) for browser updates;
- React/Vite dashboard with request creation, live flow lanes, metrics, worker controls, and a job inspector;
- Prometheus-compatible metrics;
- Docker Compose development stack;
- dependency-free in-memory mode for learning and fast tests;
- Kafka integration planned as the next event-distribution stage.

The detailed architecture and future milestones are in [PLAN.md](./PLAN.md). Design lessons and trade-offs are in [docs/LEARNING_LOG.md](./docs/LEARNING_LOG.md).

## Quick start

Requirements: Docker Desktop or another Docker engine with Compose.

```bash
docker compose up --build
```

Open:

- Dashboard: [http://localhost:5173](http://localhost:5173)
- API: [http://localhost:3000](http://localhost:3000)
- Liveness: [http://localhost:3000/health/live](http://localhost:3000/health/live)
- Metrics: [http://localhost:3000/metrics](http://localhost:3000/metrics)

PostgreSQL and Redis stay on the internal Compose network, avoiding collisions with services already running on the host. The named Docker volumes preserve data between normal restarts.

Stop the stack without deleting persisted data:

```bash
docker compose down
```

## Use the dashboard

1. Keep the demo worker running.
2. Submit the pre-filled email job.
3. Watch the job move from **Queued** to **Processing** to **Completed**.
4. Select the card to inspect its payload, result, lease attempt, and event timeline.
5. Click **Fail next job**, then submit another job to observe retry backoff.
6. Set retries to `0` and fail the next job to send it directly to **Dead letter**.
7. Open that job and use **Retry job** to return it to the queue.
8. Add a delay to watch the job remain in **Delayed** until eligible.

The worker controls are development-only teaching tools. Real workers use the claim, heartbeat, complete, and fail endpoints.

## Architecture

```text
React dashboard
  |-- commands and snapshots (REST)
  +-- lifecycle updates (SSE)
                 |
             Express API
                 |
       PostgreSQL transaction
     jobs + leases + events + outbox
                 |
            outbox relay
                 |
          Redis event channel
            |           |
       SSE clients   API instances

Demo/real workers -> discover queues -> atomically claim in PostgreSQL
```

PostgreSQL is authoritative. Redis distributes low-latency notifications but does not decide lease ownership. If Redis is temporarily unavailable, accepted jobs remain durable in PostgreSQL and unpublished outbox rows retry later.

## Core API

| Method | Route | Purpose |
|---|---|---|
| `POST` | `/v1/jobs` | Submit a job; accepts `Idempotency-Key` |
| `GET` | `/v1/jobs` | List/filter jobs |
| `GET` | `/v1/jobs/:jobId` | Read current job state |
| `GET` | `/v1/jobs/:jobId/events` | Read immutable job history |
| `POST` | `/v1/jobs/:jobId/cancel` | Cancel or request cooperative cancellation |
| `POST` | `/v1/jobs/:jobId/retry` | Retry a dead-letter job |
| `POST` | `/v1/queues/:queue/jobs/claim` | Claim eligible work with a lease |
| `POST` | `/v1/jobs/:jobId/heartbeat` | Extend the active lease |
| `POST` | `/v1/jobs/:jobId/complete` | Acknowledge successful work |
| `POST` | `/v1/jobs/:jobId/fail` | Record failure and apply queue retry policy |
| `GET` | `/v1/system/snapshot` | Bootstrap the dashboard |
| `GET` | `/v1/events/stream` | Receive SSE lifecycle updates |
| `GET` | `/v1/queues` | Read queue summaries |
| `GET` | `/v1/workers` | Read demo worker state |
| `GET` | `/metrics` | Prometheus text exposition |

### Create a job

```bash
curl http://localhost:3000/v1/jobs \
  -X POST \
  -H 'content-type: application/json' \
  -H 'idempotency-key: welcome-user-123' \
  -d '{
    "queue": "email",
    "type": "send-welcome-email",
    "payload": {"userId": "user_123"},
    "priority": 5,
    "maxRetries": 3,
    "delayMs": 0
  }'
```

### Worker lifecycle

```bash
curl http://localhost:3000/v1/queues/email/jobs/claim \
  -X POST \
  -H 'content-type: application/json' \
  -d '{"visibilityTimeoutMs": 30000}'
```

Use the returned job and lease IDs:

```bash
curl http://localhost:3000/v1/jobs/JOB_ID/complete \
  -X POST \
  -H 'content-type: application/json' \
  -d '{"leaseId":"LEASE_ID","result":{"messageId":"msg_123"}}'
```

## Run without Docker

This starts the API with an in-memory store and event broker. Data disappears when the process exits.

```bash
npm ci
npm --prefix web ci
npm start
```

In another terminal:

```bash
npm --prefix web run dev
```

Copy `.env.example` to `.env` and load those variables if you want a host-run API to connect to host-accessible PostgreSQL and Redis instances.

## Development checks

```bash
npm run check:all
```

This performs strict backend type-checking, runs the lifecycle tests, type-checks the React application, and creates its production build.

Current automated behavior coverage includes:

- core submit/claim/complete lifecycle;
- idempotent submission;
- stale lease rejection;
- priority ordering;
- delayed-job eligibility;
- retry scheduling;
- dead-lettering and manual recovery;
- cooperative cancellation.

## Delivery guarantees

DJQ targets **at-least-once delivery**. A worker may finish an external side effect and crash before acknowledging it, so an attempt can be delivered again after lease expiry. Workers must make external side effects idempotent.

The queue guarantees that:

- one current lease owns an attempt;
- stale or expired leases cannot acknowledge work;
- accepted Docker-mode jobs are durable in PostgreSQL;
- claiming and lease creation happen in one transaction;
- retry policy belongs to the queue, not the worker;
- lifecycle changes create immutable events;
- PostgreSQL/Redis publication uses an outbox rather than an unsafe dual write.

This project does not claim exactly-once execution or multi-region consensus.

## Kafka later

Kafka is deliberately outside the initial critical delivery path. The next-stage design publishes versioned lifecycle events from the existing outbox into Kafka for audit, analytics, webhooks, and other independent consumers. PostgreSQL will remain authoritative for current job and lease state.
