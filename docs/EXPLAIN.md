# DJQ — Explained

## 1. Goal

Build distributed job queue from first principles that makes invisible system behavior visible.

Learning goals:
- concurrency, leases, visibility timeouts, heartbeats
- atomic claims, retries, dead-lettering, idempotency
- persistence trade-offs, failure recovery, at-least-once delivery
- observable system: events, metrics, live UI

Portfolio goal: one-command demo (`docker compose up --build`) → create job in browser → watch it flow through queue → inspect failures/retries. Must work without reading source.

Central rule: producers submit jobs, workers pull jobs, queue owns delivery/leases/retries/lifecycle. Scheduler only releases delayed jobs when eligible — never assigns work to specific worker.

## 2. What It Is

Full-stack **Distributed Job Queue** (DJQ) with control plane.

Not a message broker (RabbitMQ/SQS/Kafka). Job queue with:
- durable state in PostgreSQL
- competing workers claiming work via `FOR UPDATE SKIP LOCKED`
- leases preventing duplicate execution
- retries with exponential backoff → dead letter queue
- live browser visualization via SSE

Two modes:
- **Docker mode** (real): PostgreSQL + Redis + Express + React. Durable, multi-process capable.
- **In-memory mode** (dev/test): no infra, data lost on restart. Same `JobStore` contract, fast tests.

Delivery guarantee: **at-least-once**. One lease owns attempt, stale lease cannot ack. Worker may crash after side-effect but before `complete` → job redelivered after lease expiry. Workers must make side-effects idempotent.

## 3. How It Functions — End-to-End Flow

```
Producer (curl or React form)
  → POST /v1/jobs (with Idempotency-Key)
  → Express validates → JobStore.submit() → PostgreSQL tx: jobs + job_events + outbox_events
  → outbox relay publishes to Redis
  → Redis → SSE clients (browser) + other API instances

Worker (demo worker or real worker)
  → POST /v1/queues/:queue/jobs/claim {visibilityTimeoutMs}
  → PostgreSQL tx: SELECT ... FOR UPDATE SKIP LOCKED → creates lease (leaseId, leaseExpiresAt) → status: queued → processing
  → does work
  → heartbeat? POST /v1/jobs/:id/heartbeat {leaseId, extendByMs}
  → complete? POST /v1/jobs/:id/complete {leaseId, result} → completed
  → fail? POST /v1/jobs/:id/fail {leaseId, error} → queue calculates retry delay → retrying (available_at = now + backoff) → queued again, or dead_letter if retries exhausted

Failure path:
  worker crash → lease expires → reaper makes job claimable again → another worker claims → stale lease rejected if old worker tries complete

Browser:
  GET /v1/system/snapshot (bootstrap) + GET /v1/events/stream (SSE) → React reconciles → flow lanes update live → inspector shows timeline
```

Key invariant: Redis notifies, PostgreSQL decides. If Redis down, jobs stay durable in PostgreSQL, outbox retries later.

## 4. Architecture

```
React dashboard (Vite)
  |-- REST: commands + snapshot
  +-- SSE: lifecycle updates
  |
  Express API (stateless, can scale horizontally)
       |
  PostgreSQL transaction (source of truth)
  jobs + leases + events + outbox
       |
  outbox relay (polls outbox, publishes)
       |
  Redis (pub/sub event channel)
    |           |
       SSE clients   API instances

Workers → discover queues → atomically claim in PostgreSQL
```

Why both PG and Redis:
- **PostgreSQL**: authoritative jobs, leases, idempotency, immutable event history, outbox. Uses transactions + advisory lock pattern.
- **Redis**: hot path — ready signals, pub/sub for cross-process SSE, rate limits, ephemeral state. Can be flushed without data loss.

Later: PostgreSQL outbox → Kafka for durable fan-out (audit, analytics, webhooks). Kafka not needed for lease ownership.

## 5. Job Lifecycle

State machine (`src/domain/job.ts:1`):

```
                 cancel
        +--------------------------------> CANCELLED
        |
DELAYED ----due----> QUEUED ----claim----> PROCESSING ----complete----> COMPLETED
                       ^                       |
                       |                       +----cancel----> CANCEL_REQUESTED
                       |                       |
                       |                       +----lease expires----+
                       |                                               |
                       +----retry due---- RETRYING <----failure--------+
                                             |
                                      retries exhausted
                                             |
                                             v
                                        DEAD_LETTER
```

Termination: `completed`, `cancelled`, `dead_letter` final.

Events (`JOB_EVENT_TYPES`): `submitted`, `claimed`, `heartbeat`, `completed`, `failed`, `retry_scheduled`, `dead_lettered`, `cancel_requested`, `cancelled`, `manual_retry`, `lease_expired`. Every transition creates immutable `job_events` row.

Priority: higher priority queued first. `available_at` controls eligibility (delay + retry backoff). Backoff = exponential, queue-owned (worker never calculates).

## 6. Core Concepts

| Concept | Why |
|---|---|
| **Lease** | Job → processing not forever. Lease = ownership until deadline. `leaseId` + `leaseExpiresAt`. Only current unexpired lease can heartbeat/complete/fail. Prevents old worker overwriting new worker. |
| **Visibility timeout** | How long claim lasts before re-queued if no heartbeat/ack. Passed on claim, default 30s. |
| **Idempotency key** | `POST /v1/jobs` + `Idempotency-Key` header → same `(producer_id, key)` returns same job (201 first, 200 repeat). Prevents duplicate on retry. Currently global, future: per-producer scope. |
| **Delayed jobs** | `delayMs` or `scheduleAt` → status `delayed`, `available_at` in future → not claimable until due. |
| **Retry + DLQ** | `fail` increments attempt, computes backoff, moves to `retrying` or `dead_letter` when `attempts > maxRetries`. Manual `POST /v1/jobs/:id/retry` brings DLQ back to `queued`. |
| **Cancellation** | Queued/delayed/retrying → immediate `cancelled`. Processing → `cancel_requested` (cooperative, worker should check and stop). |
| **Outbox** | Same PG tx writes job + outbox row. Relay publishes to Redis. Avoids dual-write failure (PG commit OK but Redis publish fails → job lost). Reconciler replays unpublished rows. |
| **SSE + Snapshot** | Snapshot gives coherent initial view (works offline). SSE streams ordered events with monotonic ID, `Last-Event-ID` for reconnect. UI polls snapshot every 5s as reconciliation fallback. |

## 7. What Things Do What

### Backend (`src/`)

| Path | Role |
|---|---|
| `src/domain/job.ts` | Types: `Job`, `JobStatus`, `JobEvent`, `QueueSummary`, `SystemSnapshot`, `QueueError`. No infra imports. |
| `src/application/job-store.ts` | Interface `JobStore` — contract all stores implement: `submit`, `get`, `list`, `events`, `snapshot`, `queues`, `claim`, `heartbeat`, `complete`, `fail`, `cancel`, `retry`. |
| `src/application/event-broker.ts` | `EventBroker` interface + `MemoryEventBroker` (in-memory pub/sub). |
| `src/application/demo-worker.ts:1` | Single-concurrency demo worker. Polls `claim`, sleeps `processingTimeMs`, completes or fails if `failNext` armed. Development-only. |
| `src/infrastructure/in-memory-job-store.ts` | Map-based store for tests/learning. JS single-thread claim appears atomic — not true distributed. Not durable. |
| `src/infrastructure/postgres-job-store.ts` | Real store. PG pool, migrations, `FOR UPDATE SKIP LOCKED` claim tx, outbox writes. Authoritative. |
| `src/infrastructure/redis-event-broker.ts` | Redis pub/sub adapter implements `EventBroker`. Cross-process events. |
| `src/infrastructure/outbox-relay.ts` | Polls `outbox_events` where `published_at IS NULL`, publishes via broker, marks published. Retries on failure. |
| `src/infrastructure/migrate.ts` | Runs SQL migrations (queues, jobs, job_events, outbox_events). |
| `src/api/app.ts` | Express app factory `createApp({store,broker,worker})`. Routes, validation, CORS, error handling, SSE endpoint (`/v1/events/stream`), metrics (`/metrics`), health checks. |
| `src/server.ts` | Bootstrap: picks broker (Redis vs memory), picks store (PG vs memory), starts relay, starts demo worker, handles SIGINT/SIGTERM graceful shutdown. |

### API Routes (`src/api/app.ts:67`)

- `POST /v1/jobs` — submit, respects `Idempotency-Key`
- `GET /v1/jobs`, `GET /v1/jobs/:jobId`, `GET /v1/jobs/:jobId/events` — read
- `POST /v1/queues/:queue/jobs/claim`, `POST /v1/jobs/:id/heartbeat|complete|fail` — worker lifecycle
- `POST /v1/jobs/:id/cancel`, `POST /v1/jobs/:id/retry` — operator
- `GET /v1/system/snapshot`, `GET /v1/events/stream` (SSE), `GET /v1/queues`, `GET /v1/workers` — observability
- `GET /health/live`, `GET /health/ready`, `GET /metrics` (Prometheus) — ops
- `POST /v1/demo/worker/*` — dev controls (start/stop/fail-next/configure)

### Frontend (`web/src/`)

| Path | Role |
|---|---|
| `web/src/App.tsx` | Root component: snapshot polling + SSE subscription, metric grid, flow board, event feed, inspector. `FLOW_COLUMNS` maps statuses to lanes. |
| `web/src/api.ts` | REST client + `subscribeToEvents` (EventSource wrapper with reconnect). |
| `web/src/styles.css` | Layout for lanes, cards, inspector, timeline. |
| `web/Dockerfile` | Vite dev server in Compose, proxies API via `VITE_PROXY_TARGET`. |
| `shared/contracts/` | Shared TS types between API and web (request/response/event shapes). Single source for transport. |

### Infra / Ops

| File | Role |
|---|---|
| `compose.yaml` | 4 services: `web` (5173), `api` (3000), `postgres` (16), `redis` (7) + named volumes. Healthchecks gate startup. API depends on DB+Redis healthy. |
| `Dockerfile` | Node 24, `npm ci`, builds API. |
| `package.json` | Scripts: `dev`, `start`, `test`, `typecheck`, `check:all`. Deps: express, pg, redis. |
| `migrations/` | SQL for tables + indexes: claim path `(queue,status,available_at,priority)`, lease reaper `(status, lease_expires_at)`, idempotency unique. |

## 8. Data Model

- **queues**: `name` PK, `status` (active/paused), `visibility_timeout_ms`, `max_retries`
- **jobs**: `id` UUID PK, `queue_name`, `type`, `payload` JSONB, `status`, `priority`, `attempts`, `max_retries`, `available_at`, `lease_id`, `lease_expires_at`, `result`/`last_error` JSONB, `idempotency_key`+`producer_id`, timestamps
- **job_events**: `id`, `job_id`, `event_type`, `from_status`, `to_status`, `metadata` JSONB, `created_at` — append-only
- **outbox_events**: `id`, `topic`, `event_type`, `aggregate_id`, `payload`, `created_at`, `published_at`, `attempts`, `last_error` — relay pattern

Index claim path critical for contention; without it competing workers scan full table.

## 9. How To Run / Verify

```bash
docker compose up --build   # dashboard http://localhost:5173, api http://localhost:3000
docker compose down         # keep volumes

# without docker (in-memory)
npm ci && npm --prefix web ci
npm start                   # api with memory store
npm --prefix web run dev    # vite

# checks
npm run check:all           # typecheck + tests + web build
```

Dashboard demo steps (README): submit email job → watch Queued→Processing→Completed → inspect payload/lease/events → Fail next job → retry/backoff → set retries 0 → DLQ → Retry job → add delay → Delayed lane.

## 10. Guarantees & Limits

Guarantees: single current lease, stale-lease ack rejected, durable PG commit, atomic claim tx, queue-owned retry delay, immutable events, outbox avoids lost publish.

Not guaranteed: exactly-once execution, multi-region consensus, arbitrary code execution, unbounded payloads. See `PLAN.md` for milestones and `docs/LEARNING_LOG.md` for trade-off decisions.

## 11. Next Stage

Kafka opt-in profile: outbox also publishes versioned lifecycle events to Kafka topics (partition by jobId/queue), consumers for audit/history/webhooks with idempotency + DLQ. PG stays authority for current state.
