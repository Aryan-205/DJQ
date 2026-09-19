# Distributed Job Queue — Learning and Portfolio Plan

## 1. Project goal

Build a distributed job queue from first principles, make its internal behavior visible through an interactive frontend, and document the reasoning behind it. The finished project should be useful as both:

- a learning project for concurrency, failure handling, persistence, and distributed-systems trade-offs;
- a portfolio project that can be run locally, explored visually, demonstrated under failure, and explained in an interview.

The central rule is simple: producers submit jobs, workers pull and execute jobs, and the queue owns delivery, leases, retries, and lifecycle transitions. The scheduler only releases delayed jobs when they become eligible; it does not assign work to individual workers.

## 2. What success looks like

A reviewer can clone the repository, run one command, open the web app, create a job, and watch this flow in real time:

```text
create form -> API -> durable queue -> worker -> completion
    |                    |             |
    |                    |             +-> heartbeat / failure
    |                    +-> retry with backoff -> dead-letter queue
    |
    +-> live event stream -> visual job timeline
```

The demo will also prove that:

1. duplicate submissions with the same idempotency key create one job;
2. only one worker can hold the active lease for a job;
3. work becomes available again after a worker crashes and its lease expires;
4. a stale worker cannot complete a job using an old lease;
5. failures retry with bounded exponential backoff and eventually enter the dead-letter queue;
6. scheduled jobs are not claimable before their scheduled time;
7. queue depth, throughput, failures, and processing latency are observable;
8. every important state change appears in a live per-job timeline;
9. the entire happy path and selected failure scenarios can be triggered from the browser without using `curl`.

## 3. Learning outcomes

By completing the project, we should be able to explain:

- at-least-once delivery and why exactly-once execution is usually an application-level illusion;
- acknowledgements, leases, visibility timeouts, and heartbeats;
- atomic job claiming and race-condition prevention;
- idempotent producers and idempotent workers;
- retry policy, exponential backoff, jitter, and poison jobs;
- delayed jobs, priority, fairness, and starvation;
- graceful shutdown and worker crash recovery;
- horizontal scaling, database contention, and queue partitioning;
- metrics, structured logs, tracing, and operational debugging;
- REST snapshots versus real-time event streams, reconnect behavior, and frontend consistency.

Every milestone should add a short note to `docs/LEARNING_LOG.md` covering what was built, why it works, what can fail, and what we would change at larger scale.

## 4. Scope

### In scope

- versioned HTTP API for producers, workers, and administrators;
- named queues;
- job lifecycle and event history;
- atomic claim with a lease and visibility timeout;
- completion, failure, cancellation, and heartbeats;
- automatic retries and a dead-letter queue;
- delayed jobs and priority;
- idempotent submission;
- PostgreSQL-backed persistence;
- multiple API processes and multiple workers;
- an interactive web application for creating and inspecting jobs;
- a live flow view showing state transitions, leases, attempts, workers, retries, and failures;
- queue, worker, and dead-letter views with filters and drill-down details;
- browser controls for safe demo scenarios such as fail-next-job and stop a demo worker;
- authentication/authorization, rate limiting, metrics, and an operational dashboard;
- automated tests and a reproducible failure demo.

### Deliberately out of scope for the first release

- arbitrary user code execution inside the queue server;
- multi-region consensus;
- exactly-once delivery guarantees;
- Kafka on the initial job-delivery critical path (it is added later for durable event distribution);
- a production Kubernetes operator;
- unbounded payload storage (large payloads should eventually use object storage and references).

## 5. Technology choices

| Concern | Choice | Reason |
|---|---|---|
| Runtime | Node.js 24 + TypeScript | Available locally, fast feedback, and readable types for the state machine |
| HTTP API | Express 5 | Familiar routing/middleware model with clean support for validation, auth, SSE, and error handling |
| Durable store | PostgreSQL | Transactions and `FOR UPDATE SKIP LOCKED` make atomic claims explicit |
| Fast queue path | Redis | Ready-job indexes/signals, worker presence, rate limits, and short-lived operational state |
| Local environment | Docker Compose | Reproducible API, database, and worker setup |
| Backend tests | Node test runner + TypeScript compiler | Native execution with strict static checking and no large test framework |
| Frontend | React + TypeScript + Vite | Fast local development and a strong portfolio-friendly component model |
| Live updates | Server-Sent Events (SSE) | Simple ordered server-to-browser updates with built-in reconnection |
| Frontend state | REST snapshot + event reducer | Makes synchronization behavior explicit and testable |
| Metrics | Prometheus-compatible endpoint | Common operational model and easy local visualization |
| Tracing | OpenTelemetry | Shows producer-to-worker execution across processes |
| Later event backbone | Kafka | Durable fan-out for analytics, audit consumers, webhooks, and integrations after the core queue works |
| Visualization | Purpose-built flow and timeline components | Shows queue behavior without hiding it behind a generic admin table |

PostgreSQL remains the source of truth for jobs, leases, idempotency, and immutable events. Redis accelerates the hot path but must not be the only place a job exists. A transactional outbox records Redis publication work in the same PostgreSQL transaction as the job change; a relay updates Redis, and a reconciler repairs missed entries. If Redis is flushed or unavailable, jobs are delayed rather than lost and can be rebuilt from PostgreSQL.

Kafka is intentionally later. Once the queue is reliable, the PostgreSQL outbox can also publish lifecycle events to Kafka for multiple independent consumers. Kafka is not required to decide who owns a job lease, so adding it does not create a second authority for job state.

## 6. Architecture

```text
Browser UI
  |-- commands (REST) --------------------+
  |-- initial state (REST snapshot) <-----|--------------------+
  +-- live updates (SSE) <----------------|--------------+     |
                                         v              |     |
Producer -----------------------> +------------------+   |     |
Admin --------------------------> | Express API(s)   |---+     |
                                  +---------+--------+         |
                                            |                  |
                               transaction writes              |
                                            v                  |
                                     PostgreSQL ---------------+
                       jobs + leases + events + outbox
                                            |
                                      outbox relay
                                            |
                                            v
                               Redis ready path / signals
                                  |                 |
                         Scheduler / reaper     Worker pool
                       releases eligible jobs  claim in PostgreSQL
                                  |                 |
                                  +--------+--------+
                                           |
                                metrics, logs, traces

Later: PostgreSQL outbox -> Kafka -> analytics / audit / webhooks / integrations
```

PostgreSQL is the source of truth. Redis tells workers that work may be available, but the PostgreSQL transaction decides whether a claim succeeds and records the lease. API instances and workers remain stateless so they can scale horizontally. The frontend is a projection of server state: it sends commands but never invents lifecycle transitions locally.

## 7. Job state machine

```text
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

Terminal states are `COMPLETED`, `CANCELLED`, and `DEAD_LETTER`. State changes must happen through queue operations; callers cannot write arbitrary statuses.

## 8. Correctness invariants

These are more important than endpoint count:

1. A job has at most one current lease.
2. Only the current, unexpired lease can heartbeat, complete, or fail a job.
3. Claiming and recording the lease are atomic.
4. A successful completion is terminal and cannot be overwritten by a late worker.
5. The same producer identity plus idempotency key returns the same job.
6. `attempts` increments only when execution is claimed.
7. A delayed or retrying job cannot be claimed before `available_at`.
8. Retry delay is calculated by the queue, never by the worker.
9. Cancellation is immediate before processing and cooperative during processing.
10. Every lifecycle change creates an immutable event for debugging.

## 9. API plan

### Producer

```text
POST   /v1/jobs
GET    /v1/jobs/:jobId
POST   /v1/jobs/:jobId/cancel
POST   /v1/jobs/:jobId/retry
GET    /v1/jobs/:jobId/events
```

`POST /v1/jobs` accepts `Idempotency-Key` and fields such as `queue`, `type`, `payload`, `priority`, `scheduleAt`, `maxRetries`, and `retryPolicy`. It returns immediately with a job ID.

### Worker

```text
POST   /v1/queues/:queue/jobs/claim
POST   /v1/jobs/:jobId/heartbeat
POST   /v1/jobs/:jobId/complete
POST   /v1/jobs/:jobId/fail
```

A claim returns a job, a unique `leaseId`, and `leaseExpiresAt`. Completion, failure, and heartbeat requests must include that lease ID.

### Queue administration and monitoring

```text
POST   /v1/queues
GET    /v1/queues/:queue
POST   /v1/queues/:queue/pause
POST   /v1/queues/:queue/resume
GET    /v1/jobs?queue=&status=&cursor=
GET    /v1/queues/:queue/stats
GET    /v1/workers
GET    /v1/system/snapshot
GET    /v1/events/stream
GET    /health/live
GET    /health/ready
GET    /metrics
```

`GET /v1/system/snapshot` gives the frontend a consistent initial view. `GET /v1/events/stream` is an SSE connection carrying ordered job, queue, lease, and worker events. Each event has a monotonic event ID so the browser can reconnect with `Last-Event-ID` and recover missed updates. The UI periodically reconciles with a fresh snapshot instead of assuming the stream can never lose an event.

## 10. Frontend product plan

The frontend is the visual control plane and learning surface for the project. It should make the invisible parts of a job queue understandable without implying guarantees the backend does not provide.

### Primary demo journey

```text
1. Open Create Job
2. Choose queue, type, payload, priority, delay, and retry settings
3. Submit and receive the job ID immediately
4. Watch the job enter the queued lane
5. See a worker claim it and display the active lease countdown
6. Watch heartbeats, completion, failure, retry delay, or lease expiry
7. Open the job drawer for payload, result/error, attempts, and full event history
```

### Main screens

#### 1. System overview

- queue cards with queued, processing, retrying, completed, failed, and dead-letter counts;
- throughput and latency summaries;
- connected/idle/busy worker counts;
- live connection status and last synchronized time;
- recent system events.

#### 2. Create job playground

- queue and job-type selectors;
- JSON payload editor with validation;
- priority, delay or scheduled time, maximum retries, and retry-policy fields;
- optional idempotency key;
- generated `curl` equivalent so the UI also teaches the API;
- presets for success, transient failure, permanent failure, long-running work, and worker-crash demos.

#### 3. Live flow view

Display jobs moving through meaningful lanes:

```text
SUBMITTED -> DELAYED -> QUEUED -> PROCESSING -> COMPLETED
                           ^          |
                           |          +-> RETRYING
                           |                 |
                           +-----------------+

PROCESSING -> CANCEL_REQUESTED
RETRYING   -> DEAD_LETTER
ANY ACTIVE -> CANCELLED when allowed
```

Each job card shows its short ID, type, queue, attempt, priority, age, assigned worker, and lease countdown. Movement between lanes is driven by backend events. Animation may explain a transition, but it must not delay or fabricate state.

#### 4. Job inspector

- current status and timestamps;
- request payload and completion result or last error;
- attempt history;
- current/previous lease IDs in shortened form;
- worker ownership and heartbeat history;
- retry calculation and next eligible time;
- immutable event timeline;
- allowed actions such as cancel or manually retry, based on current server state.

#### 5. Queues and workers

- searchable job table with queue/status/type/date filters and cursor pagination;
- queue details, configuration, pause/resume, and depth history;
- worker cards showing identity, status, concurrency, active jobs, last heartbeat, and completed count;
- dead-letter view with error summaries and manual retry controls.

### Demo controls

Development-only controls make failure modes reproducible:

- start, pause, and resume demo workers;
- change simulated processing speed;
- fail the next job with a chosen transient or permanent error;
- stop a worker after it claims a job to demonstrate lease expiry;
- submit a burst of jobs to show concurrency and priority;
- reset demo data with an explicit confirmation.

These controls call documented development endpoints and must be disabled outside demo mode. The UI should label simulation clearly so it is never confused with real worker behavior.

### Real-time synchronization rules

1. Load a REST snapshot before opening the event stream.
2. Apply SSE events through one deterministic reducer.
3. Track the last applied event ID and ignore duplicates.
4. On reconnect, send `Last-Event-ID`; if replay is unavailable, fetch a new snapshot.
5. Show `live`, `reconnecting`, or `stale` connection state in the UI.
6. Optimistic updates are allowed for form feedback, not for job lifecycle status.
7. Keep only a bounded event window in browser memory; fetch older history on demand.

### Accessibility and responsive behavior

- status must be communicated through text/icons as well as color;
- keyboard users must be able to create, filter, inspect, and act on jobs;
- live updates should use a polite ARIA region and avoid excessive announcements;
- respect reduced-motion preferences;
- desktop uses the lane visualization, while smaller screens use a timeline/list representation;
- JSON, IDs, timestamps, and errors must remain selectable and copyable.

## 11. Initial data model

### `queues`

- `name` (primary key)
- `status` (`active` or `paused`)
- `visibility_timeout_ms`
- `max_retries`
- `created_at`, `updated_at`

### `jobs`

- `id` (UUID primary key)
- `queue_name`
- `type`
- `payload` (JSONB)
- `status`
- `priority`
- `attempts`, `max_retries`
- `available_at`
- `lease_id`, `lease_expires_at`
- `result` (JSONB), `last_error` (JSONB)
- `idempotency_key`, `producer_id`
- `created_at`, `started_at`, `completed_at`, `updated_at`

Important indexes:

- claim path: `(queue_name, status, available_at, priority DESC, created_at)`;
- lease reaping: `(status, lease_expires_at)`;
- idempotency: unique `(producer_id, idempotency_key)` when a key exists.

### `job_events`

- `id`
- `job_id`
- `event_type`
- `from_status`, `to_status`
- `metadata` (JSONB)
- `created_at`

### `outbox_events`

- `id` (monotonically sortable identifier)
- `topic` and `event_type`
- `aggregate_id` (usually the job ID)
- `payload` (JSONB)
- `created_at`, `published_at`
- `attempts`, `last_error`

The outbox is written in the same PostgreSQL transaction as the job mutation. Relays publish to Redis initially and Kafka later. Consumers must tolerate duplicate delivery.

## 12. Milestones

### Milestone 0 — Foundation (complete)

Deliverables:

- [x] architecture and learning plan;
- [x] dependency-free TypeScript project scaffold;
- [x] Express HTTP layer with centralized JSON/error middleware;
- [x] TypeScript compiler plus native Node.js TypeScript development/test tooling;
- [x] Docker Compose foundation for API, PostgreSQL, and Redis;
- [x] in-memory job store behind an interface;
- [x] submit, status, claim, and complete endpoints;
- [x] lease validation and idempotent submission;
- [x] automated tests for the first lifecycle;
- [x] first learning-log entry and API examples;
- [x] frontend experience, live-event model, and demo journey planned;

Acceptance test:

```text
submit -> queued -> claim -> processing -> complete -> completed
```

A wrong lease must be rejected and a repeated idempotency key must return the original job.

### Milestone 1 — Interactive frontend and live local demo

Deliverables:

- [x] React/Vite frontend scaffold with shared API contracts;
- [x] in-memory lifecycle events with monotonic event IDs;
- [x] job list, system snapshot, and SSE endpoints;
- [x] create-job playground with JSON validation and generated `curl` command;
- [x] live flow lanes and job inspector timeline;
- [x] a demo worker that makes jobs visibly move through the happy path;
- [x] reconnect/stale indicators and snapshot reconciliation;
- [x] manual browser end-to-end verification of live transitions;
- [ ] automated frontend component and browser tests.

Acceptance test: create a job entirely from the browser and watch it move from `queued` to `processing` to `completed` without refreshing. Opening the job must show the same ordered events returned by the API.

### Milestone 2 — Durable core

Deliverables:

- [x] wire the PostgreSQL repository through the Docker Compose environment;
- [x] schema migrations for queues, jobs, job events, and outbox events;
- [x] PostgreSQL repository adapter;
- [x] atomic claim using a transaction and `FOR UPDATE SKIP LOCKED`;
- [x] transactional outbox and Redis publication relay;
- [ ] Redis ready-job index/signals with PostgreSQL reconciliation;
- [ ] Redis-backed worker presence and heartbeat expiry;
- [ ] worker process with configurable concurrency;
- [x] integration test against an isolated real PostgreSQL database;
- [x] graceful shutdown and health endpoints.

Acceptance test: run two workers concurrently against 1,000 jobs and prove every job reaches completion with no simultaneously valid duplicate lease.

Frontend checkpoint: refresh the browser or restart the API and recover the same durable jobs and event history from PostgreSQL.

### Milestone 3 — Reliability

Deliverables:

- [x] lease expiry and automatic re-queue;
- [x] heartbeat extension;
- [x] failure endpoint and error recording;
- [x] bounded exponential backoff;
- [ ] randomized retry jitter;
- [x] dead-letter queue and manual retry;
- [x] queued/delayed cancellation plus cooperative processing cancellation;
- [ ] fault-injection tests that kill workers mid-job.

Acceptance test: kill a worker after it claims a job, observe lease expiry, and show another worker completing the job. Then demonstrate rejection of the stale worker's completion.

Frontend checkpoint: use the demo controls to stop a worker and visibly follow lease expiry, re-queue, a second claim, and stale-lease rejection in the job timeline.

### Milestone 4 — Scheduling and fairness

Deliverables:

- [x] `scheduleAt` and `delay` support;
- [ ] scheduler/reaper process;
- [x] priority ordering;
- [ ] aging/starvation-prevention strategy;
- [ ] queue pause/resume;
- [ ] per-queue and per-worker concurrency controls;
- [ ] clock-boundary and ordering tests.

Acceptance test: scheduled jobs stay hidden until eligible, higher-priority jobs run first, and old low-priority work still makes progress.

Frontend checkpoint: delayed and retrying cards show their next eligible time, and the flow view explains priority ordering without hiding starvation prevention.

### Milestone 5 — Distributed operation

Deliverables:

- [ ] multiple API instances and workers in Compose;
- [ ] load and contention tests;
- [ ] claim batching and long polling;
- [ ] queue partitioning design and optional prototype;
- [ ] benchmark report comparing throughput and latency;
- [ ] documented consistency and scaling trade-offs.

Acceptance test: scale API and worker replicas during a load test without job loss; publish p50/p95/p99 claim and completion latency.

Frontend checkpoint: the worker view updates as replicas join, leave, become busy, or stop heartbeating, while large job volumes fall back to aggregated counts instead of animating every item.

### Milestone 6 — Kafka event distribution

Deliverables:

- [ ] add Kafka to an opt-in Docker Compose profile;
- [ ] publish versioned job lifecycle events from the PostgreSQL outbox;
- [ ] define topic naming, partition keys, retention, and schema evolution rules;
- [ ] build one audit/history consumer and one webhook/notification consumer;
- [ ] implement consumer idempotency and dead-letter handling;
- [ ] test replay, duplicate delivery, consumer restart, and partition ordering;
- [ ] document when Redis, PostgreSQL, and Kafka should each be used.

Acceptance test: replay the lifecycle topic into a fresh audit consumer and reconstruct the same ordered history for each job without changing authoritative queue state.

Frontend checkpoint: expose Kafka consumer lag and event-publication health as operational information, while continuing to read authoritative job state from the API.

### Milestone 7 — Production and portfolio finish

Deliverables:

- [ ] producer, worker, and admin API keys with role-based authorization;
- [ ] input limits, rate limiting, and secret-safe structured logging;
- [ ] Prometheus metrics and OpenTelemetry traces;
- [ ] polished dashboard charts, empty/loading/error states, and responsive layout;
- [ ] authenticated producer, worker, and admin frontend experiences;
- [ ] dead-letter inspection and manual retry workflow;
- [ ] optional signed completion webhook;
- [ ] polished README, architecture decision records, diagrams, and demo video/GIF;
- [ ] CI for formatting, tests, migrations, and container build.

Acceptance test: a new reviewer can follow the README, create and trace a job from the browser, run the failure demo, inspect metrics, and understand the design trade-offs without reading all source files.

## 13. Testing strategy

- **Unit tests:** transition rules, validation, backoff math, lease checks, and idempotency.
- **Repository contract tests:** run the same behavior suite against in-memory and PostgreSQL adapters.
- **Integration tests:** real HTTP server plus real PostgreSQL.
- **Concurrency tests:** many claimers competing for a fixed job set.
- **Fault tests:** process kill, expired lease, stale acknowledgement, transient database failure.
- **Load tests:** throughput and latency at different worker counts and payload sizes.
- **Frontend unit tests:** event reducer, connection state, formatters, validation, and allowed actions.
- **Frontend component tests:** create form, flow cards, filters, timeline, and reconnect states.
- **Browser tests:** create/observe/inspect happy path plus worker-crash/retry flow.
- **Contract tests:** shared request, response, snapshot, and event schemas stay compatible across API and UI.
- **End-to-end demo:** producer submits jobs, workers process them, and the dashboard reflects events.

Tests should use controllable clocks and deterministic IDs where timing or identity matters. Avoid tests that depend on arbitrary sleeps.

## 14. Repository shape

```text
DJQ/
├── src/
│   ├── domain/          # job types, rules, errors
│   ├── application/     # queue use cases
│   ├── infrastructure/  # PostgreSQL, Redis, outbox, Kafka, telemetry
│   ├── api/             # Express app, routes, middleware, controllers
│   └── server.ts        # process composition and graceful shutdown
├── web/
│   ├── src/
│   │   ├── api/         # REST client, SSE client, reconciliation
│   │   ├── components/  # flow, job, queue, and worker views
│   │   ├── features/    # create job, inspector, dashboard, demo controls
│   │   └── app/         # routing and composition
│   └── test/
├── shared/
│   └── contracts/       # API and event schemas shared by backend/frontend
├── test/
├── migrations/
├── docs/
│   ├── adr/
│   └── LEARNING_LOG.md
├── scripts/
├── compose.yaml
├── Dockerfile
├── package.json
└── PLAN.md
```

The domain and application layers must not import PostgreSQL, HTTP, or frontend details. This lets the in-memory implementation teach behavior now and the durable adapter replace it without rewriting queue rules. Shared contracts describe transport shapes only; the frontend must not import backend persistence or domain internals.

## 15. Portfolio narrative

The README should eventually tell this story:

1. **Problem:** asynchronous work must survive crashes and duplicate requests.
2. **Naive design:** pop a message and hope the worker finishes.
3. **Failure:** a crashed worker loses work; a retry can duplicate side effects.
4. **Solution:** durable state, leases, idempotency, retries, and observable events.
5. **Evidence:** automated race/fault tests and benchmark results.
6. **Visualization:** the live UI shows the exact events behind claims, heartbeats, retries, and recovery.
7. **Trade-offs:** at-least-once delivery, PostgreSQL contention limits, and when a broker such as Kafka or SQS is the better choice.

Good portfolio evidence is measured behavior, not a long feature list. Preserve benchmark outputs, failure-demo commands, architecture decisions, and screenshots as the project grows.

## 16. Immediate next steps

1. Add immutable in-memory job events and record every existing transition through Express routes.
2. Add job-list, system-snapshot, and SSE event-stream endpoints.
3. Scaffold the React/Vite frontend and shared transport contracts.
4. Build the create-job playground, demo worker, live flow, and job timeline.
5. Replace the in-memory adapter with PostgreSQL, then add the Redis ready path through an outbox.
6. Add Kafka only after the core delivery/recovery behavior and frontend demo are reliable.

## 17. Definition of done

The project is done when the behavior in Section 2 is reproducible from the browser, correctness invariants are tested, operational signals are visible, the live UI agrees with server state after disconnects and restarts, and the documentation honestly states both guarantees and limitations. A feature is not complete until it has a test, an observable outcome, and a short explanation of its distributed-systems trade-off.
