# Learning Log

This log records the reasoning behind each checkpoint, not just the code that changed.

## Checkpoint 0 — The first vertical slice

### What we built

The first runnable path supports:

```text
submit -> queued -> claim with lease -> processing -> complete -> completed
```

It also rejects completion with the wrong lease, deduplicates submissions that share an idempotency key, and chooses higher-priority queued work first.

### Why start in memory?

The in-memory adapter gives fast, deterministic tests for queue behavior before database concerns are introduced. The application depends on a `JobStore` interface, so PostgreSQL can replace this adapter without changing the HTTP contract or the domain vocabulary.

This adapter is intentionally not distributed or durable. Restarting the process loses every job, and separate API processes would each own different state. Those are explicit limitations, not properties we should conceal.

### Why does a claim return a lease?

Changing a job to `processing` forever would lose it if the worker crashed. A lease says that a worker owns the attempt only until a deadline. Later, a reaper will make expired work eligible again. The lease ID also prevents an old worker from completing a job after another worker has reclaimed it.

The current checkpoint validates lease ownership and expiry on completion. Re-queuing expired leases belongs to the reliability milestone.

### What guarantee do we want?

The target is **at-least-once delivery**. A worker can finish an external side effect and crash before acknowledging it, so the queue may deliver that job again. Leases prevent permanent loss but cannot guarantee exactly-once side effects. Workers must therefore make their own effects idempotent, for example by storing a unique operation key when sending a payment request.

### Current shortcuts to remove

- Job state exists only in one process.
- Idempotency keys are global instead of scoped to an authenticated producer.
- There is no lease reaper, heartbeat, failure transition, retry, or dead-letter queue yet.
- JavaScript's single event loop makes an in-memory claim appear atomic; PostgreSQL will need an explicit transaction.
- The API has basic validation but does not yet have queue configuration, authentication, or rate limits.

### Next experiment

Create the PostgreSQL schema and a repository contract test. Run the same lifecycle suite against both adapters, then add a concurrency test that starts many claims against a fixed set of jobs. This will make the difference between an in-process method call and a genuinely atomic distributed claim concrete.

## Checkpoint 1 — Committing to the application stack

### Stack decision

The API now uses Express with TypeScript. Node.js executes erasable TypeScript syntax directly, while the TypeScript compiler performs strict static checks. Docker Compose defines the API, PostgreSQL, and Redis development services. The current store is still in memory so the next persistence step can be measured against a passing behavior suite.

### Why both PostgreSQL and Redis?

They have different jobs. PostgreSQL is authoritative for durable job state, leases, idempotency, and immutable events. Redis provides a low-latency ready-work signal/index, worker presence, rate limits, and disposable operational state. A worker may discover a candidate through Redis, but a PostgreSQL transaction decides whether that worker actually owns the lease.

Writing independently to PostgreSQL and Redis would create a dual-write failure: the database commit could succeed while Redis publication fails. The planned transactional outbox records publication work beside the job mutation. A relay retries the Redis update, and periodic reconciliation rebuilds Redis from PostgreSQL. Losing Redis can delay work but must not lose accepted jobs.

### Why is Kafka later?

Kafka becomes valuable when multiple independent systems need durable ordered lifecycle events—for audit, analytics, webhooks, and integrations. It is unnecessary for the first claim/complete path and would add operational concepts before leases and recovery are proven. Later, Kafka will consume the same outbox pattern without becoming the authority for job status.

## Checkpoint 2 — Full-stack observable MVP

### What now works

The Docker stack runs a React control plane, Express API, PostgreSQL, Redis, and an automatic demo worker. A job created in the browser is committed to PostgreSQL with an event and outbox row, claimed in a PostgreSQL transaction, processed, completed, and displayed in the browser through Redis and SSE without refreshing.

The browser was manually verified through the full create → queued → processing → completed path. Rebuilding the API container preserved the earlier job and its event history. A separate PostgreSQL integration database also passed a test where ten concurrent claim loops processed fifty jobs with fifty unique claims.

### Why use a snapshot and a stream?

The frontend first loads `/v1/system/snapshot`, which gives it a coherent view even if it was offline. SSE then tells it when lifecycle state changes. The current UI deliberately fetches a fresh snapshot after each event and periodically, favoring correctness and clarity over maximum throughput. A later optimization can apply versioned events through a reducer and reconcile less often.

### Why keep an in-memory mode?

In-memory mode is valuable for domain tests and learning because it has no infrastructure setup. Docker mode is the meaningful distributed demonstration: PostgreSQL owns durable state and Redis carries cross-process notifications. Both modes implement the same `JobStore` contract.

### Remaining production gaps

- Redis currently distributes lifecycle events; a dedicated ready-job index and reconciliation loop remain future work.
- The included demo worker is intentionally single-concurrency and runs with the API process.
- Queue pause/resume, role-based authentication, rate limiting, retry jitter, and automated browser tests remain roadmap items.
- Kafka remains a later event-distribution milestone, not part of lease ownership.
