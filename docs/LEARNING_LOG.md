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
