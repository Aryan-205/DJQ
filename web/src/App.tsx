import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react";

import type {
  CreateJobRequest,
  Job,
  JobEvent,
  JobStatus,
  SystemSnapshot,
} from "../../shared/contracts/index.ts";
import {
  cancelJob,
  configureWorker,
  createJob,
  getJobEvents,
  getSnapshot,
  retryJob,
  subscribeToEvents,
  workerCommand,
} from "./api.ts";

const EMPTY_SNAPSHOT: SystemSnapshot = {
  jobs: [],
  queues: [],
  recentEvents: [],
  generatedAt: new Date(0).toISOString(),
  worker: {
    id: "demo-worker-1",
    running: false,
    state: "stopped",
    processingTimeMs: 1800,
    completed: 0,
    failed: 0,
    failNext: false,
    lastSeenAt: new Date(0).toISOString(),
  },
};

const FLOW_COLUMNS: Array<{
  label: string;
  eyebrow: string;
  statuses: JobStatus[];
}> = [
  { label: "Delayed", eyebrow: "Not eligible", statuses: ["delayed"] },
  { label: "Queued", eyebrow: "Ready to claim", statuses: ["queued"] },
  {
    label: "Processing",
    eyebrow: "Lease active",
    statuses: ["processing", "cancel_requested"],
  },
  { label: "Retrying", eyebrow: "Backoff window", statuses: ["retrying"] },
  { label: "Completed", eyebrow: "Acknowledged", statuses: ["completed", "cancelled"] },
  { label: "Dead letter", eyebrow: "Needs attention", statuses: ["dead_letter"] },
];

export function App() {
  const [snapshot, setSnapshot] = useState<SystemSnapshot>(EMPTY_SNAPSHOT);
  const [selectedJobId, setSelectedJobId] = useState<string>();
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [connection, setConnection] = useState<"live" | "reconnecting" | "loading">(
    "loading",
  );
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [notice, setNotice] = useState<string>();
  const [, setClock] = useState(Date.now());

  const refresh = useCallback(async () => {
    try {
      const next = await getSnapshot();
      setSnapshot(next);
      setError(undefined);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Could not load the queue");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    const poll = window.setInterval(() => void refresh(), 5_000);
    const clock = window.setInterval(() => setClock(Date.now()), 1_000);
    const unsubscribe = subscribeToEvents(
      () => void refresh(),
      (state) => setConnection(state),
    );
    return () => {
      window.clearInterval(poll);
      window.clearInterval(clock);
      unsubscribe();
    };
  }, [refresh]);

  const selectedJob = snapshot.jobs.find((job) => job.id === selectedJobId);
  useEffect(() => {
    if (!selectedJobId) {
      setEvents([]);
      return;
    }
    void getJobEvents(selectedJobId).then(setEvents).catch(() => setEvents([]));
  }, [selectedJobId, snapshot.generatedAt]);

  const counts = useMemo(() => {
    const active = snapshot.jobs.filter((job) =>
      ["queued", "delayed", "retrying", "processing", "cancel_requested"].includes(job.status),
    ).length;
    return {
      total: snapshot.jobs.length,
      active,
      processing: snapshot.jobs.filter((job) => job.status === "processing").length,
      completed: snapshot.jobs.filter((job) => job.status === "completed").length,
      attention: snapshot.jobs.filter((job) => job.status === "dead_letter").length,
    };
  }, [snapshot.jobs]);

  async function perform(action: () => Promise<unknown>, success: string) {
    try {
      setError(undefined);
      await action();
      setNotice(success);
      window.setTimeout(() => setNotice(undefined), 2_500);
      await refresh();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Action failed");
    }
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-lockup">
          <div className="brand-mark" aria-hidden="true">
            DQ
          </div>
          <div>
            <p className="overline">Distributed job queue</p>
            <h1>Queue Observatory</h1>
          </div>
        </div>
        <div className="topbar-actions">
          <span className={`connection-pill ${connection}`}>
            <span className="connection-dot" />
            {connection === "live" ? "Live stream" : connection === "loading" ? "Connecting" : "Reconnecting"}
          </span>
          <span className="environment-pill">Local / development</span>
        </div>
      </header>

      <main>
        <section className="hero-row">
          <div>
            <p className="section-kicker">System control plane</p>
            <h2>See every request become work.</h2>
            <p className="hero-copy">
              Submit a job, watch a worker lease it, and inspect every transition from acceptance
              to acknowledgement.
            </p>
          </div>
          <div className="sync-note">
            <span>Last snapshot</span>
            <strong>{loading ? "Loading…" : timeAgo(snapshot.generatedAt)}</strong>
          </div>
        </section>

        {error && (
          <div className="alert error" role="alert">
            <span>{error}</span>
            <button onClick={() => setError(undefined)}>Dismiss</button>
          </div>
        )}
        {notice && <div className="toast">{notice}</div>}

        <section className="metric-grid" aria-label="Queue summary">
          <Metric label="Jobs observed" value={counts.total} accent="neutral" />
          <Metric label="Active work" value={counts.active} accent="amber" />
          <Metric label="Processing now" value={counts.processing} accent="blue" />
          <Metric label="Completed" value={counts.completed} accent="green" />
          <Metric label="Needs attention" value={counts.attention} accent="red" />
        </section>

        <section className="workspace-grid">
          <CreateJobPanel
            onCreated={(job) => {
              setSelectedJobId(job.id);
              setNotice(`Job ${shortId(job.id)} accepted`);
              void refresh();
            }}
            onError={setError}
          />
          <WorkerPanel
            snapshot={snapshot}
            onAction={(action, message) => perform(action, message)}
          />
        </section>

        <section className="flow-section">
          <div className="section-heading">
            <div>
              <p className="section-kicker">Live lifecycle</p>
              <h2>Request flow</h2>
            </div>
            <div className="legend">
              <span><i className="legend-dot normal" /> normal</span>
              <span><i className="legend-dot warning" /> waiting</span>
              <span><i className="legend-dot danger" /> attention</span>
            </div>
          </div>

          <div className="flow-board">
            {FLOW_COLUMNS.map((column, index) => {
              const jobs = snapshot.jobs.filter((job) => column.statuses.includes(job.status));
              return (
                <div className="flow-column" key={column.label}>
                  <div className="flow-column-head">
                    <div>
                      <span>{column.eyebrow}</span>
                      <h3>{column.label}</h3>
                    </div>
                    <b>{jobs.length}</b>
                  </div>
                  <div className="job-stack">
                    {jobs.length === 0 ? (
                      <div className="empty-lane">
                        <span>{index === 0 ? "◷" : index === 5 ? "!" : "·"}</span>
                        No jobs
                      </div>
                    ) : (
                      jobs.slice(0, 12).map((job) => (
                        <JobCard
                          key={job.id}
                          job={job}
                          selected={selectedJobId === job.id}
                          onClick={() => setSelectedJobId(job.id)}
                        />
                      ))
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </section>

        <section className="lower-grid">
          <EventFeed events={snapshot.recentEvents} onSelect={setSelectedJobId} />
          <JobInspector
            job={selectedJob}
            events={events}
            onClose={() => setSelectedJobId(undefined)}
            onCancel={(job) => perform(() => cancelJob(job.id), "Cancellation requested")}
            onRetry={(job) => perform(() => retryJob(job.id), "Job returned to the queue")}
          />
        </section>
      </main>

      <footer>
        <span>DJQ / queue observability lab</span>
        <span>PostgreSQL · Redis · Express · React</span>
      </footer>
    </div>
  );
}

function Metric({
  label,
  value,
  accent,
}: {
  label: string;
  value: number;
  accent: "neutral" | "amber" | "blue" | "green" | "red";
}) {
  return (
    <article className={`metric-card ${accent}`}>
      <span>{label}</span>
      <strong>{String(value).padStart(2, "0")}</strong>
      <i />
    </article>
  );
}

function CreateJobPanel({
  onCreated,
  onError,
}: {
  onCreated: (job: Job) => void;
  onError: (message: string) => void;
}) {
  const [queue, setQueue] = useState("email");
  const [type, setType] = useState("send-welcome-email");
  const [payload, setPayload] = useState('{\n  "userId": "user_123",\n  "email": "hello@example.com"\n}');
  const [priority, setPriority] = useState(5);
  const [maxRetries, setMaxRetries] = useState(3);
  const [delayMs, setDelayMs] = useState(0);
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    try {
      const parsed: unknown = JSON.parse(payload);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("Payload must be a JSON object");
      }
      setSubmitting(true);
      const request: CreateJobRequest = {
        queue,
        type,
        payload: parsed as Record<string, unknown>,
        priority,
        maxRetries,
        delayMs,
      };
      onCreated(await createJob(request, idempotencyKey || undefined));
    } catch (caught) {
      onError(caught instanceof Error ? caught.message : "Could not create job");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <article className="panel create-panel">
      <div className="panel-title">
        <div>
          <p className="section-kicker">Producer</p>
          <h2>Create a job</h2>
        </div>
        <span className="method-badge">POST /v1/jobs</span>
      </div>
      <form onSubmit={submit}>
        <div className="form-row two">
          <label>
            Queue
            <input value={queue} onChange={(event) => setQueue(event.target.value)} required />
          </label>
          <label>
            Job type
            <input value={type} onChange={(event) => setType(event.target.value)} required />
          </label>
        </div>
        <label>
          Payload
          <textarea value={payload} onChange={(event) => setPayload(event.target.value)} rows={5} />
        </label>
        <div className="form-row three">
          <label>
            Priority
            <input type="number" min="-100" max="100" value={priority} onChange={(e) => setPriority(Number(e.target.value))} />
          </label>
          <label>
            Retries
            <input type="number" min="0" max="20" value={maxRetries} onChange={(e) => setMaxRetries(Number(e.target.value))} />
          </label>
          <label>
            Delay (ms)
            <input type="number" min="0" value={delayMs} onChange={(e) => setDelayMs(Number(e.target.value))} />
          </label>
        </div>
        <label>
          Idempotency key <span className="optional">optional</span>
          <input value={idempotencyKey} onChange={(event) => setIdempotencyKey(event.target.value)} placeholder="checkout-order-42" />
        </label>
        <button className="primary-button" type="submit" disabled={submitting}>
          {submitting ? "Submitting…" : "Submit job"}
          <span>→</span>
        </button>
      </form>
    </article>
  );
}

function WorkerPanel({
  snapshot,
  onAction,
}: {
  snapshot: SystemSnapshot;
  onAction: (action: () => Promise<unknown>, message: string) => void;
}) {
  const worker = snapshot.worker;
  return (
    <article className="panel worker-panel">
      <div className="panel-title">
        <div>
          <p className="section-kicker">Execution</p>
          <h2>Demo worker</h2>
        </div>
        <span className={`worker-state ${worker.state}`}><i /> {worker.state}</span>
      </div>
      <div className="worker-identity">
        <div className="worker-glyph">W1</div>
        <div>
          <strong>{worker.id}</strong>
          <span>{worker.activeJobId ? `Running ${shortId(worker.activeJobId)}` : "Waiting for eligible work"}</span>
        </div>
      </div>
      <div className="worker-stats">
        <div><span>Completed</span><b>{worker.completed}</b></div>
        <div><span>Failed</span><b>{worker.failed}</b></div>
        <div><span>Processing</span><b>{(worker.processingTimeMs / 1000).toFixed(1)}s</b></div>
      </div>
      <label className="range-label">
        <span>Simulation speed</span>
        <input
          type="range"
          min="300"
          max="5000"
          step="100"
          value={worker.processingTimeMs}
          onChange={(event) =>
            onAction(
              () => configureWorker(Number(event.target.value)),
              "Worker speed updated",
            )
          }
        />
      </label>
      <div className="worker-actions">
        <button
          className="secondary-button"
          onClick={() => onAction(() => workerCommand(worker.running ? "stop" : "start"), worker.running ? "Worker stopped" : "Worker started")}
        >
          {worker.running ? "Stop worker" : "Start worker"}
        </button>
        <button
          className={`danger-button ${worker.failNext ? "armed" : ""}`}
          onClick={() => onAction(() => workerCommand("fail-next"), "Next attempt will fail")}
        >
          {worker.failNext ? "Failure armed" : "Fail next job"}
        </button>
      </div>
      <p className="worker-note">Use “Fail next job” to watch backoff and retry behavior in the live lanes.</p>
    </article>
  );
}

function JobCard({ job, selected, onClick }: { job: Job; selected: boolean; onClick: () => void }) {
  const leaseLeft = job.leaseExpiresAt
    ? Math.max(0, Math.ceil((Date.parse(job.leaseExpiresAt) - Date.now()) / 1000))
    : undefined;
  return (
    <button className={`job-card status-${job.status} ${selected ? "selected" : ""}`} onClick={onClick}>
      <span className="job-card-top"><b>{job.type}</b><i>P{job.priority}</i></span>
      <code>{shortId(job.id)}</code>
      <span className="job-meta"><span>{job.queue}</span><span>attempt {job.attempts}/{job.maxRetries + 1}</span></span>
      {leaseLeft !== undefined && <span className="lease-bar"><i style={{ width: `${Math.min(100, (leaseLeft / 30) * 100)}%` }} />lease {leaseLeft}s</span>}
      {job.status === "retrying" && <span className="available-note">ready {timeAgo(job.availableAt)}</span>}
    </button>
  );
}

function EventFeed({ events, onSelect }: { events: JobEvent[]; onSelect: (id: string) => void }) {
  return (
    <article className="panel event-panel">
      <div className="panel-title"><div><p className="section-kicker">Event stream</p><h2>Recent activity</h2></div><span className="live-label"><i /> live</span></div>
      <div className="event-list">
        {events.length === 0 ? <div className="panel-empty">Events will appear when a job is submitted.</div> : events.slice(0, 16).map((event) => (
          <button key={event.id} className="event-row" onClick={() => onSelect(event.jobId)}>
            <span className={`event-icon event-${event.type}`}>{eventGlyph(event.type)}</span>
            <span><b>{eventLabel(event.type)}</b><small>{shortId(event.jobId)} · {event.queue}</small></span>
            <time>{timeAgo(event.createdAt)}</time>
          </button>
        ))}
      </div>
    </article>
  );
}

function JobInspector({
  job,
  events,
  onClose,
  onCancel,
  onRetry,
}: {
  job?: Job;
  events: JobEvent[];
  onClose: () => void;
  onCancel: (job: Job) => void;
  onRetry: (job: Job) => void;
}) {
  if (!job) {
    return <article className="panel inspector empty-inspector"><span className="inspector-glyph">↗</span><h2>Inspect a job</h2><p>Select any card or event to see payloads, leases, attempts, and the immutable lifecycle history.</p></article>;
  }
  const cancellable = ["queued", "delayed", "retrying", "processing"].includes(job.status);
  return (
    <article className="panel inspector">
      <div className="panel-title">
        <div><p className="section-kicker">Job inspector</p><h2>{job.type}</h2></div>
        <button className="icon-button" onClick={onClose} aria-label="Close inspector">×</button>
      </div>
      <div className="inspector-identity"><code>{job.id}</code><StatusBadge status={job.status} /></div>
      <div className="inspector-grid">
        <div><span>Queue</span><b>{job.queue}</b></div>
        <div><span>Attempts</span><b>{job.attempts} / {job.maxRetries + 1}</b></div>
        <div><span>Priority</span><b>{job.priority}</b></div>
        <div><span>Created</span><b>{formatTime(job.createdAt)}</b></div>
      </div>
      <details open><summary>Payload</summary><pre>{JSON.stringify(job.payload, null, 2)}</pre></details>
      {job.result && <details open><summary>Result</summary><pre>{JSON.stringify(job.result, null, 2)}</pre></details>}
      {job.lastError && <details open className="error-details"><summary>Last error</summary><pre>{JSON.stringify(job.lastError, null, 2)}</pre></details>}
      <div className="timeline-heading"><h3>Lifecycle</h3><span>{events.length} events</span></div>
      <ol className="timeline">
        {events.map((event) => <li key={event.id}><i /><div><b>{eventLabel(event.type)}</b><span>{event.fromStatus ? `${event.fromStatus} → ` : ""}{event.toStatus}</span></div><time>{formatTime(event.createdAt)}</time></li>)}
      </ol>
      <div className="inspector-actions">
        {cancellable && <button className="secondary-button" onClick={() => onCancel(job)}>Cancel job</button>}
        {job.status === "dead_letter" && <button className="primary-button compact" onClick={() => onRetry(job)}>Retry job <span>↻</span></button>}
      </div>
    </article>
  );
}

function StatusBadge({ status }: { status: JobStatus }) {
  return <span className={`status-badge status-${status}`}>{status.replace("_", " ")}</span>;
}

function shortId(id: string) {
  return id.slice(0, 8);
}

function formatTime(value: string) {
  return new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date(value));
}

function timeAgo(value: string) {
  const seconds = Math.round((Date.now() - Date.parse(value)) / 1000);
  if (Math.abs(seconds) < 2) return "now";
  if (seconds < 0) return `in ${Math.abs(seconds)}s`;
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  return minutes < 60 ? `${minutes}m ago` : `${Math.floor(minutes / 60)}h ago`;
}

function eventLabel(type: string) {
  return type.split("_").map((part) => part[0]?.toUpperCase() + part.slice(1)).join(" ");
}

function eventGlyph(type: string) {
  if (type === "completed") return "✓";
  if (type.includes("fail") || type === "dead_lettered") return "!";
  if (type === "claimed") return "→";
  if (type === "heartbeat") return "♥";
  if (type.includes("retry")) return "↻";
  if (type.includes("cancel")) return "×";
  return "+";
}
