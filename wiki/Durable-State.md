# Durable state

By default cronstable is **stateless**: run history, retry ladders, the next-fire
index and the Prometheus counters live in memory and reset with the process,
and that zero-disk story is a feature. The optional **`state`** section adds
the other half: a durable, restart-surviving store for the things that are
worth keeping (a run ledger, pending retries, `@reboot` boot markers, metric
counters), plus the scheduling features that only make sense after a store
exists (missed-run catch-up, a depends-on-past gate, output archival, SLA
trends). It is implemented in `cronstable/state.py` (the
`FilesystemStateBackend`) and wired into the scheduler in `cronstable/cron.py`.

> **Everything on this page is opt-in.** Without a `state` section the backend
> is never constructed, no file is ever written, and the in-memory behavior
> is unchanged. Adding `state` never gates a plain scheduled fire on the
> store either: a slow or dead store degrades the stateful features; it never
> stalls scheduling.

**Terms used on this page.** The **store** is the directory tree under
`state.path`. A **stream** is one append-only sequence of records in the store
(one per job per feature, such as `runs/backup`). The **run ledger** is the
durable stream of finished-run records per job (distinct from the
[web dashboard's browser-side run ledger](Web-Dashboard#run-ledger); see
[the durable run ledger](#the-durable-run-ledger)). A **watermark** is a
derived "last fired" cursor, computed as the maximum over a stream's immutable
records, never stored as a mutable file. A **lease** is a TTL claim guarded by
an advisory `flock`. A **manifest** is a node's periodic record of which job
names (plus which shared artifact scopes and dag names) its loaded config
defines, the anchor for garbage collection.

**On this page:**
[Quickstart](#quickstart) ·
[The state section](#the-state-section) ·
[One backend, two topologies](#one-backend-two-topologies) ·
[The store model](#the-store-model) ·
[The durable run ledger](#the-durable-run-ledger) ·
[Missed-run catch-up](#missed-run-catch-up) ·
[In-flight runs and crash reconciliation](#in-flight-runs-and-crash-reconciliation) ·
[Depends-on-past](#depends-on-past-onlyiflastsucceeded) ·
[Output archival](#output-archival-and-secret-redaction) ·
[Restart-surviving retries](#restart-surviving-retries) ·
[@reboot once per OS boot](#reboot-once-per-os-boot) ·
[Durable Prometheus counters](#restart-durable-prometheus-counters) ·
[SLA trends](#sla-trends-over-the-ledger) ·
[When the store is unavailable](#when-the-store-is-unavailable-onstoreunavailable) ·
[Garbage collection and manifests](#garbage-collection-and-manifests) ·
[Rate limiting](#rate-limiting-maxopspersecond) ·
[Job-facing state](#job-facing-state) ·
[Administering the store](#administering-the-store) ·
[Observing the store](#observing-the-store) ·
[Operational notes](#operational-notes)

## Quickstart

One required key turns the whole feature set on:

```yaml
state:
  path: /var/lib/cronstable
```

With that alone, and no per-job changes, cronstable gains:

* a **[durable run ledger](#the-durable-run-ledger)** per job, rehydrated into
  the dashboard's history after a restart;
* **[in-flight run records](#in-flight-runs-and-crash-reconciliation)** for
  every job: a run interrupted by a daemon crash surfaces as an `unknown`
  ledger row instead of silently vanishing;
* **[restart-surviving retries](#restart-surviving-retries)** for every job
  with `onFailure.retry`: a pending retry re-arms across a daemon restart at
  its absolute deadline;
* **[`@reboot` once per OS boot](#reboot-once-per-os-boot)**: an `@reboot` job
  runs once per boot per host, not once per daemon restart;
* **[restart-durable Prometheus counters](#restart-durable-prometheus-counters)**,
  so `cronstable_job_*` totals no longer reset to zero on every restart;
* the **[`GET /jobs/{name}/trends`](#sla-trends-over-the-ledger)** endpoint,
  SLA aggregates over the ledger.

The remaining features are per-job opt-ins on top of the store:
[`onMissed`](#missed-run-catch-up) (catch up runs missed while the daemon was
down), [`onlyIfLastSucceeded`](#depends-on-past-onlyiflastsucceeded) (skip
while the last run failed), and
[`archiveOutput`](#output-archival-and-secret-redaction) (persist captured
output).

## The `state` section

```yaml
state:
  path: /var/lib/cronstable       # required; a local dir, or a shared mount
  topology: auto               # optional; auto | single-node | shared
  deploymentId: my-app         # optional; namespace inside a shared store
  maxRunsPerJob: 1000          # optional; durable retention per job
  onStoreUnavailable: degrade  # optional; degrade | fail-closed
  gcGraceSeconds: 604800       # optional; GC grace (7 days); <= 0 disables GC
  maxOpsPerSecond: 0           # optional; token-bucket op cap; 0 = unlimited
  slotTtlSeconds: 30           # optional; cluster concurrency-slot lease TTL
```

| Key | Default | Meaning |
| --- | --- | --- |
| `path` | *(required)* | Directory the store lives under. A local directory gives single-node restart durability; an Amazon S3 Files / EFS mount gives the same durability fleet-wide. See [one backend, two topologies](#one-backend-two-topologies). |
| `topology` | `auto` | Whether the store is shared between hosts. `auto` probes the mount's filesystem type; `single-node` / `shared` override the probe. |
| `deploymentId` | *(none)* | Namespace inside the store, so several deployments can share one mount without touching each other's records (each also garbage-collects only its own namespace). Unset means the `default` namespace. |
| `maxRunsPerJob` | `1000` | How many finished-run records (and archived outputs) to retain per job. `<= 0` means unbounded. Bounds the ledger every durable feature reads. `1` is raised to `2` and logged once at startup; see [operational notes](#operational-notes). |
| `onStoreUnavailable` | `degrade` | What the durable-truth gates do when the store cannot answer. See [when the store is unavailable](#when-the-store-is-unavailable-onstoreunavailable). |
| `gcGraceSeconds` | `604800` | How long a job's streams must be unreferenced *and* idle before [garbage collection](#garbage-collection-and-manifests) deletes them. `<= 0` disables automatic GC. |
| `maxOpsPerSecond` | `0` | Token-bucket cap on store operations, for request-billed mounts. `0` = unlimited. See [rate limiting](#rate-limiting-maxopspersecond). |
| `slotTtlSeconds` | `30` | TTL of the per-job slot lease behind [`concurrencyScope: cluster`](Clustering-and-Leader-Election), renewed at a third of the TTL while the job runs here. A crashed holder's slot frees after at most this long. Must be `>= 5`: a tiny TTL leaves no room for renew latency on a network mount and would expire live holders. |
| `jobApi` | *(see the next table)* | The job-facing state endpoint (a nested block). See [job-facing state](#job-facing-state). |

The `jobApi` block (present only when `state` is, because it has no store to
serve otherwise) takes these sub-keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Run the loopback endpoint and inject its address into every job. Set `false` to keep the durable scheduler features but expose nothing to job commands. |
| `listen` | *(ephemeral)* | Override the bind, as an `http://host:port` or `https://host:port` URL. Unset binds an OS-assigned ephemeral port on `127.0.0.1`, reachable only from this host's jobs. A `unix://` path is not accepted (the job CLI speaks TCP only). A non-loopback host is refused unless `allowNonLoopbackBind` is also set: the endpoint serves per-run bearer tokens and staged job secrets. An `https://` address needs the `tls.cert` and `tls.key` rows that follow, and a named host, because jobs dial the address they are handed and no certificate covers a wildcard bind. Jobs receive the configured host as `CRONSTABLE_STATE_URL`. |
| `maxValueBytes` | `1048576` | Cap on one KV / cursor value in bytes; a larger set is refused. |
| `maxArtifactBytes` | `67108864` | Cap on one artifact payload in bytes; a larger put is refused. |
| `lockTtlSeconds` | `30` | TTL of a [job mutex/semaphore](#mutex-and-semaphore) lease, renewed by the daemon at a third of the TTL. Must be `>= 5`, for the same reason as `slotTtlSeconds`. |
| `allowNonLoopbackBind` | `false` | Explicit opt-in required for `listen` to bind a non-loopback host. If you enable it on a plaintext `http://` address, the daemon logs a warning at startup: per-run bearer tokens and staged job secrets then cross the network in the clear. Give the endpoint an `https://` address with the `tls` rows that follow, or terminate TLS in front of it. |
| `tls.cert` | *(none)* | Certificate (chain) an `https://` `listen` serves; required together with `tls.key`. The TLS context is built once at startup, so replacing these files takes effect on the next daemon restart. Client certificates are not required here: the per-run bearer token already authenticates the caller. See [listener TLS](Listener-TLS). |
| `tls.key` | *(none)* | Private key for `tls.cert`. |
| `tls.ca` | *(none)* | Trust anchor handed to every job as `CRONSTABLE_STATE_CACERT`, so the [job-facing CLI](CLI-Reference#job-facing-state-commands) can verify a certificate no public root signed. It verifies the endpoint. It does not authenticate jobs, and a job has no way to turn verification off. |

### Per-job stateful options

These options live on the job, but they read the durable store. Unless
`state` is configured, all except `onlyIfLastSucceeded` are inert, with a
startup warning where they would otherwise silently do nothing.
`onlyIfLastSucceeded` alone also works without a store, from the in-memory
history; its memory then resets on restart (see
[depends-on-past](#depends-on-past-onlyiflastsucceeded)):

| Option | Default | Meaning |
| --- | --- | --- |
| `onMissed` | `skip` | What to do about runs missed while the daemon was down: `skip` (classic cron), `run-once` (coalesce into one launch), `run-all` (replay each missed slot, bounded). See [missed-run catch-up](#missed-run-catch-up). |
| `startingDeadlineSeconds` | *(none)* | Bound on the catch-up window: missed slots older than this are dropped (must be `> 0` when set). Also bounds how stale a [pending retry](#restart-surviving-retries) may be and still re-arm. |
| `catchupJitterSeconds` | `0` | Deterministic per-job spread of boot backfills, so a fleet restart does not fire everything at once (must be `>= 0`). |
| `onlyIfLastSucceeded` | `false` | Depends-on-past gate: skip scheduled fires while the job's most recent real outcome is a failure. See [depends-on-past](#depends-on-past-onlyiflastsucceeded). |
| `archiveOutput` | `false` | Persist the job's captured output to the store, one archived record per finished run. See [output archival](#output-archival-and-secret-redaction). |
| `redactArchivedSecrets` | `true` | Scrub recognizable secrets from output before archiving it (only applies with `archiveOutput`). |
| `secrets` | *(none)* | Run-scoped secrets staged for the job over the loopback endpoint (each `{name, value\|fromFile\|fromEnvVar}`). Needs `jobApi` enabled. See [run-scoped secrets](#run-scoped-secrets). |
| `stateAllowedScopes` | *(none)* | Extra scope names (besides the job's own name and `global`) this job's state calls may explicitly name. See [scopes](#scopes). |

The full option schema is in the
[configuration reference](Configuration-Reference).

## One backend, two topologies

There is deliberately **one backend, not a set of plugins**: the
`FilesystemStateBackend` needs only a POSIX filesystem with atomic rename and
advisory `flock`, and the *mount*, not the code, decides its reach:

* **`state.path` on a local directory** (ext4, xfs, NTFS, ...): single-node
  restart durability. Runs, retries, markers and counters survive daemon
  restarts and host reboots.
* **`state.path` on an Amazon S3 Files / EFS mount** (which presents as
  NFSv4): the identical code gets S3 durability *plus* fleet-wide reach,
  because the mount honors atomic rename and advisory locks across every
  host that mounts it. The run ledger then merges every node's runs, catch-up
  watermarks span the fleet, and [trends](#sla-trends-over-the-ledger) answer
  for the whole deployment.

`topology: auto` (the default) probes `/proc/mounts` for the filesystem type
under `path`. It treats the NFS family, CIFS/SMB, and other network
filesystems as `shared`, and everything else as `single-node`. **Windows and
macOS cannot probe**, so `auto` resolves to `single-node` there; set
`topology: shared` explicitly when the path really is a shared mount. The
daemon logs the resolved topology at startup and exports it as the `topology`
label on the `cronstable_state_info` metric.

Several distinct deployments can point at one shared mount: give each its own
`deploymentId` and they occupy disjoint namespaces (records, leases, GC).
Several *nodes of the same deployment* share one `deploymentId`, which is
exactly what makes the fleet-wide features work.

The same shared mount can also back the cluster's *leader* election: the
[`cluster.backend: filesystem`](Clustering-and-Leader-Election) leadership
backend runs its election over a private, embedded instance of this store.
Sharing a directory (same `path` *and* `deploymentId`) with the `state`
section is legal and recommended when both are used. The stream namespaces
are disjoint, and the election's instance runs none of this page's chores (no
manifests, no [GC](#garbage-collection-and-manifests), no counters).

## The store model

The store's design goal is that **no half-written or hostile file can ever
break the daemon**, on any backing store, including an object-backed mount
with no built-in rename:

* **One immutable object per record.** A record is written once, to a temp
  file under `tmp/`, then atomically renamed into place. Thereafter it is
  only read or deleted, never rewritten. Every record is wrapped as
  `{"schemaVersion": "v1", "data": {...}}`.
* **Quarantine, never crash.** A record this build cannot understand (an
  unknown `schemaVersion`, truncated JSON from a crash mid-write) is moved to
  `quarantine/` on read and skipped, never guessed at and never fatal. A plain
  I/O error (a transient NFS failure) leaves the record in place and skips it
  for that read only. The two readers that would act on "absent" as permanent
  opt out of that skip with a **strict** read that propagates the error
  instead: the DAG
  [mapped-expansion read](Orchestration-and-DAGs#fan-out-dynamic-mapping)
  retries later, and the [GC blob sweep](#garbage-collection-and-manifests)
  stands down for that pass.
* **Derived watermarks, no mutable cursors.** The "last fired" cursor is
  computed as the maximum over the immutable records, so nothing depends on
  rewriting an existing file, and the answer is order-independent even when
  several nodes append to one stream.
* **Advisory-`flock` TTL leases.** Coordination uses a lease with a monotonic
  fence, guarded by a `flock` over a dedicated lock file under `leases/`,
  never over a data file, which the atomic rename swaps out. The scheduler
  takes two lease families here:

  * the per-job concurrency slot (`slots/<job>`, behind
    [`concurrencyScope: cluster`](Clustering-and-Leader-Election));
  * the cross-node retry claim (`retry-claim/<job>`; see
    [restart-surviving retries](#restart-surviving-retries)).

  A lease file is its fence counter's only home, so these named, long-lived
  lease families are **never deleted at any age**. Fence values persist in
  durable records (a `slots/<job>` replace-cancel names the fence it
  targets), so no grace window makes a fence reset safe.

  [GC](#garbage-collection-and-manifests) reclaims only the per-run DAG
  advance leases (`dagadvance/<dag>/<runKey>`, one uniquely-named lease per
  run, nothing persisting their fences beyond the run document itself), and
  only after they are *provably* dead for a whole grace window: both the
  recorded expiry and the last write older than `gcGraceSeconds`. Locked
  read-modify-writes run in a worker thread so a blocking lock can never
  freeze the event loop.

* **Writes never stall scheduling.** Durable writes are fire-and-forget
  background tasks; a failed write is dropped with a warning and counted
  (`cronstable_state_dropped_writes_total`). The few *reads* on scheduling paths
  (the depends-on-past gate, the catch-up watermark, rehydration) are bounded
  by a 10-second timeout, past which the caller falls back. An unresponsive
  NFS server degrades the stateful features, never job launches.

The layout under `<path>/<deploymentId>/`:

```text
<path>/<deploymentId>/
├── records/              # one directory per stream, one JSON file per record
│   ├── runs%2F<job>/     #   the run ledger
│   ├── logs%2F<job>/     #   archived output (archiveOutput)
│   ├── catchup%2F<job>/  #   catch-up open/close checkpoints
│   ├── retries%2F<job>/  #   pending/settled retry records
│   ├── reboot%2F<job>/   #   @reboot boot markers
│   ├── inflight%2F<job>/ #   in-flight run records (open/closed)
│   ├── slots%2F<job>/    #   cluster concurrency-slot cancel requests
│   ├── counters%2F<host>/#   Prometheus counter snapshots
│   ├── artifacts%2F<scope>/ # artifact records (job-facing state / DAG XCom)
│   ├── manifests/        #   per-node job manifests (the GC anchor)
│   └── meta/             #   the store's version stamp
├── docs/                 # mutable job-facing documents (KV, cursors,
│                         #   idempotency claims) and dag_run documents
├── blobs/                # content-addressed artifact payloads (sha256)
├── leases/               # lock + lease files (only per-run DAG advance
│                         #   leases are ever GC'd; see the GC section)
├── quarantine/           # records quarantined on read
└── tmp/                  # write-temps, atomically renamed into records/
```

Stream and job names are percent-encoded into filenames injectively (safe on
case-insensitive filesystems and around Windows reserved device names), so two
distinct job names can never collide on one path. The per-job streams (runs,
logs, retries, catch-up, in-flight, slots) are scoped by **job name**, not
job-set id, so a job's durable history survives ordinary config reloads
instead of being orphaned by every edit.

A **version stamp** (the `meta` stream) is written once at first start. When
a store is stamped by a *newer* record scheme than this build understands,
the daemon logs a pointed warning at startup. The records themselves are
still handled by the quarantine-on-read rule, so a rolling downgrade degrades
loudly rather than corrupting anything. See `cronstable state migrate-schema`
under [administering the store](#administering-the-store) for the forward
path.

## The durable run ledger

Every finished run is appended to the job's `runs/<job>` stream (outcome,
timestamps, duration, exit code: the same shape as `GET /jobs/{name}/runs`),
pruned to `maxRunsPerJob`. This ledger is the substrate everything else on
this page reads: the catch-up watermark, the depends-on-past gate, and the
trends endpoint.

On the first successful backend start of a process, the in-memory run history
is **rehydrated** from the ledger once: the [web dashboard](Web-Dashboard)'s
history drawer, sparklines, and `GET /jobs/{name}/runs` show runs from before
the restart, immediately, instead of starting blank. Rehydration is read-only
and bounded (with a slow store the daemon skips it with a warning; history
then fills in as jobs run, exactly as with no rehydration).

A job's *latest* run is the one with the newest finish time, not the last
record in the stream. Each node appends through a per-job chain, which orders
that node's own writes but cannot order the appends a peer makes from its own
process on the same mount, so an interleaved append can leave an older run
last.

Everything that answers "the latest run" folds by finish time instead: the
rehydrated `last_run`, the retry ladder's superseded-by-run watermark, the
`maxTimeSinceSuccess` reference, the `onlyIfLastSucceeded` memo, and the
`last_duration`, `last_cpu_seconds` and `last_rss_bytes` fields in the `stats`
block. Two runs sharing a finish instant resolve to the later one in the
stream.

One kind of row becomes the latest whatever its instant: a run of *this* node
that a crash interrupted. Its record stands in the instant the run started,
and `concurrencyPolicy: Allow` is the default, so an overlapping instance of
the same job routinely finishes after that and folding would hide the crash
behind it. A slot takeover that reconciles *another* node's interrupted run
says nothing about what ran here, so it takes the fold like everything else
and cannot displace a newer local run.

The run *listings* are a different question, and nothing re-orders them at
read time. A restart rebuilds the ring in finish order; after that it grows in
the order this node observed the completions, so a peer's interleaved append,
or a crash-reconciled row standing in a past instant, leaves the tail out of
finish order. A listing shows every row it holds, so an inverted pair moves a
cell rather than changing a verdict, and an interrupted run reads better where
it was seen than a long way back in the history.

> **A different feature with a similar name:** the
> [web dashboard's opt-in run ledger](Web-Dashboard#run-ledger) records
> finished runs into the *browser's* IndexedDB, per viewer, for the anomaly
> heuristics. It predates durable state and stays browser-local. The
> durable run ledger on this page is server-side, shared by every viewer,
> and feeds scheduling decisions.

## Missed-run catch-up

Classic cron never runs a missed slot: if the machine was down at 03:00, the
03:00 job does not happen. With a store configured, the durable
last-run watermark makes the anacron-style alternative possible, per job:

```yaml
jobs:
  - name: nightly-report
    command: ./report.sh
    schedule: "0 3 * * *"
    onMissed: run-once              # coalesce everything missed into one run
    startingDeadlineSeconds: 43200  # ... but only if we are < 12 h late
    catchupJitterSeconds: 300       # spread fleet backfills over 5 minutes
```

| `onMissed` | After downtime spanning N missed slots |
| --- | --- |
| `skip` *(default)* | nothing: schedule forward, classic cron |
| `run-once` | exactly **one** launch, no matter how many slots were missed |
| `run-all` | one launch **per missed slot**, hard-capped at 100 per cycle (a warning names the drop; set `startingDeadlineSeconds` to bound the window, or prefer `run-once`) |

How the evaluation works:

* **Computed from the durable watermark.** On startup, after the backend and,
  in a cluster, the owner election are up, cronstable reads each `onMissed`
  job's last durable run and steps the schedule forward from it, occurrence
  by occurrence, **in the job's own time zone frame, DST-safe**. A missed
  slot is what the live scheduler would have fired, not a naive interval
  division.
* **A first-ever run is never "missed".** A job with no record under this
  store has no reference point, so it schedules forward (the same rule
  anacron and systemd timers apply). Catch-up starts mattering from the
  second boot on.
* **`startingDeadlineSeconds` bounds the window.** Slots older than the
  deadline are dropped, so a week-long outage cannot stampede a `run-all` job
  (the name and semantics deliberately mirror the Kubernetes CronJob field).
* **`catchupJitterSeconds` spreads the fleet.** Each job's backfill starts at
  a deterministic offset in `[0, jitter)` derived from the job name, stable
  across boots and identical on every node. A whole fleet restarting after an
  outage staggers its backfills without any coordination or RNG.
* **Cluster-gated.** Under [leader election](Clustering-and-Leader-Election),
  only the job's current owner evaluates and replays its missed runs; if
  ownership moves mid-jitter or mid-backfill, the backfill stops rather than
  double-running against the new owner.
* **Checkpointed, at-least-once.** An `open` checkpoint (with the watermark
  it was computed from) is recorded before a backfill is scheduled and a
  `close` after it completes, in the job's `catchup/<job>` stream. A restart
  mid-backfill (or mid-jitter) resumes from the open checkpoint's watermark
  instead of silently forfeiting the owed runs. A backfill that a pause or
  a change of ownership stopped part-way leaves its checkpoint open, and
  the next restart of the node that owns the job resumes it the same way.
  An evaluation that owes nothing closes any checkpoint still open,
  including the pin a pause leaves (see
  [Pausing Jobs](Pausing-Jobs#what-a-pause-does)), so a later restart does
  not resume it. The trade is at-least-once: a crash between the last
  backfill launch and the `close` record replays, and the boot that resumes
  a cycle recounts it from its watermark.
  Scheduled [DAGs](Orchestration-and-DAGs) keep the same discipline in their
  own `catchup-dag/<dag>` streams.
* **Backfills are plain runs, minus the ladder.** Each backfilled launch
  respects `concurrencyPolicy` (serialized, waiting for the job to go idle;
  `Forbid` waits unbounded) but launches *without* the retry ladder, so a
  failing backfill cannot cancel a legitimate pending retry or burn the
  shared retry budget toward a premature `onPermanentFailure`.

This catches up across **daemon downtime**, judged against the durable
watermark. It is unrelated to the scheduler's small intra-process catch-up
window for slow passes. A *running* daemon that crosses a forward clock jump
still follows cron's no-catch-up-after-an-outage rule until the next restart
evaluates the watermark.

## In-flight runs and crash reconciliation

The run ledger records *finished* runs, so a run the daemon crashed under
used to leave no trace at all: not failed, not cancelled, absent. With a
store configured, every job also gets an **in-flight record**, and two
reconciliation passes turn an interrupted run into a visible ledger row
instead of a silent gap:

* **Open and closed records.** When a job goes from zero to one live
  instances on a node, an `open` record (host, a per-process token, pid,
  start instant, job digest) lands in the job's `inflight/<job>` stream; when
  the last instance finishes, a `closed` record follows. This is on for every
  job whenever `state` is configured, with no per-job option. On a
  request-billed mount the cost is about two extra fire-and-forget writes
  (plus a prune) per run.
* **Same-host reconciliation at rehydration.** Once per backend start (after
  the ledger warm, before the retry re-arm), an `open` record from *this
  host* whose writing process is gone is closed (reason `reconciled-crash`)
  and a synthetic ledger row appended. Three guards keep live runs safe:

  * a record written by this very process is skipped (a `state`-section
    reload rebuilds the backend under live runs);
  * live local instances take precedence over the ledger;
  * a recorded pid that still exists is left alone with a warning, because a
    daemon crash does not stop the job processes it spawned.

  The pid guard checks identity, not bare existence: a live pid whose process
  started after the record was written is a recycled pid, not that run, and
  the record is closed like any other crash.

* **Cross-host reconciliation on a slot takeover.** For a
  [`concurrencyScope: cluster`](Clustering-and-Leader-Election) job, the node
  that wins the job's slot lease fresh also closes a foreign holder's
  orphaned `open` record (reason `reconciled-takeover`). The caveat: an
  expired slot proves the previous holder made **no successful renewal** for
  a full TTL. It does *not* prove the process died (it may still be running
  if it lost store access; that overlap is the slot's documented
  at-least-once trade). The synthetic row's `fail_reason` therefore says
  "daemon crash, or the node lost access to the state store mid-run", never
  asserting a crash.
* **The synthetic row is a non-verdict.** The reconciled run lands in the
  ledger with outcome `unknown`:
  [`onlyIfLastSucceeded`](#depends-on-past-onlyiflastsucceeded) ignores it,
  the [trends](#sla-trends-over-the-ledger) success rate excludes it, and it
  carries no `started_at`, so duration statistics are untouched. Its
  `fail_reason` names the original start instant and host, and the
  [web dashboard](Web-Dashboard) renders `unknown` neutrally (gray, never
  success-green).
* **The catch-up watermark is `onMissed`-aware.** Under the default
  [`onMissed: skip`](#missed-run-catch-up) the row carries `finished_at` (the
  interrupted run's *start* instant), so the durable watermark advances over
  exactly the interrupted slot: it counts as attempted, and later missed
  occurrences are unaffected. Under `run-once` / `run-all` the instant is
  stored as `interruptedAt` *instead of* `finished_at`, leaving the watermark
  untouched. The interrupted occurrence is still owed to catch-up, because
  crash recovery must not silently downgrade those jobs to at-most-once.

## Depends-on-past: `onlyIfLastSucceeded`

For pipelines where running on top of a failure makes things worse (an
incremental sync, a ratcheting migration), `onlyIfLastSucceeded: true` skips a
job's scheduled fires while its most recent **real outcome** is a failure:

```yaml
jobs:
  - name: incremental-sync
    command: ./sync.sh --incremental
    schedule: "*/15 * * * *"
    onlyIfLastSucceeded: true
```

* **Newest real outcome wins.** The gate reads both the in-memory history and
  the durable ledger, and judges whichever `success`/`failure` is newest. The
  in-memory history is updated synchronously, so it never lags the durable
  write; the durable ledger sees runs from *other nodes* on a shared mount.
  `cancelled` and `skipped` records are ignored in both: a skipped tick does
  not clear the gate; only a genuine success re-opens it.
* **A still-running instance has not "succeeded"**, so it blocks the gate too,
  except under `concurrencyPolicy: Replace`, whose contract is that a new
  fire supersedes the running one. There the gate judges the last *finished*
  outcome.
* **No prior run allows.** A first-ever fire has nothing to depend on and is
  never blocked.
* **Only scheduled and `@reboot` fires are gated.** Retries, catch-up
  backfills, and manual `POST /jobs/{name}/start` triggers deliberately
  bypass it: the retry ladder exists precisely to run after a failure, and a
  manual trigger is the operator overriding the gate.
* **Store trouble follows the policy.** When the ledger cannot be read, the
  gate decides from the in-memory view under the default `degrade` (fail
  open); under
  [`onStoreUnavailable: fail-closed`](#when-the-store-is-unavailable-onstoreunavailable)
  it blocks instead. The gate also works with *no* `state` section at all,
  from memory only, so it is not restart-surviving. Pairing it with a store
  is what makes "the last run failed" survive restarts and span a fleet.

## Output archival and secret redaction

[Captured output](Output-Capturing) is normally in-memory and bounded per run.
`archiveOutput: true` additionally persists each finished run's captured
output to the job's `logs/<job>` stream, pruned to the same `maxRunsPerJob`
bound as the run ledger. The archive is a lightweight record of what the
23:00 run printed, surviving restarts and, on a shared mount, visible from
any node.

Before anything is written, `redactArchivedSecrets` (default `true`) scrubs
recognizable secrets from the output: `KEY=value` assignments whose key looks
secret-bearing (`PASSWORD`, `SECRET`, `TOKEN`, `API_KEY`,
`AWS_SECRET_ACCESS_KEY`, ...), `Authorization: Bearer`/`Basic` headers,
credentials embedded in URLs and connection strings
(`scheme://user:password@host`, including the user-less
`redis://:password@host` form), and well-known token formats (cloud keys,
GitHub personal access tokens, JWTs, private key blocks).

Job output routinely embeds credentials by accident; a crashed script echoing
its environment is the classic case. An archive multiplies the exposure, so
redaction is on by default. Set `redactArchivedSecrets: false` only when the
output itself is the artifact and the store is trusted. The redaction is
best-effort pattern matching, not a guarantee, and the archive files are
additionally created `0o600` (see [operational notes](#operational-notes)).

## Restart-surviving retries

Without a store, a pending retry is an in-memory timer: restart the daemon
mid-ladder and the remaining attempts are gone. With `state` configured, the
ladder becomes durable **automatically** for every job with
`onFailure.retry`, with no new per-job option:

* **Arming persists a `pending` record** (stream `retries/<job>`) carrying
  the attempt number, the **absolute** `notBefore` deadline, and the job's
  per-job config digest.
* **Record-before-run:** immediately before a retry launches, the record is
  settled (`launched`), so a crash right after the launch does not replay it.
  Every other end of the ladder settles too: succeeded, superseded by a fresh
  scheduled fire, cancelled, budget exhausted, ownership moved, job removed.
* **A graceful shutdown deliberately does *not* settle.** The pending record
  is exactly what the next boot re-arms.

On boot, a pending record **re-arms the ladder at its persisted position**:
the task sleeps only the *remaining* time to `notBefore` (zero if the
deadline passed while the daemon was down), and then re-checks the
[cluster gate](Clustering-and-Leader-Election) exactly like a never-restarted
ladder. Because the deadline is absolute, a retry armed for 04:00 fires at
04:00 (or immediately, if you restart at 05:00), not "backoff seconds after
whenever the daemon happened to come back".

A pending record is **settled instead of re-armed** when:

* the job's **per-job config digest** changed (`cronstable.fingerprint.job_digest`,
  deliberately stricter than the whole-set job-set id, so editing an
  *unrelated* job does not drop this job's retry, while any
  behavior-affecting edit to *this* job does: the old ladder must not run
  the new definition);
* the job was **removed or disabled**;
* the retry **budget is exhausted** under the current config;
* the record is **older than the job's `startingDeadlineSeconds`** (when
  set), the same "not worth replaying" bound catch-up honors;
* for an `@reboot` job, the machine **actually rebooted**: the fresh boot run
  supersedes the stale ladder.

Ambiguity always settles: with live ladders, cluster gates, and boot markers
involved, the wrong move is a double-run, so **the bias is no-run over
double-run** (at-most-once on the launch side, because of record-before-run).
The at-least-once residue lives on the write side instead: the pending-record
write is fire-and-forget, so a hard crash in the instant between arming a
retry and the record landing loses that re-arm (counted in
`cronstable_state_dropped_writes_total{kind="retry"}` when the store rejects the
write outright).

**`@reboot` keep-alive continuity.** An `@reboot` job with
`maximumRetries: -1` is a minimal process supervisor: start a process at
boot, restart it forever when it dies. Without durable retries, a daemon
restart breaks that loop (the job already "ran this boot", and the in-memory
ladder died with the old process). With a store, when the
[boot marker](#reboot-once-per-os-boot) shows the boot run already happened
during *this* OS boot, the pending retry is re-armed instead of superseded,
so the supervised process keeps getting restarted across cronstable's own
restarts.

**Cross-node retry resume.** On a shared store the ladder can also survive
the *node*, not only the process. Resume is active only when all three hold:
the store's resolved topology is `shared`, leader election is configured
(`electLeader`), and the cluster manager is running.

Resume applies to `Leader` / `PreferLeader` ladders that are not `@reboot`.
`EveryNode` ladders stay strictly per-node (every node runs its own copy, so
a foreign pending on the shared stream is another node's live ladder), and
`@reboot` ladders are anchored to a host's boot, so an abandoned `@reboot`
keep-alive still ends cluster-wide, as described earlier. While resume is
active:

* **An ownership move hands the ladder off.** When the cluster moves a job's
  ownership off-node mid-ladder, the old owner writes a `handoff` record
  (attempt, job digest, a now-due deadline, `fromHost`) instead of settling
  the ladder dead, and writes *no* `cancelled` run-history record: the
  attempt is moving, not dying. On a single-node store the legacy behavior
  is unchanged (settled `owner-moved`, plus the cancelled row).
* **A crashed owner's `pending` stays newest.** The new owner's claim scan
  (spawned from the housekeeping pass about once a minute) claims a
  `handoff` immediately, because the owner positively relinquished it. It
  claims a *foreign* `pending` only after that record is stale, 30 seconds
  past due. That grace covers a live owner whose fire is slightly late. It
  deliberately cannot cover an owner deferring on a closed cluster gate,
  whose re-check cadence is its own ladder delay; the consume-time re-check
  described later covers that case.
* **Claims are leased and re-checked.** To claim a record, the claimer:

  * validates it (digest match, job enabled, retry budget,
    `startingDeadlineSeconds`, no locally-known newer run);
  * acquires the job's `retry-claim/<job>` lease (TTL 30 seconds);
  * **re-reads** the newest record under the lease, which must be unchanged;
  * checks superseded-by-run against the **durable** ledger, because the run
    that resolved the ladder most likely happened on another host, which this
    node's in-memory history knows nothing about. A newer durable run settles
    the record `superseded-by-run` instead of claiming it.

  Only then does the claimer append its own `pending` (with its host and
  `claimedFrom`), wait for that write to land before releasing the lease,
  and re-arm the local ladder exactly like rehydration: absolute deadline,
  only the remaining delay slept.

* **The consume-time re-check is load-bearing.** While resume is active, a
  due retry's launch decision serializes on the *same* claim lease and
  re-checks that the newest ladder record still belongs to this host. When
  the newest record is foreign (a claimer's `pending`, or its settled
  `launched` after it already fired), this node stops its local ladder
  silently, writing no settle, so the claimer's record stays newest. This,
  not the staleness grace, is what protects a gate-deferred owner. Read or
  acquire failures follow
  [`onStoreUnavailable`](#when-the-store-is-unavailable-onstoreunavailable):
  `degrade` proceeds unserialized, `fail-closed` defers.
* **The contract is at-least-once, not exactly-once.** The lease, the
  re-read and the re-check close every race a responsive store lets them
  close. A store outage at the wrong instant can still let a claimed attempt
  and its original owner both fire, the same trade as every other cross-node
  guarantee on this page.
* **Mixed-version fleets are safe.** Older builds treat the unknown
  `handoff` record kind as not-pending and skip it: a partially upgraded
  fleet may lose a handoff (the ladder is not resumed there), but it never
  double-runs one.

## `@reboot` once per OS boot

Without a store, `@reboot` means "once per daemon start": restart cronstable
and every `@reboot` job runs again. With `state` configured, a standalone
(non-cluster-deferred) `@reboot` job runs **once per OS boot per host**:

* **Boot identity.** The source depends on the platform:

  * Linux: `/proc/sys/kernel/random/boot_id` (exact).
  * Elsewhere, where an uptime source exists: the boot time derived from
    uptime (`GetTickCount64` on Windows, `/proc/uptime` on POSIX), compared
    with a 60-second tolerance.
  * macOS and the BSDs, which have neither: the kernel's own record of the
    boot instant (`kern.boottime`, with psutil).

  Only when every source fails does the daemon fall back to treating each
  start as a fresh boot (the job then runs every daemon start, the stateless
  behavior).

  One Windows caveat: Fast Startup ("Shut down" with hiberboot enabled, the
  client default) hibernates the kernel session, so the next power-on resumes
  the same uptime counter and reads as the *same* boot. `@reboot` does not
  re-fire after it, only after a Restart (which always begins a new boot). To
  get a fire on every power-on, disable Fast Startup (`powercfg /h off`).

* **Record-then-run.** The marker (stream `reboot/<job>`: host, boot
  id/time, job digest) is written *before* the launch, so a crash between
  record and spawn errs toward not re-running, the same at-most-once
  ordering as the cluster's `reboot_ran` path.
* **A redefined job runs again.** The marker is scoped to the job's config
  digest, so changing the job's definition re-fires it this boot, mirroring
  the cluster path's job-set scoping.
* **A surviving boot run blocks a relaunch.** When a starting daemon's
  [in-flight reconciliation](#in-flight-runs-and-crash-reconciliation) finds
  the previous daemon's run of an `@reboot` job still running on this host,
  the launch is skipped, by the boot gate and the deferred cluster launch
  paths alike. That holds even when no marker covers this boot (the marker
  may predate this build, or the boot instant may be unknowable), because the
  one-shot is still running. The remembered pid is pinned to its process
  start time, so a pid the OS recycled never counts as the survivor. The skip
  leaves the marker stream exactly as it was, so if the survivor dies before
  the next daemon start, the marker alone decides then, erring at-least-once.
* **Store trouble follows the policy.** When the marker cannot be read or
  written, the daemon runs the job anyway under the default `degrade`
  (at-least-once, exactly the stateless behavior). Under
  `onStoreUnavailable: fail-closed`, an unverifiable marker skips the boot
  run.

Cluster mode is unaffected: `Leader`/`PreferLeader` `@reboot` deferral and
dedupe under `electLeader` keep working through the gossip/lease `reboot_ran`
mechanism described in
[clustering and leader election](Clustering-and-Leader-Election#reboot-jobs-under-leader-election);
this boot marker covers the standalone and `EveryNode` cases those paths do
not.

## Restart-durable Prometheus counters

Prometheus copes with counter resets, but a scheduler that restarts nightly
exports permanently tiny counters and defeats long-range queries. With
`state` configured, the per-job counter accumulators (runs by outcome,
retries, permanent failures, start failures, the duration histogram, last
success/failure timestamps) are **snapshotted** to a host-scoped stream
(`counters/<host>`), piggybacked on the per-run persist task and throttled to
at most one write per 15 seconds. A final unthrottled snapshot lands at
shutdown.

On boot the snapshot is **seeded back** (added into the fresh accumulators)
once per process, only for jobs still present in the config. Histogram state
is restored only when the configured bucket bounds are unchanged. The result:
`cronstable_job_runs_total` and the other job counters carry on across
restarts instead of resetting to zero.

This is **lossy-durable by design**: a hard crash forfeits at most the events
since the last snapshot (up to 15 seconds' worth), which Prometheus reads as a
small, ordinary counter reset. `cronstable_start_time_seconds` still resets per
process, deliberately, because it measures the process. See
[metrics with Prometheus](Metrics-with-Prometheus) for the families
themselves.

## SLA trends over the ledger

`GET /jobs/{name}/trends` answers "what is this job's success rate this week"
without a metrics stack: the same stats object as `GET /jobs/{name}/runs`
(total, success, failure, cancelled, success rate excluding cancelled,
avg/min/max/last duration), computed per window over the **durable run
ledger**:

```json
{
  "name": "nightly-report",
  "source": "durable",
  "generated_at": "2026-07-04T12:00:00+00:00",
  "windows": {
    "1h":  { "total": 4,   "success": 4,  "...": "..." },
    "24h": { "total": 96,  "success": 95, "...": "..." },
    "7d":  { "total": 672, "success": 668, "...": "..." },
    "30d": { "...": "..." },
    "all": { "...": "..." }
  }
}
```

The horizon is bounded by `maxRunsPerJob` retention; on a shared mount the
ledger merges **every node's** runs, so the numbers are fleet-wide. Each
window's `last_duration`, `last_cpu_seconds` and `last_rss_bytes` come from
the run in that window with the newest finish time, whichever node wrote it.
When the store is unavailable the endpoint degrades to the in-memory history
(`"source": "memory"`) rather than erroring, so it always answers. It is
authenticated like every other data endpoint (bearer token when configured);
see the [HTTP control API](HTTP-API).

## When the store is unavailable: `onStoreUnavailable`

A store can be slow, unmounted, or gone. `state.onStoreUnavailable` picks
which way the *durable-truth gates* err; **plain scheduled fires are never
gated on the store under either policy**:

| | `degrade` *(default)* | `fail-closed` |
| --- | --- | --- |
| Philosophy | behave exactly as the stateless daemon would | prefer not running over running wrong |
| Failed durable writes | dropped with a warning, counted in `cronstable_state_dropped_writes_total` | same (writes are never blocking) |
| `onlyIfLastSucceeded` gate | decides from the in-memory history (fail open) | **blocks** the fire |
| A due durable retry | proceeds on the in-memory ladder | **defers** and re-checks, like a closed cluster gate |
| The cluster concurrency slot claim ([`concurrencyScope: cluster`](Clustering-and-Leader-Election)) | launches with **node-local** enforcement only for that run (a warning names the reason) | **skips** the launch, like a closed cluster gate |
| Serializing a due retry with [cross-node claims](#restart-surviving-retries) | proceeds unserialized (at-least-once) | **defers** and re-checks |
| `@reboot` boot marker unreadable/unwritable | runs the job (at-least-once) | **skips** the boot run |
| Scheduled fires | never gated | never gated |

`degrade` is the right default for almost everyone: the store adds features,
and losing it subtracts exactly those features. Choose `fail-closed` when a
gated job running against unverifiable state is worse than it not running at
all (the same reasoning as `clusterPolicy: Leader`'s skip-over-double-run
bias).

## Garbage collection and manifests

Jobs get renamed and deleted; without GC their streams would sit in the store
forever. The store cleans up after itself, conservatively, anchored on
**manifests**:

* Every node records a **manifest** (stream `manifests/<host>`): its host,
  job-set id, the job names of its loaded config, plus the shared artifact
  scopes and dag names that config can write. The node writes it on backend
  start and every 6 hours.
* A **GC pass** runs every 24 hours per process, plus on demand with
  [`cronstable state gc`](#administering-the-store). It deletes a job's
  streams (runs, logs, catch-up, retries, reboot markers, in-flight records,
  slot cancel records, artifact streams) when **no recent manifest**
  references the job *and* its newest record is older than `gcGraceSeconds`
  (default 7 days). "No recent manifest" spans *any* node and *any* job set
  under the same `deploymentId`. Counter and manifest streams of hosts no
  recent manifest names are collected likewise.
* **Artifacts age out with their scope.** A removed scope's `artifacts/`
  stream (a removed job's artifacts, or a pruned dag_run's XCom) ages out
  under the same manifest-anchored grace rules as every other stream. The run
  documents of a dag removed from the config are deleted after the dag has
  been absent from every config and recent manifest for a full grace window
  (terminal runs only; an active or still-owned run is never touched). After
  each successful pass, content-addressed payload **blobs** that no surviving
  artifact record references *and* that are older than the grace are swept.
  All of it is biased to KEEP:

  * artifact streams and dag-run documents stay unmanaged until every recent
    manifest advertises its scopes and dags, so a mixed-version fleet is safe
    and management starts one grace window after an upgrade;
  * the blob sweep stands down with a logged reason when any artifact stream
    cannot be enumerated or any record read;
  * a newly written or re-published blob is age-guarded.

* **Only per-run DAG advance leases are reclaimed.** A `dagadvance/` lease
  whose recorded expiry *and* last write are both older than the grace is
  deleted along with its `.lock` side-file. Run keys are unique, so nothing
  persists such a lease's fence beyond its own run document. Every other
  lease file (slot, retry claim, election, job lock) is never deleted at
  any age: their fences persist in durable records, and a reset fence
  could re-collide with a persisted replace-cancel. Orphaned lock
  side-files (a document's `.lock` whose document is gone, a bare lease
  `.lock` without its lease) are swept after a full grace window of idleness.
* The pass also sweeps crashed write-temp files older than a day and
  quarantined records older than the grace.
* **Never touched:** unrecognized streams and the `meta` stream. A store
  shared by several deployments is safe: each namespace GCs only itself, and
  anything GC does not positively recognize as garbage stays.
* **Deferred until it can prove absence:** nothing is deleted until the
  retained manifest history spans one full grace window. A fresh store, or
  the first passes after upgrading a store that predates manifests,
  therefore collects nothing for the first `gcGraceSeconds`, rather than
  treating "nobody has manifested yet" as "nobody wants this".

The manifest-plus-grace design means a node that is only *down* does not
lose its jobs' history (its last manifest stays recent for the grace period),
while a job genuinely deleted from every config ages out a week later. Set
`gcGraceSeconds` to cover your longest plausible full-fleet outage, or `<= 0`
to disable automatic GC entirely and run `cronstable state gc` yourself. Values
between `1` and `86399` are rejected at parse time: a grace shorter than the
manifest cadence would make every live peer's manifests look stale and hand
their state to the collector.

## Rate limiting: `maxOpsPerSecond`

On a request-billed shared mount, a busy store (many jobs, tight schedules,
archived output) has a literal price. `state.maxOpsPerSecond` puts a token
bucket over **every backend operation except lease operations** (burst = one
second's tokens). Operations past the rate queue rather than fail. The delay
is invisible to scheduling: writes are already background tasks, and bounded
reads still honor their timeout. The throttling is observable as
`cronstable_state_throttled_ops_total` /
`cronstable_state_throttle_wait_seconds_total`.
`0` (the default) disables the limiter, the right choice for a local
directory.

Lease operations bypass the bucket deliberately: a lease renew queued behind
a burst of bulk writes could overshoot its TTL, expiring a live holder's
lease and double-running the very job the lease exists to fence. The
coordination traffic is a handful of small operations per running slot-gated
job, so exempting it costs little.

## Job-facing state

Everything so far hands the durable store to the *scheduler*. `state.jobApi`
(on by default whenever `state` is configured) hands it to the *jobs* too:
the daemon runs a small HTTP endpoint bound to loopback and injects its
address plus a per-run bearer token into every job's environment, so a job
command can reach the store through six commands. The commands are thin
clients of that endpoint. There is no coordination service to run and no
client library to install, only the daemon that is already running the job.

The calls route through the daemon, rather than each job opening the store
itself, because three of the six primitives *need* the live daemon. A mutex
must be renewed while the job holds it and released the instant the run ends.
A run-scoped secret is staged in memory and dropped when the run ends. Every
call is scoped and authorized by *which run is calling*, which the injected
token establishes without the job proving anything.

### The injected environment

When `jobApi` is enabled, every job launched sees these variables (all are
strings; an unknown scheduled time is the empty string):

| Variable | Meaning |
| --- | --- |
| `CRONSTABLE_STATE_URL` | Base URL of the endpoint, for example `http://127.0.0.1:54321`, or the configured `https://` address. |
| `CRONSTABLE_STATE_TOKEN` | The per-run bearer token, revoked when the run ends. |
| `CRONSTABLE_STATE_CACERT` | Path to the CA the commands verify the endpoint against, injected only when `state.jobApi.tls.ca` is set. |
| `CRONSTABLE_RUN_ID` | A unique id for this run. |
| `CRONSTABLE_JOB_NAME` | The job name (the default *scope*, described later). |
| `CRONSTABLE_ATTEMPT` | The retry attempt number (`0` on the first fire). |
| `CRONSTABLE_SCHEDULED_AT` | The scheduled fire time (ISO-8601), or empty. |
| `CRONSTABLE_HOST` | The host name. |

The commands read these; you rarely touch them directly. Set
`state.jobApi.enabled: false` to keep the durable scheduler features while
injecting nothing and running no endpoint.

### Scopes

Every KV / cursor / artifact / lock call lands in a **scope**, a namespace
that defaults to the calling **job's own name**, so one job cannot read
another's keys by accident, or by design: naming any *other* scope is
authorized, not only defaulted-away.

A run may always act in its own scope and the conventional shared `global`
namespace (`--global`, for deliberate cross-job coordination). Naming any
other scope needs that name in the job's `stateAllowedScopes` list, or the
loopback endpoint answers `403`. Without an entry there, `--scope NAME`
cannot be used to reach into an unrelated job's private state (which is that
job's own name). Secrets are always scoped to the single run they were staged
for.

### Durable key/value

`cronstable state get|set|delete|keys` is a restart-surviving map, scoped per job
by default. It coexists with the `cronstable state` [admin
subcommands](#administering-the-store) (backup / gc / ...); the action name
tells them apart.

```shell
cronstable state set last-cursor 12345
value=$(cronstable state get last-cursor)      # -> 12345
cronstable state set config '{"n": 3}' --json  # store parsed JSON, not a string
cronstable state keys                          # one key per line
cronstable state delete last-cursor
```

`get` on a missing key prints nothing and exits `4`, so a script can branch on
absence. `set` refuses a value larger than `maxValueBytes`.

### Cursor / watermark

`cronstable cursor advance NAME VALUE` moves a monotonic watermark: the stored
value only ever goes to `max(current, VALUE)`, so an out-of-order or replayed
batch never walks it backwards, and on a shared store several nodes converge
on the furthest point. A numeric value compares numerically (`9 < 10`);
anything else compares as the string it is, which orders ISO-8601 timestamps
correctly (`2026-06 < 2026-07`). This is the ETL "process only what is new"
pattern:

```shell
since=$(cronstable cursor get watermark 2>/dev/null || echo 0)
# ... export rows with id > $since, tracking the new maximum ...
cronstable cursor advance watermark "$new_max"
```

Pass `--force` to set the value even if it moves the cursor backwards (a
deliberate rewind). Both `cursor get` and `cursor advance` print the
resulting value.

### Idempotency keys

`cronstable idempotent KEY` claims a key once, fleet-wide: the first caller wins
(exit `0`, do the work), every later caller loses (exit `5`, skip). A
transport or store error exits `1` instead, so an outage is distinguishable
from "already done". It is the "run this side effect at most once" guard for
a retried or duplicated run:

```shell
if cronstable idempotent "charge-$(date -u +%F)"; then
  charge-the-invoices          # runs at most once per day across the fleet
fi
```

`--ttl SECONDS` makes the claim expire (a bounded dedupe window; the default
`0` is a permanent claim); `--release` drops a claim so the key can be won
again. Like every cronstable coordination primitive this is at-least-once,
not exactly-once. A caller that wins the claim then crashes before finishing
has "claimed but not done" work, which is why the claim guards an
*idempotent* side effect.

### Mutex and semaphore

`cronstable lock` is a fleet-wide lock backed by the same TTL lease the cluster
concurrency slots use. The daemon holds the lease on the run's behalf, renews
it while the job holds the lock, and releases it the instant the job releases
*or the run ends*, so a job that crashes or forgets to unlock never leaks a
lock (the lease also self-frees by its TTL as the backstop). `lock run` is the
convenient form: it holds the lock while the wrapped command runs and always
releases it afterward, even when the command fails or is signaled:

```shell
# only one holder of "db-maintenance" runs across the whole fleet at a time:
cronstable lock run db-maintenance --scope global --wait --timeout 60 \
  -- /usr/local/bin/compact-db.sh
```

`--permits N` makes it a **semaphore** of `N` concurrent holders instead of a
mutex (`N = 1`). `N` must be between `1` and `1024`: the acquire probes
permits sequentially, so an oversized count is refused rather than turned
into that many store operations per pass. `--wait --timeout S` blocks up to
`S` seconds for a free permit (`--timeout` defaults to `0`, so `--wait` alone
makes a single pass and gives up, exactly like omitting it). Without
`--wait`, a taken lock returns immediately (exit `3`).

For manual control, `cronstable lock acquire NAME` prints a hold token and
`cronstable lock release TOKEN` frees it. The acquire reply also carries the
lease's monotonic **fence** token, for a job that needs true fencing on top
of the lock. The limit: like every distributed lock this is at-least-once. A
holder that loses its lease to a store outage keeps running, and the fence is
how a careful job fences its own writes.

### Artifact store

`cronstable artifact put NAME [FILE]` publishes a small blob (from `FILE` or
stdin) under a name that a later run, or a peer node, reads back with
`cronstable artifact get NAME`. Payloads are content-addressed (identical bytes
store once) and read newest-wins:

```shell
build-report > report.csv
cronstable artifact put latest-report report.csv     # prints the sha256
# ... a later run, possibly on another node ...
cronstable artifact get latest-report -o report.csv
cronstable artifact list                             # one name per line
```

`put` refuses a payload larger than `maxArtifactBytes`. Artifacts are durable
and accumulate until their scope is [garbage
collected](#garbage-collection-and-manifests) with the rest of a removed job's
state. Blobs deduplicate across scopes, and a payload blob no surviving
artifact record references is swept by the same GC pass after it is older
than the grace.

### Run-scoped secrets

A job's `secrets:` block stages secrets for the run over the endpoint, rather
than placing them in the environment where they would show in
`/proc/<pid>/environ` or a `ps -E`. Each secret is resolved fresh per run,
served only to that run, and dropped when the run ends. It never touches the
durable store. The same `value` / `fromFile` / `fromEnvVar` source triple
every other cronstable secret uses:

```yaml
jobs:
  - name: build-report
    command: |
      token=$(cronstable secret get API_TOKEN)
      build-report --token "$token"
    schedule: "0 6 * * *"
    secrets:
      - name: API_TOKEN
        fromEnvVar: REPORT_API_TOKEN     # or value:, or fromFile:
```

`cronstable secret get NAME` prints the value (exit `4` if it was not staged);
`cronstable secret list` prints one staged name per line (names, not their
values). Declaring `secrets` needs a `state` section with `jobApi` enabled;
otherwise the config is rejected.

A full worked config is in
[`example/job-state/cronstable.yaml`](https://github.com/ptweezy/cronstable/tree/main/example/job-state).
The wire protocol (the `/v1/` endpoints) is in the
[HTTP control API](HTTP-API) reference, and every command's flags and exit
codes are in the [command-line reference](CLI-Reference).

## Administering the store

The `cronstable state` subcommands administer the store of the `state:` section
in your config (`-c/--config` works both before and after the subcommand:
`cronstable -c X state gc` and `cronstable state gc -c X` are equivalent). Exit
codes: `0` success, `1` error, `2` usage. Full flags and examples are in the
[command-line reference](CLI-Reference); in summary:

| Command | Does |
| --- | --- |
| `cronstable state backup -o FILE.tar.gz` | Writes an owner-only (`0o600`) `.tar.gz` of the store (records, documents, blobs, and leases; `tmp/` and `quarantine/` excluded). Safe against a live daemon. |
| `cronstable state restore FILE.tar.gz [--force]` | Restores a backup into the store; refuses a non-empty store without `--force` (which merges, keeping the newer lease fences), and sanitizes archive members. Not safe while a daemon uses the store. |
| `cronstable state migrate --dest PATH [--dest-deployment-id ID]` | Copies the store between paths/mounts (local ↔ Amazon S3 Files / EFS) with torn-read-safe atomic placement; then point `state.path` at the new home. |
| `cronstable state gc [--dry-run]` | Runs a manual [GC pass](#garbage-collection-and-manifests); reports the reclaimed streams and orphaned artifact blobs, or why the blob sweep was skipped. |
| `cronstable state check` | Probes writability and prints an inventory of the store. |
| `cronstable state migrate-schema [--dry-run]` | Rewrites records of older *known* record schemes to the current one. `v1` is the only scheme so far, so today this reports and converts nothing; unknown versions are left to quarantine-on-read. |

The admin code (`cronstable/state_admin.py`) is imported only when a `state`
subcommand runs, so the stateless install pays nothing for it.

## Observing the store

The store exports its own health at `GET /metrics` alongside the job
families (see [metrics with Prometheus](Metrics-with-Prometheus)):

* `cronstable_state_info{backend,topology}`: what is configured;
* `cronstable_state_ops_total{op}` / `cronstable_state_op_errors_total{op}` /
  `cronstable_state_op_seconds_total{op}`: operation counts, errors, and
  in-store latency (divide seconds by ops for the mean) per operation
  (`append` / `list` / `derive-max` / `prune` / lease operations / `gc` / ...);
* `cronstable_state_lock_acquisitions_total` /
  `cronstable_state_lock_wait_seconds_total`: advisory-lock contention
  (emitted once nonzero);
* `cronstable_state_throttled_ops_total` /
  `cronstable_state_throttle_wait_seconds_total`: the
  [`maxOpsPerSecond`](#rate-limiting-maxopspersecond) limiter (emitted once
  nonzero; lease operations never show up here, because they
  [bypass the bucket](#rate-limiting-maxopspersecond); a queued renew
  could overshoot its TTL and double-run the job the lease fences);
* `cronstable_state_dropped_writes_total{kind}`: durable writes that failed
  and were dropped (`kind`: `run-record`, `checkpoint`, `retry`,
  `reboot-marker`, `inflight`, `counters`, `manifest`). **This is the one to
  alert on**: a rising rate means the durable features are silently
  degrading.

When a backend read fails at scrape time, the daemon omits the state families
from that scrape (the job and daemon families still serve) rather than failing
it with a `500`.

The [web dashboard](Web-Dashboard) adds a browser view over the same store:
an opt-in **durable state inspector** with per-primitive tabs over the
store's documents and records, fed by the
[state inspector endpoints](HTTP-API#state-inspector-endpoints). Its page
documents the UI.

## Operational notes

* **File modes.** The store's directories are created `0o700` and its data
  files `0o600`, both further narrowed by your umask: records can carry job
  output, which routinely includes things that should not be world-readable.

  Mode bits are POSIX-only. Windows does not map them onto ACLs, so there the
  store inherits its parent directory's ACL. On a multi-user Windows host,
  root the store under a directory whose ACL is restricted to the daemon's
  account; see
  [running on Windows](Running-on-Windows#posix-file-modes-are-advisory-only)
  for an `icacls` recipe. The same applies to `cronstable state backup`
  output.
* **Same user on shared stores.** Because records are `0o600`, **every node
  sharing a store must run cronstable as the same user**; two nodes running as
  different users silently hide half the history from each other (a
  persistent `EACCES` on reads is the symptom, and the log warning says
  exactly this).
* **Clocks on shared mounts.** Lease expiry and record ordering compare
  wall-clock timestamps *across hosts* on a shared mount, so fleet-wide use
  assumes bounded clock skew. Run NTP (or your platform's equivalent) on
  every node that mounts the store. Irrelevant to single-node use.
* **`maxRunsPerJob` below 2.** The ledger prune keeps the newest records by
  write order, and no node can order a peer's appends against its own, so a
  retention of `1` leaves no room for a pair that landed inverted: the prune
  would keep the older run and delete the newer one. cronstable raises `1` to
  `2` and logs the adjustment once at startup.

  The prune stays write-ordered on purpose, because a crash-reconciled record
  under `onMissed: run-once` or `run-all` deliberately carries no finish time,
  and a finish-keyed prune would have no key for exactly those records.
* **Encryption at rest is the mount's job.** The store writes plain JSON
  files and delegates at-rest encryption to the filesystem underneath:
  LUKS/dm-crypt locally, or the EFS / S3 encryption options on a shared
  mount. Secret [redaction of archived output](#output-archival-and-secret-redaction)
  reduces what lands in the files; it does not replace encrypting the volume.
* **Backups.** `cronstable state backup` is safe to run against a live daemon
  (immutable records mean a backup never races a rewrite); pair it with
  `state restore` / `state migrate` for moves between hosts or mounts
  (those two are *not* safe against a store a daemon is actively using;
  stop the daemon first).

## See also

- [Orchestration and DAGs](Orchestration-and-DAGs): the durable workflow tier built entirely on this store (dag_run documents, XCom over the artifact store, per-run advance leases).
- [Configuration Reference](Configuration-Reference): the `state` section and per-job option schema.
- [Command-Line Reference](CLI-Reference): the `cronstable state` administration subcommands.
- [Failure Detection and Retries](Failure-Detection-and-Retries): the retry ladder these records make durable.
- [Clustering and Leader Election](Clustering-and-Leader-Election): the owner gate catch-up and retries re-check.
- [Output Capturing](Output-Capturing): what `archiveOutput` persists.
- [HTTP Control API](HTTP-API): `GET /jobs/{name}/trends`, the run endpoints, and the job-facing `/v1/` loopback endpoints.
- [Listener TLS](Listener-TLS): serving this endpoint (and the web API) over `https://`, and the CA jobs verify it against.
- [Web Dashboard](Web-Dashboard): the rehydrated history views, the separate browser-side run ledger, and the durable state inspector.
- [Metrics with Prometheus](Metrics-with-Prometheus): the `cronstable_state_*` families and the restart-durable job counters.
