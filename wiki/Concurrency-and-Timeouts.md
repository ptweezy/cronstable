# Concurrency and timeouts

This page documents how cronstable handles overlapping runs of the same job
(`concurrencyPolicy`, and how far that policy reaches: `concurrencyScope`)
and how it bounds the duration of a single run (`executionTimeout`,
`killTimeout`). These options are per-job (settable in `defaults`) and govern
only one launch of a job. They have no effect across different jobs. By
default they also reach no further than one daemon process.
`concurrencyScope` can widen `Forbid` and `Replace` to a whole fleet sharing
a [durable state store](Durable-State).

**On this page:**
[Overview](#overview) ·
[Option summary](#option-summary) ·
[Concurrency policy](#concurrency-policy) ·
[Concurrency across a cluster](#concurrency-across-a-cluster) ·
[Execution timeout](#execution-timeout) ·
[Cancellation and killTimeout](#cancellation-and-killtimeout) ·
[Scope and interaction](#scope-and-interaction)

## Overview

A job is identified by its `name`. The daemon tracks, per name, a list of
currently-running instances. When a scheduled time arrives (or a manual start
is requested through the [HTTP control API](HTTP-API)), the daemon checks
whether any instance of that job is already running and consults
`concurrencyPolicy` before launching a new one.

That tracking is local to one daemon process. By default
(`concurrencyScope: node`) the local list is all that counts.
`concurrencyScope: cluster` makes `Forbid` and `Replace` additionally consult
a slot lease in the shared [state store](Durable-State), extending their
reach to instances on other nodes (see
[concurrency across a cluster](#concurrency-across-a-cluster)). The local
check always runs first, and the cluster gate is additive.

Independently, each running instance carries a deadline derived from
`executionTimeout`. On expiry it is cancelled, and `killTimeout` controls the
SIGTERM-then-SIGKILL escalation used during any cancellation. Both signals go
to the job's whole **process group**, and each job is spawned in its own
session, so a cancellation takes down the command's descendants too, not only
the process cronstable spawned.

The SIGTERM-then-SIGKILL escalation is the POSIX spelling. On Windows the
same two-step runs with the platform's own primitives: the graceful step
delivers a trappable `CTRL_BREAK_EVENT` to the job's process group (each job
is spawned in its own group), and the forced step kills the live process
tree with `taskkill /F /T`. `killTimeout` bounds the wait between the two.
See [running on Windows](Running-on-Windows), including the one degradation
(a daemon with no console cannot deliver the break, so the graceful step
becomes the tree kill immediately).

## Option summary

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `concurrencyPolicy` | enum: `Allow`, `Forbid`, `Replace` | `Allow` | Behavior when a launch is requested while another instance of the same job is still running. |
| `concurrencyScope` | enum: `node`, `cluster` | `node` | How far `concurrencyPolicy` reaches. With `node`, only this process's running instances count. With `cluster`, `Forbid` and `Replace` also exclude instances on other nodes sharing the [`state` store](Durable-State). See [concurrency across a cluster](#concurrency-across-a-cluster). |
| `executionTimeout` | float (seconds, `> 0` when set) | none (`null`) | Maximum wall-clock duration of a single run. On expiry the run is cancelled and assigned return code `-100`. |
| `killTimeout` | float (seconds, `>= 0`) | `30` | When a run is cancelled, seconds to wait after the graceful process-group signal (SIGTERM on POSIX, `CTRL_BREAK_EVENT` on Windows) before the unconditional kill (process-group SIGKILL on POSIX, `taskkill /F /T` tree kill on Windows). See [running on Windows](Running-on-Windows). |

Types come from the strictyaml schema: `concurrencyPolicy` is
`Enum(["Allow", "Forbid", "Replace"])`, `concurrencyScope` is
`Enum(["node", "cluster"])`, and `executionTimeout` and `killTimeout`
are `Float()`. Defaults come from `DEFAULT_CONFIG`. All four options are
optional (`Opt(...)` in the schema).

Numeric ranges are enforced after parsing: `killTimeout >= 0` and, when set,
`executionTimeout > 0`. A violating value raises a `ConfigError` at config
load. Two `concurrencyScope: cluster` combinations are likewise refused at
load rather than left silently inert. See
[concurrency across a cluster](#concurrency-across-a-cluster).

See the [configuration reference](Configuration-Reference) for where these
options sit in the document and how `defaults` apply.

## Concurrency policy

When `maybe_launch_job` runs for a job and one or more instances of that
name are already running in this process, it logs a warning
(`Job <name>: still running and concurrencyPolicy is <policy>`) and then acts
according to `concurrencyPolicy`. This local check always runs first. For a
`concurrencyScope: cluster` job, a launch that clears it must then also claim
the job's cluster slot (see
[concurrency across a cluster](#concurrency-across-a-cluster)).

### Allow (default)

The new instance is started immediately alongside the existing one(s).
Multiple instances of the same job can run concurrently with no bound on
their number. Each instance is tracked and reaped independently.

```yaml
jobs:
  - name: ingest
    command: ./ingest.sh
    schedule: "* * * * *"
    concurrencyPolicy: Allow
```

### Forbid

If any instance is still running, the new launch is skipped entirely. No new
process is started. The already-running instance continues unaffected.
This applies equally to scheduled launches and to retry-triggered launches.
The instances considered are this process's own. With
`concurrencyScope: cluster`, an instance running on another node that shares
the state store also forbids the launch (see
[concurrency across a cluster](#concurrency-across-a-cluster)).

```yaml
jobs:
  - name: ingest
    command: ./ingest.sh
    schedule: "* * * * *"
    concurrencyPolicy: Forbid
```

### Replace

Every currently-running instance of the job is cancelled, then a new instance
is started. Before canceling, the scheduler sets `replaced = True` on each
outgoing instance. This flag changes how the finished run is reaped:

- The replaced run is **not** treated as a failure: `_handle_finished_job`
  returns early when `replaced` is set, logging
  `Job <name> was replaced by a newer instance`.
- Because it is not a failure, it is **not reported** (no Mail/Sentry/Shell/Webhook
  reporters fire for it) and it does **not** trigger
  [retries](Failure-Detection-and-Retries). `cancel()` itself does not set a
  return code, so the run's own `wait()` task records whatever it happened to
  see: the signal-derived code, or `-100` had its own `executionTimeout`
  expired first. That value is irrelevant: the reaper short-circuits on
  `replaced` before inspecting it.

Cancellation of the outgoing instance uses the same process-group
SIGTERM/`killTimeout`/SIGKILL sequence described under
[cancellation and killTimeout](#cancellation-and-killtimeout).
`maybe_launch_job` awaits each `cancel()` before starting the replacement, so
the replacement starts only after the old instance has terminated.

This inline cancel-then-launch applies to instances running in this process.
With `concurrencyScope: cluster`, replacing an instance on another node means
asking that node to cancel it, and the replacement launch waits until that
node yields. See [replace across the cluster](#replace-across-the-cluster).

```yaml
jobs:
  - name: sync
    command: ./sync.sh
    schedule: "* * * * *"
    concurrencyPolicy: Replace
    killTimeout: 10
```

## Concurrency across a cluster

`concurrencyPolicy` on its own reaches only the instances tracked by one
daemon process. A fleet of nodes sharing a [durable state store](Durable-State)
can widen `Forbid` and `Replace` to the whole fleet with the per-job
`concurrencyScope` option (settable in `defaults`; see the
[configuration reference](Configuration-Reference)):

```yaml
state:
  path: /mnt/shared/cronstable-state   # the same store on every node
  topology: shared

jobs:
  - name: ingest
    command: ./ingest.sh
    schedule: "* * * * *"
    concurrencyPolicy: Forbid
    concurrencyScope: cluster
```

`concurrencyScope: node` (the default) is the classic behavior described
earlier: only this process's running instances are considered.
`concurrencyScope: cluster` makes `Forbid` and `Replace` also exclude
instances of the job on other nodes sharing the `state` store. The local
check still runs first and is unchanged, and the cluster gate is additive. It
works with or without a `cluster:` section: the shared store, not leader
election, is what coordinates the nodes.

### Requirements

Two combinations are refused at config load rather than left silently inert:

- `concurrencyScope: cluster` requires a `state` section somewhere in the
  final assembled config (a config directory may keep `state` and jobs in
  different files). Without one, parsing fails with ``concurrencyScope:
  cluster requires a `state` section (the shared store is what coordinates
  the nodes), but none is configured; offending job(s): ...``, naming every
  offending job.
- `concurrencyScope: cluster` with `concurrencyPolicy: Allow` raises
  `Job <name>: concurrencyScope: cluster has no effect with
  concurrencyPolicy: Allow (the default); set Forbid or Replace, or drop
  concurrencyScope`. `Allow` places no bound on concurrent instances, so
  there is nothing for the cluster to gate.

### The slot lease

Every launch of a cluster-scoped job (scheduled, retry, catch-up backfill,
deferred `@reboot`, or a manual API start) first claims a TTL **slot lease**
named `slots/<job name>` in the state store (the cron `state` store, not the
leadership store). The claim is a single choke point in `maybe_launch_job`,
and each store operation in it is bounded (10 seconds), so an unresponsive
mount cannot stall the scheduler pass.

- The lease TTL is `state.slotTtlSeconds` (default `30`; a value below 5
  raises `state.slotTtlSeconds must be >= 5`). While the job runs here, the
  holder renews the lease every third of the TTL. A node that crashes mid-run
  stops renewing, and its slot frees itself after at most one TTL.
- Lease operations bypass the `state.maxOpsPerSecond` token bucket: a renew
  queued behind bulk writes could overshoot its TTL and double-run the job
  the lease fences.
- The slot is released when the job's **last** local instance finishes
  (claims are refcounted, so overlapping instances share one lease). If the
  release fails, a `state: failed to release the concurrency slot for <name>
  (...); it frees by TTL` message is logged. TTL expiry is always the
  fallback.
- The lease is held under a process-unique identity (log messages show the
  node's display name), so a restarted daemon can never adopt its
  predecessor's slot.
- A `state`-section reload that names the same store (the same `path` and
  `deploymentId` as the backend resolves them, so a respelled path or an
  explicit `deploymentId: default` is the same store) rebuilds the backend
  under the live run and keeps its slot lease and renewer (a changed
  `slotTtlSeconds` applies from the next renew). A reload that moves the
  store leaves the lease behind to lapse by TTL and logs `Job <name>: its
  cluster concurrency slot stays in the previous state store while the run
  continues here; a peer may launch it once that lease expires`; the next
  launch claims in the new store.

Manual starts through the [HTTP control API](HTTP-API) go through the same
gate, consistent with the local behavior: a manual start was already subject
to a node-local `Forbid`.

### Forbid across the cluster

If a live instance on another node holds the slot, the launch is skipped with
a warning naming the holder:
`Job <name> skipped: its cluster concurrency slot is held by <node>
(concurrencyPolicy: Forbid, concurrencyScope: cluster)`. Nothing further
happens for that occurrence, exactly like a local `Forbid` skip.

### Replace across the cluster

`Replace` cannot signal a process on another machine, so the requester asks
the holder to yield instead of canceling it directly:

1. The requester appends an immutable **cancel record** to the
   `slots/<job name>` stream, targeted at the holder's exact lease fence, and
   logs `Job <name>: cluster Replace: asking the current slot holder (<node>)
   to yield; the launch is re-attempted when the slot frees`. Fence targeting
   makes a stale request inert: a takeover always bumps the fence.
2. The holder's renew task observes the cancel within about a third of the
   slot TTL and logs `Job <name>: node <host> requested this instance be
   replaced (concurrencyPolicy: Replace, concurrencyScope: cluster);
   cancelling`. Its instances are then marked replaced (the same
   not-a-failure treatment as a local `Replace`: no reports, no retries) and
   cancelled in the background, and the finish path releases the slot. The
   holder keeps renewing the lease while the old instance drains (up to
   `killTimeout`), so the slot frees on release rather than by expiry. A
   drain longer than about twice `slotTtlSeconds` outlasts the pursuit,
   which then skips the launch (no-run over double-run), so raise
   `slotTtlSeconds` for jobs with a large `killTimeout`.
3. The requester waits in a **background pursuit task**, never inline on the
   scheduler pass: waiting a holder out takes up to two slot TTLs and would
   stall every other due job. When the slot frees, by release or TTL expiry,
   it re-attempts the launch through every normal gate and logs
   `Job <name>: launched after the previous cluster slot holder yielded
   (concurrencyPolicy: Replace)` on success.
4. The pursuit is bounded at **twice the slot TTL**. If the holder never
   yields, the requester abandons this launch: `Job <name>: the foreign
   holder (<node>) did not yield its cluster concurrency slot within <N>s;
   skipping this launch (no-run over double-run)`.

### When the store cannot answer

A store that is down, unresponsive, or whose denied claim cannot even be
confirmed by a follow-up read leaves the gate unanswerable, and
[`state.onStoreUnavailable`](Durable-State#when-the-store-is-unavailable-onstoreunavailable)
decides:

- `degrade` (the default) launches anyway, enforcing `concurrencyPolicy` on
  this node only for that run: `Job <name>: cannot claim its cluster
  concurrency slot (...); enforcing concurrencyPolicy on this node only for
  this run (onStoreUnavailable: degrade)`.
- `fail-closed` skips the launch: `Job <name> skipped: cannot claim its
  cluster concurrency slot (...) and onStoreUnavailable is fail-closed`.

A lock-fidelity probe, run once per backend and latched, catches a store
whose file locks are demonstrably no-ops (some FUSE filesystems grant two
exclusive locks on one file). That store's claims are then treated per
`onStoreUnavailable`, with an error logged once: `state: the store's file
locks cannot be trusted for cluster-wide concurrency (...);
concurrencyScope: cluster claims degrade per onStoreUnavailable`.

### What the cluster gate does and does not guarantee

The contract is **at-least-once**, not exactly-once. The gate closes the
routine overlap windows, but these remain open by design:

- **A holder that loses its slot keeps running.** The daemon never cancels
  work over a store blip. If a store outage outlasts the slot TTL and
  another node takes the slot over, the original holder logs `Job <name>:
  its cluster concurrency slot was taken over by <node> while it is still
  running here (a store outage outlasted the slot TTL?); the run continues
  -- the overlap is the documented at-least-once trade`. A `Forbid` peer
  that then wins the slot overlaps the still-running original.
- **`degrade` trades the gate for availability.** While the store cannot
  answer, only node-local enforcement applies to launches made under
  `onStoreUnavailable: degrade`.
- **Windows locks are same-host only.** On Windows the store's file locks
  have no cross-host reach, so a cluster slot claim there only fences
  daemons on the same host (`topology: auto` resolves to `single-node` on
  Windows and logs an advisory; see
  [one backend, two topologies](Durable-State#one-backend-two-topologies)).
- **Replace can skip the launch.** The pursuit abandons the launch after
  twice the slot TTL rather than risk a double run: the bias is no-run over
  double-run.
- **Slot expiry compares wall clocks across hosts**, so the shared-mount
  clock discipline in [durable state](Durable-State) applies: run NTP on
  every node.

Winning a slot whose previous holder went silent also reconciles that
holder's interrupted run into the durable ledger as an `unknown` outcome. An
expired slot proves the holder stopped renewing, not that its process died.
See [durable state](Durable-State) for the in-flight records behind that.

## Execution timeout

`executionTimeout` bounds the wall-clock duration of a single run. It is unset
by default (`null`), meaning a run may take arbitrarily long.

### Deadline mechanism

When a run starts and `executionTimeout` is set, `RunningJob.start` records an
absolute deadline using a monotonic clock:

```
execution_deadline = time.perf_counter() + executionTimeout
```

The deadline uses `time.perf_counter()` rather than wall-clock time, so it is
immune to system clock adjustments while the job runs.

When the run is awaited (`RunningJob.wait`):

- If no deadline is set, cronstable waits indefinitely for the process to exit.
- If a deadline is set, the remaining time is
  `execution_deadline - time.perf_counter()`. If that remaining time is `> 0`,
  cronstable awaits the process exit under `asyncio.wait_for(..., timeout)`.
  If it is already `<= 0`, the timeout path runs immediately.

On timeout (the remaining time elapses, or was non-positive), cronstable:

1. Logs `Job <name> exceeded its executionTimeout of <N> seconds, cancelling
   it...`.
2. Sets the run's return code to `-100`.
3. Calls `cancel()` to terminate the run and its process group (see the next
   section).

A `-100` return code is therefore the marker of a timeout-induced termination.
For a normal (non-replaced) run, `retcode = -100` is non-zero, so a job with
the default `failsWhen.nonzeroReturn` treats the timeout as a failure, which is
then reported and may be retried. See
[failure detection and retries](Failure-Detection-and-Retries) for what happens
after a timeout-induced failure. When the timed-out run was a `Replace`
victim, the `replaced` flag suppresses failure handling regardless of the
`-100` code.

```yaml
jobs:
  - name: maybe-hangs
    command: |
      echo "starting..."
      sleep 2
      echo "all done."
    schedule:
      minute: "*"
    captureStderr: true
    executionTimeout: 1   # seconds; cancel the run if still alive after 1s
```

## Cancellation and killTimeout

Both an `executionTimeout` expiry and `concurrencyPolicy: Replace` invoke
cancellation (`RunningJob.cancel`). It terminates the run and everything the
run spawned. On POSIX each job is started in a fresh session
(`start_new_session`), so the job and every descendant share one process
group (the child's own pid), which cancellation then signals as a unit
(`os.killpg` in `cronstable/platform.py`). Windows has no equivalent at spawn
time, so descendants are reached through the live process tree instead
(`taskkill`, described later). The sequence is:

1. Send SIGTERM to the whole process group. This reaches the descendants even
   when the process cronstable spawned has already exited. If the group cannot
   be signaled (it is already empty, or `killpg` failed), fall back to
   SIGTERM on the direct child with `proc.terminate()`. A `ProcessLookupError`
   (process already gone) is ignored.
2. Wait up to `killTimeout` seconds for the direct child to exit, using
   `asyncio.wait_for(proc.wait(), killTimeout)`. If it has not exited by then,
   log `Job <name> did not gracefully terminate after <N> seconds, killing
   it...`.
3. Send SIGKILL to the whole process group, **unconditionally**, whether or
   not the direct child exited within `killTimeout`. The child exiting says
   nothing about descendants sharing its group, and those are what hold the
   job's stdout/stderr pipes open. A group that is already empty makes this a
   no-op. Where the group cannot be signaled, fall back to `proc.kill()` on
   the direct child.

Signaling the group is what makes `executionTimeout` a bound on the run's
*work* rather than on its root process alone. A command like
`sh -c 'helper & main'` leaves `helper` behind holding the write-ends of the
job's stdout/stderr pipes. Killing only the shell would leave the pipes open,
so the run would never finish draining, its slot would never be released, and
under `concurrencyPolicy: Forbid` the job would never run again. Killing the
group takes the helper down with the shell.

On Windows each job is likewise spawned in its own process group
(`CREATE_NEW_PROCESS_GROUP`), and the steps map onto the platform's own
primitives. Step 1 delivers `CTRL_BREAK_EVENT` to the group, a trappable
request the job can handle (`signal.SIGBREAK` in Python,
`SetConsoleCtrlHandler` natively) to flush and exit. Step 3 shells out to
`taskkill /F /T`, which force-kills the live parent/child process tree (the
`taskkill` run itself is bounded at 10 seconds; on failure the fallback is
`proc.kill()` on the direct child).

One degradation: delivering a break needs a console shared with the daemon,
so where there is none (a service context) step 1 becomes the tree kill
immediately and no graceful signal reaches the job. See
[running on Windows](Running-on-Windows).

`killTimeout` defaults to `30` seconds and must be `>= 0`. A value of `0` is
valid and means the group SIGKILL follows almost immediately after the group
SIGTERM (the `asyncio.wait_for` with a zero timeout gives the process
essentially no grace period).

`killTimeout` gives a job time to flush buffers and clean up after the
graceful signal. Raise it for jobs that need longer to shut down, and lower
it for jobs that may ignore the graceful signal and must be force-killed
quickly. The guidance applies on both platforms (the graceful signal is
SIGTERM on POSIX and `CTRL_BREAK_EVENT` on Windows), with one Windows caveat:
a daemon without a console cannot deliver the break, and there `killTimeout`
has nothing to bound because the tree kill runs at once. See
[running on Windows](Running-on-Windows).

As defense in depth, a descendant that escaped the group entirely (it called
`setsid` itself, or on Windows it was already orphaned when `taskkill` walked
the tree) cannot strand the run either. A killed run's wait for its
stdout/stderr streams to reach EOF is bounded at 30 seconds, a fixed constant
deliberately independent of `killTimeout`, which is legitimately `0` for jobs
that must be force-killed at once but whose already-captured output should
not be discarded.

When that bound expires the readers are cancelled, the output captured so far
is kept, cronstable closes its end of the job's pipes, and the run leaves the
running set, at the cost only of output the escaped descendant would have
produced afterwards.

```yaml
jobs:
  - name: ignores-sigterm
    command: |
      trap "echo '(ignoring SIGTERM)'" TERM
      echo "starting..."
      sleep 10
      echo "all done."
    schedule:
      minute: "*"
    captureStderr: true
    executionTimeout: 1
    killTimeout: 0.5   # SIGKILL 0.5s after the (ignored) SIGTERM
```

This example's trap spelling is POSIX (`sh` trapping SIGTERM). The Windows
equivalent traps `SIGBREAK`: the graceful step there delivers
`CTRL_BREAK_EVENT` to the job's process group, and the escalation timing
`killTimeout` illustrates is the same (the forced step is the
`taskkill /F /T` tree kill). See [running on Windows](Running-on-Windows),
including the no-console case where the graceful step cannot be delivered.

## Scope and interaction

- **Per run.** `executionTimeout` and `killTimeout` apply to a single instance
  of a job. The deadline is established at that instance's `start` and is not
  shared across instances. With `concurrencyPolicy: Allow`, each concurrent
  instance has its own independent deadline.
- **Replace + timeout.** A `Replace` victim is cancelled regardless of its own
  `executionTimeout`. `killTimeout` governs its termination, and that
  termination is not reported as a failure (the `replaced` flag).
- **Manual starts.** Launches through the [HTTP control API](HTTP-API)
  (`POST /jobs/{name}/start`) take the same `maybe_launch_job` path and so
  honor `concurrencyPolicy`, including the cluster slot gate for
  `concurrencyScope: cluster` jobs.
- **Node first, cluster second.** `concurrencyScope: cluster` never changes
  the per-node behavior documented earlier. The local check runs first, and
  the [cluster gate](#concurrency-across-a-cluster) is an additional gate
  behind it.
- **startup failures vs. timeouts.** A `-100` return code specifically denotes
  a timeout-induced cancellation. A command that could not be launched at all
  (not found, for example) is assigned `127` instead, on the normal failure
  path. See [commands and environment](Commands-and-Environment) and
  [failure detection and retries](Failure-Detection-and-Retries).
