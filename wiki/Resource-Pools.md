# Resource pools

A resource pool limits the combined concurrency of jobs and DAG tasks that use
the same database, API, or other shared resource. Waiting work lives in the
state store, so another eligible daemon can dispatch it after a restart.

```yaml
state:
  path: ./state
pools:
  warehouse:
    slots: 4
    maxQueued: 100
jobs:
  - name: export
    command: python export.py
    schedule: "0 * * * *"
    pool: warehouse
    poolSlots: 2
    queuePriority: 10
    queueTimeout: 1800
```

This export occupies two of the warehouse's four slots until its command and
any [result verification](Result-Verification) finish. Jobs and executable DAG
tasks accept the same pool options. Approval gates do not occupy slots.

| Option | Default | Meaning |
| --- | --- | --- |
| `pools.<name>.slots` | Required | Total capacity, from 1 to 10000. |
| `pools.<name>.maxQueued` | `1000` | Waiting-work limit, from 1 to 10000. |
| `pool` | Unset | Pool that admits this job or task. |
| `poolSlots` | `1` | Capacity consumed by one running instance. Must fit the pool. |
| `queuePriority` | `0` | Higher integers run first. |
| `queueTimeout` | `3600` | Maximum wait in seconds; a finite positive number. |

Within a priority, admission follows enqueue time, then queue ID. The head
waits until all its slots are available. Lower-priority work also waits behind
a paused or otherwise ineligible head. Separate pools suit workloads that
need independent progress.

`EveryNode` jobs and manual starts remain assigned to the daemon that queued
them. Other daemons cannot claim these entries, even with spare capacity.
They remain waiting if that node is unavailable, until it returns or their
queue deadline expires. Leader-owned work can move to another eligible daemon.

`concurrencyPolicy` still applies when work reaches the front. `Forbid` keeps
the entry waiting for the previous instance. A DAG task waiting for capacity
keeps its current attempt number. An expired or cancelled task entry becomes
a task failure and follows the DAG's retry policy. The queue retains that
decision until the task scheduler observes it; these entries count against
the backlog limit. Recent terminal entries appear in the pool view.
If the workflow or task has already ended, the dispatcher retires its waiting
entry and releases its backlog reservation, including timed-out sensors.

A full queue or a capacity change rejects new scheduled work with a warning;
the scheduler continues servicing other jobs. Pending retries wait for admission
without consuming another attempt. A newer scheduled fire, success, or retry
cancellation also invalidates queued retries in the state store. Catch-up waits
for each of its own queued runs to finish before admitting the next or closing
its checkpoint; shutdown leaves unfinished work resumable.

Pool state cannot be read or written when the store raises an I/O error, a
pool operation exceeds its 5-second bound, a pool document cannot be read
(an I/O error on the file, a damaged body, or an unknown schema version), or
the configured store has not started. A scheduled fire, or a deferred
`@reboot` run, that arrives in that state is dropped with the same warning,
`Job <name> could not enter pool <pool>: <reason>`, and the scheduler
continues servicing other jobs. The fire also logs
`pool <pool>: retry settlement deferred` for each pool whose retry
settlement it could not write. The next enqueue, retry check, or queue
service pass that reaches the pool applies that settlement. A failed run and
a due retry in that state are covered under
[retry lifecycle](Failure-Detection-and-Retries#retry-lifecycle).

The dashboard's **Resource pools** card shows capacity, priorities, deadlines,
and cancellation controls. `GET /pools` exposes the same data.
`POST /pools/{name}/queue/{key}/cancel` cancels waiting work. A manual start of
a pooled job returns HTTP `202` with `queued`, `queueId`, and `pool`. While
pool state cannot be read or written, these three requests answer `503` with
`pool state is unavailable`. When a pool refuses a start or a queue cancel,
the request answers `409` with the pool's own message.
[Enabling the API](HTTP-API#enabling-the-api) lists the cases for both
answers.

All participating daemons must share the state store and pool configuration.
Pool admission requires reliable exclusive filesystem locks and stops when
the store cannot coordinate admission. A running claim has a renewable
30-second lease. Losing renewal cancels its process; an expired claim can be
admitted again before its queue deadline. Commands must tolerate replay after
a crash or loss of coordination.

The lease also bounds a completion that the daemon cannot write to the
store. The daemon keeps that completion and writes it again on its next
passes, and it stops renewing the claim's lease. A write that lands before
the lease lapses records the completion. Once the lease has lapsed, 30
seconds after its last renewal, the entry returns to the queue with no
completion recorded, and a job that has already finished can be admitted and
run again. An entry whose queue deadline has passed by then expires and
does not run.

Changing capacity lets existing work drain at the stored capacity before
admission switches to the configured capacity. New enqueue requests fail
while the old queue drains; tasks already queued keep their admission and
attempt number. A changed job definition, disabled job, or removed
job cancels its waiting entries when the dispatcher observes it. Pool names
must be unique across included files and configuration directories.
