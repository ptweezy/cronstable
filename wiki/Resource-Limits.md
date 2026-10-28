# Resource limits

Set `resourceLimits` on a job to cap what each of its runs can use: CPU, memory, and the number of processes. The kernel enforces the caps. Each capped run gets a cgroup of its own on Linux (cgroup v2) and a Job Object of its own on Windows, so the caps cover the run's whole process tree, including children that leave its process group.

```yaml
jobs:
  - name: nightly-model-refresh
    command: python -m models.refresh
    schedule: "0 4 * * *"
    resourceLimits:
      cpu: 1.5        # CPUs, like docker run --cpus
      memory: 512M    # powers of 1024: 512 MiB
      processes: 64   # processes (and threads, on Linux)
```

A run at its CPU cap runs more slowly. A run that goes over its memory cap is killed with every process it started, and fails with `memory limit exceeded (512 MiB)`. A run at its process cap cannot start more processes.

If the host cannot enforce a cap, the run goes ahead without it. The daemon logs one warning that names the affected jobs, and the run record lists the caps that were not enforced and why. With `required: true`, those runs fail instead.

| Platform | Mechanism | What it needs |
| --- | --- | --- |
| Linux | cgroup v2 | A cgroup delegated to cronstable: `Delegate=` on its systemd unit (the packaged unit sets `Delegate=memory pids`, and CPU caps need `Delegate=yes`), or root in a container with a writable cgroup mount and either a private cgroup namespace or a container cgroup that the runtime delegated. See [Linux setup](#linux-setup). |
| Windows | Job Objects | No setup and no elevation. See [Windows behavior](#windows-behavior). |
| macOS, FreeBSD, OpenBSD, NetBSD, illumos | none | The host cannot enforce caps. Capped jobs run without them, and runs of jobs with `required: true` fail. |

Caps are off by default, and the daemon changes no cgroups until a configuration has a capped job. A job without `resourceLimits` gets no Job Object on Windows and no cgroup on Linux, except while cronstable's cpu controller is on. That takes a job with a `cpu` cap and a delegated cpu controller, as with `Delegate=yes`; the packaged unit does not delegate it. While the controller is on, every Linux run gets a cgroup that carries its `priority` as a CPU weight (see [CPU caps and priority](#cpu-caps-and-priority)).

**On this page:**
[Setting caps](#setting-caps) ·
[What each cap does](#what-each-cap-does) ·
[What happens at a cap](#what-happens-at-a-cap) ·
[When caps cannot apply](#when-caps-cannot-apply) ·
[Requiring caps](#requiring-caps) ·
[Linux setup](#linux-setup) ·
[Windows behavior](#windows-behavior) ·
[Cancellation and leftover processes](#cancellation-and-leftover-processes) ·
[With resource monitoring](#with-resource-monitoring) ·
[Verify checks and DAG tasks](#verify-checks-and-dag-tasks) ·
[The run record](#the-run-record) ·
[Where the numbers surface](#where-the-numbers-surface) ·
[Security](#security) ·
[Troubleshooting](#troubleshooting)

## Setting caps

`resourceLimits` is a per-job option. To cap every job in a file, set it under a [`defaults:` block](Includes-and-Defaults). [DAG tasks](Orchestration-and-DAGs) accept it too (see [Verify checks and DAG tasks](#verify-checks-and-dag-tasks)).

### Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `cpu` | number or `max` | none | The most CPU the run can use, in CPUs. `1.5` is one and a half CPUs' worth of CPU time, on any number of cores. From `0.01` to `1000000`. |
| `memory` | size or `max` | none | The most memory the run's processes can hold together, as the kernel counts it. A byte count or a size such as `512M`. At least 1 MiB and below 8 EiB. |
| `processes` | int or `max` | none | The most processes the run can have at once. On Linux, threads count too. From `1` to `4194304`. |
| `required` | bool | `false` | Fail a run whose caps the host cannot enforce, instead of running it uncapped. See [Requiring caps](#requiring-caps). |

Set only the caps you need; the others stay unlimited.

### Values

- `cpu` is a number in Python `float()` syntax with ASCII digits, such as `0.5` or `2`.
- `memory` is a whole number of bytes, or a number followed by a unit: `B`, `K`, `M`, `G`, or `T`. `K`, `M`, `G`, and `T` can be followed by `i`, `B`, or `iB`, so `512M`, `512Mi`, `512MB`, and `512MiB` are the same size. Every unit is a power of 1024, as with Docker's `--memory`: `1K` is 1024 bytes. Units are case-insensitive. The number can have a decimal part (`1.5G`), `_` between digits, a leading `+`, and a space before the unit. A fractional byte count is truncated toward zero.
- `processes` is a whole number.
- `max`, in any letter case, lifts a cap. A job uses it to remove a cap that it inherits from `defaults:`.
- A cap can come from [environment-variable interpolation](Environment-Variable-Interpolation), such as `memory: ${JOB_MEMORY}`. An interpolated cap follows the same rules as a literal. `required` is a boolean: it takes `true` or `false`, and a `${VAR}` there is a schema error.
- A map that sets no cap, or sets every cap to `max`, gives the job no caps, even with `required: true`.

A value that cannot be read, or that is out of range, is a `ConfigError` at load time that names the job and the key:

```text
Job nightly-model-refresh: resourceLimits.memory: cannot read '512X' as a size; use a byte count or a number with a K, M, G, or T suffix (powers of 1024), at least 1 MiB and below 8 EiB, such as 512M, or max
```

The `cpu` and `processes` messages follow the same pattern (`cannot read 'two' as a CPU count; ...`, `cannot read 'many' as a process count; ...`). The range errors read:

- `resourceLimits.cpu must be a finite number between 0.01 and 1000000 (CPUs)`
- `resourceLimits.memory must be at least 1 MiB and below 8 EiB`
- `resourceLimits.processes must be between 1 and 4194304`

A cap with no value, such as a bare `cpu:`, is refused with `cannot read ''`. A bare `required:` and a bare `resourceLimits:` with no map are schema errors.

### Inheriting and lifting caps

Under `defaults:`, `resourceLimits` merges key by key with the job's own map. A job can change one cap and keep the others, or lift an inherited cap with `max`:

```yaml
defaults:
  resourceLimits:
    cpu: 1
    memory: 1G

jobs:
  - name: report-render
    command: render-reports
    schedule: "*/30 * * * *"
    resourceLimits:
      memory: 4G      # cpu: 1 still applies
  - name: warehouse-load
    command: load-warehouse
    schedule: "0 2 * * *"
    resourceLimits:
      cpu: max        # no CPU cap; memory: 1G still applies
```

A bare `resourceLimits:` cannot clear an inherited map, so set each cap that you want to lift to `max`.

`resourceLimits` is part of the [job-set ID](Job-Set-ID) only when the job has caps. The ID uses the normalized values, so `memory: 512M` and `memory: 536870912` match.

## What each cap does

| Cap | Linux (cgroup v2) | Windows (Job Object) |
| --- | --- | --- |
| `cpu` | `cpu.max`, a quota of `cpu` × 100 ms of CPU time per 100 ms period (1 ms at least). The run's processes share that quota on any cores, and the kernel throttles them for the rest of each period. | A hard CPU rate cap of `cpu` divided by the machine's logical CPUs (`os.cpu_count()`), in steps of 1/10000 of the machine. A cap at or above the number of logical CPUs sets no rate. |
| `memory` | `memory.max`, rounded down to the page size. It counts the run's anonymous memory, its page cache, kernel memory charged to it, and its socket buffers. cronstable sets `memory.swap.max` to `0` where swap accounting is on, so swap cannot stretch the cap, and `memory.oom.group` to `1`, so an OOM kill ends every process in the run. | `JobMemoryLimit`: the commit charge (private committed memory) of all the run's processes together. Commit charge is neither the working set nor RSS. It includes memory backed by the page file, so paging cannot stretch the cap. |
| `processes` | `pids.max`. It counts tasks, so every thread counts, and so does the job's shell. | `ActiveProcessLimit`. It counts processes, not threads. cmd.exe counts for a string `command`, and a venv's `python.exe` launcher counts as one process and the interpreter that it starts as another. A conhost.exe that Windows starts in the Job Object does not count. |

Some Linux behavior depends on the kernel version:

- The CPU cap needs Linux 4.15 or later, the first version whose cgroup v2 has the cpu controller.
- The memory cap needs `memory.oom.group` (Linux 4.19). On an older kernel, the run's memory cap is recorded as not enforced.
- A forced kill writes `cgroup.kill` (Linux 5.14). On an older kernel, cronstable sends `SIGKILL` to each process in the cgroup instead. Either way it also sends `SIGKILL` to the process group.
- The record's `peak_memory_bytes` comes from `memory.peak` (Linux 5.19). On an older kernel it is `null`.
- Before Linux 6.11, and on a later kernel whose cgroup2 mount has `pids_localevents`, the kernel's count behind `process_limit_hits` also takes in forks that a limit above the run refused, such as the unit's `TasksMax`. There, a run whose `pids.peak` (Linux 6.1) stayed below its cap records `0`.
- A 32-bit kernel accepts a `pids.max` of at most 32768, so there a larger process cap is recorded as not enforced.

## What happens at a cap

### CPU

A run at its CPU cap keeps running more slowly and does not fail for it. On Linux the run record's `cpu_throttled_seconds` says how long the kernel held the run back.

### Memory

A run that goes over its memory cap ends with every process in it:

- On Linux, the kernel first reclaims what it can, such as page cache. When that is not enough, the OOM killer ends every process in the run's cgroup together. The job's own process exits on `SIGKILL`, so the run's exit code is `-9`.
- On Windows, an allocation that would take the commit charge past the cap fails; in Python it raises `MemoryError`. cronstable then terminates every process in the Job Object with exit code `1`, the code that `taskkill /F` leaves. The kill follows the Job Object's memory-limit message. Windows does not guarantee that message, so a watcher thread also checks each memory-capped job's peak commit about once a second and ends a job whose peak passed its cap.

The run fails with `memory limit exceeded (512 MiB)`, with the cap written in binary units, whatever `failsWhen` says. Its record has `memory_exceeded: true`. The job's `onFailure` reporters and retry policy run as for any failed run.

On Linux, `memory_exceeded` counts only kills that the run's own cap caused. A run ended by a host-wide OOM kill or by a parent cgroup's limit is not marked; with the default `failsWhen`, it fails on its exit code.

### Processes

At its process cap, the run cannot start another process, or another thread on Linux. On Linux `fork` and `clone` fail with `EAGAIN`, and on Windows process creation fails with error 1816 (`ERROR_NOT_ENOUGH_QUOTA`). Nothing is killed, so how the run handles the refusal is up to the job. The record's `process_limit_hits` counts how often the run hit the cap.

## When caps cannot apply

### What the daemon logs

On macOS, the BSDs, and illumos, each config load logs one warning:

```text
resourceLimits is set (job nightly-model-refresh, dag etl task load), but this platform has neither cgroup v2 nor Job Objects, so those jobs run without caps; runs of jobs with resourceLimits.required fail
```

On every platform, the daemon checks what the host can enforce when it applies a configuration with capped jobs, at startup and on each reload. On Linux this is also when it sets up its [cgroup layout](#the-cgroup-layout), before that pass launches any job. It logs the result once per change of what the host can enforce or of the set of capped jobs:

- When every cap applies, at `INFO`: `resource limits are enforced with cgroup v2` (or `with Windows Job Objects`).
- When none can apply, a `WARNING` with the reason and what to do:

  ```text
  resourceLimits is set (job nightly-model-refresh, dag etl task load), but cronstable cannot enforce it: cgroup /sys/fs/cgroup/system.slice/cronstable.service is not delegated to cronstable (/sys/fs/cgroup/system.slice/cronstable.service is not writable). Run cronstable in a delegated cgroup: Delegate= on its systemd unit (the packaged unit sets Delegate=memory pids), or a container with a writable cgroup namespace. Those jobs run without caps.
  ```

  When some of those jobs set `required`, the warning ends with `Runs of job nightly-model-refresh fail until the caps can be enforced (resourceLimits.required).`
- When only some caps can apply, a `WARNING` that names the missing caps and only the jobs that set them, for example `... cannot enforce the memory cap: cgroup <path> does not offer the memory controller. Delegate the memory and pids controllers to cronstable's cgroup. Those jobs run without that cap.` Under the packaged unit, a `cpu` cap gets `... cannot enforce the cpu cap: the cpu controller is not delegated to cronstable. To cap CPU, run systemctl edit cronstable and add Delegate=yes under [Service]; that makes cronstable a CPU group of its own among the system's services. Those jobs run without that cap.` (see [CPU caps and priority](#cpu-caps-and-priority)). In a cgroup that is not a systemd service, such as a user scope, the hint reads `Delegate the cpu controller to cronstable's cgroup, for example with Delegate=yes on its systemd unit`.

A failed Linux setup runs again at the next config apply. The start of a capped run also retries it, at most every 30 seconds. A retry that changes what the host can enforce updates `GET /node` and logs the result.

Runs that go ahead without their caps are not logged one by one. An unexpected per-run failure, on a host that otherwise enforces caps, logs one `WARNING` for each distinct reason:

```text
resource limits could not be applied to a run of nightly-model-refresh: <reason> (each distinct failure is logged once)
```

### What the run records

A run whose caps did not all apply has `enforced: false` in its [`limits` record](#the-run-record), with the missing caps in `unenforced` and the cause in `reason`. The dashboards mark it (see [Where the numbers surface](#where-the-numbers-surface)).

[`GET /node`](HTTP-API#get-node) reports what the host can enforce in its `limits` object: `available`, `mechanism` (`cgroup-v2`, `job-object`, or `null`), `resources` (the caps it can apply), `reason`, and `hint`. Before a configuration has capped jobs, this is a read-only check.

## Requiring caps

```yaml
resourceLimits:
  memory: 2G
  required: true
```

With `required: true`, a run whose configured caps cannot all be enforced fails without starting, instead of going ahead uncapped. Its failure reason is:

```text
resource limits could not be enforced: <reason> (resourceLimits.required is set)
```

A run refused before it starts is a start failure. Its exit code is `127`, it counts in `cronstable_job_start_failures_total`, and a `verify` check is recorded as skipped (`command did not start`). The job's reporters and retry policy run as for any failed run.

A required run also fails with this reason when its process cannot be moved into its cgroup or Job Object:

- On Linux, the process joins its cgroup before the command starts. A process that cannot join never runs the command, and the run is a start failure as above.
- On Windows, cronstable ends the process. The run reports exit code `1` instead of `127`, it is not a start failure, and a `verify` check is recorded as skipped (`command failed`).

cronstable also ends a Linux process that joins its cgroup but leaves it before cronstable confirms the join. That run reports exit code `-9`, it is not a start failure, and a `verify` check is recorded as skipped (`command failed`). The daemon logs each of these failures as it logs any unexpected per-run failure (see [What the daemon logs](#what-the-daemon-logs)).

## Linux setup

### What cronstable needs

- cgroup v2 mounted at `/sys/fs/cgroup`. A host on cgroup v1 or the hybrid layout gets `no cgroup v2 hierarchy is mounted at /sys/fs/cgroup` and the hint `Boot the host with the unified cgroup v2 hierarchy (systemd.unified_cgroup_hierarchy=1)`.
- A delegated cgroup. cronstable reads its own cgroup from `/proc/self/cgroup`. That cgroup's directory, `cgroup.procs`, and `cgroup.subtree_control` must be writable by cronstable.
  - For a non-root daemon, that ownership is the delegation. systemd chowns these files, among others, for a unit with `Delegate=` and `User=`. A systemd user manager gives its user every cgroup it creates, delegated or not, so a non-root daemon treats any cgroup its user owns as delegated, including the scope of a desktop terminal. Setup then moves every process in that cgroup into `daemon/`, the terminal's own processes included. Start the daemon in a scope of its own with `systemd-run --user --scope` (see [Custom units](#custom-units)).
  - A root daemon also needs a `trusted.delegate` or `user.delegate` extended attribute of `1` on the cgroup, or the cgroup must be the root of a cgroup namespace, as in a container. Only the system manager sets these attributes, on the cgroup of a unit with `Delegate=`: `trusted.delegate` from systemd 244, and `user.delegate` from systemd 251. A scope that a user manager starts (`systemd-run --user`) gets neither. On systemd before 244, run the daemon as a non-root `User=` with `Delegate=` instead.
  - The host's root cgroup is never used (`cronstable runs in the root cgroup`).
- The memory and pids controllers in that cgroup's `cgroup.controllers`, and the cpu controller for CPU caps. A missing controller makes its cap unenforceable, and the other caps still apply.

### The packaged systemd unit

The `.deb` and `.rpm` packages install `cronstable.service` with these lines:

```ini
KillMode=mixed
Delegate=memory pids
OOMPolicy=continue
```

- `Delegate=memory pids` hands the unit's cgroup to cronstable, owned by the `cronstable` user, with the memory and pids controllers. Memory and process caps work, and cronstable and its jobs share CPU with the other system services by nice value. CPU caps need `Delegate=yes` (see [CPU caps and priority](#cpu-caps-and-priority)). systemd before 236 reads `Delegate=` only as `yes` or `no`: it logs a parse error for the list and leaves the unit undelegated.
- `OOMPolicy=continue` keeps the service running when the kernel kills a run at its memory cap. systemd 243 and later already give a unit with `Delegate=` this policy, so the line states it for readers, and it still holds if a drop-in changes `Delegate=`. systemd before 243 does not act on OOM kills; it logs a warning for the unknown key and ignores the line.
- `KillMode=mixed` sends the stop signal to cronstable alone, so it can drain its runs.

On systemd before 240, `systemctl reload cronstable` fails with `Failed at step CGROUP` and `Device or resource busy` once cronstable has set up its [cgroup layout](#the-cgroup-layout). Those versions start `ExecReload=` in the unit's own cgroup, and the kernel refuses to add a process to a cgroup that enables controllers for its children, as cronstable's cgroup does from then until the service restarts. The RHEL 8 package has the fix from systemd-239-78 (RHEL 8.9). On an affected host, reload with `systemctl kill --kill-who=main -s HUP cronstable`; plain `systemctl kill -s HUP cronstable` also signals every running job. Config edits load on their own at the next housekeeping pass, about once a minute.

### Custom units

For a unit of your own, add both lines, for example in a drop-in:

```ini
# /etc/systemd/system/cronstable.service.d/limits.conf
[Service]
Delegate=memory pids
OOMPolicy=continue
```

Then run `systemctl daemon-reload` and restart the service. Use `Delegate=yes` instead for CPU caps, and on systemd before 236, which reads `Delegate=` only as a boolean. With `User=`, systemd before 235 does not chown `cgroup.subtree_control`, so cronstable reports the caps as unenforceable there.

To try caps without root or a service, start cronstable in a delegated user scope:

```bash
systemd-run --user --scope -p Delegate=yes cronstable -c ./cronstable.yaml
```

From systemd 252, the user manager delegates the cpu, memory, and pids controllers by default.

### CPU caps and priority

Wherever the cpu controller is on, the kernel makes each child cgroup a CPU group of its own and shares contended CPU between the groups by their weights. A nice value, and so a job's [`priority`](Commands-and-Environment#priority), orders only the threads inside one group. The packaged unit leaves the cpu controller out, so cronstable and its jobs share CPU with the other system services by nice value.

Under the packaged unit, cronstable's cgroup has no cpu controller, unless another unit made systemd turn it on for all of the system's services. Without it, a job's `cpu` cap does not apply, and its memory and process caps still do. The warning gives the reason `the cpu controller is not delegated to cronstable`, and [`GET /node`](HTTP-API#get-node) lists only `memory` and `processes` in `limits.resources`, with the same reason and hint. To cap CPU, run `sudo systemctl edit cronstable`, add these lines, and restart the service:

```ini
[Service]
Delegate=yes
```

`Delegate=yes` makes systemd turn on the cpu controller for the system's services, so cronstable becomes a CPU group of its own among them and gets the share of contended CPU that any other service gets. `CPUWeight=` in the same drop-in changes that share. The io controller goes on there too, so cronstable is also an I/O group of its own among the services, and `IOWeight=` sets its I/O weight. `priority` then orders cronstable's jobs only among themselves: an `idle` job yields to cronstable's other jobs, but not to other services' work.

Where its cgroup offers the cpu controller, cronstable turns it on in that cgroup while some job or DAG task has a `cpu` cap. When a reload removes the last `cpu` cap, cronstable turns the controller off once no run cgroup holds a cpu cap, so a run that is still going keeps its cap to the end. A run with a cpu cap from an older configuration, such as the verify check of a run that started before the reload, turns the controller on for its own duration. While the controller is on:

- Every run gets a cgroup under `runs/`, uncapped runs included, with a `cpu.weight.nice` from its `priority`: `19` for `idle`, `10` for `below-normal`, `-5` for `above-normal`, `-10` for `high`, and the daemon's own nice for `normal`. The `daemon/` cgroup gets the daemon's own nice. A capped run that is already going when the controller turns on gets its weight then, and an uncapped one stays in `daemon/` until it ends. The weight follows the configured level even when the kernel refuses to renice the run to a raised one, and it orders the run only against cronstable's other runs, within cronstable's own share.
- The kernel shares CPU per run, not per thread: a run with four busy threads gets the same share as a one-thread run at the same priority. The run's nice value still orders the threads inside its cgroup.
- An uncapped run's cgroup holds only its weight. The run has no `limits` record, and its metrics, report templates, and log lines are those of an uncapped run. A forced cancel kills its whole cgroup, and the resource monitor reads its processes and CPU time from the cgroup. If that cgroup cannot be made or joined, the run starts in `daemon/`, and the daemon logs why at `DEBUG`, once per distinct reason.

While a cgroup below enables the cpu controller for its own children, the kernel refuses to turn it off, and cronstable tries again whenever a later run starts or ends, and at each config apply. If the kernel refuses to turn it on, for example on a kernel built with real-time group scheduling (`CONFIG_RT_GROUP_SCHED`), the `cpu` caps are recorded as not enforced with `could not enable the cpu controller in <path>: <error>`, and memory and process caps still apply.

On Windows, a CPU cap is a rate cap on the run's Job Object, which groups no other processes, so there `priority` orders a job against every process on the host.

### Containers

Per-run caps in a container need these:

- cronstable running as root in the container.
- A private cgroup namespace, which Docker and Podman use by default on cgroup v2 hosts, or a container cgroup that the runtime delegated through systemd, as runc's systemd cgroup driver does. A privileged Kubernetes pod shares the host's cgroup namespace, so it gets caps only in the second case; check `limits` on [`GET /node`](HTTP-API#get-node) (see [Resource limits in a container](Production-Deployment#resource-limits-in-a-container)).
- A writable cgroup mount. Docker mounts `/sys/fs/cgroup` read-only by default; `--security-opt writable-cgroups=true` (Docker Engine 28.0 and later) or `--privileged` makes it writable. Kubernetes pods also get a read-only mount by default.

The published images (`ghcr.io/ptweezy/cronstable` and `docker.io/ptweezy/cronstable`) run as uid 65534, and a default container has the read-only mount, so caps do not apply there: capped jobs run uncapped, and the daemon logs the warning once at startup. To bound resources there, set the container's own limits, such as `docker run --cpus --memory --pids-limit` or a pod's `resources.limits`. Those cover the daemon and all of its jobs together.

### The cgroup layout

```text
<cronstable's cgroup>/            memory and pids on for children, and cpu while cpu control is on
  daemon/                         cronstable and every process that was here
  runs/                           the same controllers for its children
    3f9c2a1b7d4e-nightly-model-refresh/   one cgroup per capped run, or per run while cpu is on
```

cgroup v2 lets a cgroup turn on controllers for its children only while it has no processes of its own. So the first time a configuration has capped jobs, or a capped run starts, cronstable creates `daemon/` and `runs/` and moves every process in its cgroup into `daemon/`: itself, a one-file binary's bootloader process, a container's init, and job processes left from earlier runs. It then turns on the memory and pids controllers. A process that cannot be moved, such as one that is exiting, makes setup retry for about 2 seconds and then fail with `process <pid> in it could not be moved (it may be exiting)`.

Each run with a cgroup gets `runs/<12 hex digits>-<job name>`, with characters outside `A-Z`, `a-z`, `0-9`, `.`, `_`, and `-` replaced by `_`, and the name cut to 48 characters. A job that moves itself into a cgroup below that one stays inside its run: its caps, counters, and the kill apply there too. Before Linux 6.11, and with `pids_localevents`, `process_limit_hits` does not count process-cap hits inside that cgroup. cronstable removes a run's cgroup after its last process exits. Once, at setup, it also removes the empty run cgroups that an earlier daemon left behind, and leaves any that still hold processes. cronstable stays in `daemon/` after a reload that removes every capped job.

A second cronstable that starts in the `daemon/` cgroup, for example through `docker exec` in a container, shares the first daemon's `daemon/` and `runs/`, and its cpu controller too. Either daemon's config apply can turn cpu control on or off for both, and a run with a cpu cap turns it on. When the other daemon turns it on, a daemon weighs its own runs in progress at its next config apply or cpu-capped run. Neither daemon turns it off while a run of either one holds a cpu cap, so a running cpu cap lasts to the end of its run. To give a second capped daemon its own cpu control, run it in a cgroup of its own.

[`GET /node`](HTTP-API#get-node) and the dashboard's node meter keep describing the whole cgroup, `daemon/` and `runs/` included.

## Windows behavior

- Each capped run gets an unnamed Job Object. cronstable starts the job's process suspended, puts it in the Job Object, and then resumes it, so every process the job starts is in the Job Object too. While cronstable runs, a process that it cannot resume is terminated, and the run records `cronstable could not resume the job's process: <reason>`.
- cronstable never lets the Job Object's processes break away (see [Security](#security) for how a job can still start processes outside it). From its creation until the job's process resumes, the Job Object ends its processes when its last handle closes, and cronstable clears that limit right after the resume. So a run that has started outlives a hard stop of the daemon, such as `taskkill /F` or a crash. It keeps its CPU and process caps, and allocations past its memory cap still fail, but no daemon ends it at the memory cap from then on. A hard stop after the process joined its Job Object and before it resumed ends that process with exit code `0`, and it never runs. A hard stop in the shorter window between the spawn and the join (a fraction of a millisecond, a few milliseconds for the first run, plus any wait for a free worker thread) leaves the process suspended: it never runs or exits. Task Manager lists it as Suspended, and you can end it there.
- If Windows refuses to clear that kill-on-close limit, the run keeps running with its caps and the limit on, so a hard stop of the daemon ends it. The daemon logs `Windows could not clear the kill-on-close limit of the Job Object of a run of <name>: <error>. The run keeps its caps, and it ends if cronstable exits first (each distinct failure is logged once)` at `WARNING`.
- Dynamic Fair Share Scheduling, which Remote Desktop Services turns on, blocks CPU rate caps with error 50, and the run's CPU cap is recorded as not enforced. If cronstable itself runs in a Job Object with a CPU rate cap, each run's rate is relative to that job's. A CPU cap at or above the number of logical CPUs sets no rate and counts as enforced.
- On a 32-bit build, a memory cap above 4294967295 bytes is recorded as not enforced. A run with no memory cap in force records `peak_memory_bytes` as `null` once its peak reaches 4294967295 bytes, the most that a 32-bit build can read.
- A run with no console to inherit, such as under the [Windows service](Windows-Service), gets a conhost.exe that Windows starts inside the run's Job Object. Its commit counts toward the memory cap, and a monitored run's process tree and RSS include it, but the process cap does not count it.
- One watcher thread receives every Job Object's messages. It wakes at least once a second and queries each running memory-capped job. If it stops on its own, cronstable logs one `ERROR`: `the Job Object watcher stopped: <reason>. Runs that start from now on get no memory cap until cronstable restarts.` From then on, [`GET /node`](HTTP-API#get-node) leaves `memory` out of `limits.resources` and gives that reason, and runs that start record their memory cap as not enforced (`the Job Object watcher stopped: waiting on its completion port failed (error N)`), so required runs with a memory cap fail. CPU and process caps still apply. Restarting cronstable restores memory caps.

## Cancellation and leftover processes

A cancellation or timeout of a capped run starts with the graceful step that every run gets: `SIGTERM` to the process group on Linux, `CTRL_BREAK_EVENT` on Windows. The forced step after `killTimeout` goes through the container. On Linux cronstable writes `cgroup.kill` and also sends `SIGKILL` to the process group, so the step reaches every process in the run's cgroup, including descendants that left the group with `setsid`, and a process that left the cgroup but kept the group. On Windows `TerminateJobObject` ends every process in the Job Object, including ones in a process group of their own (`CREATE_NEW_PROCESS_GROUP`), and they exit with code `1`. When the container kill cannot reach the job, for example because its process never joined the container, cronstable uses the process-group kill (`SIGKILL` to the group, or `taskkill /F /T`). See [cancellation and killTimeout](Concurrency-and-Timeouts#cancellation-and-killtimeout).

If a capped run is canceled while it is starting, for example when the daemon shuts down during catch-up, cronstable kills its process, through the container first, and releases the container. A DAG task canceled this way gives back its pool slot, and its DAG run claims the task again later (see [crash-resume and the fleet](Orchestration-and-DAGs#crash-resume-and-the-fleet)).

When a capped run's own process exits, background processes that it left behind stay in its cgroup or Job Object, still capped, until they exit. A leftover that later goes over the memory cap is killed with everything else in the container; the run's record is final by then. On Windows, that kill comes from the daemon that started the run: after that daemon exits, a leftover at the memory cap gets failed allocations and keeps running. On Linux, a leftover's CPU cap ends when cpu control turns off. cronstable removes a Linux run cgroup when the run ends. If leftover processes still hold the cgroup or the kernel has not released it yet, cronstable removes it once it is empty, either when a later cgroup-backed run starts or ends, or at setup when the next daemon starts. A Windows Job Object closes when its last process exits.

## With resource monitoring

When a job sets both `resourceLimits` and [`monitorResources`](Resource-Monitoring), the monitor reads the run's processes from its cgroup or Job Object instead of walking the process tree, and reads CPU totals from the kernel's accounting (`cpu.stat` on Linux, the Job Object's accounting on Windows). The CPU time is exact, including children that start and exit between two samples, and processes that left the process group still count. `max_rss_bytes` keeps its meaning: the sampled peak RSS. If a run's process could not be put in its container, the monitor walks the process tree instead.

The memory cap counts the kernel's figure: cgroup memory on Linux, which includes page cache, and commit charge on Windows. Neither is RSS. The **Resources** chart plots sampled RSS against the cap line, so the two can differ, and a run can be killed while its sampled RSS is below the cap. The record's `peak_memory_bytes` is the kernel's figure, or `null` where a 32-bit Windows build cannot read it (see [Windows behavior](#windows-behavior)).

## Verify checks and DAG tasks

A [verify check](Result-Verification) runs under its job's caps, in a container of its own. A run that hit its memory cap never runs its check: the verification is recorded as `skipped` (`command failed`), and the run fails with `memory limit exceeded (512 MiB)`. With `monitorResources` on, the run's resource figures end when its own process ends, before the check starts.

The run's `limits` record covers only the run's own processes, and the check has a record of its own: the `limits` object of the run's `verification` result. A check that goes over the memory cap fails the run with `verification failed: memory limit exceeded (512 MiB)`, and its record has `memory_exceeded: true`. The web dashboard marks that run with a red `check memory cap` chip and the terminal dashboard with a `check memory cap` marker. The run's own `memory_exceeded`, its `memory cap` chip, `cronstable_job_memory_limit_exceeded_total`, the statsd `memory_limit_exceeded` line, `CRONSTABLE_MEMORY_LIMIT_EXCEEDED`, and the Event Log's `memory_limit_exceeded=1` count only kills of the run itself.

[DAG tasks](Orchestration-and-DAGs) accept `resourceLimits` with the same rules and inherit it from `defaults:`. Each task instance gets its own container. A capped task instance's record in the durable `dag_run` document (`GET /dags/{name}/runs/{run_key}`) carries a `limits` object. The config-apply warnings name a task as `dag etl task load`. Task instances do not appear in the per-job Prometheus families.

An approval gate runs no process. A gate that sets `resourceLimits` is a `ConfigError` (`dag 'etl': task 'publish-gate': an approval gate runs no process, so it cannot use resourceLimits`), and a gate ignores caps inherited from `defaults:`.

## The run record

Every run of a capped job carries a `limits` object. Runs of uncapped jobs have none.

| Field | Type | Description |
| --- | --- | --- |
| `cpu` | number or `null` | The CPU cap, in CPUs. `null` when it is not set. |
| `memory_bytes` | int or `null` | The memory cap, in bytes. |
| `processes` | int or `null` | The process cap. |
| `required` | bool | The job's `resourceLimits.required`. |
| `enforced` | bool | `true` when every configured cap applied for the whole run. |
| `mechanism` | string or `null` | `cgroup-v2` or `job-object`; `null` when no cap applied. A cap lost partway through the run counts as not applied. |
| `unenforced` | list | The caps that did not apply: `cpu`, `memory`, or `processes`. |
| `reason` | string or `null` | Why those caps did not apply, with several reasons separated by semicolons. |
| `refused` | bool | `true` when `required` refused the run: it never started, or cronstable ended its process because the process could not be moved into its cgroup or Job Object. |
| `memory_exceeded` | bool | `true` when the run hit its memory cap and was killed. |
| `process_limit_hits` | int or `null` | How often the run hit its process cap. `null` without a process cap, or when cronstable cannot read the count. |
| `peak_memory_bytes` | int or `null` | The most memory the kernel counted for the run, the figure the memory cap is measured on: `memory.peak` on Linux, the job's peak commit charge on Windows. Reported whenever the run's process got into its cgroup or Job Object, with or without a memory cap. With a memory cap on Windows it is at most the cap: Windows counts a refused allocation in its peak, so cronstable counts a peak above the cap as a memory-cap hit and records the cap itself as the peak. |
| `cpu_throttled_seconds` | number or `null` | How long the kernel throttled the run at its CPU cap. Linux only, with a CPU cap. |

If cronstable cannot read a run's kernel counters at the end, the record still carries the caps and `enforced`, with `memory_exceeded: false` and `null` counters.

## Where the numbers surface

| Surface | What appears |
| --- | --- |
| [Web Dashboard](Web-Dashboard) | A `caps 1.5 CPU · 512 MiB · 64 procs` chip in the job drawer. In the **History** tab, a red `memory cap` chip for a memory kill, a red `check memory cap` chip for a verify check killed at its memory cap, and an amber `caps not enforced` chip for a run whose caps did not all apply. In the **Resources** tab, dashed cap lines on the CPU and memory charts. For a capped job without monitoring, the **Resources** tab lists recent capped runs with their enforcement, kernel peak, throttled time, and cap hits. |
| [Terminal Dashboard](Terminal-Dashboard) | A caps line such as `caps  1.5 CPU · 512 MiB · 64 procs (required)` in the **Resources** tab, a `limits` cell per run (`memory cap`, `caps off`, `kernel peak`, `throttled`, process cap hits), and a `memory cap`, `check memory cap`, or `caps off` marker in a run's **History** detail. |
| [HTTP API](HTTP-API) | The configured caps as `limits` on [`GET /jobs`](HTTP-API#get-jobs) and `GET /jobs/{name}`, present only for capped jobs. The run's `limits` record on `last_run` and on [`GET /jobs/{name}/runs`](HTTP-API#get-jobsnameruns) entries, and a verify check's record as `verification.limits`. On [`GET /jobs/{name}/resources`](HTTP-API#get-jobsnameresources), the caps plus every recent run with a `resources` or a `limits` record. The host's capability as `limits` on [`GET /node`](HTTP-API#get-node). |
| [Prometheus](Metrics-with-Prometheus#per-job) | For capped jobs: `cronstable_job_cpu_limit_cpus`, `cronstable_job_memory_limit_bytes`, and `cronstable_job_process_limit` (each when that cap is set), `cronstable_job_memory_limit_exceeded_total` (jobs with a memory cap; restart-durable with a `state:` store), and `cronstable_job_last_run_limits_enforced` (`1` or `0`). |
| [statsd](Metrics-with-Statsd#on-stop) | A `<prefix>.memory_limit_exceeded:1\|c` counter line on the stop datagram of a run killed at its memory cap. |
| [Reporting](Reporting) | The `limits` [template variable](Reporting#templating) (the run's record, or `None`) and `CRONSTABLE_LIMITS_ENFORCED` (`1`, `0`, or empty without caps) and `CRONSTABLE_MEMORY_LIMIT_EXCEEDED` (`1` or empty) in the [shell reporter's environment](Reporting#environment-variables). |
| [Windows Event Log](Windows-Event-Log#insertion-strings) | `limits_enforced=1\|0 memory_limit_exceeded=1\|0` at the end of a capped run's detail string. |
| [Durable State](Durable-State) | The run ledger record carries the `limits` object, so it survives restarts. |
| [MCP](MCP) | The caps on `cron_list_jobs` and `cron_get_job`, and the run's `limits` record on their `last_run`. The `limits` record on `cron_list_runs` entries and the `cronstable://jobs/{name}/runs` resource. Caps and `limits` records on `cron_get_job_resources`, the capability on `cron_get_node`, and the Prometheus families through `cron_query_metrics`. |

## Security

Caps bound how much a run uses; they are not a sandbox. On Linux, cronstable creates each run's cgroup as its own user, so a job that runs as that same user owns its cgroup files and can raise its caps or move its processes out of the cgroup. To keep a job from changing its caps, run it as a different user with [`user`](Configuration-Reference#privilege-switching), which needs a root daemon. On Windows, every job runs under cronstable's own account, so a job can open cronstable's process, duplicate cronstable's handle to the run's Job Object, and use it to lift the run's caps or to start processes outside the Job Object. A process that another service starts at the job's request, such as through WMI's `Win32_Process.Create` or Task Scheduler, also runs outside the Job Object with no caps. A forced cancel does not end a process outside the Job Object, and the run record does not show caps that a job lifted.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| The warning says `... is not writable` | The daemon's cgroup is not delegated. Add `Delegate=memory pids` or `Delegate=yes` to its systemd unit (see [Custom units](#custom-units)), or see [Containers](#containers). |
| The warning says `has no trusted.delegate or user.delegate attribute` | A root daemon's cgroup carries no delegation mark. Add `Delegate=` to its system unit. On systemd before 244, which sets no mark, run the daemon as a non-root `User=` (see [What cronstable needs](#what-cronstable-needs)). |
| The warning says `no cgroup v2 hierarchy is mounted at /sys/fs/cgroup` | The host runs cgroup v1 or the hybrid layout. Boot it with `systemd.unified_cgroup_hierarchy=1`. |
| The warning says `does not offer the memory controller` | The parent cgroup does not delegate that controller. Delegate the memory and pids controllers to cronstable's cgroup. |
| The warning says `the cpu controller is not delegated to cronstable` | The unit delegates only memory and pids, as the packaged unit does. To cap CPU, add `Delegate=yes` with `systemctl edit cronstable` (see [CPU caps and priority](#cpu-caps-and-priority)). |
| An `idle` job keeps its CPU share against other services | The unit has `Delegate=yes`, or another unit turned the cpu controller on for the system's services, so cronstable is a CPU group of its own, and `priority` orders a job only against cronstable's other jobs. `CPUWeight=` on the unit sets cronstable's share (see [CPU caps and priority](#cpu-caps-and-priority)). |
| `systemctl reload cronstable` fails with `Failed at step CGROUP` | systemd before 240 starts `ExecReload=` in the unit's cgroup, which takes no process once the cgroup layout exists. Use `systemctl kill --kill-who=main -s HUP cronstable` (see [The packaged systemd unit](#the-packaged-systemd-unit)). |
| The whole service stops when a job hits its memory cap | The unit sets `OOMPolicy=stop` or `OOMPolicy=kill`, for example in a drop-in. systemd 243 and later apply `OOMPolicy=continue` to a unit with `Delegate=` unless a line sets another policy. Set `OOMPolicy=continue` (see [Custom units](#custom-units)). |
| Runs fail with `resource limits could not be enforced` | The job sets `required: true` and the host cannot enforce a cap. The `reason` in the message, and `limits.reason` on `GET /node`, say why. |
| A run fails with `memory limit exceeded` while its sampled RSS looked lower | The cap counts cgroup memory (including page cache) on Linux and commit charge on Windows, not RSS. Compare `peak_memory_bytes`. |
| Processes or threads fail to start below the expected count | On Linux every thread counts against `processes`, and so does the shell. On Windows cmd.exe and a venv launcher count. |
| The CPU cap is not enforced on a Remote Desktop host | Dynamic Fair Share Scheduling blocks CPU rate caps on Windows. |

For other problems, see [Troubleshooting and FAQ](Troubleshooting).

## See also

- [Configuration Reference](Configuration-Reference#resource-limits): the option rows and load-time validation.
- [Resource Monitoring](Resource-Monitoring): sampled CPU time and RSS per run, charted against the caps.
- [Concurrency and Timeouts](Concurrency-and-Timeouts): cancellation, `killTimeout`, and the forced step.
- [Includes, Defaults, and Multi-File Config](Includes-and-Defaults): how `resourceLimits` merges under `defaults:`.
- [Orchestration and DAGs](Orchestration-and-DAGs): `resourceLimits` on DAG tasks.
- [Installation](Installation): the `.deb` and `.rpm` packages and their systemd unit.
- [Production and Container Deployment](Production-Deployment): running the published images.
- [Running on Windows](Running-on-Windows): the rest of the Windows process model.
- [HTTP Control API](HTTP-API): the `limits` fields on jobs, runs, resources, and `GET /node`.
