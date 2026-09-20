"""Regenerate the reference v1 state store in ``tests/fixtures/state_v1/``.

Use the package APIs to write scheduler state, job state, artifacts and blobs,
pool queues, DAG runs, and leases. ``tests/test_state_golden.py`` loads the
saved store and compares it with newly generated files. This detects
unintended changes to filenames, fields, defaults, or encoding before they
affect existing stores.

Usage::

    PYTHONPATH=. python tests/gen_state_golden.py

Regenerate only for an intentional format change, and document the change
in the commit. Upgraded daemons must still read existing stores.

Control nondeterministic values before writing:

* Clocks: ``state._now``, ``jobstate._now``, ``dagrun._now``, ``cron.get_now``,
  and the time functions used by pools.py share a manually advanced ``Clock``.
* IDs: ``os.urandom`` and ``uuid.uuid4`` use a fixed sequence. This fixes
  process instance IDs, record filenames, DAG run IDs, and pool ticket owners.
* Identity: The hostname, process token, PID, OS boot ID, and boot time use
  constant values.
* Configuration: Fixed job and report shell defaults preserve the saved
  digests across platforms. These tests execute no commands.

Await each write and drain the scheduler's write queues before the next step
to keep filename sequence numbers reproducible. Compare file contents only;
empty directories, file permissions, and modification times are excluded.
"""

import asyncio
import contextlib
import copy
import datetime
import json
import os
import sys
import uuid
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cronstable import config as config_mod  # noqa: E402
from cronstable import cron as cron_mod  # noqa: E402
from cronstable import dag, dagrun, jobstate, pools, state  # noqa: E402
from cronstable import platform as platform_mod  # noqa: E402
from cronstable.config import parse_config_string  # noqa: E402
from cronstable.job import JobOutputStream  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "fixtures", "state_v1")

HOST = "golden-host"
PROC = "goldenproc00"
#: Above every platform's pid ceiling, so no live process ever owns it and
#: the golden in-flight run always reads as interrupted.
DEAD_PID = 2147483646
BOOT_ID = "00000000-0000-4000-8000-00000000b007"
BOOT_TIME = 1767182400.0  # 2025-12-31T12:00:00Z

#: 2026-01-01T00:00:00Z
EPOCH = 1767225600.0

#: The retry deadline sits far in the future so a test that rehydrates the
#: tree re-arms the ladder without it ever coming due.
RETRY_NOT_BEFORE = datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc)
#: A pause is capped at 30 days, so the standing one cannot outlive the
#: real calendar; a test that wants it live reads the store at REHYDRATE_AT,
#: inside its window.
PAUSE_SECONDS = 7 * 86400
REHYDRATE_AT = datetime.datetime(2026, 1, 4, tzinfo=datetime.timezone.utc)

CONFIG = """
pools:
  database:
    slots: 2
    maxQueued: 8
jobs:
  - name: nightly
    command: "echo nightly"
    schedule: "0 2 * * *"
    archiveOutput: true
  - name: flaky
    command: "exit 23"
    schedule: "*/5 * * * *"
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 60
        maximumDelay: 600
        backoffMultiplier: 2
  - name: crashed
    command: "sleep 3600"
    schedule: "0 * * * *"
  - name: held
    command: "echo held"
    schedule: "* * * * *"
  - name: boot
    command: "echo boot"
    schedule: "@reboot"
  - name: pooled
    command: "echo pooled"
    schedule: "0 3 * * *"
    pool: database
dags:
  - name: release
    tasks:
      - id: build
        command: "echo build"
      - id: gate
        type: approval
        dependsOn:
          - build
      - id: publish
        dependsOn:
          - gate
        command: "echo publish"
"""


@contextlib.contextmanager
def fixed_shell_defaults():
    """Load the golden config with the shells used to write its digests.

    YAML requires a command when configuring a report shell, so pin the
    defaults here to preserve the saved report command of None. Replace the
    defaults with a copy so parsed jobs retain their fixed report shells
    after this context exits, without changing other tests' defaults.
    """
    defaults = copy.deepcopy(config_mod.DEFAULT_CONFIG)
    defaults["shell"] = "/bin/sh"
    for action in ("onFailure", "onPermanentFailure", "onSuccess", "onLate"):
        defaults[action]["report"]["shell"]["shell"] = "/bin/sh"
    with mock.patch.object(config_mod, "DEFAULT_CONFIG", defaults):
        yield


class Clock:
    """Provide one manually advanced wall clock for all writers."""

    def __init__(self, now=EPOCH):
        self.now = now

    def advance(self, seconds=1.0):
        self.now += seconds
        return self.now

    def time(self):
        return self.now

    def monotonic(self):
        return self.now

    def get_now(self, timezone):
        return datetime.datetime.fromtimestamp(self.now, timezone)

    def at(self, offset=0.0):
        return datetime.datetime.fromtimestamp(
            self.now + offset, datetime.timezone.utc
        )


class _Counter:
    """Deterministic stand-ins for ``os.urandom`` and ``uuid.uuid4``."""

    def __init__(self):
        self.count = 0

    def urandom(self, size):
        self.count += 1
        return self.count.to_bytes(size, "big")

    def uuid4(self):
        self.count += 1
        return uuid.UUID(int=self.count)


@contextlib.contextmanager
def pinned(clock):
    """Pin every source of nondeterminism for the generator's lifetime."""
    ids = _Counter()
    patches = [
        mock.patch.object(state, "_now", clock.time),
        mock.patch.object(jobstate, "_now", clock.time),
        mock.patch.object(dagrun, "_now", clock.time),
        mock.patch.object(cron_mod, "get_now", clock.get_now),
        # pools.py reads time.time()/time.monotonic() directly.
        mock.patch.object(pools, "time", clock),
        mock.patch.object(pools, "uuid", SimpleNamespace(uuid4=ids.uuid4)),
        mock.patch.object(os, "urandom", ids.urandom),
        mock.patch.object(platform_mod, "os_boot_id", lambda: BOOT_ID),
        mock.patch.object(platform_mod, "os_boot_time", lambda: BOOT_TIME),
    ]
    with contextlib.ExitStack() as stack:
        for patch in patches:
            stack.enter_context(patch)
        yield


def _run_info(clock, outcome, exit_code, *, took, fail_reason=None):
    output = JobOutputStream()
    output.close()
    return cron_mod.JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=clock.at(-took),
        finished_at=clock.at(),
        fail_reason=fail_reason,
        output=output,
    )


async def _drain(cron):
    """Let every queued durable write land before the next step."""
    for _ in range(50):
        pending = [t for t in cron._pending_state_writes if not t.done()]
        if not pending:
            return
        await asyncio.gather(*pending)
    raise RuntimeError("state writes never settled")


async def generate(root):
    """Write the golden store under ``root`` and return the Cron that did."""
    clock = Clock()
    with fixed_shell_defaults(), pinned(clock):
        return await _generate(str(root), clock)


async def _generate(root, clock):
    yaml = (
        "state:\n  path: {}\n  topology: single-node\n"
        "  jobApi:\n    enabled: false\n".format(json.dumps(root))
        + CONFIG
    )
    cron = cron_mod.Cron(None, config_yaml=yaml)
    cron._state_host = HOST
    cron._proc_token = PROC
    # the generator is the only launcher: no service task may race it.
    cron._pools.service = lambda: None
    cron._dag._spawn_advance = lambda ref: None
    await cron.start_stop_state(parse_config_string(yaml, "").state_config)
    backend = cron.state_backend
    assert backend is not None, "the golden store failed to start"
    jobs = cron.cron_jobs
    # a finished run piggybacks a counter snapshot, throttled on the event
    # loop's clock; only the explicit snapshot below may land.
    cron._counter_snapshot_next = float("inf")

    # --- the scheduler's own streams ---------------------------------------
    clock.advance()
    await cron._persist_manifest()

    # nightly: two archived successes.
    for line in ("first night", "second night"):
        clock.advance(86400)
        await cron._persist_run_record(
            "nightly",
            _run_info(clock, "success", 0, took=12.5),
            [("stdout", line), ("stderr", "token=hunter2 " + line)],
        )

    # flaky: a failure with its ladder armed and still pending.
    clock.advance(60)
    await cron._persist_run_record(
        "flaky",
        _run_info(
            clock,
            "failure",
            23,
            took=0.25,
            fail_reason="command exited with code 23",
        ),
    )
    cron._persist_retry_pending(jobs["flaky"], 1, RETRY_NOT_BEFORE)
    await _drain(cron)

    # crashed: an in-flight run whose daemon and process are both gone.
    clock.advance(60)
    await cron._persist_inflight_open(
        jobs["crashed"],
        SimpleNamespace(proc=SimpleNamespace(pid=DEAD_PID)),
    )

    # held: ran, finished cleanly (open then closed), then paused.
    clock.advance(60)
    await cron._persist_inflight_open(
        jobs["held"], SimpleNamespace(proc=SimpleNamespace(pid=DEAD_PID))
    )
    clock.advance(2)
    await cron._persist_inflight_closed("held")
    await cron._persist_run_record(
        "held", _run_info(clock, "success", 0, took=2.0)
    )
    clock.advance(60)
    await cron.pause_job_by_name(
        "held", duration=PAUSE_SECONDS, note="golden pause", by="golden"
    )
    await _drain(cron)

    # nightly was paused and resumed: the newest record wins.
    clock.advance(60)
    await cron.pause_job_by_name("nightly", note="brief", by="golden")
    await _drain(cron)
    clock.advance(60)
    await cron.resume_job_by_name("nightly", by="golden")
    await _drain(cron)

    # boot: the @reboot marker for this OS boot.
    clock.advance(60)
    assert await cron._reboot_boot_gate(jobs["boot"]) is True
    await _drain(cron)

    # a completed catch-up cycle.
    clock.advance(60)
    await cron._checkpoint_catchup("nightly", "open", clock.at().isoformat())
    clock.advance(1)
    await cron._checkpoint_catchup("nightly", "close", clock.at().isoformat())

    clock.advance(60)
    cron._counters_seeded = True
    await cron._persist_counter_snapshot()

    # --- the job-facing state API -------------------------------------------
    clock.advance(60)
    await jobstate.kv_set(backend, "nightly", "last-batch", {"id": 41})
    await jobstate.kv_set(backend, "nightly", "unicode", "café ☃")
    await jobstate.cursor_advance(backend, "nightly", "offset", 1200)
    await jobstate.idempotency_claim(backend, "nightly", "send-report")
    await jobstate.artifact_put(
        backend,
        "nightly",
        "report.txt",
        b"golden artifact payload",
        meta={"contentType": "text/plain"},
    )

    # --- the pool queue: one running ticket, one waiting --------------------
    clock.advance(60)
    first = await cron._pools.enqueue(jobs["pooled"], key="ticket-running")
    clock.advance()  # queue order is by queuedAt
    await cron._pools.enqueue(jobs["pooled"], key="ticket-queued")
    assert await cron._pools.acquire("database", first["id"]) is not None

    # --- DAG runs -----------------------------------------------------------
    clock.advance(60)
    await _dag_runs(cron, clock)

    # --- leases: one held, one released (the fence counter's home) ----------
    clock.advance(60)
    assert await backend.acquire_lease("slots/held", HOST + "#held", 3600.0)
    done = await backend.acquire_lease("slots/nightly", HOST + "#done", 30.0)
    assert done is not None
    await backend.release_lease(done)
    return cron


async def _dag_runs(cron, clock):
    """Two runs of one DAG: one parked at its gate, one finished."""
    dagcfg = cron.cron_dags["release"]
    spec = dagcfg.spec
    scheduler = cron._dag

    async def step(run_key, transform):
        clock.advance()
        return await scheduler._mutate(
            "release", run_key, scheduler._wrap(transform)
        )

    async def run_task(run_key, task_id, pid):
        await step(
            run_key, dag.plan_and_claim(spec, clock.now, PROC, HOST, {})
        )
        await step(run_key, dag.set_task_pid(task_id, PROC, pid, clock.now))
        clock.advance(5)
        await step(
            run_key,
            dag.mark_task_finished(
                task_id,
                success=True,
                exit_code=0,
                fail_reason=None,
                now=clock.now,
                task=spec.by_id[task_id],
            ),
        )

    for run_key, approve in (("manual-gated", False), ("manual-done", True)):
        clock.advance(60)
        assert await scheduler._create_doc(dagcfg, run_key, None, "manual")
        await run_task(run_key, "build", 4001)
        # the next plan opens the gate and parks the run on it.
        await step(
            run_key, dag.plan_and_claim(spec, clock.now, PROC, HOST, {})
        )
        if not approve:
            continue
        await step(
            run_key,
            dag.apply_approval(
                "gate",
                approved=True,
                by="golden",
                now=clock.now,
                on_reject=spec.by_id["gate"].on_reject,
            ),
        )
        await run_task(run_key, "publish", 4002)
        # one more plan terminates the run.
        await step(
            run_key, dag.plan_and_claim(spec, clock.now, PROC, HOST, {})
        )


async def close(cron):
    await _drain(cron)
    for ticket in list(cron._pools.held.values()):
        ticket.valid = False
    await cron._pools.close()
    await cron._dag.shutdown()
    if cron.state_backend is not None:
        await cron.state_backend.stop()


def tree(root):
    """Map relative POSIX paths under ``root`` to file contents."""
    found = {}
    for path, _dirs, names in os.walk(root):
        for name in names:
            full = os.path.join(path, name)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            with open(full, "rb") as fobj:
                found[rel] = fobj.read()
    return found


def main():
    import shutil

    if os.path.isdir(OUT):
        shutil.rmtree(OUT)

    async def run():
        await close(await generate(OUT))

    asyncio.run(run())
    files = tree(OUT)
    print("wrote {} files under {}".format(len(files), OUT))


if __name__ == "__main__":
    main()
