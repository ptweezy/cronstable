"""Shared fixtures, constants, and helpers for the test_cron_* split files.

Extracted from the former tests/test_cron.py so the six split modules can
import them by name. The autouse fixed_current_time fixture freezes
cronstable.cron.get_now at 1999-12-31 12:00:00 in every module that
imports it.
"""

import datetime

import pytest

import cronstable.cron
from tests._commands import cmd_print, cmd_sleep, yaml_command
from tests._configs import job_yaml


async def _noop():
    # awaitable stand-in for a monkeypatched async launch_scheduled_job
    return None


FIXED_TIME = datetime.datetime(
    year=1999, month=12, day=31, hour=12, minute=0, second=0
)


def _set_now(monkeypatch, holder):
    # a controllable clock: holder["now"] is a naive datetime, localized to
    # the requested timezone (adopt the tz when naive, convert when aware).
    # The autouse fixed_current_time fixture is this clock frozen at
    # FIXED_TIME.
    def get_now(timezone):
        now = holder["now"]
        if timezone is not None:
            now = (
                now.replace(tzinfo=timezone)
                if now.tzinfo is None
                else now.astimezone(timezone)
            )
        return now

    monkeypatch.setattr("cronstable.cron.get_now", get_now)


@pytest.fixture(autouse=True)
def fixed_current_time(monkeypatch):
    _set_now(monkeypatch, {"now": FIXED_TIME})


JOB_THAT_SUCCEEDS = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="foobar"))
    + '\n    schedule: "@reboot"\n'
)


CONCURRENT_JOB = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_sleep(30))
    + """
    schedule: "@reboot"
    captureStdout: true
    concurrencyPolicy: {policy}
"""
)


TWO_JOBS = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "*/5 * * * *"
    captureStdout: true
  - name: beta
    command:
      - echo
      - beta
    schedule: "@reboot"
    enabled: false
"""


class _FakeMesh:
    """A stand-in leadership backend capturing provider installs/lifecycle."""

    def __init__(self, config):
        self.config = config
        self.job_summaries_provider = None
        self.node_stats_provider = None
        self.started = False
        self.stopped = False

    def set_job_summaries_provider(self, p):
        self.job_summaries_provider = p

    def set_node_stats_provider(self, p, share=True):
        self.node_stats_provider = p
        self.node_stats_share = share

    def tls_files_changed(self):
        return False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True


DT = datetime.datetime
UTC = datetime.timezone.utc


def _reboot_job(name="boot", policy="Leader", enabled=True):
    import types

    return types.SimpleNamespace(
        name=name, clusterPolicy=policy, schedule="@reboot", enabled=enabled
    )


def _reboot_mgr(
    *, leader=None, conflict=False, node="node-a", available=None, ran=()
):
    ran_set = set(ran)

    class _Mgr:
        node_name = node
        distribution = "single-leader"

        def has_conflict(self):
            return conflict

        def leader_name(self):
            return leader

        def is_leader(self):
            # mirrors the real seam: leader iff the elected name is ours
            return self.leader_name() == self.node_name

        def available_leader_name(self):
            # quorum-free owner used by PreferLeader; an isolated node leads
            # its own reachable set, so default to self.
            return node if available is None else available

        def is_available_leader(self):
            return self.available_leader_name() == self.node_name

        def reboot_ran(self, name):
            return name in ran_set

        async def mark_reboot_ran(self, name):
            ran_set.add(name)

    return _Mgr()


_WEB_ONE_JOB = job_yaml("alpha", schedule="*/5 * * * *")


_SECONDS_JOB = """
jobs:
  - name: sec
    command: echo sec
    schedule: "*/15 * * * * * *"
  - name: min
    command: echo min
    schedule: "* * * * *"
"""


_EVERY_SECOND_AND_MINUTE = """
jobs:
  - name: tick
    command: echo tick
    schedule: "* * * * * * *"
  - name: noon
    command: echo noon
    schedule: "0 12 * * *"
"""


def _drive_cron(monkeypatch, holder, config_yaml):
    """A Cron wired to the controllable clock, recording (name, slot-second)
    at each launch by reading the de-dup slot spawn_jobs just set."""
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=config_yaml)
    launched = []

    async def fake_launch(job):
        launched.append((job.name, cron._last_run_slot[job.name].second))

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    return cron, launched


def _seed_due(cron, *names):
    """Make the named jobs due *now* by seeding the next-fire index at the
    current (frozen) clock, so a direct spawn_jobs(False) call services them.
    Mirrors what a real pass does once the loop has been running -- the loop's
    start-up seeding is strictly-future, which deliberately skips the current
    slot."""
    now = cronstable.cron.get_now(datetime.timezone.utc)
    for name in names:
        cron._set_next_fire(name, now)


_THREE_DUE = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
  - name: b
    command: echo b
    schedule: "* * * * *"
  - name: c
    command: echo c
    schedule: "* * * * *"
"""


_RELOAD_BEFORE = """
jobs:
  - name: keep
    command: echo keep
    schedule: "* * * * *"
  - name: drop
    command: echo drop
    schedule: "* * * * *"
"""


_RELOAD_AFTER = """
jobs:
  - name: keep
    command: echo keep
    schedule: "* * * * *"
  - name: added
    command: echo added
    schedule: "*/5 * * * *"
"""


_SLA_STALE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    sla:
      maxTimeSinceSuccessSeconds: 3600
"""


_SLA_RUNTIME_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    sla:
      maxRuntimeSeconds: 600
"""


STALE = cronstable.cron.SLA_CHECK_STALE
LATE = cronstable.cron.SLA_CHECK_LATE
RUNTIME = cronstable.cron.SLA_CHECK_RUNTIME


def _sla_report_recorder(monkeypatch):
    reports = []

    async def fake(ctx, report_config):
        reports.append((ctx, report_config))

    monkeypatch.setattr(cronstable.cron, "report_sla_breach", fake)
    return reports


_SUBMINUTE_NOFIRE = """
jobs:
  - name: sec
    command: echo sec
    schedule: "5/15 * * * * * *"
"""
