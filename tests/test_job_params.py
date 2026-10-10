"""Run parameters for plain jobs.

A job declares ``params:`` the way a workflow does, and a manual start
supplies values. Covers:
  * the declaration at config load, and the job digest.
  * ``Cron.start_job``: resolution, refusal, and the retry ladder.
  * delivery to a real process, through the variables and ``cronstable
    param``, with and without a ``state:`` section.
  * the carriers: the pool entry, the in-flight record, and the run record.
  * ``POST /jobs/{name}/start``, ``GET /jobs``, and MCP ``cron_run_job``.

The workflow side of the feature lives in tests/test_dag_params.py.
"""

import asyncio
import datetime
import json
import sys

import pytest

from cronstable import fingerprint, params
from cronstable.config import ConfigError, parse_config_string
from cronstable.cron import (
    RUN_HISTORY_LIMIT,
    ApiActionError,
    Cron,
    JobRunInfo,
    _job_run_info_from_dict,
)
from cronstable.job import JobRetryState, RunningJob
from cronstable.params import ParamError
from cronstable.pools import NAMESPACE as POOL_NAMESPACE
from tests._helpers import _drain_pending, _reap_running, _wait_until
from tests.test_state_dag_run import _start_web

_PY = sys.executable

_JOBS = """
jobs:
  - name: report
    command: 'x'
    schedule: '0 0 1 1 *'
    captureStdout: true
    params:
      - name: region
        default: eu
        allowed:
          - eu
          - us
        description: Region to report on
      - name: rows
        type: integer
        default: 100
        minimum: 1
      - name: dry
        type: boolean
        default: false
  - name: plain
    command: 'x'
    schedule: '0 0 1 1 *'
    captureStdout: true
"""

_DEFAULTS = {"region": "eu", "rows": 100, "dry": False}

# prints the parameter variables the process sees, sorted, as JSON
_ECHO = [
    _PY,
    "-c",
    "import json, os; print(json.dumps({k: v for k, v in "
    "sorted(os.environ.items()) if k.startswith('CRONSTABLE_PARAM_')}))",
]


async def _cron(dag_cron, yaml=_JOBS, command=None):
    cron = await dag_cron(yaml)
    for job in cron.cron_jobs.values():
        job.command = command or _ECHO
    return cron


async def _finish(cron):
    await _reap_running(cron)
    await _drain_pending(cron)


def _printed(cron, name):
    """The JSON the last run of ``name`` printed."""
    (line,) = [text for _stream, text in cron.last_run[name].output.lines]
    return json.loads(line)


# A job with a retry ladder. Its command picks its own outcome from the
# values the run takes, so a test drives a real ladder with nothing mocked:
#   act: fail (the default)  exit 3. With the path of a marker file as the
#                            last argument, only the first such run does:
#                            it creates the file, and each later one stays
#                            up.
#   act: hold                stay up
#   act: linger              exit 0 after one second
#   act: pass                exit 0
_LADDER = """
jobs:
  - name: report
    command:
      - '{py}'
      - '-c'
      - |
        import os, sys, time
        act = os.environ['CRONSTABLE_PARAM_ACT']
        once = sys.argv[1] if act == 'fail' and sys.argv[1:] else None
        again = once is not None and os.path.exists(once)
        if once is not None:
            open(once, 'a').close()
        if act == 'hold' or again:
            time.sleep(60)
        if act == 'linger':
            time.sleep(1)
        sys.exit(3 if act == 'fail' else 0)
{once}    schedule: '0 0 1 1 *'
    captureStdout: true
    concurrencyPolicy: {policy}
    concurrencyScope: {scope}
    onMissed: {on_missed}
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: {delay}
        maximumDelay: 600
        backoffMultiplier: 2
    params:
      - name: region
        default: eu
        allowed:
          - eu
          - us
      - name: act
        default: fail
        allowed:
          - fail
          - pass
          - hold
          - linger
"""


def _ladder_yaml(
    policy="Allow",
    delay=300,
    on_missed="skip",
    once=None,
    slots=None,
    scope="node",
):
    """The ``_LADDER`` job. ``once`` is the path of its marker file,
    ``slots`` puts the job in a pool of that size, and ``scope`` is its
    ``concurrencyScope``.

    The command is part of the YAML, so a daemon and its restart compute
    the same job digest.
    """
    text = _LADDER.format(
        py=_PY,
        once="      - '{}'\n".format(once) if once else "",
        policy=policy,
        scope=scope,
        delay=delay,
        on_missed=on_missed,
    )
    if slots is None:
        return text
    return (
        "pools:\n  database:\n    slots: {}\n    maxQueued: 8\n".format(slots)
        + text.replace(
            "    captureStdout: true\n",
            "    captureStdout: true\n    pool: database\n",
        )
    )


async def _settle(cron):
    """Reap the running instances, then run their completions (the report
    and the retry arm) and the state writes those queue."""
    await _reap_running(cron)
    await cron._drain_completions()
    # a retry task the completion created queues its pending record on its
    # first step
    for _ in range(5):
        await asyncio.sleep(0)
    await _drain_pending(cron)


async def _retry_records(cron, name="report"):
    """``name``'s durable retry records, oldest first."""
    records = await cron.state_backend.list_records(cron._retry_stream(name))
    return [(r["kind"], r.get("reason"), r["attempt"]) for r in records]


async def _arm_ladder(cron, name="report"):
    """Fail a scheduled run, which takes the defaults, so that retry #1 is
    pending. Returns the live retry state, whose task sleeps out the delay.
    """
    await cron.launch_scheduled_job(cron.cron_jobs[name])
    await _settle(cron)
    ladder = cron.retry_state[name]
    assert ladder.count == 1 and not ladder.task.done()
    assert (await _retry_records(cron, name))[-1] == ("pending", None, 1)
    return ladder


async def _stop_ladder(cron, name="report"):
    """End a live ladder as a graceful shutdown does: nothing settles."""
    await cron.cancel_job_retries(name, settle=None)
    await asyncio.sleep(0)


def _reports(monkeypatch):
    """Record each report hook that fires, as ``(hook, region)``."""
    fired = []

    def record(hook):
        async def _report(running, *args, **kwargs):
            fired.append((hook, running.params["region"]))

        return _report

    for hook in ("failure", "permanent_failure", "success"):
        monkeypatch.setattr(RunningJob, "report_" + hook, record(hook))
    return fired


# A job whose cluster-wide concurrency slot another node can hold. The six
# second slot TTL makes a Replace pursuit poll once a second.
_SLOT = """
jobs:
  - name: s
    command: 'x'
    schedule: '0 0 1 1 *'
    captureStdout: true
    concurrencyScope: cluster
    concurrencyPolicy: Replace
    params:
      - name: region
        default: eu
"""

_HOLD = [_PY, "-c", "import time; time.sleep(60)"]


async def _slot_cron(dag_cron, yaml=_SLOT, command=None):
    """A daemon for ``_SLOT`` and the lease of another node on the slot."""
    cron = await dag_cron(yaml, extra_state="  slotTtlSeconds: 6\n")
    cron.cron_jobs["s"].command = command or _ECHO
    foreign = await cron.state_backend.acquire_lease(
        "slots/s", "nodeB#tokB", 30.0
    )
    assert foreign is not None
    return cron, foreign


async def _stop_run(cron, running):
    """Cancel one running instance and record it."""
    running.cancelled = True
    await running.cancel()
    await _settle(cron)


# --------------------------------------------------------------------------
# The declaration
# --------------------------------------------------------------------------


def test_a_job_declares_params_like_a_workflow():
    cfg = parse_config_string(_JOBS, "")
    jobs = {job.name: job for job in cfg.jobs}
    assert [p.name for p in jobs["report"].params] == ["region", "rows", "dry"]
    assert [p.default for p in jobs["report"].params] == ["eu", 100, False]
    assert jobs["plain"].params == ()
    assert params.declaration(jobs["report"].params)[0] == {
        "name": "region",
        "type": "string",
        "default": "eu",
        "allowed": ["eu", "us"],
        "description": "Region to report on",
    }


@pytest.mark.parametrize(
    "old, new, message",
    [
        pytest.param(
            "        default: 100\n",
            "        required: true\n",
            "job 'report': param 'rows': a job cannot require a parameter, "
            "because its scheduled runs use the defaults",
            id="required",
        ),
        pytest.param(
            "name: rows",
            "name: api_token",
            "job 'report': param 'api_token': the name reads as a secret",
            id="secret-like-name",
        ),
        pytest.param(
            "default: eu",
            "default: mars",
            "job 'report': param 'region': default 'mars' must be one of: "
            "eu, us",
            id="default-outside-allowed",
        ),
        pytest.param(
            "name: rows",
            "name: REGION",
            "job 'report': param 'REGION': the name repeats 'region'",
            id="duplicate-ignoring-case",
        ),
    ],
)
def test_job_declaration_rejections(old, new, message):
    assert old in _JOBS
    with pytest.raises(ConfigError) as err:
        parse_config_string(_JOBS.replace(old, new), "")
    assert message in str(err.value)


def test_params_is_a_job_key_that_defaults_cannot_set():
    with pytest.raises(ConfigError):
        parse_config_string(
            "defaults:\n  params:\n    - name: a\n      default: b\n" + _JOBS,
            "",
        )


def test_a_dag_task_template_has_no_params_of_its_own():
    cfg = parse_config_string(
        "state:\n  path: /tmp/x\n"
        "dags:\n  - name: d\n    params:\n"
        "      - name: mode\n        default: a\n"
        "    tasks:\n      - id: t\n        command: 'x'\n",
        "",
    )
    assert cfg.dags[0].task_templates["t"].params == ()
    assert [p.name for p in cfg.dags[0].spec.params] == ["mode"]


def test_the_declaration_is_part_of_the_job_digest_only_when_set():
    def digest(yaml, name):
        jobs = {j.name: j for j in parse_config_string(yaml, "").jobs}
        return fingerprint.job_digest(jobs[name])

    base = digest(_JOBS, "report")
    # a job that declares none keeps the digest it has without the key
    bare = _JOBS.split("    params:")[0] + "  - name: plain\n" + (
        "    command: 'x'\n    schedule: '0 0 1 1 *'\n"
        "    captureStdout: true\n"
    )
    assert digest(bare, "plain") == digest(_JOBS, "plain")
    assert "params" not in fingerprint.canonical_job(
        parse_config_string(bare, "").jobs[0]
    )
    # a changed default, bound, or allowed list is a different job
    for old, new in (
        ("default: eu", "default: us"),
        ("minimum: 1", "minimum: 2"),
        ("          - us\n", "          - us\n          - apac\n"),
        ("type: integer", "type: number"),
    ):
        assert digest(_JOBS.replace(old, new), "report") != base, old
    # a description is display text
    assert digest(
        _JOBS.replace("Region to report on", "Where to run"), "report"
    ) == base


# --------------------------------------------------------------------------
# Starting a job
# --------------------------------------------------------------------------


async def test_start_job_resolves_and_delivers_the_values(dag_cron):
    cron = await _cron(dag_cron)
    started = await cron.start_job("report", {"region": "us", "rows": 5})
    assert started == {
        "queued": None,
        "params": {"region": "us", "rows": 5, "dry": False},
    }
    (running,) = cron.running_jobs["report"]
    assert running.params == started["params"]
    await _finish(cron)
    assert _printed(cron, "report") == {
        "CRONSTABLE_PARAM_DRY": "false",
        "CRONSTABLE_PARAM_REGION": "us",
        "CRONSTABLE_PARAM_ROWS": "5",
    }
    assert cron.last_run["report"].params == started["params"]


async def test_a_start_with_no_values_uses_the_defaults(dag_cron):
    cron = await _cron(dag_cron)
    for supplied in (None, {}):
        started = await cron.start_job("report", supplied)
        assert started == {"queued": None, "params": _DEFAULTS}
        await _finish(cron)
        assert _printed(cron, "report") == {
            "CRONSTABLE_PARAM_DRY": "false",
            "CRONSTABLE_PARAM_REGION": "eu",
            "CRONSTABLE_PARAM_ROWS": "100",
        }
    assert cron.last_run["report"].params == _DEFAULTS


async def test_a_scheduled_launch_uses_the_current_defaults(dag_cron):
    cron = await _cron(dag_cron)
    await cron.launch_scheduled_job(cron.cron_jobs["report"])
    await _finish(cron)
    assert cron.last_run["report"].params == _DEFAULTS
    # a reload changes a default: the next run reads the new declaration
    reloaded = parse_config_string(_JOBS.replace("default: eu", "default: us"), "")
    job = {j.name: j for j in reloaded.jobs}["report"]
    job.command = _ECHO
    cron.cron_jobs["report"] = job
    await cron.launch_scheduled_job(job)
    await _finish(cron)
    assert cron.last_run["report"].params == {**_DEFAULTS, "region": "us"}
    assert _printed(cron, "report")["CRONSTABLE_PARAM_REGION"] == "us"


@pytest.mark.parametrize(
    "supplied, errors",
    [
        ({"region": "mars"}, {"region": "must be one of: eu, us"}),
        ({"rows": "5"}, {"rows": "must be an integer"}),
        ({"rows": 0}, {"rows": "must be at least 1"}),
        ({"colour": "red"}, {"colour": "is not a declared parameter"}),
        ({"dry": 1}, {"dry": "must be true or false"}),
    ],
)
async def test_refused_values_start_nothing(dag_cron, supplied, errors):
    cron = await _cron(dag_cron)
    with pytest.raises(ParamError) as err:
        await cron.start_job("report", supplied)
    assert err.value.errors == errors
    assert str(err.value) == "invalid parameters for job 'report'"
    assert not cron.running_jobs and "report" not in cron.last_run


async def test_a_job_with_no_declaration_takes_no_values(dag_cron):
    cron = await _cron(dag_cron)
    with pytest.raises(ParamError) as err:
        await cron.start_job("plain", {"region": "us"})
    assert str(err.value) == "job 'plain' declares no parameters"
    assert err.value.errors == {"region": "is not a declared parameter"}
    assert not cron.running_jobs
    # with none, it starts and records no values
    assert await cron.start_job("plain", {}) == {"queued": None, "params": None}
    (running,) = cron.running_jobs["plain"]
    assert running.params is None
    await _finish(cron)
    assert cron.last_run["plain"].params is None
    assert "params" not in cron.last_run["plain"].to_dict()
    assert _printed(cron, "plain") == {}


async def test_a_start_with_values_stays_outside_the_retry_ladder(dag_cron):
    cron = await _cron(dag_cron, command=[_PY, "-c", "raise SystemExit(3)"])
    ladder = JobRetryState(1.0, 2.0, 60.0)
    ladder.count = 1
    cron.retry_state["report"] = ladder
    # no values: the run is one more attempt of the ladder
    await cron.start_job("report")
    (running,) = cron.running_jobs["report"]
    assert running.retry_state is ladder and running.params == _DEFAULTS
    await running.wait()
    cron.running_jobs["report"].remove(running)
    # supplied values: one attempt of its own
    await cron.start_job("report", {"region": "us"})
    (running,) = cron.running_jobs["report"]
    assert running.retry_state is None
    assert running.params == {**_DEFAULTS, "region": "us"}
    await running.wait()
    cron.running_jobs["report"].remove(running)
    assert cron.retry_state["report"] is ladder and ladder.count == 1
    cron.retry_state.pop("report")


async def test_a_successful_start_with_values_leaves_the_retry_ladder(
    dag_cron,
):
    cron = await _cron(dag_cron)
    ladder = JobRetryState(1.0, 2.0, 60.0)
    ladder.count = 1
    cron.retry_state["report"] = ladder
    # other values succeed: the default-valued run the ladder repeats is
    # still owed its retry
    await cron.start_job("report", {"region": "us"})
    (running,) = cron.running_jobs["report"]
    assert running.supplied_params
    await _finish(cron)
    await cron._drain_completions()
    assert cron.last_run["report"].outcome == "success"
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    # the defaults succeed: the retry has nothing left to repeat
    await cron.start_job("report")
    (running,) = cron.running_jobs["report"]
    assert not running.supplied_params
    await _finish(cron)
    await cron._drain_completions()
    await _drain_pending(cron)
    assert "report" not in cron.retry_state and ladder.cancelled


async def test_cancelling_a_start_with_values_leaves_the_retry_ladder(
    dag_cron, monkeypatch
):
    cron = await dag_cron(_ladder_yaml())
    reports = _reports(monkeypatch)
    ladder = await _arm_ladder(cron)
    await cron.start_job("report", {"region": "us", "act": "hold"})
    assert await cron.cancel_job_by_name("report") == 1
    await _settle(cron)
    assert cron.last_run["report"].outcome == "cancelled"
    # like its success, its cancellation leaves the default-valued run the
    # retry it is owed
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    assert not ladder.task.done()
    assert (await _retry_records(cron))[-1] == ("pending", None, 1)
    assert reports == [("failure", "eu")]
    assert cron.last_run["report"].supplied_params
    await _stop_ladder(cron)


@pytest.mark.parametrize("act", ["pass", "fail"])
async def test_a_restart_after_a_start_with_values_re_arms_the_retry(
    dag_cron, act
):
    yaml = _ladder_yaml()
    cron = await dag_cron(yaml)
    await _arm_ladder(cron)
    await cron.start_job("report", {"region": "us", "act": act})
    await _settle(cron)
    assert cron.last_run["report"].params == {"region": "us", "act": act}
    await _stop_ladder(cron)
    # the next daemon on the store warms its history from the ledger, and
    # does not take the run for the one that resolved the retry
    restarted = await dag_cron(yaml)
    await _drain_pending(restarted)
    assert (await _retry_records(restarted))[-2:] == [
        ("pending", None, 1),
        ("pending", None, 1),
    ]
    assert restarted.retry_state["report"].count == 1
    await _stop_ladder(restarted)


async def test_a_peer_claims_the_retry_after_a_start_with_values(dag_cron):
    cron = await dag_cron(_ladder_yaml())
    await _arm_ladder(cron)
    await cron.start_job("report", {"region": "us", "act": "pass"})
    await _settle(cron)
    await _stop_ladder(cron)
    stream = cron._retry_stream("report")
    pending = (await cron.state_backend.list_records(stream))[-1]
    # a node that saw the run does not count it as newer than the retry
    armed = datetime.datetime.fromisoformat(pending["at"])
    assert cron._last_completed_at["report"] < armed
    # and neither does the durable ledger that a claiming node reads, so
    # the claim lands
    cron._state_host = "node-b"
    assert await cron._claim_retry_under_lease(
        "report",
        cron.cron_jobs["report"],
        pending,
        1,
        datetime.datetime.fromisoformat(pending["notBefore"]),
    )
    await _drain_pending(cron)
    claimed = (await cron.state_backend.list_records(stream))[-1]
    assert claimed["kind"] == "pending" and claimed["host"] == "node-b"
    assert claimed["claimedFrom"] == pending["host"]


async def _retry_in_flight(dag_cron, tmp_path, **kwargs):
    """A ``_LADDER`` daemon whose retry #1 launched and is still running."""
    cron = await dag_cron(
        _ladder_yaml(delay=0.2, once=tmp_path / "once", **kwargs)
    )
    ladder = await _arm_ladder(cron)
    await _wait_until(ladder.task.done)
    await _drain_pending(cron)
    (attempt,) = cron.running_jobs["report"]
    assert attempt.retry_state is ladder and ladder.count == 1
    assert (await _retry_records(cron))[-1] == ("settled", "launched", 1)
    return cron, ladder, attempt


async def test_a_start_with_values_that_replaces_a_retry_ends_the_ladder(
    dag_cron, tmp_path, monkeypatch
):
    cron, ladder, attempt = await _retry_in_flight(
        dag_cron, tmp_path, policy="Replace"
    )
    reports = _reports(monkeypatch)
    await cron.start_job("report", {"region": "us", "act": "pass"})
    assert attempt.replaced
    await _settle(cron)
    # the replaced attempt arms no retry and the replacement carries no
    # ladder, so the sequence has no attempt left: it ends, and says so
    assert "report" not in cron.retry_state and ladder.cancelled
    assert (await _retry_records(cron))[-1] == ("settled", "replaced", 1)
    assert "retry" not in cron._job_to_dict("report", cron.cron_jobs["report"])
    assert reports == [("success", "us")]


async def test_a_start_without_values_that_replaces_a_retry_keeps_the_ladder(
    dag_cron, tmp_path
):
    cron, ladder, attempt = await _retry_in_flight(
        dag_cron, tmp_path, policy="Replace"
    )
    await cron.start_job("report")
    assert attempt.replaced
    # the run takes the attempt's place in the ladder, which goes on
    (successor,) = [r for r in cron.running_jobs["report"] if r is not attempt]
    assert successor.retry_state is ladder
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    assert (await _retry_records(cron))[-1] == ("settled", "launched", 1)
    await _stop_run(cron, successor)


async def test_a_start_with_values_that_replaces_a_run_leaves_a_pending_retry(
    dag_cron, tmp_path
):
    cron = await dag_cron(
        _ladder_yaml(policy="Replace", once=tmp_path / "once")
    )
    ladder = await _arm_ladder(cron)
    # a start without values shares the ladder and stays up while retry #1
    # still waits to fire
    await cron.start_job("report")
    (shares,) = cron.running_jobs["report"]
    assert shares.retry_state is ladder
    await cron.start_job("report", {"region": "us", "act": "pass"})
    assert shares.replaced
    await _settle(cron)
    # the retry never fired, so the default-valued run is still owed it
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    assert not ladder.task.done()
    assert (await _retry_records(cron))[-1] == ("pending", None, 1)
    await _stop_ladder(cron)


async def test_a_start_with_values_that_replaces_another_leaves_the_ladder(
    dag_cron,
):
    cron = await dag_cron(_ladder_yaml(policy="Replace"))
    # a sequence between attempts: retry #1 ran and failed, and its
    # completion has yet to arm retry #2
    ladder = JobRetryState(1.0, 2.0, 60.0)
    ladder.count = 1
    cron.retry_state["report"] = ladder
    await cron.start_job("report", {"region": "us", "act": "hold"})
    # the run it replaces is no attempt of the ladder, so the sequence stays
    await cron.start_job("report", {"region": "us", "act": "pass"})
    await _settle(cron)
    assert cron.last_run["report"].outcome == "success"
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    assert await _retry_records(cron) == []
    cron.retry_state.pop("report")


async def test_a_start_with_values_that_replaces_a_first_run_ends_no_ladder(
    dag_cron, tmp_path
):
    once = tmp_path / "once"
    once.write_text("")  # every default-valued run stays up
    cron = await dag_cron(_ladder_yaml(policy="Replace", once=once))
    await cron.launch_scheduled_job(cron.cron_jobs["report"])
    (first,) = cron.running_jobs["report"]
    fresh = cron.retry_state["report"]
    assert first.retry_state is fresh and fresh.count == 0
    await cron.start_job("report", {"region": "us", "act": "pass"})
    assert first.replaced
    await _settle(cron)
    # no run failed, so no retry sequence is in progress to end
    assert cron.retry_state["report"] is fresh and not fresh.cancelled
    assert await _retry_records(cron) == []


async def test_forbid_holds_a_due_retry_behind_a_start_with_values(
    dag_cron, tmp_path, monkeypatch
):
    cron = await dag_cron(
        _ladder_yaml(policy="Forbid", delay=0.3, once=tmp_path / "once")
    )
    reports = _reports(monkeypatch)
    ladder = await _arm_ladder(cron)
    await cron.start_job("report", {"region": "us", "act": "linger"})
    (one_off,) = cron.running_jobs["report"]
    # the retry comes due while the one-off runs. It waits: a launch that
    # Forbid refused would spend the attempt
    await asyncio.sleep(0.6)
    assert cron.running_jobs["report"] == [one_off]
    assert not ladder.task.done()
    assert (await _retry_records(cron))[-1] == ("pending", None, 1)
    await _settle(cron)
    assert cron.last_run["report"].outcome == "success"
    # once the one-off ends, the retry runs with the defaults as the next
    # attempt of its ladder
    await _wait_until(lambda: cron.running_jobs.get("report"), tries=500)
    (retry,) = cron.running_jobs["report"]
    assert retry.retry_state is ladder and not retry.supplied_params
    await _drain_pending(cron)
    assert (await _retry_records(cron))[-1] == ("settled", "launched", 1)
    assert reports == [("failure", "eu"), ("success", "us")]
    await _stop_run(cron, retry)


@pytest.mark.parametrize("policy", ["Allow", "Replace"])
async def test_only_forbid_holds_a_due_retry_behind_a_start_with_values(
    dag_cron, tmp_path, policy
):
    cron = await dag_cron(
        _ladder_yaml(policy=policy, delay=0.3, once=tmp_path / "once")
    )
    ladder = await _arm_ladder(cron)
    await cron.start_job("report", {"region": "us", "act": "hold"})
    (one_off,) = cron.running_jobs["report"]
    # the retry comes due while the one-off runs, and the policy admits it:
    # Allow starts it beside the one-off, and Replace in its place
    await _wait_until(ladder.task.done, tries=500)
    await _drain_pending(cron)
    assert (await _retry_records(cron))[-1] == ("settled", "launched", 1)
    (attempt,) = [r for r in cron.running_jobs["report"] if r is not one_off]
    assert attempt.retry_state is ladder and not attempt.supplied_params
    assert one_off.replaced == (policy == "Replace")
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    one_off.cancelled = True
    await one_off.cancel()
    await _stop_run(cron, attempt)


async def test_a_start_the_concurrency_policy_refuses_says_so(dag_cron):
    cron = await _cron(
        dag_cron, command=[_PY, "-c", "import time; time.sleep(30)"]
    )
    cron.cron_jobs["report"].concurrencyPolicy = "Forbid"
    await cron.start_job("report")
    # the second start launches nothing, so it answers no values either
    with pytest.raises(ApiActionError) as err:
        await cron.start_job("report", {"region": "us"})
    assert err.value.status == 409
    assert err.value.message == (
        "job 'report' was not started: its concurrencyPolicy (Forbid) "
        "admitted no new run"
    )
    (running,) = cron.running_jobs["report"]
    assert running.params == _DEFAULTS
    running.cancelled = True
    await running.cancel()
    await _finish(cron)


async def test_a_start_that_waits_for_the_cluster_slot_says_so(dag_cron):
    # Replace across the cluster: another node holds the job's slot, so
    # this node asks it to yield and the start waits
    cron, foreign = await _slot_cron(dag_cron)
    ladder = JobRetryState(1.0, 2.0, 60.0)
    ladder.count = 1
    cron.retry_state["s"] = ladder
    assert await cron.start_job("s") == {
        "queued": None,
        "params": {"region": "eu"},
        "pending": True,
    }
    assert not cron.running_jobs.get("s")
    pursuit = cron._slot_pursuits["s"]
    await cron.state_backend.release_lease(foreign)  # the other node yields
    await asyncio.wait_for(pursuit, timeout=30)
    # the run that starts is the one the caller asked for: a start without
    # values is one more attempt of the ladder
    (running,) = cron.running_jobs["s"]
    assert running.params == {"region": "eu"} and not running.supplied_params
    assert running.retry_state is ladder
    assert "s" not in cron._slot_pursuit_launch
    await _settle(cron)


async def test_a_launch_that_claims_the_cluster_slot_stands_the_wait_down(
    dag_cron,
):
    cron, foreign = await _slot_cron(dag_cron, command=_HOLD)
    assert (await cron.start_job("s", {"region": "us"}))["pending"] is True
    pursuit = cron._slot_pursuits["s"]
    await cron.state_backend.release_lease(foreign)  # the other node yields
    # the caller asks again before the pursuit's next poll and gets the slot
    assert await cron.start_job("s", {"region": "us"}) == {
        "queued": None,
        "params": {"region": "us"},
    }
    (running,) = cron.running_jobs["s"]
    # the launch that waited is older than this run, so it is never made
    assert "s" not in cron._slot_pursuit_launch
    await asyncio.wait([pursuit], timeout=30)
    assert pursuit.cancelled() and "s" not in cron._slot_pursuits
    assert cron.running_jobs["s"] == [running] and not running.replaced
    await _stop_run(cron, running)


async def test_a_scheduled_fire_that_claims_the_cluster_slot_runs_once(
    dag_cron,
):
    cron, foreign = await _slot_cron(dag_cron, command=_HOLD)
    job = cron.cron_jobs["s"]
    await cron.launch_scheduled_job(job)  # refused: a pursuit waits with it
    pursuit = cron._slot_pursuits["s"]
    await cron.state_backend.release_lease(foreign)  # the other node yields
    await cron.launch_scheduled_job(job)  # the next fire gets the slot
    (running,) = cron.running_jobs["s"]
    await asyncio.wait([pursuit], timeout=30)
    # the pursuit does not replace the newer run with the fire it held
    assert cron.running_jobs["s"] == [running] and not running.replaced
    assert pursuit.cancelled()
    await _stop_run(cron, running)


async def test_two_starts_that_wait_for_the_cluster_slot_hear_their_own_values(
    dag_cron,
):
    cron, foreign = await _slot_cron(dag_cron)
    # a retry sequence between attempts, which neither start is part of
    ladder = JobRetryState(1.0, 2.0, 60.0)
    ladder.count = 1
    cron.retry_state["s"] = ladder
    first, second = await asyncio.gather(
        cron.start_job("s", {"region": "us"}),
        cron.start_job("s", {"region": "ap"}),
    )
    assert first == {
        "queued": None,
        "params": {"region": "us"},
        "pending": True,
    }
    assert second == {
        "queued": None,
        "params": {"region": "ap"},
        "pending": True,
    }
    pursuit = cron._slot_pursuits["s"]
    await cron.state_backend.release_lease(foreign)
    await asyncio.wait_for(pursuit, timeout=30)
    # had both started, Replace would have let the newer one win
    (running,) = cron.running_jobs["s"]
    assert running.params == {"region": "ap"} and running.supplied_params
    await _settle(cron)
    # the launch that gave way carried no ladder, so the sequence stays
    assert cron.retry_state.pop("s") is ladder and not ladder.cancelled


async def _retry_waits_for_the_slot(dag_cron, tmp_path, pools=""):
    """A cluster-wide ``_LADDER`` daemon whose retry #1 came due while
    another node held the job's slot. The daemon consumed the retry, and
    the retry's launch waits in the Replace pursuit. Returns the other
    node's lease as well. ``pools`` is a ``pools:`` section to configure
    beside the job, which stays outside it.
    """
    cron = await dag_cron(
        pools
        + _ladder_yaml(
            policy="Replace",
            scope="cluster",
            delay=0.5,
            once=tmp_path / "once",
        ),
        extra_state="  slotTtlSeconds: 6\n",
    )
    ladder = await _arm_ladder(cron)
    # the failed run gave the slot back, and the other node takes it before
    # the retry comes due
    await _wait_until(lambda: "report" not in cron._slot_leases)
    foreign = await cron.state_backend.acquire_lease(
        "slots/report", "nodeB#tokB", 30.0
    )
    assert foreign is not None
    await _wait_until(ladder.task.done, tries=500)
    await _drain_pending(cron)
    assert (await _retry_records(cron))[-1] == ("settled", "launched", 1)
    assert cron._slot_pursuit_launch["report"] == {
        "with_retries": True,
        "params": None,
    }
    return cron, ladder, foreign


async def test_a_start_with_values_that_waits_in_a_retrys_place_ends_the_ladder(
    dag_cron, tmp_path
):
    cron, ladder, foreign = await _retry_waits_for_the_slot(dag_cron, tmp_path)
    job = cron.cron_jobs["report"]
    pursuit = cron._slot_pursuits["report"]
    # a start without values takes the retry's place in the pursuit, and
    # would run as the same attempt of the ladder
    assert (await cron.start_job("report"))["pending"] is True
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    # a start with values carries no ladder. The attempt gives way to it, so
    # the sequence has none left: it ends, and says so
    started = await cron.start_job("report", {"region": "us", "act": "pass"})
    assert started["pending"] is True
    assert "report" not in cron.retry_state and ladder.cancelled
    await _drain_pending(cron)
    assert (await _retry_records(cron))[-1] == ("settled", "replaced", 1)
    assert "retry" not in cron._job_to_dict("report", job)
    await cron.state_backend.release_lease(foreign)  # the other node yields
    await asyncio.wait_for(pursuit, timeout=30)
    (running,) = cron.running_jobs["report"]
    assert running.supplied_params and running.retry_state is None
    await _settle(cron)
    assert cron.last_run["report"].outcome == "success"


async def test_a_catch_up_run_that_waits_in_a_retrys_place_ends_the_ladder(
    dag_cron, tmp_path
):
    cron, ladder, foreign = await _retry_waits_for_the_slot(dag_cron, tmp_path)
    job = cron.cron_jobs["report"]
    pursuit = cron._slot_pursuits["report"]
    # a backfill launches without retries (see _backfill). Its launch takes
    # the retry's place in the pursuit, and the sequence ends as it does
    # for a start with values
    assert await cron.maybe_launch_job(job, with_retries=False) is False
    assert "report" not in cron.retry_state and ladder.cancelled
    await _drain_pending(cron)
    assert (await _retry_records(cron))[-1] == ("settled", "replaced", 1)
    assert "retry" not in cron._job_to_dict("report", job)
    await cron.state_backend.release_lease(foreign)  # the other node yields
    await asyncio.wait_for(pursuit, timeout=30)
    (running,) = cron.running_jobs["report"]
    assert not running.supplied_params and running.retry_state is None
    await _stop_run(cron, running)


@pytest.mark.parametrize("launch", ["values", "catch-up", "no values"])
async def test_a_launch_that_claims_the_cluster_slot_ahead_of_a_waiting_retry(
    dag_cron, tmp_path, launch
):
    cron, ladder, foreign = await _retry_waits_for_the_slot(dag_cron, tmp_path)
    job = cron.cron_jobs["report"]
    pursuit = cron._slot_pursuits["report"]
    await cron.state_backend.release_lease(foreign)  # the other node yields
    # a launch gets the slot before the pursuit's next poll, so the pursuit
    # never makes the retry's launch
    if launch == "catch-up":
        assert await cron.maybe_launch_job(job, with_retries=False) is True
    else:
        values = (
            {"region": "us", "act": "hold"} if launch == "values" else None
        )
        assert "pending" not in await cron.start_job("report", values)
    (running,) = cron.running_jobs["report"]
    await asyncio.wait([pursuit], timeout=30)
    assert pursuit.cancelled()
    await _drain_pending(cron)
    if launch == "no values":
        # the run is the attempt in the retry's place
        assert running.retry_state is ladder
        assert cron.retry_state["report"] is ladder and not ladder.cancelled
        assert (await _retry_records(cron))[-1] == ("settled", "launched", 1)
    else:
        # the run carries no ladder, so the sequence has no attempt left
        assert running.retry_state is None
        assert "report" not in cron.retry_state and ladder.cancelled
        assert (await _retry_records(cron))[-1] == ("settled", "replaced", 1)
    await _stop_run(cron, running)


@pytest.mark.parametrize("launch", ["values", "catch-up"])
async def test_a_launch_that_never_parks_leaves_the_retry_the_pursuit_took(
    dag_cron, tmp_path, launch
):
    cron, ladder, foreign = await _retry_waits_for_the_slot(dag_cron, tmp_path)
    job = cron.cron_jobs["report"]
    pursuit = cron._slot_pursuits["report"]
    cron._state_on_unavailable = "fail-closed"
    backend = cron.state_backend
    real_acquire, real_read = cron._acquire_slot_lease, backend.read_lease
    await backend.release_lease(foreign)  # the other node yields

    async def _no_answer(*args, **kwargs):
        # only this claim goes unanswered
        cron._acquire_slot_lease, backend.read_lease = real_acquire, real_read
        raise asyncio.TimeoutError

    async def _acquire_after_the_pursuit_took_its_launch(_backend, _lease):
        # the pursuit's poll sees the free slot and takes the retry's
        # launch while this claim is in flight
        await _wait_until(
            lambda: "report" not in cron._slot_pursuit_launch, tries=400
        )
        backend.read_lease = _no_answer
        return None

    cron._acquire_slot_lease = _acquire_after_the_pursuit_took_its_launch
    # a fail-closed claim that gets no answer starts nothing and parks
    # nothing, so the launch takes no attempt's place
    if launch == "catch-up":
        assert await cron.maybe_launch_job(job, with_retries=False) is None
    else:
        with pytest.raises(ApiActionError) as err:
            await cron.start_job("report", {"region": "us", "act": "hold"})
        assert err.value.status == 503
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    # the pursuit makes the retry's launch, and the run is the attempt
    await asyncio.wait_for(pursuit, timeout=30)
    (running,) = cron.running_jobs["report"]
    assert running.retry_state is ladder and not running.supplied_params
    await _drain_pending(cron)
    assert (await _retry_records(cron))[-1] == ("settled", "launched", 1)
    await _stop_run(cron, running)


async def test_a_start_that_cluster_forbid_refuses_is_not_taken_for_a_wait(
    dag_cron,
):
    cron, _foreign = await _slot_cron(dag_cron)
    job = cron.cron_jobs["s"]
    await cron.launch_scheduled_job(job)  # refused: a pursuit waits with it
    pursuit = cron._slot_pursuits["s"]
    # the launch that waits would run as the attempt of this retry sequence
    ladder = JobRetryState(1.0, 2.0, 60.0)
    ladder.count = 1
    cron.retry_state["s"] = ladder
    # under Forbid the other node's hold refuses the start outright, and
    # the pursuit that waits holds another launch
    job.concurrencyPolicy = "Forbid"
    with pytest.raises(ApiActionError) as err:
        await cron.start_job("s", {"region": "us"})
    assert err.value.status == 409
    assert err.value.message == (
        "job 's' was not started: its concurrencyPolicy (Forbid) admitted "
        "no new run"
    )
    # a start that is refused takes no launch's place, so the sequence stays
    assert cron.retry_state.pop("s") is ladder and not ladder.cancelled
    pursuit.cancel()
    await asyncio.wait([pursuit])
    assert not cron.running_jobs.get("s")


async def test_a_job_reads_its_values_with_the_param_command(dag_cron, tmp_path):
    out = tmp_path / "out.json"
    script = (
        "import json, subprocess, sys\n"
        "run = lambda *a: subprocess.run([sys.executable, '-m', 'cronstable',"
        " 'param', *a], capture_output=True, text=True)\n"
        "seen = {'get': run('get', 'region').stdout,"
        " 'bool': run('get', 'dry').stdout,"
        " 'missing': run('get', 'nope').returncode,"
        " 'list': run('list').stdout.split(),"
        " 'dump': json.loads(run('dump').stdout)}\n"
        "open(" + repr(str(out)) + ", 'w').write(json.dumps(seen))\n"
    )
    cron = await _cron(dag_cron, command=[_PY, "-c", script])
    await cron.start_job("report", {"region": "us", "dry": True})
    await _finish(cron)
    assert cron.last_run["report"].outcome == "success"
    seen = json.loads(out.read_text())
    assert seen == {
        "get": "us\n",
        "bool": "true\n",
        "missing": 4,
        "list": ["dry", "region", "rows"],
        "dump": {"region": "us", "rows": 100, "dry": True},
    }


async def test_values_reach_a_job_without_a_state_section():
    # no `state:` means no loopback endpoint: the variables still arrive
    cron = Cron(None, config_yaml=_JOBS)
    for job in cron.cron_jobs.values():
        job.command = _ECHO
    assert cron._job_api is None
    started = await cron.start_job("report", {"rows": 7})
    assert started["params"] == {**_DEFAULTS, "rows": 7}
    (running,) = cron.running_jobs["report"]
    await running.wait()
    await cron._handle_finished_job(running)
    assert _printed(cron, "report") == {
        "CRONSTABLE_PARAM_DRY": "false",
        "CRONSTABLE_PARAM_REGION": "eu",
        "CRONSTABLE_PARAM_ROWS": "7",
    }
    assert cron.last_run["report"].params == started["params"]


async def test_a_job_with_params_drops_the_ones_the_daemon_inherited(
    dag_cron, monkeypatch
):
    monkeypatch.setenv("CRONSTABLE_PARAM_STALE", "from-the-daemon")
    monkeypatch.setenv("CRONSTABLE_PARAM_REGION", "from-the-daemon")
    cron = await _cron(dag_cron)
    await cron.start_job("report")
    await cron.start_job("plain")
    await _finish(cron)
    # the declaring job sees its own run's values and no others
    assert _printed(cron, "report") == {
        "CRONSTABLE_PARAM_DRY": "false",
        "CRONSTABLE_PARAM_REGION": "eu",
        "CRONSTABLE_PARAM_ROWS": "100",
    }
    # a job that declares none keeps the environment it inherits
    assert _printed(cron, "plain") == {
        "CRONSTABLE_PARAM_REGION": "from-the-daemon",
        "CRONSTABLE_PARAM_STALE": "from-the-daemon",
    }


# --------------------------------------------------------------------------
# The carriers: the run record, the in-flight record, the pool entry
# --------------------------------------------------------------------------


def test_the_run_record_round_trips_the_values():
    cron = Cron(None, config_yaml=_JOBS)
    output = cron.last_run  # any object: the stream is not serialized
    info = JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=None,
        finished_at=__import__("datetime").datetime(
            2026, 1, 1, tzinfo=__import__("datetime").timezone.utc
        ),
        fail_reason=None,
        output=output,
        params={"region": "us", "rows": 5, "dry": True},
    )
    record = info.to_dict(include_series=True)
    assert record["params"] == {"region": "us", "rows": 5, "dry": True}
    back = _job_run_info_from_dict(json.loads(json.dumps(record)))
    assert back.params == record["params"]
    # a record with no values, or a damaged one, rehydrates with none
    del record["params"]
    assert _job_run_info_from_dict(record).params is None
    record["params"] = ["not", "a", "map"]
    assert _job_run_info_from_dict(record).params is None


def test_the_run_record_marks_a_start_that_supplied_values():
    def info(**kwargs):
        return JobRunInfo(
            outcome="success",
            exit_code=0,
            started_at=None,
            finished_at=datetime.datetime(
                2026, 1, 1, tzinfo=datetime.timezone.utc
            ),
            fail_reason=None,
            output=None,  # the stream is not serialized
            params={"region": "us"},
            **kwargs,
        )

    record = info(supplied_params=True).to_dict()
    # the run instant goes under a key of its own, so the fold over ranAt
    # that the retry ladder's guards read never sees the run
    assert record["suppliedParams"] is True and "ranAt" not in record
    assert record["suppliedRanAt"] == record["finished_at"]
    back = _job_run_info_from_dict(json.loads(json.dumps(record)))
    assert back.supplied_params and not back.supersedes_retries
    # every other run keeps the record it has without the marker
    plain = info().to_dict()
    assert plain["ranAt"] == plain["finished_at"]
    assert "suppliedParams" not in plain and "suppliedRanAt" not in plain
    assert _job_run_info_from_dict(plain).supersedes_retries
    # a damaged marker reads as a run that supplied none
    record["suppliedParams"] = "yes"
    assert not _job_run_info_from_dict(record).supplied_params


async def test_only_catch_up_counts_a_start_that_supplied_values(dag_cron):
    cron = await _cron(dag_cron)
    await cron.launch_scheduled_job(cron.cron_jobs["report"])
    await _finish(cron)
    await cron.start_job("report", {"region": "us"})
    await _finish(cron)
    stream = cron._run_stream("report")
    scheduled, supplied = await cron.state_backend.list_records(stream)

    async def newest_run(**kwargs):
        return await cron.durable_last_completed_at("report", **kwargs)

    # the retry ladder's guards read the newest run that took the defaults,
    # and catch-up reads the newest run of all
    assert await newest_run() == scheduled["ranAt"]
    assert await newest_run(include_supplied=True) == supplied["finished_at"]
    assert supplied["suppliedRanAt"] == supplied["finished_at"]
    assert supplied["suppliedParams"] is True and "ranAt" not in supplied
    # a long pause that buries both rows under held slots moves neither
    for second in range(RUN_HISTORY_LIMIT + 10):
        await cron.state_backend.append_record(
            stream,
            {
                "outcome": "skipped",
                "skip_reason": "paused",
                "finished_at": "2999-01-01T00:00:{:02d}+00:00".format(second),
            },
        )
    assert await newest_run() == scheduled["ranAt"]
    assert await newest_run(include_supplied=True) == supplied["finished_at"]


async def test_the_ledger_and_the_runs_route_carry_the_values(dag_cron):
    import aiohttp

    cron = await _cron(dag_cron)
    await cron.start_job("report", {"region": "us"})
    await _finish(cron)
    stored = await cron.state_backend.list_records(cron._run_stream("report"))
    assert stored[-1]["params"] == {**_DEFAULTS, "region": "us"}
    base = await _start_web(cron)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(base + "/jobs/report/runs") as r:
                assert r.status == 200
                body = await r.json()
    finally:
        await cron.start_stop_web_app(None)
    assert body["runs"][0]["params"] == {**_DEFAULTS, "region": "us"}


async def test_the_inflight_record_carries_the_values_to_reconciliation(
    dag_cron,
):
    cron = await _cron(dag_cron, command=[_PY, "-c", "import time; time.sleep(30)"])
    await cron.start_job("report", {"rows": 9})
    await _drain_pending(cron)
    stream = cron._inflight_stream("report")
    (record,) = await cron.state_backend.list_records(stream)
    assert record["kind"] == "open"
    assert record["params"] == {**_DEFAULTS, "rows": 9}
    # the run a crash interrupted is recorded with the values it had
    cron._reconcile_open_record(
        "report", cron.cron_jobs["report"], record, "reconciled-crash"
    )
    assert cron.last_run["report"].outcome == "unknown"
    assert cron.last_run["report"].params == {**_DEFAULTS, "rows": 9}
    await _drain_pending(cron)
    rows = await cron.state_backend.list_records(cron._run_stream("report"))
    assert rows[-1]["outcome"] == "unknown"
    assert rows[-1]["params"] == {**_DEFAULTS, "rows": 9}
    (running,) = cron.running_jobs["report"]
    running.cancelled = True
    await running.cancel()
    await _finish(cron)
    assert cron.last_run["report"].outcome == "cancelled"
    assert cron.last_run["report"].params == {**_DEFAULTS, "rows": 9}


@pytest.mark.parametrize("on_missed", ["skip", "run-once", "run-all"])
async def test_a_crash_during_a_start_with_values_leaves_the_retry_ladder(
    dag_cron, on_missed
):
    yaml = _ladder_yaml(on_missed=on_missed)
    cron = await dag_cron(yaml)
    await _arm_ladder(cron)
    await cron.start_job("report", {"region": "us", "act": "hold"})
    await _drain_pending(cron)
    inflight = cron._inflight_stream("report")
    opened = (await cron.state_backend.list_records(inflight))[-1]
    # the crash: the run dies with its daemon and records no completion
    (running,) = cron.running_jobs["report"]
    running.proc.kill()
    await running.wait()
    cron._remove_running_instance(running)
    await cron._job_api.finish_run(running.state_token)
    await _stop_ladder(cron)
    restarted = await dag_cron(yaml)
    await _drain_pending(restarted)
    # the next daemon reconciles the interrupted run, and does not take it
    # for the one that resolved the retry
    assert (await _retry_records(restarted))[-1] == ("pending", None, 1)
    assert restarted.retry_state["report"].count == 1
    assert restarted.last_run["report"].outcome == "unknown"
    # the in-flight record carried the marker to the reconciled row
    assert opened["kind"] == "open" and opened["suppliedParams"] is True
    runs = restarted._run_stream("report")
    row = (await restarted.state_backend.list_records(runs))[-1]
    assert row["outcome"] == "unknown" and row["suppliedParams"] is True
    # only onMissed: skip stamps a run instant on an interrupted run
    assert "ranAt" not in row
    assert ("suppliedRanAt" in row) == (on_missed == "skip")
    await _stop_ladder(restarted)


_POOLED = """
pools:
  database:
    slots: 1
    maxQueued: 4
""" + _JOBS.replace(
    "    captureStdout: true\n    params:",
    "    captureStdout: true\n    pool: database\n    params:",
)


async def test_the_pool_entry_carries_the_values_through_the_queue(
    dag_cron, monkeypatch
):
    cron = await _cron(dag_cron, _POOLED)
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    started = await cron.start_job("report", {"region": "us", "rows": 2})
    assert started["queued"] is not None
    assert started["params"] == {"region": "us", "rows": 2, "dry": False}
    # a second start with no values queues behind it, with none in its entry
    second = await cron.start_job("report")
    assert second["params"] == _DEFAULTS
    assert not cron.running_jobs
    pool = await cron.state_backend.read_document(POOL_NAMESPACE, "database")
    entries = {e["id"]: e["payload"] for e in pool["entries"].values()}
    assert entries[started["queued"]]["params"] == started["params"]
    assert "params" not in entries[second["queued"]]
    # each admitted run launches with what its entry carried
    await cron._pools.tick()
    (running,) = cron.running_jobs["report"]
    assert running.params == started["params"]
    assert running.retry_state is None and running.supplied_params
    await _finish(cron)
    assert _printed(cron, "report")["CRONSTABLE_PARAM_REGION"] == "us"
    await cron._pools.tick()
    (running,) = cron.running_jobs["report"]
    assert running.params == _DEFAULTS and not running.supplied_params
    await _finish(cron)
    assert _printed(cron, "report")["CRONSTABLE_PARAM_REGION"] == "eu"


async def _pool_entries(cron):
    """The entries of the ``database`` pool, oldest first."""
    pool = await cron.state_backend.read_document(POOL_NAMESPACE, "database")
    return sorted(pool["entries"].values(), key=lambda e: e["queuedAt"])


def _retry_count(entry):
    """The ladder position a pool entry carries, or ``None`` for no ladder."""
    return (entry["payload"].get("retry") or {}).get("count")


@pytest.mark.parametrize("policy", ["Allow", "Forbid"])
async def test_pooled_cancel_of_a_start_with_values_keeps_the_queued_retry(
    dag_cron, monkeypatch, policy
):
    cron = await dag_cron(_ladder_yaml(policy=policy, delay=0.5, slots=1))
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    await cron.launch_scheduled_job(cron.cron_jobs["report"])
    await cron._pools.tick()
    await _settle(cron)  # the scheduled run fails and arms retry #1
    ladder = cron.retry_state["report"]
    await cron.start_job("report", {"region": "us", "act": "hold"})
    await cron._pools.tick()  # the pool admits the one-off
    (one_off,) = cron.running_jobs["report"]
    assert one_off.supplied_params
    # the retry comes due while the one-off runs, and queues behind it.
    # Forbid holds it there too: a pooled retry waits in its queue entry
    await _wait_until(ladder.task.done)
    await _drain_pending(cron)
    assert await cron.cancel_job_by_name("report") == 1
    await _settle(cron)
    # the cancellation leaves the default-valued run its retry
    assert cron.retry_state["report"] is ladder and not ladder.cancelled
    assert (await _retry_records(cron))[-1] == ("pending", None, 1)
    (retry,) = [e for e in await _pool_entries(cron) if _retry_count(e) == 1]
    assert retry["state"] == "queued"
    assert not retry["payload"].get("retryCancelled")
    # which the pool then admits as attempt #1
    await cron._pools.tick()
    (attempt,) = cron.running_jobs["report"]
    assert attempt.retry_state.count == 1 and not attempt.supplied_params
    await _stop_run(cron, attempt)


async def test_pooled_cancel_of_a_start_with_values_keeps_the_next_runs_ladder(
    dag_cron, monkeypatch
):
    cron = await dag_cron(_ladder_yaml(slots=1))
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    reports = _reports(monkeypatch)
    await cron.start_job("report", {"region": "us", "act": "hold"})
    await cron._pools.tick()  # the one-off holds the only slot
    # a scheduled fire queues behind it with a fresh ladder
    await cron.launch_scheduled_job(cron.cron_jobs["report"])
    fresh = cron.retry_state["report"]
    assert await cron.cancel_job_by_name("report") == 1
    await _settle(cron)
    assert cron.retry_state["report"] is fresh and not fresh.cancelled
    (queued,) = [e for e in await _pool_entries(cron) if _retry_count(e) == 0]
    assert queued["state"] == "queued"
    assert not queued["payload"].get("retryCancelled")
    # so the scheduled run is admitted with its ladder, and its failure
    # arms retry #1
    await cron._pools.tick()
    (scheduled,) = cron.running_jobs["report"]
    assert scheduled.retry_state is not None
    await _settle(cron)
    assert reports == [("failure", "eu")]
    assert (await _retry_records(cron))[-1] == ("pending", None, 1)
    await _stop_ladder(cron)


async def test_pooled_start_with_values_that_replaces_a_retry_ends_the_ladder(
    dag_cron, tmp_path, monkeypatch
):
    cron = await dag_cron(
        _ladder_yaml(
            policy="Replace", delay=0.3, once=tmp_path / "once", slots=2
        )
    )
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    job = cron.cron_jobs["report"]
    await cron.launch_scheduled_job(job)
    await cron._pools.tick()
    await _settle(cron)  # the scheduled run fails and arms retry #1
    await _wait_until(cron.retry_state["report"].task.done)
    await _drain_pending(cron)
    await cron._pools.tick()  # the pool admits the retry, which stays up
    (attempt,) = cron.running_jobs["report"]
    assert attempt.retry_state.count == 1
    await cron.start_job("report", {"region": "us", "act": "pass"})
    await cron._pools.tick()  # the pool admits the one-off, which replaces it
    assert attempt.replaced
    await _settle(cron)
    assert "report" not in cron.retry_state
    assert (await _retry_records(cron))[-1] == ("settled", "replaced", 1)
    assert "retry" not in cron._job_to_dict("report", job)


_POOLED_SLOT = (
    "pools:\n  database:\n    slots: 1\n    maxQueued: 4\n"
    + _SLOT.replace(
        "    captureStdout: true\n",
        "    captureStdout: true\n    pool: database\n",
    )
)


async def test_a_pooled_start_waits_for_the_cluster_slot_in_its_queue_entry(
    dag_cron, monkeypatch
):
    cron, foreign = await _slot_cron(dag_cron, _POOLED_SLOT, command=_HOLD)
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    started = await cron.start_job("s", {"region": "us"})
    assert started["queued"] is not None and "pending" not in started
    # the pool admits the entry and the other node's hold on the slot
    # refuses the launch, so the entry goes back to wait
    await cron._pools.tick()
    assert not cron.running_jobs.get("s")
    pursuit = cron._slot_pursuits["s"]
    await cron.state_backend.release_lease(foreign)  # the other node yields
    # the pursuit sees the slot free before the dispatcher ticks again. It
    # only asked the other node to yield: it starts no run of its own and
    # queues no second entry
    await asyncio.wait_for(pursuit, timeout=30)
    assert not cron.running_jobs.get("s")
    assert len(await _pool_entries(cron)) == 1
    await cron._pools.tick()  # the entry re-attempts itself
    (running,) = cron.running_jobs["s"]
    assert running.params == {"region": "us"} and running.supplied_params
    assert len(await _pool_entries(cron)) == 1
    await _stop_run(cron, running)


async def test_a_pooled_start_with_values_stands_down_a_waiting_retry(
    dag_cron, tmp_path, monkeypatch
):
    cron, ladder, foreign = await _retry_waits_for_the_slot(
        dag_cron,
        tmp_path,
        pools="pools:\n  database:\n    slots: 1\n    maxQueued: 4\n",
    )
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    pursuit = cron._slot_pursuits["report"]
    # a reload puts the job in the pool while the retry's launch waits
    cron.cron_jobs["report"].pool = "database"
    await cron.state_backend.release_lease(foreign)  # the other node yields
    await cron.start_job("report", {"region": "us", "act": "hold"})
    # the pool admits the one-off, which gets the cluster slot before the
    # pursuit's next poll
    await cron._pools.tick()
    (running,) = cron.running_jobs["report"]
    await asyncio.wait([pursuit], timeout=30)
    assert pursuit.cancelled()
    await _drain_pending(cron)
    # the entry of a manual start launches with retries and carries no
    # ladder, so the sequence ends as it does for a start outside a pool
    assert running.supplied_params and running.retry_state is None
    assert "report" not in cron.retry_state and ladder.cancelled
    assert (await _retry_records(cron))[-1] == ("settled", "replaced", 1)
    await _stop_run(cron, running)


async def test_a_pooled_launch_queues_the_values_it_is_given(
    dag_cron, monkeypatch
):
    # the launch path every caller shares, not only a manual start
    cron = await _cron(dag_cron, _POOLED)
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    values = {**_DEFAULTS, "rows": 4}
    assert await cron.maybe_launch_job(
        cron.cron_jobs["report"], with_retries=False, params=values
    )
    pool = await cron.state_backend.read_document(POOL_NAMESPACE, "database")
    (entry,) = pool["entries"].values()
    assert entry["payload"]["params"] == values
    await cron._pools.cancel("database", entry["id"], "test over")


async def test_refused_values_queue_nothing(dag_cron, monkeypatch):
    cron = await _cron(dag_cron, _POOLED)
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    with pytest.raises(ParamError):
        await cron.start_job("report", {"region": "mars"})
    assert (await cron._pools.snapshot())[0]["queued"] == 0


# --------------------------------------------------------------------------
# HTTP and MCP
# --------------------------------------------------------------------------


async def test_http_start_with_params(dag_cron):
    import aiohttp

    cron = await _cron(dag_cron)
    base = await _start_web(cron)
    url = base + "/jobs/report/start"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(base + "/jobs") as r:
                listed = {j["name"]: j for j in await r.json()}
            assert [p["name"] for p in listed["report"]["params"]] == [
                "region",
                "rows",
                "dry",
            ]
            assert listed["report"]["params"][1] == {
                "name": "rows",
                "type": "integer",
                "default": 100,
                "minimum": 1,
            }
            assert "params" not in listed["plain"]
            async with s.post(url, json={"params": {"region": "us"}}) as r:
                assert r.status == 200
                assert await r.json() == {
                    "started": "report",
                    "params": {**_DEFAULTS, "region": "us"},
                }
            await _finish(cron)
            assert _printed(cron, "report")["CRONSTABLE_PARAM_REGION"] == "us"
            # no body, an empty object, and an empty map start the job
            # with its defaults
            for kwargs in ({}, {"json": {}}, {"json": {"params": {}}}):
                async with s.post(url, **kwargs) as r:
                    assert r.status == 200, kwargs
                    assert await r.json() == {
                        "started": "report",
                        "params": _DEFAULTS,
                    }
                await _finish(cron)
            # a body that is not a JSON object, and one that holds another
            # field, start nothing: a misspelled `params` is not dropped
            for kwargs in (
                {"data": b"not json"},
                {"json": [1, 2]},
                {"json": {"param": {"region": "us"}}},
            ):
                async with s.post(url, **kwargs) as r:
                    assert r.status == 400, kwargs
                    assert (await r.json())["error"]
            async with s.post(url, json={"param": {}, "params": {}}) as r:
                assert (await r.json())["error"] == (
                    'unknown start field "param"; the body accepts params'
                )
            assert not cron.running_jobs
            # refused values start nothing and say why
            async with s.post(url, json={"params": {"rows": "5", "x": 1}}) as r:
                assert r.status == 400
                assert await r.json() == {
                    "error": "invalid parameters for job 'report'",
                    "paramErrors": {
                        "rows": "must be an integer",
                        "x": "is not a declared parameter",
                    },
                }
            async with s.post(url, json={"params": [1]}) as r:
                assert r.status == 400
            assert not cron.running_jobs
            # a job that declares none
            plain = base + "/jobs/plain/start"
            async with s.post(plain, json={"params": {"a": 1}}) as r:
                assert r.status == 400
                body = await r.json()
                assert body["error"] == "job 'plain' declares no parameters"
                assert body["paramErrors"] == {
                    "a": "is not a declared parameter"
                }
            async with s.post(plain) as r:
                assert r.status == 200
                assert await r.json() == {"started": "plain"}
            await _finish(cron)
            async with s.post(base + "/jobs/ghost/start") as r:
                assert r.status == 404
    finally:
        await cron.start_stop_web_app(None)


async def test_http_start_that_waits_for_the_cluster_slot(dag_cron):
    import aiohttp

    cron, foreign = await _slot_cron(dag_cron)
    ladder = JobRetryState(1.0, 2.0, 60.0)
    ladder.count = 1
    cron.retry_state["s"] = ladder
    base = await _start_web(cron)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                base + "/jobs/s/start", json={"params": {"region": "us"}}
            ) as r:
                # the request is accepted, and nothing runs yet
                assert r.status == 200
                assert await r.json() == {
                    "started": "s",
                    "params": {"region": "us"},
                    "pending": True,
                }
    finally:
        await cron.start_stop_web_app(None)
    assert not cron.running_jobs.get("s")
    pursuit = cron._slot_pursuits["s"]
    await cron.state_backend.release_lease(foreign)  # the other node yields
    await asyncio.wait_for(pursuit, timeout=30)
    # this node asked the holder of that lease to yield
    (asked,) = await cron.state_backend.list_records("slots/s")
    assert asked["kind"] == "cancel" and asked["fence"] == foreign.fence
    # and the run that starts is the one the caller asked for: the supplied
    # values, outside the retry ladder
    (running,) = cron.running_jobs["s"]
    assert running.supplied_params and running.retry_state is None
    await _settle(cron)
    assert _printed(cron, "s") == {"CRONSTABLE_PARAM_REGION": "us"}
    assert cron.retry_state.pop("s") is ladder


async def test_http_start_answers_503_when_the_slot_claim_gets_no_answer(
    dag_cron, monkeypatch
):
    import aiohttp

    cron = await _cron(dag_cron, _SLOT)
    cron._state_on_unavailable = "fail-closed"

    async def _no_answer(*args, **kwargs):
        raise asyncio.TimeoutError

    monkeypatch.setattr(cron.state_backend, "acquire_lease", _no_answer)
    monkeypatch.setattr(cron.state_backend, "read_lease", _no_answer)
    base = await _start_web(cron)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(base + "/jobs/s/start") as r:
                # the store's silence is not the concurrency policy's refusal
                assert r.status == 503
                assert await r.json() == {
                    "error": "job 's' was not started: the state store "
                    "cannot answer for its cluster concurrency slot, and "
                    "onStoreUnavailable is fail-closed"
                }
    finally:
        await cron.start_stop_web_app(None)
    # nothing started, and nothing waits to start later
    assert not cron.running_jobs and not cron._slot_pursuits


async def test_http_start_needs_the_params_scope_to_choose_values(dag_cron):
    import aiohttp

    cron = await _cron(dag_cron)
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "ui": False,
            "authTokens": [
                {"value": "t-control", "scopes": ["control"], "label": "ops"},
                {
                    "value": "t-params",
                    "scopes": ["control", "params"],
                    "label": "release",
                },
            ],
        }
    )
    url = "http://127.0.0.1:{}/jobs/report/start".format(
        cron.web_runner.addresses[0][1]
    )

    def auth(token):
        return {"Authorization": "Bearer " + token}

    try:
        async with aiohttp.ClientSession() as s:
            body = {"params": {"region": "us"}}
            async with s.post(url, json=body, headers=auth("t-control")) as r:
                assert r.status == 403
                assert "'params' permission" in (await r.json())["error"]
            assert not cron.running_jobs
            # the same token starts the job with its defaults
            async with s.post(url, headers=auth("t-control")) as r:
                assert r.status == 200
                assert (await r.json())["params"] == _DEFAULTS
            await _finish(cron)
            async with s.post(url, json=body, headers=auth("t-params")) as r:
                assert r.status == 200
                assert (await r.json())["params"]["region"] == "us"
            await _finish(cron)
    finally:
        await cron.start_stop_web_app(None)


async def test_http_start_of_a_pooled_job_answers_202_with_the_values(
    dag_cron, monkeypatch
):
    import aiohttp

    cron = await _cron(dag_cron, _POOLED)
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    base = await _start_web(cron)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                base + "/jobs/report/start", json={"params": {"rows": 3}}
            ) as r:
                assert r.status == 202
                body = await r.json()
    finally:
        await cron.start_stop_web_app(None)
    assert body["queued"] == "report" and body["pool"] == "database"
    assert body["params"] == {**_DEFAULTS, "rows": 3}
    await cron._pools.cancel("database", body["queueId"], "test over")
