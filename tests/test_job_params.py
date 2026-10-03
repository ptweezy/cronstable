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

import json
import sys

import pytest

from cronstable import fingerprint, params
from cronstable.config import ConfigError, parse_config_string
from cronstable.cron import (
    Cron,
    JobRunInfo,
    _job_run_info_from_dict,
)
from cronstable.job import JobRetryState
from cronstable.params import ParamError
from cronstable.pools import NAMESPACE as POOL_NAMESPACE
from tests._helpers import _drain_pending, _reap_running
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
    # the wrapper the scheduler-side callers use
    assert await cron.start_job_by_name("report") is None
    await _finish(cron)
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
    # with none, it starts as it always has and records no values
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
    # no values: the run is one more attempt of the ladder, as before
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
    assert running.retry_state is None
    await _finish(cron)
    assert _printed(cron, "report")["CRONSTABLE_PARAM_REGION"] == "us"
    await cron._pools.tick()
    (running,) = cron.running_jobs["report"]
    assert running.params == _DEFAULTS
    await _finish(cron)
    assert _printed(cron, "report")["CRONSTABLE_PARAM_REGION"] == "eu"


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
            # no body, an empty map, a body that is not JSON, and a JSON
            # body with other keys all start the job with its defaults
            for kwargs in (
                {},
                {"json": {"params": {}}},
                {"data": b"not json"},
                {"json": {"other": 1}},
                {"json": [1, 2]},
            ):
                async with s.post(url, **kwargs) as r:
                    assert r.status == 200, kwargs
                    assert await r.json() == {
                        "started": "report",
                        "params": _DEFAULTS,
                    }
                await _finish(cron)
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
