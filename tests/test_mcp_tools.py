"""The MCP server's tool/resource/prompt handlers against a real ``Cron``.

``test_mcp.py`` covers the protocol plumbing (initialize, visibility, the
HTTP transport, config); this file drives the individual tool handlers
through ``MCPHandler.handle_message``: the observe/act surface against a
stateless in-process :class:`~cronstable.cron.Cron`, and the dags/state
surface against a real ``FilesystemStateBackend`` + DAG scheduler, reusing
the ``test_state_dag_run.py`` harness (real task subprocesses via
``[sys.executable, ...]`` argv, a pump instead of the daemon's reaper).
"""

import datetime
import json
import sys
import types

import jsonschema
import pytest

from cronstable import mcp as mcp_mod
from cronstable.config import _build_mcp_config, parse_config_string
from cronstable.cron import (
    PAUSE_BY_MAX,
    PAUSE_DEFAULT_SECONDS,
    WEB_ROUTES,
    Cron,
    JobRunInfo,
    _required_web_scope,
)
from cronstable.job import JobOutputStream
from cronstable.mcp import MCPHandler
from cronstable.resources import ResourceUsage
from tests.test_state_dag_run import (
    _drive,
    _set_cmd,
    _teardown,
)
from tests.test_state_dag_run import (
    _make_cron as _make_state_cron,
)

_PY = sys.executable
_UTC = datetime.timezone.utc

_YAML = """
jobs:
  - name: hello
    command: echo hi
    schedule: "* * * * *"
  - name: nightly
    command: backup
    schedule: "0 3 * * *"
    enabled: false
  - name: heavy
    command: echo hi
    schedule: "*/5 * * * *"
    monitorResources:
      interval: 0.5
      history: 50
"""

_ALL_TOOLSETS = ["observe", "dags", "state", "act"]


def _handler(mcp=None, yaml=_YAML):
    cron = Cron(None, config_yaml=yaml)
    cron.web_config = {}
    cfg = _build_mcp_config(
        {
            "enabled": True,
            "readOnly": False,
            "toolsets": _ALL_TOOLSETS,
            **(mcp or {}),
        }
    )
    return MCPHandler(cron, cfg)


async def _req(handler, method, params=None, mid=1, notif=False):
    msg = {"jsonrpc": "2.0", "method": method}
    if not notif:
        msg["id"] = mid
    if params is not None:
        msg["params"] = params
    return await handler.handle_message(msg)


async def _call(handler, name, arguments=None):
    resp = await _req(
        handler, "tools/call", {"name": name, "arguments": arguments or {}}
    )
    assert "error" not in resp, resp
    _check_result(handler, name, resp["result"])
    return resp["result"]


def _check_result(handler, name, result):
    """What every tool result keeps: an error is one readable block; a
    success is its summary, then structuredContent as JSON text, and
    conforms to the tool's declared outputSchema."""
    if result.get("isError"):
        assert len(result["content"]) == 1, result
        return
    _summary, data = result["content"]
    assert json.loads(data["text"]) == result["structuredContent"]
    schema = handler._tool_by_name[name]["listing"].get("outputSchema")
    if schema is not None:
        jsonschema.Draft202012Validator(schema).validate(
            result["structuredContent"]
        )


def _run_info(outcome, *, dur=1.0, exit_code=0):
    now = datetime.datetime.now(_UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=now - datetime.timedelta(seconds=dur),
        finished_at=now,
        fail_reason=None,
        output=JobOutputStream(),
    )


# ---------------------------------------------------------------------------
# module helpers: _dumps fallback, the Prometheus exposition parser
# ---------------------------------------------------------------------------


def test_dumps_falls_back_for_nonportable_values():
    # a non-finite float fails the fleet-portability gate of _json but must
    # not 500 a transient MCP response: the stdlib fallback encodes it.
    assert mcp_mod._dumps(float("inf")) == b"Infinity"
    assert mcp_mod._dumps({"v": 1}) == b'{"v":1}'


_EXPO = """\
# HELP cronstable_job_runs_total Total runs.
# TYPE cronstable_job_runs_total counter
cronstable_job_runs_total{job="hello"} 3
cronstable_job_runs_total{job="nightly"} 1
cronstable_up 1
{not a metric line}
process_cpu_seconds_total NaN
"""


def test_parse_prometheus_filters_and_caps():
    samples, total = mcp_mod._parse_prometheus(_EXPO, "runs_total", 1)
    assert total == 2  # both labeled samples matched...
    assert len(samples) == 1  # ...but the page is capped at limit
    assert samples[0] == {
        "name": "cronstable_job_runs_total",
        "labels": '{job="hello"}',
        "value": "3",
    }


def test_parse_prometheus_unfiltered_keeps_values_as_strings():
    samples, total = mcp_mod._parse_prometheus(_EXPO, None, 100)
    assert total == 4  # comment + malformed lines skipped
    by_name = {s["name"]: s for s in samples}
    assert by_name["cronstable_up"]["labels"] == ""
    # a NaN gauge round-trips untouched because values stay strings
    assert by_name["process_cpu_seconds_total"]["value"] == "NaN"


def test_filter_metric_samples_matches_render_then_parse():
    # The MCP metrics query now filters structured samples straight from the
    # families (mcp._filter_metric_samples over prometheus.iter_family_samples)
    # instead of rendering the whole exposition and regex-reparsing it. Pin
    # the two paths together so they cannot drift: over a mix of counters,
    # gauges, info and histogram-bucket samples -- including special-char
    # label values, unlabelled samples and +Inf/NaN -- the structured result
    # must equal _parse_prometheus(render_families(...)) for every match/cap.
    from cronstable import prometheus

    fam_runs = prometheus.MetricFamily(
        "cronstable_job_runs", "counter", "runs"
    )
    fam_runs.add({"job_name": 'a"b\\c', "status": "success"}, 3)
    fam_runs.add({"job_name": "nightly", "status": "failure"}, 1)
    fam_up = prometheus.MetricFamily("cronstable_up", "gauge", "up")
    fam_up.add({}, 1)
    fam_info = prometheus.MetricFamily("cronstable_build", "info", "build")
    fam_info.add({"version": "1.2.3"}, 1)
    fam_dur = prometheus.MetricFamily(
        "cronstable_job_duration_seconds", "histogram", "dur"
    )
    fam_dur.add({"job_name": "nightly", "le": "1.0"}, 2, suffix="_bucket")
    fam_dur.add({"job_name": "nightly", "le": "+Inf"}, 5, suffix="_bucket")
    fam_empty = prometheus.MetricFamily("cronstable_none", "gauge", "none")
    families = [fam_runs, fam_up, fam_info, fam_dur, fam_empty]

    text = prometheus.render_families(families, openmetrics=False)
    for match in (None, "runs", "job", "up", "build", "nomatch"):
        for limit in (1, 3, 100):
            expected = mcp_mod._parse_prometheus(text, match, limit)
            got = mcp_mod._filter_metric_samples(
                prometheus.iter_family_samples(families), match, limit
            )
            assert got == expected, (match, limit, got, expected)


# ---------------------------------------------------------------------------
# handle_message protocol edges
# ---------------------------------------------------------------------------


async def test_missing_method_request_and_notification():
    h = _handler()
    resp = await h.handle_message({"jsonrpc": "2.0", "id": 1})
    assert resp["error"]["code"] == mcp_mod.INVALID_REQUEST
    assert resp["error"]["message"] == "missing method"
    assert await h.handle_message({"jsonrpc": "2.0"}) is None


async def test_invalid_params_request_and_notification():
    h = _handler()
    resp = await _req(h, "ping", params=[1])
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS
    assert await _req(h, "ping", params=[1], notif=True) is None


async def test_tools_call_requires_string_name_and_object_arguments():
    h = _handler()
    resp = await _req(h, "tools/call", {"arguments": {}})
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS
    resp = await _req(
        h, "tools/call", {"name": "cron_get_status", "arguments": [1]}
    )
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS


async def test_mcp_error_in_notification_is_swallowed():
    h = _handler()
    # a notification whose handler raises MCPError (unknown tool) -> no reply
    resp = await _req(
        h, "tools/call", {"name": "ghost_tool", "arguments": {}}, notif=True
    )
    assert resp is None


async def test_internal_error_envelope_and_notification(monkeypatch):
    h = _handler()

    def boom():
        raise RuntimeError("kaput")

    monkeypatch.setattr(h._cron, "status_payload", boom)
    resp = await _req(
        h, "tools/call", {"name": "cron_get_status", "arguments": {}}
    )
    assert resp["error"]["code"] == mcp_mod.INTERNAL_ERROR
    assert resp["error"]["message"] == "internal error"  # no traceback leak
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_get_status", "arguments": {}},
        notif=True,
    )
    assert resp is None


# ---------------------------------------------------------------------------
# handle_http edges not covered by the transport tests
# ---------------------------------------------------------------------------


class FakeReq:
    def __init__(self, method="POST", headers=None, body=b""):
        self.method = method
        self.headers = headers or {}
        self._body = body
        self.content_length = len(body) if body else None
        # the mapping surface the auth middleware files the matched token
        # into (handle_http reads the caller's scopes from it)
        self.store = {}

    def __setitem__(self, key, value):
        self.store[key] = value

    def get(self, key, default=None):
        return self.store.get(key, default)

    async def read(self):
        return self._body


async def test_http_body_over_limit_after_read():
    h = _handler({"maxBodyBytes": 16})
    req = FakeReq(body=b"x" * 64)
    req.content_length = None  # chunked: only the read can see the size
    resp = await h.handle_http(req)
    assert resp.status == 413


async def test_http_empty_body_rejected():
    h = _handler()
    resp = await h.handle_http(FakeReq(body=b""))
    assert resp.status == 400
    assert b"empty request body" in resp.body


async def test_http_options_rejects_unlisted_and_missing_origin():
    h = _handler({"allowedOrigins": ["http://ok.example"]})
    resp = await h.handle_options(
        FakeReq(headers={"Origin": "http://evil.example"})
    )
    assert resp.status == 403
    resp = await h.handle_options(FakeReq())
    assert resp.status == 405


# ---------------------------------------------------------------------------
# initialize options / capability gating
# ---------------------------------------------------------------------------


async def test_initialize_custom_instructions():
    h = _handler({"instructions": "be careful"})
    resp = await _req(h, "initialize", {"protocolVersion": "2025-11-25"})
    assert resp["result"]["instructions"] == "be careful"


async def test_resources_and_prompts_can_be_disabled():
    cfg = _build_mcp_config({"enabled": True})
    cfg["resources"] = False
    cfg["prompts"] = False
    h = MCPHandler(_handler()._cron, cfg)
    caps = (await _req(h, "initialize", {}))["result"]["capabilities"]
    assert "resources" not in caps
    assert "prompts" not in caps
    resp = await _req(h, "resources/list")
    assert resp["error"]["code"] == mcp_mod.METHOD_NOT_FOUND
    resp = await _req(h, "prompts/list")
    assert resp["error"]["code"] == mcp_mod.METHOD_NOT_FOUND


# ---------------------------------------------------------------------------
# pagination fallbacks
# ---------------------------------------------------------------------------


async def test_page_and_limit_fall_back_on_junk():
    h = _handler()
    result = await _call(h, "cron_get_status", {"limit": "x", "offset": ["y"]})
    meta = result["structuredContent"]["page"]
    assert meta["offset"] == 0
    assert meta["limit"] == 200  # maxRows default
    assert meta["total"] == 3
    assert meta["nextOffset"] is None


async def test_page_next_offset():
    h = _handler()
    result = await _call(h, "cron_get_status", {"limit": 2})
    meta = result["structuredContent"]["page"]
    assert meta["returned"] == 2
    assert meta["nextOffset"] == 2


# ---------------------------------------------------------------------------
# observe tools against the stateless Cron
# ---------------------------------------------------------------------------


async def test_get_status_summary():
    h = _handler()
    result = await _call(h, "cron_get_status")
    assert "3 job(s)" in result["content"][0]["text"]
    names = {r["job"] for r in result["structuredContent"]["status"]}
    assert names == {"hello", "nightly", "heavy"}


async def test_list_jobs_filter_and_states():
    h = _handler()
    result = await _call(h, "cron_list_jobs", {"filter": "NIGHT"})
    rows = result["structuredContent"]["jobs"]
    assert [r["name"] for r in rows] == ["nightly"]
    result = await _call(h, "cron_list_jobs", {"state": "disabled"})
    rows = result["structuredContent"]["jobs"]
    assert [r["name"] for r in rows] == ["nightly"]
    result = await _call(h, "cron_list_jobs", {"state": "scheduled"})
    names = {r["name"] for r in result["structuredContent"]["jobs"]}
    assert names == {"hello", "heavy"}
    result = await _call(h, "cron_list_jobs", {"state": "running"})
    assert result["structuredContent"]["jobs"] == []


async def test_get_job_detail_and_not_found():
    h = _handler()
    result = await _call(h, "cron_get_job", {"name": "hello"})
    assert result["structuredContent"]["name"] == "hello"
    result = await _call(h, "cron_get_job", {"name": "ghost"})
    assert result["isError"] is True
    assert "cron_list_jobs" in result["content"][0]["text"]


async def test_get_job_requires_name():
    h = _handler()
    result = await _call(h, "cron_get_job", {})
    assert result["isError"] is True
    assert "required string argument" in result["content"][0]["text"]


async def test_list_runs_pages_from_the_end():
    h = _handler()
    for outcome in ("success", "failure", "success"):
        h._cron.run_history["hello"].append(_run_info(outcome))
    result = await _call(h, "cron_list_runs", {"name": "hello", "limit": 2})
    body = result["structuredContent"]
    assert body["totalRuns"] == 3
    assert body["returnedRuns"] == 2
    assert "3 run(s) retained, 2 returned" in result["content"][0]["text"]
    result = await _call(h, "cron_list_runs", {"name": "ghost"})
    assert result["isError"] is True


async def test_job_trends_found_and_not_found():
    h = _handler()
    result = await _call(h, "cron_get_job_trends", {"name": "hello"})
    assert result["structuredContent"] is not None
    assert "trends for job" in result["content"][0]["text"]
    result = await _call(h, "cron_get_job_trends", {"name": "ghost"})
    assert result["isError"] is True


async def test_job_resources_series_and_not_found():
    h = _handler()
    info = _run_info("success")
    info.resource_usage = ResourceUsage(
        cpu_user_seconds=1.0,
        cpu_system_seconds=0.5,
        max_rss_bytes=2048,
        samples=1,
        series=[[1.0, 5.0, 1024]],
    )
    h._cron.run_history["heavy"].append(info)
    result = await _call(h, "cron_get_job_resources", {"name": "heavy"})
    body = result["structuredContent"]
    assert body["monitored"] is True
    assert len(body["runs"]) == 1
    result = await _call(h, "cron_get_job_resources", {"name": "ghost"})
    assert result["isError"] is True


async def test_cluster_fleet_and_node_views():
    h = _handler()
    result = await _call(h, "cron_get_cluster")
    assert result["structuredContent"]["enabled"] is False
    assert "enabled=False" in result["content"][0]["text"]
    result = await _call(h, "cron_get_fleet")
    assert result["structuredContent"]["enabled"] is False
    result = await _call(h, "cron_get_node", {"history": True})
    assert "node" in result["content"][0]["text"]


async def test_query_metrics_match_and_bad_match():
    h = _handler()
    result = await _call(
        h, "cron_query_metrics", {"match": "cronstable", "limit": 5}
    )
    body = result["structuredContent"]
    assert body["match"] == "cronstable"
    assert body["returned"] <= 5
    assert all("cronstable" in s["name"] for s in body["samples"])
    result = await _call(h, "cron_query_metrics", {"match": 7})
    assert result["isError"] is True


async def test_query_metrics_walks_the_metric_universe_off_the_loop(
    monkeypatch,
):
    # A metrics query is asked for a handful of samples but must visit every
    # sample of every family to know which ones match, and it formats each
    # one on the way. That walk used to run inline, so an agent's query froze
    # job dispatch for its duration at fleet scale. It now splits the way the
    # /metrics scrape does: the family build (which reads live scheduler
    # state) stays on the loop, and the walk over that private snapshot goes
    # to the default executor at the same resident job count, the one
    # _METRICS_OFFLOAD_MIN_JOBS sets. An invariant test, not a timing one:
    # it pins WHICH THREAD each phase runs on, and that the answer is the
    # same either way.
    import threading

    h = _handler()
    cron = h._cron
    loop_thread = threading.get_ident()
    real_iter_samples = cron.metrics.iter_samples
    seen = {}

    def spy(target):
        # iter_samples reads the live state eagerly and returns a lazy
        # generator, so these two idents are the two phases: the call itself
        # is the family build, the first pull is the walk.
        seen["build"] = threading.get_ident()
        inner = real_iter_samples(target)

        def walk():
            seen["walk"] = threading.get_ident()
            yield from inner

        return walk()

    monkeypatch.setattr(cron.metrics, "iter_samples", spy)

    # the gate lives beside the /metrics one it mirrors, in cron.py
    import cronstable.cron as cron_mod

    # a small deployment stays inline: a thread hop costs more than the walk
    monkeypatch.setattr(cron_mod, "_METRICS_OFFLOAD_MIN_JOBS", 1000)
    inline = (await _call(h, "cron_query_metrics", {"limit": 500}))[
        "structuredContent"
    ]
    assert seen["build"] == loop_thread
    assert seen["walk"] == loop_thread

    seen.clear()
    # the snapshot is memoized across callers within the window, so the
    # second call would otherwise join the first's and never reach the spy
    cron._bust_response_memos()
    monkeypatch.setattr(cron_mod, "_METRICS_OFFLOAD_MIN_JOBS", 1)
    offloaded = (await _call(h, "cron_query_metrics", {"limit": 500}))[
        "structuredContent"
    ]
    # the live-state read still belongs to the loop; the walk does not
    assert seen["build"] == loop_thread
    assert seen["walk"] != loop_thread, (
        "the sample walk ran on the event loop despite the job count "
        "clearing the offload threshold"
    )
    # same output shape and the same samples either way: gauge readings can
    # move between the two calls, names and counts cannot.
    assert set(offloaded) == {"samples", "totalMatched", "returned", "match"}
    assert [s["name"] for s in offloaded["samples"]] == [
        s["name"] for s in inline["samples"]
    ]
    assert offloaded["totalMatched"] == inline["totalMatched"]
    assert offloaded["returned"] == inline["returned"] > 0
    assert offloaded["match"] is None


async def test_query_metrics_shares_one_walk_across_callers(monkeypatch):
    # The tool adopted /metrics' offload but not its shared product, so
    # every call rebuilt the whole universe and enqueued its own
    # full-universe walk on the shared executor: N polling agents cost N
    # walks of a body HISTORY sizes at 22 MB for 10,000 jobs. The
    # snapshot is now memoized like the scrape's, while match/limit stay
    # per call. A local change must still render at once, not out-wait
    # the TTL.
    h = _handler()
    cron = h._cron
    walks = []
    real_iter_samples = cron.metrics.iter_samples

    def counting(target):
        walks.append(1)
        return real_iter_samples(target)

    monkeypatch.setattr(cron.metrics, "iter_samples", counting)

    first = (await _call(h, "cron_query_metrics", {"limit": 5}))[
        "structuredContent"
    ]
    second = (await _call(h, "cron_query_metrics", {"limit": 500}))[
        "structuredContent"
    ]
    assert len(walks) == 1, "the second caller rebuilt the universe"
    # ...and the shared snapshot did NOT flatten the per-call filter
    assert first["returned"] == 5
    assert second["returned"] > 5
    assert first["totalMatched"] == second["totalMatched"]
    filtered = (
        await _call(h, "cron_query_metrics", {"match": "cronstable_job"})
    )["structuredContent"]
    assert len(walks) == 1
    assert filtered["totalMatched"] < second["totalMatched"]
    assert all("cronstable_job" in s["name"] for s in filtered["samples"])

    # a local change busts the snapshot with the rest of the memos
    cron._bust_response_memos()
    await _call(h, "cron_query_metrics", {"limit": 5})
    assert len(walks) == 2


async def test_get_version_tool():
    h = _handler()
    result = await _call(h, "cron_get_version")
    body = result["structuredContent"]
    assert body["jobs"] == 3
    assert body["job_set_id"]
    assert "3 job(s)" in result["content"][0]["text"]


async def test_tail_job_logs_and_not_found():
    h = _handler()
    result = await _call(
        h, "cron_tail_job_logs", {"name": "hello", "tail": 5, "cursor": "x"}
    )
    body = result["structuredContent"]
    assert body["name"] == "hello"
    assert body["lines"] == []
    result = await _call(h, "cron_tail_job_logs", {"name": "ghost"})
    assert result["isError"] is True


async def test_schedule_sandbox_argument_types():
    h = _handler()
    result = await _call(
        h, "cron_validate_schedule", {"expression": "* * * * *", "tz": 5}
    )
    assert result["isError"] is True
    assert "IANA timezone" in result["content"][0]["text"]
    result = await _call(
        h, "cron_explain_schedule", {"expression": "* * * * *", "seed": 5}
    )
    assert result["isError"] is True
    assert "job name" in result["content"][0]["text"]


async def test_schedule_pressure_and_suggest_argument_types():
    h = _handler()
    result = await _call(h, "cron_schedule_pressure", {"tz": 5})
    assert result["isError"] is True
    result = await _call(h, "cron_suggest_slot", {"tz": 5})
    assert result["isError"] is True
    result = await _call(h, "cron_suggest_slot", {"period": "weekly"})
    assert result["isError"] is True  # engine rejects unknown period


# ---------------------------------------------------------------------------
# act tools (job control)
# ---------------------------------------------------------------------------


async def test_run_job_success_and_confirm_gate(monkeypatch):
    h = _handler()
    launched = []

    async def fake_start(name):
        launched.append(name)

    monkeypatch.setattr(h._cron, "start_job_by_name", fake_start)
    result = await _call(h, "cron_run_job", {"name": "hello"})
    assert result["isError"] is True  # confirm missing
    assert launched == []
    result = await _call(h, "cron_run_job", {"name": "hello", "confirm": True})
    assert result["structuredContent"] == {"started": "hello"}
    assert launched == ["hello"]


async def test_run_job_disabled_surfaces_api_action_error():
    h = _handler()
    result = await _call(
        h, "cron_run_job", {"name": "nightly", "confirm": True}
    )
    assert result["isError"] is True
    assert "disabled" in result["content"][0]["text"]


class _FakeRunning:
    def __init__(self):
        self.cancelled = False
        self.proc = None


async def test_cancel_job_marks_all_instances():
    h = _handler()
    instances = [_FakeRunning(), _FakeRunning()]
    h._cron.running_jobs["hello"] = instances
    result = await _call(
        h, "cron_cancel_job", {"name": "hello", "confirm": True}
    )
    body = result["structuredContent"]
    assert body == {"cancelled": "hello", "instances": 2}
    assert all(inst.cancelled for inst in instances)


async def test_cancel_job_not_running_is_tool_error():
    h = _handler()
    result = await _call(
        h, "cron_cancel_job", {"name": "hello", "confirm": True}
    )
    assert result["isError"] is True
    assert "not running" in result["content"][0]["text"]


async def test_pause_job_confirm_gate_and_success():
    h = _handler()
    result = await _call(h, "cron_pause_job", {"name": "hello"})
    assert result["isError"] is True  # confirm missing
    assert "hello" not in h._cron._paused
    result = await _call(
        h,
        "cron_pause_job",
        {
            "name": "hello",
            "durationSeconds": 120,
            "note": "db migration",
            "confirm": True,
        },
    )
    body = result["structuredContent"]
    assert body["paused"] == "hello"
    assert body["until"] in result["content"][0]["text"]
    info = h._cron._paused["hello"]
    assert (info.by, info.channel) == ("mcp", "mcp")
    assert info.note == "db migration"
    assert (info.until - info.since).total_seconds() == 120


async def test_pause_job_default_duration():
    h = _handler()
    result = await _call(
        h, "cron_pause_job", {"name": "hello", "confirm": True}
    )
    assert result["structuredContent"]["paused"] == "hello"
    info = h._cron._paused["hello"]
    assert (
        info.until - info.since
    ).total_seconds() == PAUSE_DEFAULT_SECONDS


async def test_pause_job_unknown_and_bad_duration():
    h = _handler()
    result = await _call(
        h, "cron_pause_job", {"name": "ghost", "confirm": True}
    )
    assert result["isError"] is True
    assert "not found" in result["content"][0]["text"]
    result = await _call(
        h,
        "cron_pause_job",
        {"name": "hello", "durationSeconds": 0, "confirm": True},
    )
    assert result["isError"] is True
    assert "between 1 and" in result["content"][0]["text"]
    result = await _call(
        h,
        "cron_pause_job",
        {"name": "hello", "durationSeconds": "soon", "confirm": True},
    )
    assert result["isError"] is True
    assert "integer" in result["content"][0]["text"]
    assert "hello" not in h._cron._paused  # no rejected call took effect


async def test_resume_job_confirm_gate_clears_pause_and_noops():
    h = _handler()
    await _call(h, "cron_pause_job", {"name": "hello", "confirm": True})
    result = await _call(h, "cron_resume_job", {"name": "hello"})
    assert result["isError"] is True  # confirm missing
    assert "hello" in h._cron._paused
    result = await _call(
        h, "cron_resume_job", {"name": "hello", "confirm": True}
    )
    assert result["structuredContent"] == {"resumed": "hello"}
    assert "hello" not in h._cron._paused
    # resuming an unpaused job is still a success (idempotent no-op)
    result = await _call(
        h, "cron_resume_job", {"name": "hello", "confirm": True}
    )
    assert result["structuredContent"] == {"resumed": "hello"}
    result = await _call(
        h, "cron_resume_job", {"name": "ghost", "confirm": True}
    )
    assert result["isError"] is True
    assert "not found" in result["content"][0]["text"]


_SLA_YAML = (
    _YAML
    + """\
  - name: watched
    command: echo hi
    schedule: "0 * * * *"
    sla:
      lateAfterSeconds: 60
"""
)


async def test_observe_payloads_carry_paused_and_sla():
    # the shared payload builder's new fields must reach the observe tools
    # untouched (no tool-side field filtering)
    h = _handler(yaml=_SLA_YAML)
    await _call(
        h,
        "cron_pause_job",
        {"name": "hello", "note": "window", "confirm": True},
    )
    result = await _call(h, "cron_get_job", {"name": "hello"})
    paused = result["structuredContent"]["paused"]
    assert (paused["by"], paused["channel"]) == ("mcp", "mcp")
    assert paused["note"] == "window"
    result = await _call(h, "cron_get_job", {"name": "watched"})
    sla = result["structuredContent"]["sla"]
    assert sla["thresholds"] == {"lateAfterSeconds": 60}
    assert sla["state"] == "ok"
    result = await _call(h, "cron_list_jobs")
    rows = {r["name"]: r for r in result["structuredContent"]["jobs"]}
    assert rows["hello"]["paused"] is not None
    assert rows["watched"]["paused"] is None
    assert "sla" in rows["watched"]


# ---------------------------------------------------------------------------
# resources/read
# ---------------------------------------------------------------------------


async def test_resources_read_requires_uri():
    h = _handler()
    resp = await _req(h, "resources/read", {"uri": 7})
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS


async def test_resources_read_job_template_and_ghost():
    h = _handler()
    resp = await _req(h, "resources/read", {"uri": "cronstable://jobs/hello"})
    contents = resp["result"]["contents"][0]
    assert contents["mimeType"] == "application/json"
    assert json.loads(contents["text"])["name"] == "hello"
    resp = await _req(h, "resources/read", {"uri": "cronstable://jobs/ghost"})
    assert resp["error"]["code"] == mcp_mod.RESOURCE_NOT_FOUND


async def test_resources_read_fixed_resources():
    h = _handler()
    resp = await _req(h, "resources/read", {"uri": "cronstable://status"})
    body = json.loads(resp["result"]["contents"][0]["text"])
    assert {r["job"] for r in body["status"]} == {
        "hello",
        "nightly",
        "heavy",
    }
    resp = await _req(h, "resources/read", {"uri": "cronstable://cluster"})
    body = json.loads(resp["result"]["contents"][0]["text"])
    assert body["enabled"] is False


async def test_resources_read_state_ns_maps_action_error():
    # no state backend configured: the loader's ApiActionError must map to
    # the MCP resource-not-found protocol error, not a 500.
    h = _handler()
    resp = await _req(
        h, "resources/read", {"uri": "cronstable://state/kv/scope"}
    )
    assert resp["error"]["code"] == mcp_mod.RESOURCE_NOT_FOUND
    assert "state store" in resp["error"]["message"]


# ---------------------------------------------------------------------------
# prompts
# ---------------------------------------------------------------------------


async def test_prompt_renderers_fill_arguments():
    h = _handler()
    got = await _req(
        h,
        "prompts/get",
        {"name": "blast_radius", "arguments": {"target": "hello"}},
    )
    assert "'hello'" in got["result"]["messages"][0]["content"]["text"]
    got = await _req(h, "prompts/get", {"name": "fleet_health_summary"})
    text = got["result"]["messages"][0]["content"]["text"]
    assert "cron_get_fleet" in text
    got = await _req(
        h,
        "prompts/get",
        {
            "name": "why_did_dag_run_fail",
            "arguments": {"dag": "etl", "run_key": "r1"},
        },
    )
    text = got["result"]["messages"][0]["content"]["text"]
    assert "'etl'" in text and "'r1'" in text
    got = await _req(
        h,
        "prompts/get",
        {
            "name": "backfill_plan",
            "arguments": {"dag": "etl", "from": "a", "to": "b"},
        },
    )
    text = got["result"]["messages"][0]["content"]["text"]
    assert "backfill of dag 'etl' from a to b" in text


async def test_prompts_get_rejects_non_object_arguments():
    h = _handler()
    got = await _req(
        h, "prompts/get", {"name": "blast_radius", "arguments": [1]}
    )
    assert got["error"]["code"] == mcp_mod.INVALID_PARAMS


# ---------------------------------------------------------------------------
# summary formatters (direct payload-shape unit tests)
# ---------------------------------------------------------------------------


def test_preview_summary_shapes():
    assert mcp_mod._preview_summary(
        {"valid": False, "error": "boom"}
    ).startswith("INVALID: boom")
    assert "@reboot" in mcp_mod._preview_summary(
        {"valid": True, "reboot": True}
    )
    assert "no future runs" in mcp_mod._preview_summary(
        {"valid": True, "description": "d", "never_fires": True, "lint": []}
    )
    text = mcp_mod._preview_summary(
        {
            "valid": True,
            "description": "every minute",
            "never_fires": False,
            "lint": [{"level": "warning"}, {"level": "note"}],
            "fires": ["2026-07-18T00:00:00+00:00"],
        }
    )
    assert "1 lint warning(s), 1 note(s)" in text
    assert "first scheduled run 2026-07-18T00:00:00+00:00" in text
    # clean lint and no computed fires: the description stands alone
    bare = mcp_mod._preview_summary(
        {
            "valid": True,
            "description": "every minute",
            "never_fires": False,
            "lint": [],
            "fires": [],
        }
    )
    assert bare == "valid: every minute"


def test_why_summary_shapes():
    assert "@reboot" in mcp_mod._why_summary({"job": "j", "reboot": True})
    yes = mcp_mod._why_summary(
        {
            "job": "j",
            "reboot": False,
            "matches": True,
            "at_in_zone": "2026-07-18T09:00:00+02:00",
            "notes": [
                {"code": "day-and", "message": "AND rule"},
                {"code": "dst-gap", "message": "shifted"},
            ],
            "enabled": False,
            "checks": [],
        }
    )
    assert yes.startswith("YES")
    assert "; shifted" in yes
    assert "disabled" in yes
    yes_enabled = mcp_mod._why_summary(
        {
            "job": "j",
            "reboot": False,
            "matches": True,
            "at_in_zone": "x",
            "notes": [],
            "enabled": True,
            "checks": [],
        }
    )
    assert "cron_list_runs" in yes_enabled
    no = mcp_mod._why_summary(
        {
            "job": "j",
            "reboot": False,
            "matches": False,
            "notes": [],
            "enabled": False,
            "checks": [
                {
                    "field": "minute",
                    "label": "0",
                    "allowed": "0",
                    "matched": True,
                },
                {
                    "field": "day-of-week",
                    "label": "Tuesday",
                    "allowed": "Monday",
                    "matched": False,
                },
            ],
        }
    )
    assert no.startswith("NO: minute matched")
    assert "day-of-week Tuesday is not in Monday" in no
    assert "also disabled" in no
    nothing_matched = mcp_mod._why_summary(
        {
            "job": "j",
            "reboot": False,
            "matches": False,
            "notes": [],
            "enabled": True,
            "checks": [
                {
                    "field": "minute",
                    "label": "5",
                    "allowed": "0",
                    "matched": False,
                }
            ],
        }
    )
    assert nothing_matched.startswith("NO; minute 5 is not in 0")


async def test_why_no_run_miss_names_both_lookups():
    # cron_why_no_run and GET /schedule/why share one payload builder
    # (Cron.schedule_why_payload), which resolves a DAG's synthetic
    # dag:<name> schedule job as readily as a job. So the miss must not
    # claim only jobs were searched, and must not point at a tool that
    # cannot list a DAG schedule. The HTTP twin's reason is pinned in
    # tests/test_cron_web.py.
    yaml = (
        _YAML
        + "dags:\n  - name: sch\n    schedule: '*/5 * * * *'\n"
        "    tasks:\n      - id: a\n        command: x\n"
    )
    h = _handler(yaml=yaml)
    result = await _call(
        h, "cron_why_no_run", {"name": "ghost", "at": "2026-01-01T00:00:00Z"}
    )
    assert result["isError"] is True
    text = result["content"][0]["text"]
    assert text == (
        "no job or workflow schedule named 'ghost'. Use cron_list_jobs or "
        "cron_list_dags to find available schedules."
    )
    # the half that makes the old wording false: a dag: name answers
    result = await _call(
        h,
        "cron_why_no_run",
        {"name": "dag:sch", "at": "2026-01-01T00:00:00Z"},
    )
    assert "isError" not in result
    assert result["structuredContent"]["job"] == "dag:sch"


def test_opt_int_rejects_junk():
    assert mcp_mod._opt_int(None) is None
    assert mcp_mod._opt_int("7") == 7
    assert mcp_mod._opt_int("x") is None


# ---------------------------------------------------------------------------
# dags/state tools against a real backend + scheduler
# ---------------------------------------------------------------------------

_GATE_DAG = (
    "dags:\n  - name: ap\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
    "      - id: gate\n        type: approval\n        dependsOn:\n"
    "          - a\n"
    "      - id: b\n        command: 'x'\n        dependsOn:\n"
    "          - gate\n"
    "  - name: bf\n    schedule: '0 * * * *'\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
)


async def _state_handler(tmp_path):
    cron = await _make_state_cron(tmp_path, _GATE_DAG)
    cron.web_config = {}
    _set_cmd(cron, "ap", "a", [_PY, "-c", "pass"])
    _set_cmd(cron, "ap", "b", [_PY, "-c", "pass"])
    _set_cmd(cron, "bf", "a", [_PY, "-c", "pass"])
    cfg = _build_mcp_config(
        {"enabled": True, "readOnly": False, "toolsets": _ALL_TOOLSETS}
    )
    return MCPHandler(cron, cfg), cron


async def test_dag_tools_full_flow(tmp_path):
    h, cron = await _state_handler(tmp_path)
    try:
        result = await _call(h, "cron_list_dags")
        names = {d["name"] for d in result["structuredContent"]["dags"]}
        assert names == {"ap", "bf"}

        # trigger: confirm gate first, then the real run
        result = await _call(h, "cron_trigger_dag", {"dag": "ap"})
        assert result["isError"] is True
        result = await _call(
            h, "cron_trigger_dag", {"dag": "ghost", "confirm": True}
        )
        assert result["isError"] is True
        result = await _call(
            h, "cron_trigger_dag", {"dag": "ap", "confirm": True}
        )
        run_key = result["structuredContent"]["runKey"]
        assert result["structuredContent"]["dag"] == "ap"

        # drive to the approval gate
        await _drive(cron, "ap", run_key)

        result = await _call(h, "cron_list_dag_runs", {"dag": "ap"})
        runs = result["structuredContent"]["runs"]
        assert [r["runKey"] for r in runs] == [run_key]
        result = await _call(h, "cron_list_dag_runs", {"dag": "ghost"})
        assert result["isError"] is True

        result = await _call(
            h, "cron_get_dag_run", {"dag": "ap", "run_key": run_key}
        )
        assert result["structuredContent"]["runKey"] == run_key
        result = await _call(
            h, "cron_get_dag_run", {"dag": "ap", "run_key": "nope"}
        )
        assert result["isError"] is True

        result = await _call(
            h, "cron_get_dag_xcom", {"dag": "ap", "run_key": run_key}
        )
        assert result["structuredContent"] is not None
        result = await _call(
            h, "cron_get_dag_xcom", {"dag": "ap", "run_key": "nope"}
        )
        assert result["isError"] is True

        result = await _call(
            h,
            "cron_tail_dag_task_logs",
            {"dag": "ap", "run_key": run_key, "taskkey": "a", "tail": 5},
        )
        assert result["structuredContent"]["dag"] == "ap"
        result = await _call(
            h,
            "cron_tail_dag_task_logs",
            {"dag": "ghost", "run_key": run_key, "taskkey": "a"},
        )
        assert result["isError"] is True

        # the approval gate: argument validation, then a real approval
        result = await _call(
            h,
            "cron_decide_gate",
            {
                "dag": "ap",
                "run_key": run_key,
                "taskkey": "gate",
                "decision": "maybe",
            },
        )
        assert result["isError"] is True
        result = await _call(
            h,
            "cron_decide_gate",
            {
                "dag": "ap",
                "run_key": run_key,
                "taskkey": "gate",
                "decision": "approve",
            },
        )
        assert result["isError"] is True  # confirm missing
        result = await _call(
            h,
            "cron_decide_gate",
            {
                "dag": "ap",
                "run_key": run_key,
                "taskkey": "nope",
                "decision": "approve",
                "confirm": True,
            },
        )
        assert result["isError"] is True  # unknown gate task
        result = await _call(
            h,
            "cron_decide_gate",
            {
                "dag": "ap",
                "run_key": run_key,
                "taskkey": "gate",
                "decision": "approve",
                "by": "alice",
                "confirm": True,
            },
        )
        assert "approved gate" in result["content"][0]["text"]

        # resources/read: dag detail template (found + ghost)
        resp = await _req(h, "resources/read", {"uri": "cronstable://dags/ap"})
        detail = json.loads(resp["result"]["contents"][0]["text"])
        assert detail["name"] == "ap"
        resp = await _req(
            h, "resources/read", {"uri": "cronstable://dags/ghost"}
        )
        assert resp["error"]["code"] == mcp_mod.RESOURCE_NOT_FOUND
    finally:
        await _teardown(cron)


async def test_backfill_tool_dry_run_default_and_real(tmp_path):
    h, cron = await _state_handler(tmp_path)
    try:
        result = await _call(
            h,
            "cron_backfill_dag",
            {"dag": "ghost", "from": "a", "to": "b"},
        )
        assert result["isError"] is True

        args = {
            "dag": "bf",
            "from": "2026-01-01T00:00:00+00:00",
            "to": "2026-01-01T01:30:00+00:00",
        }
        result = await _call(h, "cron_backfill_dag", args)
        body = result["structuredContent"]
        assert body["dryRun"] is True
        assert body["wouldExecute"] is False
        assert "DRY RUN" in result["content"][0]["text"]

        # a real backfill still requires confirm=true
        result = await _call(
            h, "cron_backfill_dag", {**args, "dry_run": False}
        )
        assert result["isError"] is True

        result = await _call(
            h,
            "cron_backfill_dag",
            {**args, "dry_run": False, "confirm": True},
        )
        assert result["structuredContent"]["ok"] is True
        assert result["structuredContent"]["created"] == 2

        # an unparseable range surfaces the engine's reason as a tool error
        result = await _call(
            h,
            "cron_backfill_dag",
            {
                "dag": "bf",
                "from": "bad",
                "to": "worse",
                "dry_run": False,
                "confirm": True,
            },
        )
        assert result["isError"] is True
    finally:
        await _teardown(cron)


async def test_inspect_state_forms(tmp_path):
    from cronstable import jobstate

    h, cron = await _state_handler(tmp_path)
    try:
        result = await _call(
            h, "cron_inspect_state", {"ns": "kv/x", "stream": "runs/y"}
        )
        assert result["isError"] is True

        await jobstate.kv_set(
            cron.state_backend, "scope", "k", {"pw": "hunter2"}
        )
        result = await _call(h, "cron_inspect_state", {"ns": "kv/scope"})
        docs = result["structuredContent"]["documents"]
        assert docs and "value" not in docs[0]

        result = await _call(
            h, "cron_inspect_state", {"stream": "runs/nobody", "limit": 5}
        )
        assert result["structuredContent"]["records"] == []

        result = await _call(h, "cron_inspect_state")
        overview = result["structuredContent"]
        assert overview["enabled"] is True
        assert "enabled=True" in result["content"][0]["text"]
    finally:
        await _teardown(cron)


async def test_resources_read_state_ns_with_backend(tmp_path):
    from cronstable import jobstate

    h, cron = await _state_handler(tmp_path)
    try:
        await jobstate.kv_set(cron.state_backend, "scope", "k", 1)
        resp = await _req(
            h, "resources/read", {"uri": "cronstable://state/kv/scope"}
        )
        body = json.loads(resp["result"]["contents"][0]["text"])
        assert body["namespace"] == "kv/scope"
        # a non-inspectable namespace maps ApiActionError -> resource error
        resp = await _req(
            h, "resources/read", {"uri": "cronstable://state/runs/x"}
        )
        assert resp["error"]["code"] == mcp_mod.RESOURCE_NOT_FOUND
    finally:
        await _teardown(cron)


# ---------------------------------------------------------------------------
# config plumbing kept honest (parse -> handler round trip)
# ---------------------------------------------------------------------------


def test_mcp_config_round_trip_through_parse():
    yaml = (
        "web:\n  listen:\n    - http://127.0.0.1:8080\n"
        "mcp:\n  enabled: true\n  readOnly: false\n"
        "  toolsets:\n    - observe\n    - act\n"
    )
    cfg = parse_config_string(yaml, "t.yaml").mcp_config
    assert cfg["enabled"] is True
    assert cfg["readOnly"] is False
    assert cfg["toolsets"] == ["observe", "act"]


async def test_tools_call_unknown_tool_is_invalid_params():
    h = _handler()
    resp = await _req(h, "tools/call", {"name": "ghost_tool", "arguments": {}})
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS
    assert "unknown tool" in resp["error"]["message"]


async def test_ping_round_trips():
    h = _handler()
    resp = await _req(h, "ping")
    assert resp == {"jsonrpc": "2.0", "id": 1, "result": {}}


# ---------------------------------------------------------------------------
# fuzzing findings: falsy dry_run must preview, and non-finite numeric
# arguments must clamp/default instead of -32603
# ---------------------------------------------------------------------------


async def test_backfill_falsy_dry_run_values_still_preview(tmp_path):
    # `dry_run: null` is exactly how an MCP client or LLM encodes an
    # unspecified optional parameter; args.get("dry_run", True) applied the
    # default only when the key was ABSENT, so every present-but-falsy
    # value (null/[]/{}/""/0) fell through the preview gate into a REAL
    # backfill on a destructiveHint:true tool.  Only the literal boolean
    # false may execute.
    h, cron = await _state_handler(tmp_path)
    try:
        args = {
            "dag": "bf",
            "from": "2026-01-01T00:00:00+00:00",
            "to": "2026-01-01T01:30:00+00:00",
            "confirm": True,  # confirm alone must not defeat the preview
        }
        for falsy in (None, [], {}, "", 0):
            result = await _call(
                h, "cron_backfill_dag", {**args, "dry_run": falsy}
            )
            body = result["structuredContent"]
            assert body.get("dryRun") is True, (falsy, body)
            assert body.get("wouldExecute") is False
            assert "DRY RUN" in result["content"][0]["text"]
        # the documented real-run spelling still executes
        result = await _call(
            h, "cron_backfill_dag", {**args, "dry_run": False}
        )
        assert result["structuredContent"]["ok"] is True
    finally:
        await _teardown(cron)


async def test_numeric_arguments_survive_non_finite_json_numbers():
    # 1e999 is a well-formed RFC-8259 number the stdlib parser reads as
    # inf; int(inf) raises OverflowError, which none of the coercion
    # helpers caught -- turning a schema-valid argument into a -32603
    # protocol fault (with a server-side traceback) on 15 tool/argument
    # pairs, instead of the documented clamp-never-error behaviour.
    import json as stdjson

    inf = stdjson.loads(b'{"limit": 1e999}')["limit"]  # off-the-wire shape
    assert mcp_mod._opt_int(inf) is None
    assert mcp_mod._opt_int(float("nan")) is None
    assert mcp_mod._opt_int(float("-inf")) is None

    h = _handler()
    for args in (
        {"limit": inf},
        {"offset": inf, "limit": 5},
        {"limit": float("nan")},
    ):
        result = await _call(h, "cron_get_status", args)
        # a normal (possibly clamped) result, never a JSON-RPC error
        assert "structuredContent" in result, (args, result)
    # _call itself asserts no JSON-RPC error envelope: reaching a normal
    # tool result (even an isError one) is the fix for the tail/cursor pair
    await _call(h, "cron_tail_job_logs", {"name": "hello", "tail": inf})


def _as_caller(scopes, label="agent"):
    """Run the body as a token with ``scopes``, the way handle_http files
    the matched token; None models auth off."""
    caller = None if scopes is None else mcp_mod._Caller(label, scopes)
    return mcp_mod._caller.set(caller)


async def test_decide_gate_enforces_the_approve_scope():
    # The REST decision route is gated behind `approve`
    # (cron._WEB_SCOPE_OVERRIDES); the same action via tools/call must not
    # be reachable with `control` alone.
    h = _handler()
    args = {
        "dag": "nope",
        "run_key": "rk",
        "taskkey": "gate",
        "decision": "approve",
        "confirm": True,
    }
    # a control-scoped caller is refused before the handler runs
    token = _as_caller(frozenset({"view", "control"}))
    try:
        result = await _call(h, "cron_decide_gate", args)
    finally:
        mcp_mod._caller.reset(token)
    assert result["isError"] is True
    assert "approve" in result["content"][0]["text"]
    # an approve-holder proceeds to the handler (and fails on the unknown
    # dag, which is a different, post-authorization error)
    token = _as_caller(frozenset({"view", "approve"}))
    try:
        result = await _call(h, "cron_decide_gate", args)
    finally:
        mcp_mod._caller.reset(token)
    assert result["isError"] is True
    assert "scope" not in result["content"][0]["text"]
    # no token context (auth off) is unrestricted, exactly like REST
    result = await _call(h, "cron_decide_gate", args)
    assert "scope" not in result["content"][0]["text"]


@pytest.mark.parametrize(
    ("scopes", "visible", "hidden"),
    [
        pytest.param(
            frozenset({"view"}),
            {"cron_get_status", "cron_list_dags", "cron_inspect_state"},
            {
                "cron_run_job",
                "cron_trigger_dag",
                "cron_decide_gate",
                "cron_preview_recovery",
            },
            id="view",
        ),
        pytest.param(
            frozenset({"view", "control"}),
            {"cron_run_job", "cron_trigger_dag", "cron_preview_recovery"},
            {"cron_decide_gate"},
            id="control",
        ),
        pytest.param(
            frozenset({"view", "approve"}),
            {"cron_decide_gate", "cron_get_status"},
            {"cron_run_job", "cron_preview_recovery"},
            id="approve",
        ),
    ],
)
async def test_tools_list_shows_only_what_the_token_can_call(
    scopes, visible, hidden
):
    h = _handler()
    token = _as_caller(scopes)
    try:
        listed = {
            t["name"] for t in (await _req(h, "tools/list"))["result"]["tools"]
        }
        assert visible <= listed
        assert not hidden & listed
        # a hidden tool refuses with the scope it needs, not "unknown tool"
        for name in hidden:
            result = await _call(h, name, {})
            assert result["isError"] is True
            assert "scope" in result["content"][0]["text"], name
    finally:
        mcp_mod._caller.reset(token)


async def test_view_token_prompts_skip_tools_it_cannot_call():
    h = _handler()
    token = _as_caller(frozenset({"view"}))
    try:
        names = {
            p["name"]
            for p in (await _req(h, "prompts/list"))["result"]["prompts"]
        }
    finally:
        mcp_mod._caller.reset(token)
    # backfill_plan needs cron_backfill_dag, which needs control
    assert "backfill_plan" not in names
    assert "why_did_dag_run_fail" in names


#: The REST route (or routes) each MCP tool is the twin of: same action,
#: same in-process payload builder, so the same token scope should reach
#: both. Hand-maintained because the two authorization tables cannot be
#: joined automatically: a tool's scope keys on a tool name and
#: cron._WEB_SCOPE_OVERRIDES on a matched aiohttp resource path, with no
#: shared identifier between them. The guard below fails on an
#: unclassified tool, so a new one cannot land without someone deciding.
_TOOL_REST_TWINS = {
    "cron_list_pools": (("GET", "/pools"),),
    "cron_cancel_queued": (("POST", "/pools/{name}/queue/{key}/cancel"),),
    "cron_preview_recovery": (
        ("POST", "/dags/{name}/runs/{run_key}/recover"),
        ("POST", "/dags/{name}/recover"),
    ),
    "cron_recover_dag": (
        ("POST", "/dags/{name}/runs/{run_key}/recover"),
        ("POST", "/dags/{name}/recover"),
    ),
    "cron_get_status": (("GET", "/status"),),
    "cron_list_jobs": (("GET", "/jobs"),),
    "cron_get_job": (("GET", "/jobs/{name}"),),
    "cron_list_runs": (("GET", "/jobs/{name}/runs"),),
    "cron_get_job_trends": (("GET", "/jobs/{name}/trends"),),
    "cron_get_job_resources": (("GET", "/jobs/{name}/resources"),),
    "cron_get_cluster": (("GET", "/cluster"),),
    "cron_get_fleet": (("GET", "/fleet"),),
    # the tool's `history` argument folds in the second route
    "cron_get_node": (("GET", "/node"), ("GET", "/node/history")),
    "cron_query_metrics": (("GET", "/metrics"),),
    "cron_get_version": (("GET", "/version"),),
    "cron_tail_job_logs": (("GET", "/jobs/{name}/logs"),),
    "cron_schedule_pressure": (("GET", "/schedule/pressure"),),
    "cron_schedule_duplicates": (("GET", "/schedule/duplicates"),),
    "cron_suggest_slot": (("GET", "/schedule/suggest"),),
    # both sandboxes are schedule_preview_payload with a different count
    "cron_validate_schedule": (("GET", "/schedule/preview"),),
    "cron_explain_schedule": (("GET", "/schedule/preview"),),
    "cron_why_no_run": (("GET", "/schedule/why"),),
    "cron_list_dags": (("GET", "/dags"),),
    "cron_list_dag_runs": (("GET", "/dags/{name}/runs"),),
    "cron_get_dag_run": (("GET", "/dags/{name}/runs/{run_key}"),),
    "cron_get_dag_xcom": (("GET", "/dags/{name}/runs/{run_key}/xcom"),),
    "cron_tail_dag_task_logs": (
        ("GET", "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs"),
    ),
    # one tool, three modes: overview, one namespace, one stream
    "cron_inspect_state": (
        ("GET", "/state"),
        ("GET", "/state/documents"),
        ("GET", "/state/records"),
    ),
    "cron_run_job": (("POST", "/jobs/{name}/start"),),
    "cron_cancel_job": (("POST", "/jobs/{name}/cancel"),),
    "cron_pause_job": (("POST", "/jobs/{name}/pause"),),
    "cron_resume_job": (("POST", "/jobs/{name}/resume"),),
    "cron_trigger_dag": (("POST", "/dags/{name}/trigger"),),
    "cron_backfill_dag": (("POST", "/dags/{name}/backfill"),),
    "cron_decide_gate": (
        ("POST", "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision"),
    ),
}


def _scope_for_route(method, path):
    """The scope the web layer would demand of ``method path``.

    Asks the production decision function rather than restating its rule, so
    a change to the safe-method default or to the override table moves this
    guard with it. The stand-in request carries only what
    ``_required_web_scope`` reads: the method and the matched resource's
    canonical path.
    """
    resource = types.SimpleNamespace(canonical=path)
    route = types.SimpleNamespace(resource=resource)
    return _required_web_scope(
        types.SimpleNamespace(
            method=method,
            match_info=types.SimpleNamespace(route=route),
        )
    )


def test_tool_scopes_track_the_rest_scope_table():
    """Every MCP tool demands exactly the scope of its REST twin.

    ``/mcp`` opens to `view`, and tools/call then requires each tool's own
    scope (`control` for a mutating tool, `view` otherwise, or its
    ``mcp._TOOL_SCOPE_OVERRIDES`` entry). Promote a REST route and forget
    the tool, and a token the operator deliberately withheld that scope
    from takes exactly the withheld action through tools/call; relax one
    and the tool is gated for nothing. This guard fails on either drift,
    and on an override naming a tool that no longer exists.

    Residual gap, deliberately: the tool -> route correspondence above is
    hand-written (the tables share no identifier), so a WRONG mapping is
    invisible here. What the machine does check: every registered tool is
    classified, every route named is really registered, and the scopes
    agree wherever a mapping exists.
    """
    h = _handler()
    tools = set(h._tool_by_name)
    assert tools == set(_TOOL_REST_TWINS), (
        "a tool was added or renamed without recording its REST "
        "twin: {}".format(sorted(tools.symmetric_difference(_TOOL_REST_TWINS)))
    )
    unknown = set(mcp_mod._TOOL_SCOPE_OVERRIDES) - tools
    assert not unknown, (
        "_TOOL_SCOPE_OVERRIDES gates tools that do not exist, so the "
        "override binds nothing: {}".format(sorted(unknown))
    )
    # the /mcp floor must not sit above any tool, or its token could not
    # reach the tool at all
    floor = _scope_for_route("POST", "/mcp")
    assert floor == "view"
    served = {(method, path) for method, path, _h, _g in WEB_ROUTES}
    for tool, twins in sorted(_TOOL_REST_TWINS.items()):
        rest = set()
        for method, path in twins:
            assert (method, path) in served, (
                "{} is mapped to {} {}, which is not a registered "
                "route".format(tool, method, path)
            )
            rest.add(_scope_for_route(method, path))
        assert len(rest) == 1, (
            "{} mirrors routes demanding different scopes ({}), which one "
            "tool scope cannot express: split the tool".format(
                tool, sorted(rest)
            )
        )
        mcp_scope = h._tool_by_name[tool]["scope"]
        assert {mcp_scope} == rest, (
            "{} requires {!r} over MCP but its REST twin {} requires {!r}: "
            "a scope promoted on one surface and not the other lets a "
            "token take through tools/call exactly the action REST "
            "withholds".format(tool, mcp_scope, list(twins), rest.pop())
        )


# ---------------------------------------------------------------------------
# audit attribution: the token label, with a model-supplied `by` after it
# ---------------------------------------------------------------------------


async def test_actions_are_attributed_to_the_token_label(monkeypatch):
    h = _handler()
    decided = []
    resumed = []

    async def fake_approve(dag, run_key, taskkey, *, approved, by):
        decided.append(by)
        return {"ok": True}

    real_resume = h._cron.resume_job_by_name

    async def spy_resume(name, *, by, channel):
        resumed.append((by, channel))
        await real_resume(name, by=by, channel=channel)

    monkeypatch.setattr(h._cron._dag, "approve", fake_approve)
    monkeypatch.setattr(h._cron, "resume_job_by_name", spy_resume)
    gate = {
        "dag": "d",
        "run_key": "r",
        "taskkey": "t",
        "decision": "approve",
        "confirm": True,
    }
    token = _as_caller(frozenset({"view", "control", "approve"}), "ci-agent")
    try:
        await _call(h, "cron_pause_job", {"name": "hello", "confirm": True})
        info = h._cron._paused["hello"]
        assert (info.by, info.channel) == ("ci-agent", "mcp")
        await _call(h, "cron_resume_job", {"name": "hello", "confirm": True})
        await _call(h, "cron_decide_gate", gate)
        # the model's `by` is display text after the label, never instead
        await _call(h, "cron_decide_gate", {**gate, "by": "alice"})
    finally:
        mcp_mod._caller.reset(token)
    assert resumed == [("ci-agent", "mcp")]
    assert decided == ["ci-agent", "ci-agent (alice)"]
    # auth off: the label is `mcp`
    await _call(h, "cron_decide_gate", {**gate, "by": "bob"})
    assert decided[-1] == "mcp (bob)"


async def test_decide_gate_refuses_a_bad_by(monkeypatch):
    h = _handler()

    async def fail(*args, **kwargs):
        raise AssertionError("a rejected `by` reached the gate")

    monkeypatch.setattr(h._cron._dag, "approve", fail)
    gate = {
        "dag": "d",
        "run_key": "r",
        "taskkey": "t",
        "decision": "reject",
        "confirm": True,
    }
    result = await _call(h, "cron_decide_gate", {**gate, "by": ["alice"]})
    assert result["isError"] is True
    assert "by must be a string" in result["content"][0]["text"]
    too_long = "x" * (PAUSE_BY_MAX + 1)
    result = await _call(h, "cron_decide_gate", {**gate, "by": too_long})
    assert result["isError"] is True
    assert (
        "longer than {}".format(PAUSE_BY_MAX) in (result["content"][0]["text"])
    )


# ---------------------------------------------------------------------------
# input checks match the REST routes
# ---------------------------------------------------------------------------


def test_opt_int_treats_booleans_as_unusable():
    assert mcp_mod._opt_int(True) is None
    assert mcp_mod._opt_int(False) is None
    assert mcp_mod._opt_int(0) == 0


async def test_pause_refuses_a_boolean_duration():
    # bool is an int subclass: `true` used to read as a one-second pause.
    h = _handler()
    result = await _call(
        h,
        "cron_pause_job",
        {"name": "hello", "durationSeconds": True, "confirm": True},
    )
    assert result["isError"] is True
    assert "integer" in result["content"][0]["text"]
    assert "hello" not in h._cron._paused


async def test_boolean_limit_falls_back_like_any_unusable_value():
    h = _handler()
    result = await _call(h, "cron_get_status", {"limit": True})
    assert result["structuredContent"]["page"]["limit"] == 200


@pytest.mark.parametrize(
    ("tool", "arguments", "reason"),
    [
        pytest.param(
            "cron_preview_recovery",
            {"dag": "d", "run_key": "r", "mode": 5},
            "mode must be a string",
            id="mode-not-string",
        ),
        pytest.param(
            "cron_recover_dag",
            {"dag": "d", "run_key": "r", "plan_token": "x", "confirm": True},
            "64-character",
            id="short-plan-token",
        ),
        pytest.param(
            "cron_recover_dag",
            {
                "dag": "d",
                "run_key": "r",
                "plan_token": "a" * 64,
                "allow_config_change": "yes",
                "confirm": True,
            },
            "allow_config_change must be a boolean",
            id="allow-change-not-boolean",
        ),
    ],
)
async def test_recovery_tools_apply_the_rest_checks(
    tool, arguments, reason, monkeypatch
):
    h = _handler()

    async def fail(*args, **kwargs):
        raise AssertionError("an invalid recovery request reached the DAG")

    monkeypatch.setattr(h._cron._dag, "recover", fail)
    monkeypatch.setattr(h._cron._dag, "recover_range", fail)
    result = await _call(h, tool, arguments)
    assert result["isError"] is True
    assert reason in result["content"][0]["text"]


# ---------------------------------------------------------------------------
# argument completion
# ---------------------------------------------------------------------------

_DAG_YAML = (
    _YAML
    + "dags:\n  - name: etl\n    tasks:\n      - id: a\n        command: x\n"
    "  - name: hello\n    tasks:\n      - id: a\n        command: x\n"
)


async def _complete(handler, ref, name, value, known=None):
    params = {"ref": ref, "argument": {"name": name, "value": value}}
    if known is not None:
        params["context"] = {"arguments": known}
    resp = await _req(handler, "completion/complete", params)
    return resp.get("result", {}).get("completion"), resp.get("error")


def _prompt(name):
    return {"type": "ref/prompt", "name": name}


def _template(uri):
    return {"type": "ref/resource", "uri": uri}


async def test_completions_capability_follows_prompts_and_resources():
    caps = (await _req(_handler(), "initialize", {}))["result"]["capabilities"]
    assert caps["completions"] == {}
    cfg = _build_mcp_config({"enabled": True})
    cfg["resources"] = cfg["prompts"] = False
    h = MCPHandler(_handler()._cron, cfg)
    caps = (await _req(h, "initialize", {}))["result"]["capabilities"]
    assert "completions" not in caps
    resp = await _req(h, "completion/complete", {})
    assert resp["error"]["code"] == mcp_mod.METHOD_NOT_FOUND


async def test_complete_prompt_job_by_case_insensitive_prefix():
    h = _handler()
    got, _ = await _complete(h, _prompt("triage_job_failure"), "job", "HE")
    assert got == {"values": ["hello", "heavy"], "total": 2, "hasMore": False}
    got, _ = await _complete(h, _prompt("triage_job_failure"), "job", "")
    assert got["values"] == ["hello", "nightly", "heavy"]


async def test_complete_targets_add_dag_names_once():
    h = _handler(yaml=_DAG_YAML)
    got, _ = await _complete(h, _prompt("blast_radius"), "target", "")
    # `hello` names both a job and a DAG, and is offered once
    assert got["values"] == ["hello", "nightly", "heavy", "etl"]
    got, _ = await _complete(h, _prompt("why_did_dag_run_fail"), "dag", "e")
    assert got["values"] == ["etl"]


async def test_complete_template_variables():
    h = _handler(yaml=_DAG_YAML)
    got, _ = await _complete(
        h, _template("cronstable://jobs/{name}/runs"), "name", "n"
    )
    assert got["values"] == ["nightly"]
    got, _ = await _complete(
        h, _template("cronstable://dags/{name}"), "name", ""
    )
    assert got["values"] == ["etl", "hello"]
    # no source for this variable: nothing offered, not an error
    got, error = await _complete(
        h, _template("cronstable://state/{ns}"), "ns", "kv/"
    )
    assert error is None
    assert got["values"] == []


async def test_complete_offers_nothing_for_a_hidden_reference():
    h = _handler({"toolsets": ["observe"]}, yaml=_DAG_YAML)
    for ref, name in (
        (_prompt("why_did_dag_run_fail"), "dag"),
        (_template("cronstable://dags/{name}"), "name"),
    ):
        got, error = await _complete(h, ref, name, "")
        assert error is None
        assert got == {"values": [], "total": 0, "hasMore": False}


async def test_complete_caps_values_at_one_hundred():
    entry = "  - name: job{:03d}\n    command: x\n    schedule: '* * * * *'\n"
    yaml = "jobs:\n" + "".join(entry.format(i) for i in range(120))
    h = _handler(yaml=yaml)
    got, _ = await _complete(h, _prompt("triage_job_failure"), "job", "JOB")
    assert len(got["values"]) == mcp_mod.COMPLETION_MAX
    assert got["total"] == 120
    assert got["hasMore"] is True


@pytest.mark.parametrize(
    "params",
    [
        pytest.param({}, id="empty"),
        pytest.param(
            {"ref": _prompt("nope"), "argument": {"name": "x", "value": ""}},
            id="unknown-prompt",
        ),
        pytest.param(
            {
                "ref": _template("cronstable://nope/{x}"),
                "argument": {"name": "x", "value": ""},
            },
            id="unknown-template",
        ),
        pytest.param(
            {
                "ref": {"type": "ref/tool", "name": "x"},
                "argument": {"name": "x", "value": ""},
            },
            id="bad-ref-type",
        ),
        pytest.param(
            {
                "ref": _prompt("triage_job_failure"),
                "argument": {"name": "job", "value": 5},
            },
            id="value-not-string",
        ),
    ],
)
async def test_complete_rejects_malformed_requests(params):
    resp = await _req(_handler(), "completion/complete", params)
    assert resp["error"]["code"] == mcp_mod.INVALID_PARAMS


async def test_complete_run_keys_from_recent_runs(tmp_path):
    h, cron = await _state_handler(tmp_path)
    try:
        run_key = (
            await _call(h, "cron_trigger_dag", {"dag": "ap", "confirm": True})
        )["structuredContent"]["runKey"]
        got, _ = await _complete(
            h,
            _prompt("why_did_dag_run_fail"),
            "run_key",
            "",
            known={"dag": "ap"},
        )
        assert got["values"] == [run_key]
        got, _ = await _complete(
            h,
            _template("cronstable://dags/{name}/runs/{run_key}"),
            "run_key",
            run_key[:3].upper(),
            known={"name": "ap"},
        )
        assert got["values"] == [run_key]
        # without the DAG in context, or for an unknown one: nothing
        got, _ = await _complete(
            h, _prompt("why_did_dag_run_fail"), "run_key", ""
        )
        assert got["values"] == []
        got, _ = await _complete(
            h,
            _prompt("why_did_dag_run_fail"),
            "run_key",
            "",
            known={"dag": "ghost"},
        )
        assert got["values"] == []
    finally:
        await _teardown(cron)


# ---------------------------------------------------------------------------
# declared output schemas
# ---------------------------------------------------------------------------

_OUTPUT_SCHEMA_TOOLS = {
    "cron_get_status",
    "cron_list_jobs",
    "cron_validate_schedule",
    "cron_explain_schedule",
    "cron_why_no_run",
}


async def test_core_tools_declare_valid_output_schemas():
    h = _handler()
    listed = {
        t["name"]: t for t in (await _req(h, "tools/list"))["result"]["tools"]
    }
    declared = {n for n, t in listed.items() if "outputSchema" in t}
    assert declared == _OUTPUT_SCHEMA_TOOLS
    for name in declared:
        jsonschema.Draft202012Validator.check_schema(
            listed[name]["outputSchema"]
        )


@pytest.mark.parametrize(
    "arguments",
    [
        {"expression": "*/7 * * * *"},
        {"expression": "0 9 * * mon-fry"},
        {"expression": "@reboot"},
        {"expression": "H 3 * * *", "seed": "job"},
        {"expression": "0 0 30 2 *", "tz": "Europe/Berlin"},
    ],
)
async def test_schedule_sandbox_results_conform(arguments):
    # _call validates each result against the declared schema
    h = _handler()
    for tool in ("cron_validate_schedule", "cron_explain_schedule"):
        await _call(h, tool, arguments)


async def test_why_no_run_and_listing_results_conform():
    yaml = _YAML + "  - name: boot\n    command: x\n    schedule: '@reboot'\n"
    h = _handler(yaml=yaml)
    for name in ("hello", "nightly", "boot"):
        await _call(h, "cron_why_no_run", {"name": name, "at": "2026-07-14"})
    h._cron.run_history["hello"].append(_run_info("failure"))
    await _call(h, "cron_pause_job", {"name": "hello", "confirm": True})
    await _call(h, "cron_list_jobs")
    await _call(h, "cron_get_status", {"limit": 1})
