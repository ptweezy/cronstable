"""The ``notify:`` block: daemon/orchestration event notifications.

Covers config parsing of the notify block, the :class:`NotifyEventContext` the
reporters consume, :meth:`Cron._dispatch_notify`'s event filtering, and the DAG
scheduler's ``dag_failure`` / ``approval_waiting`` dispatch helpers. The cluster
``leader_change`` / ``quorum_loss`` edges are exercised through the cluster and
leadership suites (which drive ``_emit_cluster_role_logs``); the dispatch itself
is unit-tested here via ``_dispatch_notify``.
"""

import asyncio
import json
import logging
import types
import uuid

import pytest
import sentry_sdk
from sentry_sdk.transport import Transport

import cronstable.config as config
import cronstable.cron
from cronstable import dag
from cronstable.config import ConfigError
from cronstable.cron import Cron
from cronstable.dagrun import DagScheduler
from cronstable.job import (
    NotifyEventContext,
    SentryReporter,
    _compiled_template,
)
from tests.conftest import _cron

# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------

_NOTIFY_YAML = """
notify:
  events:
    - dag_failure
    - quorum_loss
  report:
    webhook:
      url:
        value: https://example.invalid/hook
"""


def test_notify_block_parses_events_and_report():
    nc = config.parse_config_string(_NOTIFY_YAML, "").notify_config
    assert nc is not None
    assert nc["events"] == frozenset({"dag_failure", "quorum_loss"})
    assert (
        nc["report"]["webhook"]["url"]["value"]
        == "https://example.invalid/hook"
    )
    # event-shaped default templates, not the job completed/failed ones
    assert (
        nc["report"]["mail"]["subject"]
        == "cronstable {{ event }}: {{ subject }}"
    )
    assert nc["report"]["sentry"]["fingerprint"] == [
        "cronstable",
        "{{ event }}",
        "{{ name }}",
    ]


def test_notify_events_absent_means_all():
    nc = config.parse_config_string(
        "notify:\n"
        "  report:\n"
        "    webhook:\n"
        "      url:\n"
        "        value: https://x.invalid\n",
        "",
    ).notify_config
    assert nc["events"] is None


def test_notify_absent_is_none():
    conf = config.parse_config_string(
        "jobs:\n  - name: a\n    command: 'x'\n    schedule: '@reboot'\n", ""
    )
    assert conf.notify_config is None


def test_notify_rejects_unknown_event():
    with pytest.raises(ConfigError):
        config.parse_config_string(
            "notify:\n"
            "  events:\n"
            "    - bogus\n"
            "  report:\n"
            "    webhook:\n"
            "      url:\n"
            "        value: https://x.invalid\n",
            "",
        )


def test_notify_report_merges_over_event_defaults():
    # overriding only the webhook url keeps the event-shaped default body
    nc = config.parse_config_string(_NOTIFY_YAML, "").notify_config
    body = nc["report"]["webhook"]["body"]
    assert "{{ event }}" in body and "{{ subject }}" in body


def test_notify_defaults_do_not_leak_into_job_reports():
    # a job's onFailure report keeps the standard completed/failed wording; the
    # notify defaults are a separate deepcopy.
    conf = config.parse_config_string(
        _NOTIFY_YAML
        + "jobs:\n  - name: a\n    command: 'x'\n    schedule: '@reboot'\n",
        "",
    )
    job = conf.jobs[0]
    assert (
        job.onFailure["report"]["mail"]["subject"]
        == "Cron job '{{name}}' {% if success %}completed{% else %}failed"
        "{% endif %}"
    )


# ---------------------------------------------------------------------------
# NotifyEventContext
# ---------------------------------------------------------------------------


def test_notify_event_context_template_vars():
    ctx = NotifyEventContext(
        event="dag_failure",
        success=False,
        name="etl",
        subject="DAG 'etl' run X failed",
        message="1 task(s) failed: load",
        fields={"dag": "etl", "run_key": "X", "failed_tasks": ["load"]},
    )
    tv = ctx.template_vars
    assert tv["event"] == "dag_failure"
    assert tv["subject"] == "DAG 'etl' run X failed"
    assert tv["message"] == "1 task(s) failed: load"
    assert tv["name"] == "etl"
    assert tv["success"] is False
    assert tv["fail_reason"] == "1 task(s) failed: load"
    assert tv["dag"] == "etl"
    assert tv["failed_tasks"] == ["load"]
    # run-shaped fields are all empty: no process ran.
    assert tv["stdout"] is None
    assert tv["exit_code"] is None
    assert tv["started_at"] is None
    assert ctx.failed is True


def test_notify_default_webhook_body_renders_valid_json():
    # the event-shaped default webhook body must produce valid JSON, escaping
    # quotes/newlines in the message via tojson.
    body_tmpl = config.parse_config_string(_NOTIFY_YAML, "").notify_config[
        "report"
    ]["webhook"]["body"]
    ctx = NotifyEventContext(
        event="dag_failure",
        success=False,
        name="etl",
        subject='DAG "etl" failed',
        message="line1\nline2",
        fields={},
    )
    payload = json.loads(
        _compiled_template(body_tmpl).render(ctx.template_vars)
    )
    assert "dag_failure" in payload["text"]
    assert "line1\nline2" in payload["text"]


# ---------------------------------------------------------------------------
# Cron._dispatch_notify
# ---------------------------------------------------------------------------

_JOB = "jobs:\n  - name: a\n    command: 'x'\n    schedule: '@reboot'\n"


async def _drain_notify(cron):
    # deterministically await whatever _dispatch_notify scheduled.
    await asyncio.gather(*list(cron._notify_tasks))


async def test_dispatch_notify_fires_allowed_event(monkeypatch):
    captured = []

    async def fake_report_event(ctx, report_config):
        captured.append((ctx, report_config))

    monkeypatch.setattr(cronstable.cron, "report_event", fake_report_event)
    cron = _cron(_JOB)
    cron._notify_config = {
        "events": frozenset({"dag_failure"}),
        "report": {"marker": 1},
    }
    cron._dispatch_notify(
        "dag_failure",
        success=False,
        name="etl",
        subject="s",
        message="m",
        dag="etl",
    )
    await _drain_notify(cron)
    assert len(captured) == 1
    ctx, report_config = captured[0]
    assert ctx.event == "dag_failure"
    assert ctx.template_vars["dag"] == "etl"
    assert report_config == {"marker": 1}


async def test_dispatch_notify_filters_disallowed_event(monkeypatch):
    captured = []

    async def fake(ctx, report_config):
        captured.append(ctx)

    monkeypatch.setattr(cronstable.cron, "report_event", fake)
    cron = _cron(_JOB)
    cron._notify_config = {"events": frozenset({"dag_failure"}), "report": {}}
    cron._dispatch_notify(
        "quorum_loss", success=False, name="n", subject="s", message="m"
    )
    await _drain_notify(cron)
    assert captured == []


async def test_dispatch_notify_noop_when_unconfigured(monkeypatch):
    called = []

    async def fake(ctx, report_config):
        called.append(ctx)

    monkeypatch.setattr(cronstable.cron, "report_event", fake)
    cron = _cron(_JOB)
    assert cron._notify_config is None
    cron._dispatch_notify(
        "dag_failure", success=False, name="x", subject="s", message="m"
    )
    await _drain_notify(cron)
    assert called == []


async def test_dispatch_notify_none_events_allows_all(monkeypatch):
    captured = []

    async def fake(ctx, report_config):
        captured.append(ctx.event)

    monkeypatch.setattr(cronstable.cron, "report_event", fake)
    cron = _cron(_JOB)
    cron._notify_config = {"events": None, "report": {}}
    for ev in ("dag_failure", "leader_change", "quorum_loss"):
        cron._dispatch_notify(
            ev, success=False, name="x", subject="s", message="m"
        )
    await _drain_notify(cron)
    assert set(captured) == {"dag_failure", "leader_change", "quorum_loss"}


# ---------------------------------------------------------------------------
# DAG scheduler dispatch helpers
# ---------------------------------------------------------------------------


class _FakeCron:
    """Records _dispatch_notify calls; DagScheduler.__init__ touches nothing
    else on the cron object."""

    def __init__(self):
        self.calls = []

    def _dispatch_notify(self, event, **fields):
        self.calls.append((event, fields))


def test_dag_failure_notify_lists_failed_tasks():
    fake = _FakeCron()
    sched = DagScheduler(fake)
    body = {
        "runId": "r1",
        "state": dag.FAILED,
        "tasks": {
            "a": {"state": dag.SUCCESS},
            "b": {"state": dag.FAILED},
            "c": {"state": dag.UPSTREAM_FAILED},
        },
    }
    sched._notify_dag_failure(("etl", "2026-07-22"), body)
    assert len(fake.calls) == 1
    event, fields = fake.calls[0]
    assert event == "dag_failure"
    assert fields["failed_tasks"] == ["b", "c"]
    assert fields["dag"] == "etl"
    assert fields["run_key"] == "2026-07-22"
    assert fields["run_id"] == "r1"
    assert "2 task(s) failed" in fields["message"]


def test_approval_waiting_notify_dedups():
    fake = _FakeCron()
    sched = DagScheduler(fake)
    dagcfg = types.SimpleNamespace(name="etl")
    body = {
        "tasks": {
            "gate": {"state": dag.RUNNING, "awaitingApproval": True},
            "x": {"state": dag.RUNNING},
        }
    }
    ref = ("etl", "rk")
    sched._notify_pending_approvals(dagcfg, ref, "r1", body)
    sched._notify_pending_approvals(dagcfg, ref, "r1", body)  # deduped
    assert len(fake.calls) == 1
    event, fields = fake.calls[0]
    assert event == "approval_waiting"
    assert fields["taskkey"] == "gate"
    assert fields["dag"] == "etl"
    assert fields["run_key"] == "rk"


async def test_on_terminal_fires_dag_failure_once(monkeypatch):
    fake = _FakeCron()
    sched = DagScheduler(fake)

    async def noop_release(ref):
        pass

    monkeypatch.setattr(sched, "_release", noop_release)
    body = {
        "runId": "r1",
        "state": dag.FAILED,
        "tasks": {"b": {"state": dag.FAILED}},
    }
    ref = ("etl", "rk")
    await sched._on_terminal(ref, body)
    # already terminal on the second observation: must NOT re-fire.
    await sched._on_terminal(ref, body)
    assert [c[0] for c in fake.calls] == ["dag_failure"]


async def test_on_terminal_success_does_not_notify(monkeypatch):
    fake = _FakeCron()
    sched = DagScheduler(fake)

    async def noop_release(ref):
        pass

    monkeypatch.setattr(sched, "_release", noop_release)
    body = {
        "runId": "r1",
        "state": dag.SUCCESS,
        "tasks": {"a": {"state": dag.SUCCESS}},
    }
    await sched._on_terminal(("etl", "rk"), body)
    assert fake.calls == []


async def test_on_terminal_clears_approval_dedup(monkeypatch):
    fake = _FakeCron()
    sched = DagScheduler(fake)

    async def noop_release(ref):
        pass

    monkeypatch.setattr(sched, "_release", noop_release)
    ref = ("etl", "rk")
    sched._approval_notified.add((ref[0], ref[1], "gate"))
    sched._approval_notified.add(("other", "rk2", "gate"))
    await sched._on_terminal(
        ref, {"runId": "r1", "state": dag.SUCCESS, "tasks": {}}
    )
    # the terminated run's entry is dropped; an unrelated run's is kept.
    assert sched._approval_notified == {("other", "rk2", "gate")}


# ---------------------------------------------------------------------------
# Cluster transition dispatch (leader_change / quorum_loss)
# ---------------------------------------------------------------------------


def _fake_mgr(*, quorate, leader, leader_name="node-a", node_name="node-a"):
    return types.SimpleNamespace(
        conflict_names=lambda: [],
        conflicting_sizes=lambda: [],
        conflicting_policies=lambda: [],
        cluster_size=lambda: 3,
        distribution="single-leader",
        is_quorate=lambda: quorate,
        is_leader=lambda: leader,
        leader_name=lambda: leader_name,
        node_name=node_name,
    )


async def test_emit_cluster_role_logs_fires_leader_change(monkeypatch):
    captured = []

    async def fake(ctx, report_config):
        captured.append(ctx)

    monkeypatch.setattr(cronstable.cron, "report_event", fake)
    cron = _cron(_JOB)
    cron._notify_config = {"events": None, "report": {}}
    cron.cluster_manager = _fake_mgr(quorate=True, leader=True)
    cron._was_quorate = True  # no quorum transition
    cron._was_leader = False  # leadership acquired -> a transition
    cron._emit_cluster_role_logs()
    await _drain_notify(cron)
    assert len(captured) == 1
    tv = captured[0].template_vars
    assert tv["event"] == "leader_change"
    assert tv["is_leader"] is True
    assert tv["role"] == "leader"
    assert tv["leader"] == "node-a"


async def test_emit_cluster_role_logs_fires_quorum_loss(monkeypatch):
    captured = []

    async def fake(ctx, report_config):
        captured.append(ctx)

    monkeypatch.setattr(cronstable.cron, "report_event", fake)
    cron = _cron(_JOB)
    cron._notify_config = {"events": None, "report": {}}
    cron.cluster_manager = _fake_mgr(
        quorate=False, leader=False, leader_name=None
    )
    cron._was_quorate = True  # was quorate, now not -> quorum_loss
    cron._was_leader = False
    cron._emit_cluster_role_logs()
    await _drain_notify(cron)
    events = [c.template_vars["event"] for c in captured]
    assert events == ["quorum_loss"]


async def test_emit_cluster_role_logs_quiet_without_notify(monkeypatch):
    # the common case: no notify block -> mgr methods for the payload are never
    # touched (mgr here lacks leader_name entirely), and nothing is dispatched.
    captured = []

    async def fake(ctx, report_config):
        captured.append(ctx)

    monkeypatch.setattr(cronstable.cron, "report_event", fake)
    cron = _cron(_JOB)
    assert cron._notify_config is None
    cron.cluster_manager = types.SimpleNamespace(
        conflict_names=lambda: [],
        conflicting_sizes=lambda: [],
        conflicting_policies=lambda: [],
        cluster_size=lambda: 3,
        distribution="single-leader",
        is_quorate=lambda: True,
        is_leader=lambda: True,
        node_name="node-a",
    )  # deliberately no leader_name
    cron._was_quorate = False
    cron._was_leader = False
    cron._emit_cluster_role_logs()  # must not raise
    await _drain_notify(cron)
    assert captured == []


# ---------------------------------------------------------------------------
# Integration seams: YAML -> Cron, reload, real reporters, live DAG call site
# ---------------------------------------------------------------------------


def test_notify_yaml_reaches_cron_dispatch_config():
    # the config -> Cron seam _dispatch_notify reads: a daemon built from
    # YAML carrying `notify:` must hold it without any test hand-wiring.
    cron = _cron(_NOTIFY_YAML + _JOB)
    assert cron._notify_config is not None
    assert cron._notify_config["events"] == frozenset(
        {"dag_failure", "quorum_loss"}
    )
    assert (
        cron._notify_config["report"]["webhook"]["url"]["value"]
        == "https://example.invalid/hook"
    )


def test_apply_reload_swaps_notify_config():
    # a reload that adds, edits, or removes `notify:` takes effect at once.
    cron = _cron(_JOB)
    assert cron._notify_config is None
    cron._apply_reload(config.parse_config_string(_NOTIFY_YAML + _JOB, ""))
    assert cron._notify_config is not None
    assert cron._notify_config["events"] == frozenset(
        {"dag_failure", "quorum_loss"}
    )
    cron._apply_reload(config.parse_config_string(_JOB, ""))
    assert cron._notify_config is None


async def test_report_event_flows_through_real_webhook_reporter():
    # no mocks: the NotifyEventContext shim rides the real WebhookReporter,
    # and the default notify body template posts Slack-shaped JSON.
    from cronstable.job import report_event

    from tests.test_job import _WebhookServer

    server = _WebhookServer()
    async with server as url:
        notify_cfg = config._build_notify_config(
            {"report": {"webhook": {"url": {"value": url}}}}
        )
        ctx = NotifyEventContext(
            event="dag_failure",
            success=False,
            name="etl",
            subject="DAG 'etl' run r1 failed",
            message="1 task(s) failed: t1",
            fields={"dag": "etl"},
        )
        await report_event(ctx, notify_cfg["report"])
    (request,) = server.requests
    payload = json.loads(request["body"])
    assert payload == {
        "text": (
            "cronstable dag_failure: DAG 'etl' run r1 failed\n"
            "1 task(s) failed: t1\n"
        )
    }


async def test_report_event_flows_through_real_shell_reporter(tmp_path):
    # the exact hazard _NotifyJobShim guards: ShellReporter builds a
    # subprocess env from the context, and any None value dies in os.fsencode
    # at spawn. Run the real reporter with a real subprocess and read back
    # what it saw.
    import sys

    from cronstable.job import report_event, report_hostname

    out = tmp_path / "env.json"
    code = (
        "import json,os,sys;"
        "json.dump({k: os.environ[k] for k in sys.argv[2:]},"
        " open(sys.argv[1], 'w'))"
    )
    wanted = [
        "CRONSTABLE_JOB_NAME",
        "CRONSTABLE_FAIL_REASON",
        "CRONSTABLE_HOST",
        "CRONSTABLE_RETCODE",
        "CRONSTABLE_JOB_SCHEDULE",
    ]
    notify_cfg = config._build_notify_config(
        {
            "report": {
                "shell": {
                    "command": [sys.executable, "-c", code, str(out)] + wanted
                }
            }
        }
    )
    ctx = NotifyEventContext(
        event="quorum_loss",
        success=False,
        name="node-a",
        subject="node node-a left quorum",
        message="No majority reachable.",
        fields={"quorate": False},
    )
    await report_event(ctx, notify_cfg["report"])
    seen = json.loads(out.read_text())
    assert seen["CRONSTABLE_JOB_NAME"] == "node-a"
    assert seen["CRONSTABLE_FAIL_REASON"] == "No majority reachable."
    assert seen["CRONSTABLE_HOST"] == report_hostname()
    assert seen["CRONSTABLE_RETCODE"] == "None"  # no process ran
    assert seen["CRONSTABLE_JOB_SCHEDULE"] == ""


async def test_do_advance_dispatches_approval_waiting_live(
    tmp_path, monkeypatch
):
    # the LIVE call site (dagrun._do_advance -> _notify_pending_approvals):
    # a real run parks a real approval gate and the alert fires once, with
    # the dedup holding across further advances. Only report_event is
    # recorded; everything upstream of it is production code.
    from tests.test_state_dag_run import _drive, _make_cron, _teardown

    captured = []

    async def fake_report_event(ctx, report_config):
        captured.append(ctx)

    monkeypatch.setattr(cronstable.cron, "report_event", fake_report_event)
    yaml = (
        "notify:\n  report:\n    webhook:\n      url:\n"
        "        value: https://example.invalid/hook\n"
        "dags:\n  - name: ap\n    tasks:\n"
        "      - id: gate\n        type: approval\n"
    )
    cron = await _make_cron(tmp_path, yaml)
    try:
        run_key = (await cron._dag.trigger("ap"))["runKey"]
        body = await _drive(cron, "ap", run_key)
        assert body["tasks"]["gate"]["awaitingApproval"] is True
        await _drain_notify(cron)
        assert [c.event for c in captured] == ["approval_waiting"]
        vars_ = captured[0].template_vars
        assert vars_["dag"] == "ap"
        assert vars_["taskkey"] == "gate"
        # further advances re-observe the parked gate; the dedup holds.
        await _drive(cron, "ap", run_key)
        await _drain_notify(cron)
        assert len(captured) == 1
    finally:
        await _teardown(cron)


async def test_emit_cluster_role_logs_fires_leader_loss(monkeypatch):
    captured = []

    async def fake(ctx, report_config):
        captured.append(ctx)

    monkeypatch.setattr(cronstable.cron, "report_event", fake)
    cron = _cron(_JOB)
    cron._notify_config = {"events": None, "report": {}}
    # this node lost leadership to node-b; still quorate, so the one event
    # is the loss, with the follower role and the new leader named.
    cron.cluster_manager = _fake_mgr(
        quorate=True, leader=False, leader_name="node-b"
    )
    cron._was_quorate = True
    cron._was_leader = True
    cron._emit_cluster_role_logs()
    await _drain_notify(cron)
    assert len(captured) == 1
    tv = captured[0].template_vars
    assert tv["event"] == "leader_change"
    assert tv["is_leader"] is False
    assert tv["role"] == "follower"
    assert tv["leader"] == "node-b"
    assert "lost" in tv["subject"]


async def test_emit_cluster_role_logs_quorum_regain_is_silent(monkeypatch):
    # regaining quorum is recovery: logged, never paged (the loss was the
    # alert). No event of any kind fires on the regain transition.
    captured = []

    async def fake(ctx, report_config):
        captured.append(ctx)

    monkeypatch.setattr(cronstable.cron, "report_event", fake)
    cron = _cron(_JOB)
    cron._notify_config = {"events": None, "report": {}}
    cron.cluster_manager = _fake_mgr(quorate=True, leader=False)
    cron._was_quorate = False  # was out, now quorate -> a regain transition
    cron._was_leader = False
    cron._emit_cluster_role_logs()
    await _drain_notify(cron)
    assert captured == []
    assert cron._was_quorate is True  # the transition itself was recorded


# ---------------------------------------------------------------------------
# SentryReporter: what the daemon's error events carry
# ---------------------------------------------------------------------------

_SENTRY_DSN = "http://key@sentry.invalid/1"
_SENTRY_LOG = "launching task %s failed"


@pytest.fixture
def sentry_events(monkeypatch):
    """Events the Sentry SDK sends, kept in memory.

    Teardown rebinds the client that was bound before the test.
    """
    events = []

    class MemoryTransport(Transport):
        def capture_envelope(self, envelope):
            event = envelope.get_event()
            if event is not None:
                events.append(event)

    monkeypatch.setattr("sentry_sdk.client.make_transport", MemoryTransport)
    scope = sentry_sdk.Scope.get_global_scope()
    client = scope.client
    yield events
    scope.set_client(client)


async def _sentry_report():
    # one real report: it initializes the SDK the way the daemon does
    notify_cfg = config._build_notify_config(
        {"report": {"sentry": {"dsn": {"value": _SENTRY_DSN}}}}
    )
    ctx = NotifyEventContext(
        event="dag_failure",
        success=False,
        name="etl",
        subject="DAG 'etl' run r1 failed",
        message="1 task(s) failed: t1",
    )
    await SentryReporter().report(False, ctx, notify_cfg["report"])


def _log_error_holding(value):
    # an error record with a traceback, from a frame whose locals hold `value`
    held = {"region": value}
    try:
        raise RuntimeError("no answer")
    except RuntimeError:
        logging.getLogger("cronstable").exception(_SENTRY_LOG, "t1")
    return held


def _logged_event(events):
    (event,) = [
        event
        for event in events
        if (event.get("logentry") or {}).get("message") == _SENTRY_LOG
    ]
    return event


def _frames(event):
    (exception,) = event["exception"]["values"]
    return exception["stacktrace"]["frames"]


async def _post_to_late_listener(body):
    # a listener built after the SDK initialized runs inside the SDK's
    # aiohttp integration; its handler reads the body, then logs an error
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    async def handler(request):
        await request.read()
        _log_error_holding(None)
        return web.Response()

    app = web.Application()
    app.router.add_post("/jobs/etl/start", handler)
    async with TestClient(TestServer(app)) as client:
        response = await client.post("/jobs/etl/start", json=body)
        assert response.status == 200


async def test_sentry_error_event_omits_frame_locals(sentry_events):
    marker = uuid.uuid4().hex
    await _sentry_report()
    _log_error_holding(marker)
    event = _logged_event(sentry_events)
    frames = _frames(event)
    # the event has the traceback, without the variables of its frames
    assert frames[-1]["function"] == "_log_error_holding"
    assert not any("vars" in frame for frame in frames)
    assert marker not in json.dumps(event, default=str)


async def test_sentry_error_event_omits_request_body(sentry_events):
    marker = uuid.uuid4().hex
    await _sentry_report()
    await _post_to_late_listener({"params": {"region": marker}})
    event = _logged_event(sentry_events)
    # the event describes the request, without its body
    assert event["request"]["url"].endswith("/jobs/etl/start")
    assert marker not in json.dumps(event, default=str)


async def test_sentry_report_keeps_its_own_event(sentry_events):
    await _sentry_report()
    (event,) = sentry_events
    assert "DAG 'etl' run r1 failed" in event["message"]
    assert event["level"] == "error"
    assert event["fingerprint"] == ["cronstable", "dag_failure", "etl"]
    assert event["extra"]["job"] == "etl"
    assert event["extra"]["success"] is False


async def test_sentry_client_sends_no_frame_locals_or_request_bodies(
    sentry_events,
):
    await _sentry_report()
    options = sentry_sdk.get_client().options
    assert options["include_local_variables"] is False
    assert options["max_request_body_size"] == "never"
