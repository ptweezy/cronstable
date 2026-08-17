"""Inbound heartbeat monitoring: the state machine, ingest, and monitor.

Three layers, tested where each of them lives:

* :mod:`cronstable.heartbeat` is pure, so its half is exercised against a
  frozen clock with no daemon, socket or store in sight -- which is the
  point of it being pure.
* the config layer's job is to refuse the configurations that would fail
  silently at runtime, so every rejection is pinned with the reason.
* the daemon's half needs a real aiohttp app for the ping route, because
  the interesting part of it IS the middleware: the one route in this API
  that no bearer token gates.
"""

import asyncio
import datetime

import pytest

import cronstable.cron
from cronstable import heartbeat
from cronstable.config import (
    ConfigError,
    _validate_cross_sections,
    parse_config_string,
)
from cronstable.heartbeat import (
    PING_FAIL,
    PING_START,
    PING_SUCCESS,
    PingRateLimiter,
    PingRecord,
    derive_token,
    observe,
)
from tests.test_cron_web import start_web_app  # noqa: F401

UTC = datetime.timezone.utc
T0 = datetime.datetime(2026, 8, 17, 12, 0, tzinfo=UTC)

_WEB = """
web:
  listen:
    - http://127.0.0.1:0
  pingSecret:
    value: test-ping-secret
"""

_TWO_HEARTBEATS = (
    _WEB
    + """
heartbeats:
  - name: interval
    description: every ten minutes
    periodSeconds: 600
    graceSeconds: 60
    maxRuntimeSeconds: 120
  - name: nightly
    schedule: "0 2 * * *"
    timezone: UTC
    graceSeconds: 900
"""
)


def _config(yaml):
    """Parse AND cross-validate, the way `parse_config` does on disk.

    The cross-section rules (name collisions, reachable ping URLs) only
    run at the top-level entry point, so a test that skipped them would
    be testing a config shape the daemon never sees.
    """
    config = parse_config_string(yaml, "test.yaml")
    _validate_cross_sections(config)
    return config


def _hb(yaml, index=0):
    return _config(yaml).heartbeats[index]


def _at(seconds):
    return T0 + datetime.timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# Token derivation
# ---------------------------------------------------------------------------


def test_token_is_stable_and_per_name():
    # The whole value of a derived token: the URL in a hundred crontabs
    # survives restarts and reloads, with nothing stored anywhere.
    assert derive_token("s", "backup") == derive_token("s", "backup")
    assert derive_token("s", "backup") != derive_token("s", "restore")
    assert derive_token("s", "backup") != derive_token("t", "backup")


def test_token_shape_is_url_safe_and_long_enough():
    token = derive_token("secret", "nightly-backup")
    assert len(token) == heartbeat.TOKEN_LENGTH
    # base32 lowercase: nothing a shell, a URL or a mail client mangles
    assert set(token) <= set("abcdefghijklmnopqrstuvwxyz234567")
    assert heartbeat.token_is_wellformed(token)


def test_token_derivation_is_unambiguous_across_names():
    # The length prefix is what stops "ab"+"c" and "a"+"bc" colliding; a
    # collision would silently point two heartbeats at one URL.
    assert derive_token("s", "ab-c") != derive_token("s", "abc")
    assert derive_token("s", "a\x00bc") != derive_token("s", "abc")


@pytest.mark.parametrize(
    "token",
    ["", "x" * 129, "has spaces", "Upper", "semi;colon", "sl/ash"],
)
def test_malformed_tokens_are_rejected_by_shape(token):
    assert not heartbeat.token_is_wellformed(token)


# ---------------------------------------------------------------------------
# The state machine
# ---------------------------------------------------------------------------


def test_never_pinged_ages_from_first_seen_not_from_zero():
    # A fresh boot (or a reload that adds a heartbeat) must grant a full
    # window before it can report: a restart is not evidence of a missed
    # run. But it MUST eventually report -- a backup that never ran once
    # is exactly what this feature exists to catch.
    hb = _hb(_TWO_HEARTBEATS)
    record = PingRecord()
    assert observe(hb, record, T0, first_seen=T0).state == heartbeat.STATE_NEW
    assert (
        observe(hb, record, _at(601), first_seen=T0).state
        == heartbeat.STATE_LATE
    )
    late = observe(hb, record, _at(700), first_seen=T0)
    assert late.state == heartbeat.STATE_DOWN
    assert late.reason == heartbeat.REASON_MISSED


def test_grace_window_is_visible_but_silent():
    hb = _hb(_TWO_HEARTBEATS)
    record = PingRecord().apply(PING_SUCCESS, T0)
    inside = observe(hb, record, _at(630), first_seen=T0)
    assert inside.state == heartbeat.STATE_LATE
    assert inside.alerting is False
    assert inside.overdue_seconds == 30
    outside = observe(hb, record, _at(700), first_seen=T0)
    assert outside.state == heartbeat.STATE_DOWN
    assert outside.alerting is True


def test_explicit_failure_short_circuits_the_clock():
    # The job spoke; waiting out a grace window to believe it would only
    # delay the page.
    hb = _hb(_TWO_HEARTBEATS)
    record = PingRecord().apply(PING_FAIL, T0)
    seen = observe(hb, record, _at(1), first_seen=T0)
    assert (seen.state, seen.reason) == (
        heartbeat.STATE_DOWN,
        heartbeat.REASON_FAILED,
    )
    assert seen.since == T0


def test_start_with_no_finish_overruns():
    hb = _hb(_TWO_HEARTBEATS)  # maxRuntimeSeconds: 120
    record = PingRecord().apply(PING_START, T0)
    assert observe(hb, record, _at(60), first_seen=T0).state == (
        heartbeat.STATE_UP
    )
    over = observe(hb, record, _at(180), first_seen=T0)
    assert (over.state, over.reason) == (
        heartbeat.STATE_DOWN,
        heartbeat.REASON_OVERRUN,
    )
    # the onset is when the bound elapsed, not when anyone looked
    assert over.since == _at(120)


def test_a_finish_closes_the_overrun_window_and_times_the_run():
    hb = _hb(_TWO_HEARTBEATS)
    record = PingRecord().apply(PING_START, T0).apply(PING_SUCCESS, _at(45))
    assert record.last_duration_seconds == 45
    assert observe(hb, record, _at(180), first_seen=T0).state == (
        heartbeat.STATE_UP
    )


def test_down_onset_is_derived_not_stamped():
    # Every surface must agree on WHEN it went down without waiting for a
    # monitor pass to stamp it; that is what keeps a payload from saying
    # `state: down` beside an empty onset.
    hb = _hb(_TWO_HEARTBEATS)
    record = PingRecord().apply(PING_SUCCESS, T0)
    for when in (_at(700), _at(5000)):
        assert observe(hb, record, when, first_seen=T0).since == _at(660)


def test_schedule_mode_expects_a_ping_after_each_fire():
    hb = _hb(_TWO_HEARTBEATS, 1)  # 0 2 * * * UTC, grace 900
    pinged = datetime.datetime(2026, 8, 17, 2, 0, 30, tzinfo=UTC)
    record = PingRecord().apply(PING_SUCCESS, pinged)
    tomorrow = datetime.datetime(2026, 8, 18, 2, 0, tzinfo=UTC)
    seen = observe(hb, record, pinged + datetime.timedelta(hours=6),
                   first_seen=T0)
    assert seen.state == heartbeat.STATE_UP
    assert seen.due_at == tomorrow
    # still fine inside the grace window after tomorrow's fire...
    ok = observe(hb, record, tomorrow + datetime.timedelta(minutes=10),
                 first_seen=T0)
    assert ok.state == heartbeat.STATE_LATE
    # ...and down past it
    late = observe(hb, record, tomorrow + datetime.timedelta(minutes=20),
                   first_seen=T0)
    assert late.state == heartbeat.STATE_DOWN


def test_disabled_and_paused_are_excused():
    hb = _hb(_TWO_HEARTBEATS)
    record = PingRecord()  # never pinged, long past due
    held = observe(hb, record, _at(9999), first_seen=T0, paused=True)
    assert held.state == heartbeat.STATE_PAUSED
    assert held.alerting is False
    hb.enabled = False
    off = observe(hb, record, _at(9999), first_seen=T0)
    assert off.state == heartbeat.STATE_DISABLED
    assert off.alerting is False


# ---------------------------------------------------------------------------
# The ping record
# ---------------------------------------------------------------------------


def test_record_round_trips_through_the_store_shape():
    record = (
        PingRecord()
        .apply(PING_START, T0)
        .apply(PING_SUCCESS, _at(30), exit_code=0, run_id="r1", body="hi")
    )
    assert PingRecord.from_dict(record.to_dict()) == record


@pytest.mark.parametrize(
    "body",
    [
        None,
        {},
        "not a dict",
        {"lastPingAt": "not-a-timestamp"},
        {"totalPings": True, "lastExitCode": "nope"},
        {"lastPingAt": 17, "lastKind": []},
    ],
)
def test_reading_a_corrupt_record_never_raises(body):
    # A corrupt document must not be able to wedge the monitor for a
    # heartbeat that is otherwise perfectly fine.
    record = PingRecord.from_dict(body)
    assert record.last_ping_at is None or isinstance(
        record.last_ping_at, datetime.datetime
    )
    assert record.total_pings == 0 or isinstance(record.total_pings, int)


def test_a_replayed_ping_cannot_move_the_record_backwards():
    # Two nodes racing one ping, or a retried curl arriving after a newer
    # one: the counters still move (it really happened), but the newest
    # ping's own detail is not overwritten by the older arrival.
    newest = PingRecord().apply(PING_SUCCESS, _at(60), exit_code=0)
    replayed = newest.apply(PING_FAIL, T0, exit_code=9)
    assert replayed.last_ping_at == _at(60)
    assert replayed.last_exit_code == 0
    assert replayed.last_kind == PING_SUCCESS
    assert replayed.total_pings == 2
    assert replayed.total_fails == 1
    # ...but the failure instant is still recorded, since it did happen
    assert replayed.last_fail_at == T0


def test_a_naive_stored_timestamp_reads_as_utc():
    # Only a hand-edited document produces one; dropping it would be
    # worse than assuming the zone every writer emits.
    record = PingRecord.from_dict({"lastPingAt": "2026-08-17T12:00:00"})
    assert record.last_ping_at == T0


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


def test_rate_limiter_allows_a_burst_then_throttles():
    limiter = PingRateLimiter(burst=3, rate=1.0)
    assert [limiter.allow("a", 0.0) for _ in range(4)] == [
        True,
        True,
        True,
        False,
    ]
    # a second later, one more token has dripped in
    assert limiter.allow("a", 1.0) is True
    assert limiter.allow("a", 1.0) is False


def test_rate_limiter_is_per_heartbeat_and_pruned_on_reload():
    limiter = PingRateLimiter(burst=1, rate=0.0)
    assert limiter.allow("a", 0.0) is True
    assert limiter.allow("a", 0.0) is False
    assert limiter.allow("b", 0.0) is True  # b has its own bucket
    limiter.retain({"b"})
    assert limiter.allow("a", 0.0) is True  # a was forgotten, so full again


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_a_valid_config_loads_both_spellings():
    config = _config(_TWO_HEARTBEATS)
    interval, nightly = config.heartbeats
    assert (interval.period, interval.schedule_tab) == (600, None)
    assert nightly.period is None and str(nightly.schedule_tab) == "0 2 * * *"
    assert interval.ping_token("test-ping-secret") == derive_token(
        "test-ping-secret", "interval"
    )


@pytest.mark.parametrize(
    "body, expected",
    [
        ("  - name: a\n", "needs either `periodSeconds`"),
        (
            "  - name: a\n    periodSeconds: 60\n    schedule: '* * * * *'\n",
            "keep one",
        ),
        ("  - name: a\n    periodSeconds: 0\n", "must be > 0"),
        ("  - name: a\n    periodSeconds: 60\n    graceSeconds: -1\n",
         "graceSeconds must be >= 0"),
        (
            "  - name: a\n    periodSeconds: 60\n    maxRuntimeSeconds: 0\n",
            "maxRuntimeSeconds must be > 0",
        ),
        (
            "  - name: a\n    periodSeconds: 60\n    timezone: UTC\n",
            "only apply to a `schedule`",
        ),
        ("  - name: a\n    schedule: '@reboot'\n", "names a daemon event"),
        (
            "  - name: a\n    schedule: 'not a cron expression'\n",
            "invalid schedule",
        ),
        (
            "  - name: a\n    periodSeconds: 60\n    timezone: Mars/Olympus\n",
            "unknown timezone",
        ),
    ],
)
def test_bad_heartbeat_definitions_are_refused_at_load(body, expected):
    with pytest.raises(ConfigError) as excinfo:
        _config(_WEB + "heartbeats:\n" + body)
    assert expected in str(excinfo.value)


def test_duplicate_heartbeat_names_are_refused():
    yaml = (
        _WEB
        + "heartbeats:\n"
        + "  - name: a\n    periodSeconds: 60\n"
        + "  - name: a\n    periodSeconds: 60\n"
    )
    with pytest.raises(ConfigError, match="duplicate heartbeat name"):
        _config(yaml)


def test_a_heartbeat_may_not_share_a_job_name():
    # Alerts, metric labels and the incident list all key on `name`, so a
    # collision makes "backup is down" ambiguous exactly when it matters.
    yaml = (
        _WEB
        + "jobs:\n  - name: backup\n    command: 'true'\n"
        + "    schedule: '* * * * *'\n"
        + "heartbeats:\n  - name: backup\n    periodSeconds: 60\n"
    )
    with pytest.raises(ConfigError, match="already used by a job"):
        _config(yaml)


def test_a_heartbeat_with_no_reachable_url_is_refused():
    # No pingSecret and no explicit token: the heartbeat could only ever
    # report down, because nothing has an address to ping.
    yaml = (
        "web:\n  listen:\n    - http://127.0.0.1:0\n"
        "heartbeats:\n  - name: a\n    periodSeconds: 60\n"
    )
    with pytest.raises(ConfigError, match="have no ping URL"):
        _config(yaml)


def test_heartbeats_need_a_web_section_to_be_pingable():
    yaml = (
        "heartbeats:\n  - name: a\n    periodSeconds: 60\n"
        "    token:\n      value: pinned-token\n"
    )
    with pytest.raises(ConfigError, match="need a `web` section"):
        _config(yaml)


def test_two_heartbeats_may_not_pin_the_same_token():
    yaml = (
        _WEB
        + "heartbeats:\n"
        + "  - name: a\n    periodSeconds: 60\n"
        + "    token:\n      value: same\n"
        + "  - name: b\n    periodSeconds: 60\n"
        + "    token:\n      value: same\n"
    )
    with pytest.raises(ConfigError, match="share the same explicit"):
        _config(yaml)


def test_an_explicit_token_overrides_the_derived_one():
    yaml = (
        _WEB
        + "heartbeats:\n  - name: a\n    periodSeconds: 60\n"
        + "    token:\n      value: pinned-token\n"
    )
    hb = _config(yaml).heartbeats[0]
    assert hb.ping_token("test-ping-secret") == "pinned-token"


def test_heartbeats_do_not_inherit_the_job_defaults_block():
    # `defaults:` carries job-launch keys; silently applying its reporters
    # to every heartbeat would be a surprise.
    yaml = (
        _WEB
        + "defaults:\n  onFailure:\n    report:\n      shell:\n"
        + "        command: echo job-level\n"
        + "heartbeats:\n  - name: a\n    periodSeconds: 60\n"
    )
    hb = _config(yaml).heartbeats[0]
    assert hb.onFailure["report"]["shell"]["command"] is None


# ---------------------------------------------------------------------------
# The daemon: ingest
# ---------------------------------------------------------------------------


async def _serve(start_web_app, yaml=_TWO_HEARTBEATS, **web_over):
    """A real running web app over ``yaml``; returns (cron, base URL)."""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    web = {"listen": ["http://127.0.0.1:0"], "ui": False}
    web.update(web_over)
    await start_web_app(cron, web)
    port = cron.web_runner.addresses[0][1]
    return cron, "http://127.0.0.1:{}".format(port)


@pytest.mark.asyncio
async def test_ping_is_served_without_a_bearer_token(start_web_app):
    # The whole point: the URL goes into a crontab line on a machine that
    # must never hold the dashboard's token. Every OTHER route on this
    # same daemon still 401s.
    import aiohttp

    cron, base = await _serve(
        start_web_app, authToken={"value": "dashboard-secret"}
    )
    token = derive_token("test-ping-secret", "interval")
    async with aiohttp.ClientSession() as session:
        async with session.get(base + "/heartbeats") as resp:
            assert resp.status == 401
        async with session.post(base + "/ping/" + token) as resp:
            assert resp.status == 200
            assert (await resp.json())["heartbeat"] == "interval"
    assert cron._heartbeat_records["interval"].total_pings == 1


@pytest.mark.asyncio
async def test_ping_accepts_get_and_post(start_web_app):
    # A great many things that can call a URL cannot choose the method.
    import aiohttp

    cron, base = await _serve(start_web_app)
    token = derive_token("test-ping-secret", "interval")
    async with aiohttp.ClientSession() as session:
        async with session.get(base + "/ping/" + token) as resp:
            assert resp.status == 200
        async with session.post(base + "/ping/" + token) as resp:
            assert resp.status == 200
    assert cron._heartbeat_records["interval"].total_pings == 2


@pytest.mark.asyncio
async def test_ping_signals_map_to_kinds(start_web_app):
    import aiohttp

    cron, base = await _serve(start_web_app)
    token = derive_token("test-ping-secret", "interval")
    async with aiohttp.ClientSession() as session:
        for suffix, kind in (
            ("", PING_SUCCESS),
            ("/0", PING_SUCCESS),
            ("/start", PING_START),
            ("/fail", PING_FAIL),
            ("/7", PING_FAIL),
        ):
            url = base + "/ping/" + token + suffix
            async with session.post(url) as resp:
                assert resp.status == 200, suffix
                assert (await resp.json())["kind"] == kind, suffix
    record = cron._heartbeat_records["interval"]
    assert record.last_exit_code == 7
    assert record.total_fails == 2


@pytest.mark.asyncio
async def test_unknown_and_malformed_tokens_answer_the_same_404(
    start_web_app,
):
    # Probing the URL space must learn nothing: a wrong token, a malformed
    # one and a bad signal all answer 404.
    import aiohttp

    cron, base = await _serve(start_web_app)
    token = derive_token("test-ping-secret", "interval")
    async with aiohttp.ClientSession() as session:
        for path in (
            "/ping/" + "a" * 26,
            "/ping/NOT-A-TOKEN",
            "/ping/" + token + "/nonsense",
        ):
            async with session.post(base + path) as resp:
                assert resp.status == 404, path
    # none of them touched the record
    assert "interval" not in cron._heartbeat_records


@pytest.mark.asyncio
async def test_ping_body_and_run_id_are_captured_and_bounded(start_web_app):
    import aiohttp

    cron, base = await _serve(start_web_app)
    token = derive_token("test-ping-secret", "interval")
    async with aiohttp.ClientSession() as session:
        async with session.post(
            base + "/ping/" + token + "?rid=" + "r" * 200,
            data="chunk 417: sha256 mismatch\n" * 200,
        ) as resp:
            assert resp.status == 200
    record = cron._heartbeat_records["interval"]
    assert len(record.last_body) <= cronstable.cron.HEARTBEAT_BODY_KEEP_CHARS
    assert record.last_body.startswith("chunk 417")
    # an oversized correlation id is truncated, never refused: a cosmetic
    # mistake by the caller must not turn into a false outage.
    assert len(record.last_run_id) == cronstable.cron.HEARTBEAT_RUN_ID_MAX


@pytest.mark.asyncio
async def test_ping_rate_limit_answers_429(start_web_app):
    import aiohttp

    cron, base = await _serve(start_web_app)
    cron._heartbeat_limiter = PingRateLimiter(burst=2, rate=0.0)
    token = derive_token("test-ping-secret", "interval")
    statuses = []
    async with aiohttp.ClientSession() as session:
        for _ in range(4):
            async with session.post(base + "/ping/" + token) as resp:
                statuses.append(resp.status)
    assert statuses == [200, 200, 429, 429]


@pytest.mark.asyncio
async def test_a_ping_resolves_its_own_latch_at_once(start_web_app):
    # The monitor pass finds the BAD news, because only the clock can.
    # A ping is not an absence, so the good news lands immediately rather
    # than waiting up to a housekeeping pass.
    import aiohttp

    cron, base = await _serve(start_web_app)
    cron._heartbeat_state["interval"] = (
        heartbeat.STATE_DOWN,
        heartbeat.REASON_MISSED,
        T0,
    )
    token = derive_token("test-ping-secret", "interval")
    async with aiohttp.ClientSession() as session:
        async with session.post(base + "/ping/" + token) as resp:
            assert resp.status == 200
    assert "interval" not in cron._heartbeat_state


# ---------------------------------------------------------------------------
# The daemon: monitor, payloads and counts
# ---------------------------------------------------------------------------


def test_monitor_latches_a_down_once_and_recovers_once():
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    reported = []
    cron._queue_heartbeat_report = lambda hb, obs, **kw: reported.append(
        (hb.name, obs.state, obs.reason)
    )
    cron._heartbeat_first_seen["interval"] = T0
    # inside the window: nothing
    cron._heartbeat_evaluate("interval", now=_at(60))
    assert reported == []
    # past due + grace: one report, and only one however often we look
    cron._heartbeat_evaluate("interval", now=_at(700))
    cron._heartbeat_evaluate("interval", now=_at(800))
    assert reported == [("interval", heartbeat.STATE_DOWN, "missed")]
    # a ping recovers it, once
    cron._heartbeat_records["interval"] = PingRecord().apply(
        PING_SUCCESS, _at(900)
    )
    cron._heartbeat_evaluate("interval", now=_at(901))
    cron._heartbeat_evaluate("interval", now=_at(902))
    assert reported[-1] == ("interval", heartbeat.STATE_UP, None)
    assert len(reported) == 2


def test_a_changed_reason_re_reports_but_keeps_the_onset():
    # missed -> failed is a new fact about the same outage: it reports
    # again, but the outage did not restart.
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    onsets = []
    cron._queue_heartbeat_report = lambda hb, obs, **kw: onsets.append(
        (obs.reason, kw["down_since"])
    )
    cron._heartbeat_first_seen["interval"] = T0
    cron._heartbeat_evaluate("interval", now=_at(700))
    cron._heartbeat_records["interval"] = PingRecord().apply(
        PING_FAIL, _at(800)
    )
    cron._heartbeat_evaluate("interval", now=_at(801))
    assert [reason for reason, _ in onsets] == ["missed", "failed"]
    assert onsets[0][1] == onsets[1][1] == _at(660)


def test_an_excusal_clears_the_latch_without_claiming_a_recovery():
    # Nothing came back; the operator just stopped asking.
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    reported = []
    cron._queue_heartbeat_report = lambda hb, obs, **kw: reported.append(obs)
    cron._heartbeat_first_seen["interval"] = T0
    cron._heartbeat_evaluate("interval", now=_at(700))
    assert len(reported) == 1
    cron._heartbeat_paused["interval"] = cronstable.cron.PauseInfo(
        since=_at(700), until=_at(9999), note="", by="test", channel="test"
    )
    cron._heartbeat_evaluate("interval", now=_at(800))
    assert "interval" not in cron._heartbeat_state
    assert len(reported) == 1  # no recovery report


def test_payload_never_carries_the_ping_url():
    # The URL is a write credential for the heartbeat and `view` is handed
    # out freely, so it must not appear at any scope, in any response.
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    token = derive_token("test-ping-secret", "interval")
    payload = cron.heartbeat_payload("interval", detail=True)
    assert token not in repr(payload)
    assert not any("token" in key.lower() for key in payload)
    assert token not in repr(cron.heartbeats_payload())


def test_payload_reports_the_onset_before_the_monitor_has_run():
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    cron._heartbeat_first_seen["interval"] = T0
    cron._heartbeat_records["interval"] = PingRecord().apply(PING_SUCCESS, T0)
    payload = cron.heartbeat_payload("interval", now=_at(700))
    assert payload["state"] == heartbeat.STATE_DOWN
    # no latch exists yet, and the payload still knows when it went down
    assert not cron._heartbeat_state
    assert payload["downSince"] == _at(660).isoformat()


def test_counts_are_zero_filled():
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    # anchor both on the frozen clock: the constructor stamped first_seen
    # from the real one, which would age them into their windows here.
    cron._heartbeat_first_seen = dict.fromkeys(cron.heartbeats, T0)
    counts = cron.heartbeat_counts(T0)
    assert counts["total"] == 2
    assert set(counts) == set(heartbeat.HEARTBEAT_STATES) | {"total"}
    assert counts["down"] == 0
    assert counts["new"] == 2


def test_summary_omits_heartbeats_when_none_are_configured():
    # A fleet that declares none gets the payload it always got, so a
    # client can use the key's presence as the feature probe.
    from tests._cron_helpers import _WEB_ONE_JOB

    assert "heartbeats" not in cronstable.cron.Cron(
        None, config_yaml=_WEB_ONE_JOB
    ).summary_payload()
    assert (
        cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
        .summary_payload()["heartbeats"]["total"]
        == 2
    )


def test_reload_prunes_removed_heartbeats_and_rebuilds_the_token_index():
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    cron._heartbeat_records["nightly"] = PingRecord().apply(PING_SUCCESS, T0)
    cron._heartbeat_state["nightly"] = (heartbeat.STATE_DOWN, "missed", T0)
    smaller = _config(
        _WEB + "heartbeats:\n  - name: interval\n    periodSeconds: 600\n"
    )
    cron._apply_heartbeats(smaller)
    assert set(cron.heartbeats) == {"interval"}
    assert "nightly" not in cron._heartbeat_records
    assert "nightly" not in cron._heartbeat_state
    assert set(cron._heartbeat_tokens.values()) == {"interval"}


def test_rotating_the_ping_secret_rotates_every_url():
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    before = set(cron._heartbeat_tokens)
    rotated = _config(
        _TWO_HEARTBEATS.replace("test-ping-secret", "rotated-secret")
    )
    cron._apply_heartbeats(rotated)
    assert set(cron._heartbeat_tokens).isdisjoint(before)
    assert set(cron._heartbeat_tokens.values()) == {"interval", "nightly"}


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pings_survive_a_restart_through_the_store(fs_backend):
    # Without this a restart reads as a fleet-wide silence and pages for
    # pings that did arrive.
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    cron.state_backend = fs_backend
    await cron.record_ping("interval", PING_SUCCESS, at=T0, body="done")

    successor = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    successor.state_backend = fs_backend
    await successor._rehydrate_heartbeats()
    warmed = successor._heartbeat_records["interval"]
    assert warmed.last_ping_at == T0
    assert warmed.last_body == "done"
    assert (
        successor._heartbeat_observe("interval", _at(60)).state
        == heartbeat.STATE_UP
    )


@pytest.mark.asyncio
async def test_concurrent_pings_merge_through_the_store(fs_backend):
    # Two nodes accepting pings for one heartbeat must not clobber each
    # other: the store's read-modify-write is what serialises them.
    a = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    b = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    a.state_backend = b.state_backend = fs_backend
    await asyncio.gather(
        a.record_ping("interval", PING_SUCCESS, at=T0),
        b.record_ping("interval", PING_SUCCESS, at=_at(5)),
    )
    body = await fs_backend.read_document("heartbeat", "interval")
    stored = PingRecord.from_dict(body)
    assert stored.total_pings == 2
    assert stored.last_ping_at == _at(5)


@pytest.mark.asyncio
async def test_a_broken_store_never_loses_the_in_memory_ping(fs_backend):
    # The monitor reads memory, so a dropped durable write costs a
    # restart's worth of history rather than a false alert while up.
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)

    class _Broken:
        async def mutate_document(self, *a, **kw):
            raise OSError("store is gone")

    cron.state_backend = _Broken()
    record = await cron.record_ping("interval", PING_SUCCESS, at=T0)
    assert record.last_ping_at == T0
    assert cron._heartbeat_records["interval"].last_ping_at == T0


@pytest.mark.asyncio
async def test_a_hold_rides_the_same_document_as_the_record(fs_backend):
    cron = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    cron.state_backend = fs_backend
    hold = cronstable.cron.PauseInfo(
        since=T0, until=_at(3600), note="NAS swap", by="parker", channel="api"
    )
    await cron._persist_heartbeat_pause("interval", hold)
    await cron.record_ping("interval", PING_SUCCESS, at=T0)

    successor = cronstable.cron.Cron(None, config_yaml=_TWO_HEARTBEATS)
    successor.state_backend = fs_backend
    await successor._rehydrate_heartbeats()
    # the ping write carried the hold through rather than clobbering it
    assert successor._heartbeat_paused["interval"].note == "NAS swap"
    assert successor._heartbeat_records["interval"].last_ping_at == T0
