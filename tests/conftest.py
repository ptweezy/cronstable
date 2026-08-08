"""Shared fixtures for the cronstable test suite (findings B1/B2/B6/B12).

Ground rules for this file:

- NO autouse fixtures, ever.  Everything here is opt-in by parameter name,
  so nothing can change the behavior of a test that does not request it.
  In particular the frozen-clock fixture stays in tests/_cron_helpers.py,
  where only the files that import it get it.
- Pure helpers that are not fixtures live in tests/_helpers.py; shared YAML
  constants live in tests/_configs.py.  This module holds only what needs
  pytest wiring (fixtures) plus the shared ``Req``/``_cron`` pair, which
  test files import as ``from tests.conftest import Req, _cron``.
- Heavyweight imports (cronstable.cron, cronstable.jobapi, aiohttp) happen
  inside the fixtures/helpers that need them, so loading conftest costs the
  suite nothing.

Consumers switch over file by file; the local fixtures and try/finally
teardowns they replace keep working side by side until then.
"""

import asyncio
import sys

import pytest

from tests._helpers import (
    ExitError,
    _backend,
    _drain_state_writes,
    _exit,
    _state_cfg,
)


class Req:
    """A minimal stand-in for an aiohttp request.

    Canonical copy of tests/test_ui_endpoints.py's ``Req`` (the shape the
    former test_cron.py re-declared 42x, finding B6).
    """

    def __init__(self, query=None, match=None, headers=None):
        self.query = query or {}
        self.match_info = match or {}
        self.headers = headers or {}


def _cron(yaml):
    # canonical copy of tests/test_ui_endpoints.py's _cron: a Cron parsed
    # from YAML with the web surface enabled (cron.web_config = {} is what
    # the direct-handler-call tests need, declared 44x in the former
    # test_cron.py, finding B6).
    from cronstable.cron import Cron

    cron = Cron(None, config_yaml=yaml)
    cron.web_config = {}
    return cron


# --- filesystem state backend (finding B2) ---------------------------------


@pytest.fixture
async def fs_backend(tmp_path):
    """A started FilesystemStateBackend in tmp_path, stopped on teardown.

    Replaces the ``backend = _backend(tmp_path); await backend.start()``
    prologue (229x across 6 files).  ``stop()`` is idempotent, so a test
    that stops the backend itself (e.g. to assert post-stop behavior) can
    still use this fixture.
    """
    backend = _backend(tmp_path)
    await backend.start()
    yield backend
    await backend.stop()


@pytest.fixture
async def fs_backend_factory(tmp_path):
    """Factory for the ``_backend(tmp_path, **overrides)`` cases.

    ``make(**over)`` builds a backend in tmp_path (or ``path=`` elsewhere),
    starts it unless ``start=False`` (the construction-only tests, e.g. the
    rate-limit wiring checks), and registers every started backend for
    teardown in reverse creation order.
    """
    started = []

    async def make(path=None, *, start=True, **over):
        backend = _backend(tmp_path if path is None else path, **over)
        if start:
            await backend.start()
            started.append(backend)
        return backend

    yield make
    for backend in reversed(started):
        await backend.stop()


# --- state-backed Cron factories (finding B1) --------------------------------


@pytest.fixture
async def dag_cron(tmp_path):
    """Factory for a DAG-driving Cron on a real backend, torn down like
    tests/test_state_dag_run.py's ``_teardown`` (its 150 try/finally sites).

    ``make(yaml)`` prepends ``state:\\n  path: <tmp_path>\\n`` (plus
    ``extra_state`` lines, pre-indented, e.g.
    ``"  jobApi:\\n    enabled: true\\n"`` for test_ui_endpoints.py's
    variant), starts the state layer, and returns the cron.  ``web=True``
    additionally sets ``cron.web_config = {}`` for direct handler calls.
    Teardown per cron, in reverse creation order: dag shutdown, job-api
    stop, backend stop.
    """
    crons = []

    async def make(yaml, *, extra_state="", web=False):
        from cronstable.cron import Cron

        cfg = "state:\n  path: {}\n{}".format(tmp_path, extra_state) + yaml
        cron = Cron(None, config_yaml=cfg)
        if web:
            cron.web_config = {}
        await cron.start_stop_state(_state_cfg(cfg))
        crons.append(cron)
        return cron

    yield make
    for cron in reversed(crons):
        await cron._dag.shutdown()
        await cron._stop_job_api()
        if cron.state_backend is not None:
            await cron.state_backend.stop()


@pytest.fixture
async def stateful_cron(tmp_path):
    """Factory for a scheduler-side stateful Cron, torn down like
    tests/test_state_scheduler_durability.py's ``_stop_state`` (58 sites).

    Unlike ``dag_cron``, the job YAML here does NOT gain a state section:
    ``make(yaml)`` parses the jobs as given and starts the state layer from
    a separate ``state:  path: <tmp_path>`` config (plus ``extra_state``
    lines, pre-indented), mirroring the source file's ``_stateful_cron``.
    Teardown drains pending state writes, then stops and clears the backend.
    """
    crons = []

    async def make(yaml, *, extra_state=""):
        from cronstable.cron import Cron

        cron = Cron(None, config_yaml=yaml)
        cfg = _state_cfg(
            "state:\n  path: {}\n{}".format(tmp_path, extra_state)
        )
        await cron.start_stop_state(cfg)
        assert cron.state_backend is not None
        crons.append(cron)
        return cron

    yield make
    for cron in reversed(crons):
        await _drain_state_writes(cron)
        if cron.state_backend is not None:
            await cron.state_backend.stop()
            cron.state_backend = None


# --- the loopback job-state API (findings B1/B11) ----------------------------


class JobApiHarness:
    """What the 36 four-line prologues in tests/test_state_job_api.py build:
    a started backend, a started JobStateAPI with one registered default run
    (token "tok", job "job"), and an aiohttp session already carrying the
    matching Authorization header."""

    def __init__(self, api, backend, session, ctx):
        self.api = api
        self.backend = backend
        self.session = session
        self.ctx = ctx

    def url(self, path):
        return self.api.base_url + path


@pytest.fixture
async def job_api(tmp_path):
    import aiohttp

    from cronstable.jobapi import JobStateAPI, RunContext

    backend = _backend(tmp_path)
    await backend.start()
    api = JobStateAPI(
        lambda: backend,
        base_holder="h#proc",
        config={
            "maxValueBytes": 0,
            "maxArtifactBytes": 0,
            "lockTtlSeconds": 5,
        },
    )
    await api.start()
    # the source file's _ctx() defaults, verbatim
    ctx = RunContext(
        token="tok",
        run_id="rid-tok",
        job_name="job",
        attempt=0,
        scheduled_at=None,
        host="h",
        default_scope="job",
        allowed_scopes=set(),
        secrets={},
    )
    api.register_run(ctx)
    session = aiohttp.ClientSession(
        headers={"Authorization": "Bearer tok"}
    )
    try:
        yield JobApiHarness(api, backend, session, ctx)
    finally:
        await session.close()
        await api.stop()
        await backend.stop()


# --- CLI runner (finding B12) ------------------------------------------------


class CliRunner:
    """The ExitError/_exit/_run/_cli scaffolding triplicated across
    tests/test_state_admin.py, tests/test_main.py and
    tests/test_state_job_cli.py.

    Calling the runner drives ``cronstable.__main__.main_loop`` on a fresh
    event loop with a fake ``sys.argv`` and ``sys.exit`` captured as
    :class:`tests._helpers.ExitError`, returning the exit code.  ``env=``
    sets environment variables first (test_state_job_cli.py's
    ``CRONSTABLE_STATE_URL``/``_TOKEN``); any extra monkeypatching (e.g.
    ``jobcli._http``) is the caller's, done before invoking.
    """

    ExitError = ExitError
    raise_exit = staticmethod(_exit)
    run_coro = staticmethod(asyncio.run)  # test_state_admin.py's _run

    def __init__(self, monkeypatch):
        self._monkeypatch = monkeypatch

    def __call__(self, argv, *, env=None):
        import cronstable.__main__

        loop = asyncio.new_event_loop()
        try:
            for key, value in (env or {}).items():
                self._monkeypatch.setenv(key, value)
            self._monkeypatch.setattr(sys, "argv", ["cronstable"] + list(argv))
            self._monkeypatch.setattr(sys, "exit", _exit)
            with pytest.raises(ExitError) as excinfo:
                cronstable.__main__.main_loop(loop)
            return excinfo.value.args[0]
        finally:
            loop.close()


@pytest.fixture
def cli_runner(monkeypatch):
    return CliRunner(monkeypatch)
