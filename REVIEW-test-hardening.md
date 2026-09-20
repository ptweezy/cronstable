# Test hardening pass: change report for review

Repo: `cronstable` (Python cron daemon). Branch: `main`. Base commit: `51b76ad`.
State: 81 files changed in the working tree (44 tracked files modified, 1,503
insertions and 349 deletions; 37 new files or directories). Nothing is
committed, staged, or pushed. This file is itself untracked and is not part of
the change; delete it after the review.

To see the change: `git status --short` and `git diff` for tracked files; new
files are listed in full below.

## Goal of the pass

An audit on 2026-09-19 found Python line coverage already saturated (97.2%
lines, CI floor 92) and the real gaps elsewhere. This pass addressed four of
them:

1. No test hard-killed a real daemon and restarted it.
2. About 600 lines of etcd and Kubernetes network transport sat under three
   broad `# pragma: no cover` markers, tested nowhere.
3. No property-based tests, and several CI hygiene lanes were missing.
4. The dashboard JavaScript (`cronstable/web/index.html`, about 570
   functions) had no XSS tests and no test against a real daemon.

A follow-up in the same session removed resource leaks from the existing suite
so that warnings can be errors with no finalizer exemption.

## Verification status

- Full suite, random order, every warning an error: two green runs.
  Seed 2736910009 and seed 561018574, each 5,970 passed, 70 skipped,
  1 expected failure (a strict xfail, described below).
- `ruff check`, `ruff format --check`, `mypy -p cronstable`, and
  `bandit` (medium and above) pass on the package.
- `python scripts/generate_build_files.py --check` passes.
- `actionlint` 1.7.12 passes on `release.yml` and `nightly.yml`.
- `tests/test_backend_live.py` passed 20 of 20 locally against a real etcd
  v3.5.21, a second etcd with TLS and auth enabled, and a k3s apiserver,
  through both Kubernetes transports, with `CRONSTABLE_LIVE_REQUIRED=1`.
- Combined line and branch coverage measured 96.54% on macOS before the leak
  cleanup. It was not remeasured afterwards.

Not verified:

- None of the new or changed CI jobs has run on GitHub.
- All new tests ran on macOS (Python 3.14) only. The Windows handling in
  `tests/_crash_helpers.py` and the Linux behavior of every new test are
  unverified.
- The CI `backends-live` job uses a kind cluster; the local run used k3s.
- The two hard-kill acceptance scenarios passed against a locally built
  frozen binary that predates the `pid_alive` fix.
- `.tox/py-posix` on the author's machine lacks the new pytest plugins. Rebuild
  it with `tox -r -e py-posix --notest` before running plain `pytest` there.

## Product code changes (review these first)

Each fix has a regression test. Items marked **behavior change** alter what a
user can observe.

### `cronstable/cronexpr.py` (133 lines changed)

`next()` and `occurrences()` share one spring-forward fire policy in the new
`CronTab._next_instant`. Policy: every civil match resolves in the zone with
`fold=0`; each real instant fires once; fires come in real-time order.

- **Behavior change.** `occurrences()` dropped every gap-shifted fire after the
  first. For `*/15 2 * * *` in America/New_York on 2024-03-10, the scheduler
  (`next()`) fired at 03:00, 03:15, 03:30, 03:45, while `occurrences()` (the
  dashboard, calendar feed, and pressure map) showed only 03:00.
- **Behavior change.** `next()` skipped a real fire that falls between two
  shifted ones when the shift is shorter than an hour. For `0,20,40 2 * * *`
  in Australia/Lord_Howe on 2024-10-06, the real 02:40 fire was skipped.
- When the first pending match is a gap label, the code bisects for the gap's
  end and compares the shifted instant against the first real match after it.
- Review focus: correctness of the bisect and the `careful` mode switch in
  `occurrences()`; cost on the hot path (one extra `astimezone` per aware
  `next()` call; the release perf gate has not run on this change).
- Known, unchanged: the dashboard's JavaScript cron engine resolves gap labels
  differently from the daemon. `tests/test_web_engine_parity.py` documents
  this and deliberately avoids gap labels.

### `cronstable/platform.py` (47 lines changed)

- `pid_alive()` returned True for a zombie on POSIX because `os.kill(pid, 0)`
  succeeds for one. New `_is_zombie(pid)` reads the status through psutil; an
  unreadable status reads as "not a zombie". Effect of the bug: after a daemon
  crash in a container whose PID 1 never reaps, an orphaned job's in-flight
  record stayed open forever, an `@reboot` relaunch was blocked, and a DAG
  task stayed `running`.

### `cronstable/backends/etcd.py` (86 lines changed)

- The broad `# pragma: no cover` on `_post` is removed.
- Re-authentication runs once per call, after the endpoint loop. It used to run
  once per remaining endpoint when the retry failed.
- Any status other than 200 is a failed endpoint. A 3xx or 204 with a JSON
  body used to be accepted as etcd's answer.
- Docstrings no longer cite "Docker integration tests" that never existed.

### `cronstable/backends/kubernetes.py` (184 lines changed)

- The broad pragmas on `_K8sHttpTransport` and `_K8sLibraryTransport`, and the
  one on `_native_available`, are removed.
- New `_require_status`: `observe` requires 200; `write` requires 200 or 201.
  A 3xx on the lease PUT or POST used to return True, which let a node believe
  it held a lease the apiserver never stored.
- **Behavior change.** The kubeconfig CA is the only trust root for the
  apiserver. It used to be added on top of the system trust store.
- **Behavior change.** `tokenFile` in a kubeconfig user is read, and re-read
  before each request. It used to be ignored. An inline `token` wins.
- **Behavior change.** Relative `certificate-authority`, `client-certificate`,
  and `client-key` paths resolve against the kubeconfig's directory (new
  `_kubeconfig_file`). They used to resolve against the working directory.
- `user: ~` means no credentials; a non-mapping user is a `ConfigError`. Both
  used to raise `AttributeError`.
- `wiki/Configuration-Reference.md` documents the three behavior changes.

### `cronstable/mcpcli.py`, `cronstable/jobcli.py` (4 lines each)

- Both wrap the caught `urllib.error.HTTPError` in `with ex:` so the response
  (and its connection) closes. It used to stay open until garbage collection.

### `cronstable/web/index.html` (129 lines changed; `docs/demo/index.html` regenerated)

- Escaping: `e.pokeCount`, a tab counter (`t[2]`), and `d.valueSize` are now
  passed through `esc()`.
- `hashPart()` returns null on an invalid percent escape, so `#job/%E0%A4%A`
  opens nothing instead of throwing.
- `loadPrefs` keeps the default when a stored value has another type.
- `SWIM_KEY` is `cronstable.swimBuf`. It used to collide with the swimlane
  on/off preference key. **Behavior change:** the old swimlane buffer in a
  user's browser storage is abandoned once.
- The policy sort ranks jobs without a cluster policy last explicitly.
- `agoEpoch` returns "never" for a non-finite timestamp instead of throwing
  inside `tick()`.
- Focus: `pushFocus`/`popFocus` restore focus to a rebuilt row's button;
  closed overlays get `visibility: hidden` so their controls leave the tab
  order; new `focusLater` defers focus until a panel is open, and a
  `focusTurn` counter lets the most recently opened panel keep the focus.
- CSS: `.pane { min-width: 0 }`, wrapping chips in `.term-empty`, a 560px
  rule for the DAG drawer head.
- Not fixed, pinned by a strict xfail in `tests/test_web_surfaces_e2e.py`:
  `fleetSound` plays no failure cue for a job's first failure since the
  daemon started (`prev !== undefined`).

## New test files

All paths are under `tests/` unless noted.

### Crash recovery and durability

| File | What it does |
|---|---|
| `_crash_helpers.py` | Spawns the daemon from source, finds its port through psutil, hard-kills it, reaps orphans |
| `test_crash_recovery.py` | 11 tests: kill mid-run, orphaned child, pending retry, paused job, mid-DAG run (fail and retry variants), pending gate, five-kill loop, two daemons on one store with takeover |
| `_contention_worker.py`, `test_state_contention.py` | Real OS processes contend on `mutate_document`, `acquire_lease`, pool tickets; a SIGSTOP test on the lease flock |
| `test_state_atomic_write_faults.py` | 21 tests: ENOSPC, ENOSPC mid-write, fsync EIO, rename EIO against records, documents, blobs; two real `chmod 0555` tests |
| `gen_state_golden.py`, `test_state_golden.py`, `fixtures/state_v1/` | A committed v1 store tree (39 files); the test regenerates it byte for byte and rehydrates it through a real `Cron` |
| `acceptance/harness.py`, `acceptance/test_core.py`, `acceptance/README.md`, `acceptance/workloads/tick.sh`, `tick.cmd` | Two hard-kill scenarios for the frozen binary; harness gains `kill()`, `wait_for_orphans()`, `store_files()`, `log_text()` |

### Backend transports

| File | What it does |
|---|---|
| `_fake_http.py` | Shared real-socket base with a fault queue and a fake clock |
| `_fake_etcd.py` | Fake etcd v3 JSON gateway (revisions, txns, leases, auth) |
| `_fake_kube_apiserver.py` | Fake apiserver for `coordination.k8s.io/v1` Leases with `resourceVersion` concurrency |
| `_fake_kubernetes_client.py` | Fake `kubernetes` package injected through `sys.modules` |
| `_fake_conformance.py` | Wire-shape checks that run against both the fakes and real servers |
| `test_backend_etcd_transport.py` | Failover, timeout, single re-auth, redirects refused, bad bodies, TLS, auth, elections |
| `test_backend_kubernetes_transport.py` | Kubeconfig parsing table, in-cluster loading, status mapping, elections |
| `test_backend_kubernetes_library.py` | `ApiException` mapping, object conversion, transport auto-selection |
| `test_backend_live.py` | Opt-in via `CRONSTABLE_LIVE_ETCD`, `CRONSTABLE_LIVE_ETCD_AUTH` (plus `_PASSWORD`, `_CA`), `CRONSTABLE_LIVE_K8S=1`; `CRONSTABLE_LIVE_REQUIRED=1` turns skips into failures |

The live run corrected one assumption in the fake: a PUT to a Lease that does
not exist creates it and answers 201 (Leases allow create-on-update). The
fake, the conformance check, and two tests were changed to match. Single-leader
safety still holds because a second writer with the stale version gets a 409.

### Property tests (Hypothesis)

| File | What it does |
|---|---|
| `_strategies.py` | Grammar-based cron expression generator, 31 zones chosen for awkward transitions, junk text |
| `test_properties_cronexpr.py` | Naive `next`/`prev` against a brute-force scan; `next`, `prev`, `occurrences` agree; aware fires against an independent DST oracle; parse round trips; junk raises only `ValueError`; three pinned counterexamples |
| `test_properties_parsers.py` | Crontab import, YAML config (junk and mutated configs raise only `ConfigError`), generated Task XML always loads as config, redaction is stable and removes planted secrets, JSON facade round trips, calendar lines fold to 75 octets |

### Dashboard browser tests

| File | Area |
|---|---|
| `_web_e2e.py` | Shared harness: real daemon subprocess, real tokens and scopes, `page.route` fault injection, page-error collection, optional V8 coverage via `CRONSTABLE_JS_COVERAGE` |
| `test_web_xss_e2e.py` | Hostile strings through every reachable DOM sink |
| `test_web_auth_e2e.py` | 401 modal, token save and clear, view-only gating, scopes, 403 |
| `test_web_connectivity_e2e.py` | Poll failure and recovery, abort timeout, hidden-tab polling |
| `test_web_table_e2e.py` | Filters, sort, columns, empty states, keyed row reconcile |
| `test_web_actions_e2e.py` | Run, cancel, pause, resume, bulk actions and their outcomes |
| `test_web_logs_e2e.py` | Real streaming, SSE failure shapes, search, follow, download, multi-tail |
| `test_web_keyboard_e2e.py` | Every shortcut, Escape order, Tab trap, focus restore, palette |
| `test_web_dag_e2e.py` | DAG drawer, graph, XCom, gate approve and reject, recovery |
| `test_web_prefs_e2e.py` | Themes, reduced motion, prefs persistence, phone and tablet widths |
| `test_web_surfaces_e2e.py` | Wallboard, alerts, radar, calendar, pressure, state inspector, ledger, sandbox, pair QR |

About 180 tests in total. The agent that wrote them stopped on a rate limit
before reporting, so nobody has summarized their assertions; they were run and
two flaky tests were fixed, but their depth has not been reviewed.
`scripts/js_coverage_report.py` also exists but is ignored by `.gitignore`
(`/scripts/*`) and was never run.

### Guards and tools

| File | What it does |
|---|---|
| `test_ci_lanes.py` | Pins the new CI lanes to the tree: property files in the `deep` env, mutation cells against `[tool.mutmut]`, the live lane's required mode, the mindeps pins, the OS matrix |
| `test_dev_deps_parity.py` (extended) | Fails where a missing dev dependency would turn tests into silent skips: hypothesis, crontab, nacl, zeroconf, pytest-randomly, pytest-timeout, cryptography where a wheel exists |
| `_leakfinder.py` | Opt-in plugin: `python -X tracemalloc=25 -m pytest -p tests._leakfinder -p no:randomly`; writes test id, message, and allocation site per leak |

## Changes to existing tests and test infrastructure

- `pyproject.toml` `[tool.pytest.ini_options]`: `filterwarnings = ["error", ...]`
  with one exemption (`aiohttp.web_exceptions.NotAppKeyWarning`, because
  `web.RequestKey` would import aiohttp at daemon import, which
  `tests/test_perf_invariants.py` forbids); `timeout = 270`. `pytest-randomly`
  shuffles order on every run.
- `tests/conftest.py`: Hypothesis profiles `ci` (150 examples) and `thorough`
  (5,000), selected by `CRONSTABLE_HYPOTHESIS_PROFILE`; a
  `pytest_runtest_teardown` hookwrapper that runs closers registered by
  helpers, before fixtures tear down. `stateful_cron` teardown also stops the
  job API. Review focus: whether the hook is consistent with the file's
  "no autouse fixtures" rule. The argument made in the code is that it acts
  only on what a helper the test called has registered.
- `tests/_helpers.py`: `close_at_test_end`, `run_test_end_closers`,
  `start_state(cron, cfg)`, `stop_cron_state(cron)`.
- 17 test files route every direct `cron.start_stop_state(cfg)` through
  `start_state` (a mechanical rewrite of about 80 call sites):
  `test_cron_lifecycle`, `test_cron_rehydrate`, `test_cron_scheduling`,
  `test_cron_slots`, `test_cron_web`, `test_perf_invariants`,
  `test_resume_catchup`, `test_state`, `test_state_dag_run`,
  `test_state_fleet_ha`, `test_state_job_api`,
  `test_state_scheduler_durability`, `test_ui_endpoints`, `test_web_tls`,
  plus the new `test_state_golden` and `test_state_atomic_write_faults`.
- `test_main.py`: `_loop()` registers `loop.close`.
- `test_state_scheduler_durability.py`: three "store goes down" tests stop the
  backend and job API before clearing `state_backend`.
- `test_state.py`, `test_tui.py`: two file reads use `with`.
- `test_state_job_api.py`: 2 MiB bodies are sent as `io.BytesIO`.
- `test_pool_lifecycle.py`: five tests wrote
  `job.onFailure["retry"]["maximumRetries"] = 3` into a mapping shared with
  `config.DEFAULT_CONFIG`, which changed the default for every later test. New
  `_allow_retries(job)` gives the job its own copy. **Latent product hazard,
  not fixed:** `config.mergedicts` leaves sections a job does not override as
  references to the defaults; nothing in the daemon writes through them today.
- `test_cronexpr.py`: the legacy-library differential's docstring (it runs
  everywhere since `crontab` is a dev dependency).
- `test_backend_etcd.py`, `test_backend_kubernetes.py`: docstrings only.
- `test_platform.py`: two zombie tests.

## Dependencies, tox, and CI

- `pyproject.toml`: dev extra gains `hypothesis>=6.100`, `pytest-randomly`,
  `pytest-timeout`, `crontab>=1,<2`; runtime floor `strictyaml>=1.7.3` (1.7.0
  is yanked on PyPI for an import error; found by resolving the floors);
  new `[tool.mutmut]` for six pure-logic modules.
- `scripts/generate_build_files.py`: new `minimum_requirements()`; generates
  `requirements_min.txt` (every declared `>=` floor as `==`).
  `requirements_dev.txt` regenerated.
- `tox.ini`: opt-in envs `mindeps` and `deep`. The default `envlist` is
  unchanged.
- `.github/workflows/release.yml`:
  - `tox` matrix: adds `macos-latest` for all five Pythons and
    `ubuntu-24.04-arm` for 3.10 and 3.14.
  - `tox-experimental`: adds `3.14t`.
  - One Windows cell sets `CRONSTABLE_TASKXML_LIVE=1`, runs
    `test_a_real_export_converts_and_loads`, and fails on a skip. Unverified;
    a runner-specific task could make it fail.
  - New job `tox-mindeps` (Python 3.10, `tox -e mindeps`).
  - New job `backends-live` (two etcd containers, certificate minting through
    `tests._helpers._write_tls`, `helm/kind-action@v1.12.0`, junitxml check for
    zero skips). The etcd steps were run locally as written; the kind step was
    not.
  - The browser fence step lists the ten new files and no longer counts a
    strict xfail as a skip.
  - `release` now needs `tox-mindeps` and `backends-live`.
- `.github/workflows/nightly.yml` (new): scheduled daily and on dispatch; jobs
  `deep`, `devmode` (`PYTHONDEVMODE=1`), and `mutation` (one mutmut cell per
  module, results uploaded, counts in the job summary). A local trial on
  `redact.py` gave 58 killed, 19 survived, 1 timeout of 78 mutants.
- `.gitignore`: `/mutants/`.
- Docs: `CONTRIBUTING.md`, `wiki/Contributing-and-Releasing.md`,
  `wiki/Configuration-Reference.md`.

## Known issues and open items

- A formatter pass during the session rewrote unrelated test files. Those files
  were restored with `git checkout`, and format-only hunks in the remaining
  files were reverted by script. Worth a skim of `git diff --stat -- tests` for
  any file that should not be there.
- `test_killed_job_with_an_escaped_descendant_still_finishes`
  (`tests/test_job.py`) failed once under heavy load early in the session and
  passed on every rerun. Not investigated.
- Right after the `focusTurn` change, two of five runs of
  `test_web_keyboard_e2e.py` plus `test_web_demo_mirror.py` had one
  unidentified failure; the following 12 runs and two full-suite runs were
  clean.
- The coverage floor in `tox.ini` is still 92.
- No `HISTORY.md` entry was written. User-visible fixes: the two cron engine
  fixes, `pid_alive`, the five transport fixes, the dashboard fixes, the
  `strictyaml` floor.
- Audit items not addressed: unredacted live log streaming (SSE and MCP tail),
  API and MCP behavior gaps (hostile path parameters, scope wiring in
  `mcp.handle_http`, two inputs that return -32603, the false-positive test at
  `tests/test_mcp_tools.py:1378`), untested CI scripts such as
  `.github/scripts/elf_floor.py`, shell linting, package maintainer scripts
  that never run, doc-snippet validation.

## Suggested review order

1. `cronstable/cronexpr.py` and the three pinned tests at the end of
   `tests/test_properties_cronexpr.py`.
2. `cronstable/backends/kubernetes.py` trust and credential changes.
3. `cronstable/backends/etcd.py`, `cronstable/platform.py`.
4. `cronstable/web/index.html` focus handling (`focusLater`, `pushFocus`,
   `popFocus`).
5. `tests/conftest.py` teardown hook and `tests/_helpers.py` closers.
6. `.github/workflows/release.yml` new jobs and gating; `nightly.yml`.
7. Depth of the ten `test_web_*_e2e.py` files.

## How to run

```sh
tox -r -e py-posix --notest            # rebuild the env with the new plugins
tox -e py-posix                        # the gated run
pytest -p randomly -p "randomly_seed=2736910009"   # replay a green order
CRONSTABLE_HYPOTHESIS_PROFILE=thorough pytest tests/test_properties_*.py --timeout=0
python -X tracemalloc=25 -m pytest -p tests._leakfinder -p no:randomly
```
