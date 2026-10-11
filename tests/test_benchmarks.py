"""Smoke tests for the performance benchmark tooling in benchmarks/.

The CI perf gate runs benchmarks/bench.py against both the current commit and
the previous release and diffs the two with benchmarks/compare.py, so a
harness that crashes (or silently skips everything) would take the release
gate down with it.  These tests run the suite in its minimal --smoke mode and
exercise compare.py's merge, chart, and gate logic on synthetic inputs.
"""

import datetime
import json
import os
import statistics
import subprocess
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH = os.path.join(REPO_ROOT, "benchmarks", "bench.py")
COMPARE = os.path.join(REPO_ROOT, "benchmarks", "compare.py")


def _run(args, **kwargs):
    return subprocess.run(
        [sys.executable] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        **kwargs,
    )


def _expected_gated_names():
    """The metric ids the release comparison requires, from the one source.

    Same comment-stripping parse as compare.py's _load_expected_gated, so
    the smoke net and the release integrity check read identical lists.
    """
    path = os.path.join(REPO_ROOT, "benchmarks", "expected_gated.txt")
    names = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                names.append(line)
    return names


def _importable(module):
    import importlib.util

    return importlib.util.find_spec(module) is not None


# --- the checked-in budgets.json ------------------------------------------
#
# Every other budget test in this file builds a synthetic document in
# tmp_path, so these two are the only ones that read the shipped file.  It
# needs reading: compare.py takes it with a bare doc.get("budgets", {}), so a
# renamed section, or an entry left behind under "proposed", disarms ceilings
# with the perf job exiting 0 and printing nothing.  A disarmed file is
# indistinguishable from an intended one by inspection.
#
# The limit of these checks, so they are not mistaken for cover: they cannot
# detect a stale stamp.  max/observed is scale-invariant and a maintainer
# writes both numbers in one edit, so a ceiling and an observation that are
# equally out of date score the same ~1.6x forever.  The nine entries as they
# stood before the 2026-08-07 re-stamp all pass, including the one then
# sitting at 8.3x the value CI measured.  'set' is where staleness shows, and
# reading it means comparing against a fresh CI number, which no offline test
# has.  What is covered below is shape and arming.


def _real_budgets_doc():
    path = os.path.join(REPO_ROOT, "benchmarks", "budgets.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def test_checked_in_budgets_are_armed_and_sized_against_observed_values():
    doc = _real_budgets_doc()
    # compare.py reads exactly this key. A typo here is silent.
    assert "budgets" in doc, (
        "benchmarks/budgets.json has no 'budgets' section, so compare.py "
        "arms NOTHING and the perf job still exits 0"
    )
    armed = doc["budgets"]
    assert armed, "the 'budgets' section is empty: every ceiling is disarmed"

    gated = set(_expected_gated_names())
    for name, spec in sorted(armed.items()):
        # a budget on a metric the comparison does not measure every run is
        # a ceiling that only ever reports "was not measured this run".
        assert name in gated, (
            "%s carries an absolute budget but is not in expected_gated.txt, "
            "so no run is required to measure it" % name
        )
        # 'observed' specifically, never 'local': an armed ceiling sized off a
        # developer machine is what produced the config.parse_yaml_3k false
        # red (max 2.0 from a 1.176 local figure, 2.164 on the runner). This
        # required-field loop is the check that would have caught it.
        for field in ("max", "unit", "observed", "set"):
            assert field in spec, "%s is missing %r" % (name, field)
        # 'set' is the only staleness evidence in the entry, so it has to be
        # a date rather than any truthy string.
        try:
            datetime.date.fromisoformat(str(spec["set"]))
        except (TypeError, ValueError):
            raise AssertionError(
                "%s has set=%r, which is not an ISO date; 'set' is the only "
                "record of how old the observation is" % (name, spec["set"])
            ) from None
        # compare.py prints the unit straight into the breach message, so a
        # wrong one misreports an MB ceiling as seconds to whoever triages it.
        want_unit = "MB" if name.startswith("mem.") else "s"
        assert spec["unit"] == want_unit, (
            "%s has unit=%r, expected %r" % (name, spec["unit"], want_unit)
        )
        headroom = spec["max"] / spec["observed"]
        assert 1.2 <= headroom <= 2.5, (
            "%s sits at %.2fx its observed value (%s vs %s); the file sizes "
            "ceilings at roughly +60%%. Under 1.2x false-reds on runner "
            "variance, and a ceiling at or below its own observation reds "
            "immediately. Over 2.5x the drift it permits is larger than any "
            "regression worth catching. Re-measure, update 'observed' and "
            "'set', and re-size 'max'."
            % (name, headroom, spec["max"], spec["observed"])
        )

    # an entry parked in 'proposed' is NOT read by compare.py, so a promotion
    # that forgets to move it leaves the ceiling inert while reading as live.
    overlap = set(doc.get("proposed", {})) & set(armed)
    assert not overlap, (
        "%s appear in both 'proposed' and 'budgets'; only 'budgets' is read"
        % sorted(overlap)
    )


def test_compare_actually_loads_every_checked_in_budget():
    # the assertions above read the file directly; this one reads it THROUGH
    # compare.py, so a change to how the document is shaped or loaded cannot
    # pass the structural check above and still leave the ceilings inert.
    # _load_compare() is this file's one loader: importing
    # benchmarks/compare.py a second time under the bare name `compare` would
    # squat that name for the session and hand the other tests a different
    # module object with its own state.
    compare_mod = _load_compare()
    loaded = compare_mod._load_budgets(
        os.path.join(REPO_ROOT, "benchmarks", "budgets.json")
    )
    assert set(loaded) == set(_real_budgets_doc()["budgets"]), (
        "compare.py loaded %d of %d checked-in ceilings"
        % (len(loaded), len(_real_budgets_doc()["budgets"]))
    )


def test_bench_smoke_produces_results(tmp_path):
    out = tmp_path / "smoke.json"
    proc = _run([BENCH, "--smoke", "--json", str(out)])
    assert proc.returncode == 0, proc.stdout
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["schema"] == 1
    assert doc["mode"] == "smoke"
    # Provenance the comparison depends on: the optional-backend flags
    # (compare.py hard-fails a pairing whose sides disagree on them) and the
    # per-benchmark wall clock (how the CI timeout ceiling gets triaged).
    assert isinstance(doc["orjson"], bool)
    assert isinstance(doc["uvloop"], bool)
    assert isinstance(doc["isal"], bool)
    for r in doc["results"]:
        assert r.get("elapsed_seconds", -1.0) >= 0.0, r["name"]
    assert "slowest benchmarks" in proc.stdout
    results = {r["name"]: r for r in doc["results"]}
    ran = [r for r in results.values() if not r["skipped"]]
    # The suite is exhaustive; even smoke mode must exercise the bulk of it.
    # The only expected skips are the POSIX-only RSS metrics on Windows.
    assert len(ran) >= 30, sorted(
        (r["name"], r.get("reason")) for r in results.values() if r["skipped"]
    )
    for r in ran:
        assert r["value"] >= 0.0
        assert r["unit"] in ("s", "MB", "KB")
    # The gated metrics must never silently skip.  The set is DERIVED from
    # benchmarks/expected_gated.txt rather than hand-curated: the previous
    # hand list was missing webapi.sse_frame_20k, so when the branch that
    # batched SSE delivery deleted the seam it drove, every test stayed
    # green and the dead gate would first have surfaced as a release-time
    # integrity failure.  Deriving makes the two nets one list: a metric
    # the release comparison requires is a metric --smoke must run.
    # Excused, each for an environment reason the release runner does not
    # share: the webui.* browser metrics (smoke must not launch Chromium),
    # the POSIX-only RSS metrics on Windows, push.seal_500 and
    # push.fanout_8x100 where PyNaCl is absent (the tox and WSL envs),
    # push.seal_xwing_500 where cryptography is absent, and
    # json.roundtrip_orjson_3k where orjson is absent (the tox envs).
    needs_nacl = ("push.seal_500", "push.fanout_8x100")
    for name in _expected_gated_names():
        # Ahead of the excuses: an excused metric skips, and a skipped
        # metric still has its row in the document.
        assert name in results, (
            "expected-gated metric %s is not registered" % name
        )
        if name.startswith("webui."):
            continue
        if name.startswith("mem.rss_") and os.name == "nt":
            continue
        if name in needs_nacl and not _importable("nacl"):
            continue
        if name == "push.seal_xwing_500" and not _importable("cryptography"):
            continue
        if name == "json.roundtrip_orjson_3k" and not _importable("orjson"):
            continue
        assert not results[name]["skipped"], results[name]


# Gating benchmarks that expected_gated.txt leaves out, each for the reason
# given at the bottom of that file.
_UNLISTED_GATING = {
    "pair.qr_encode_14",
    "webapi.activity_capped_bytes_500",
    "webui.swim_400x49",
}


def test_gating_benchmarks_and_expected_gated_list_agree():
    # A gating benchmark missing from the list has no dead-gate check, and
    # the release comparison reports nothing for it.
    bench = _load_bench()
    names = [spec["name"] for spec in bench._BENCHMARKS]
    assert len(names) == len(set(names)), sorted(
        name for name in set(names) if names.count(name) > 1
    )
    listed = _expected_gated_names()
    assert len(listed) == len(set(listed)), sorted(
        name for name in set(listed) if listed.count(name) > 1
    )
    gating = {spec["name"] for spec in bench._BENCHMARKS if not spec["info"]}
    # compare.py never counts an info benchmark as compared, so a listed
    # one fails every release.
    assert set(listed) <= gating, sorted(set(listed) - gating)
    assert gating - set(listed) == _UNLISTED_GATING


def test_bench_only_filter(tmp_path):
    out = tmp_path / "only.json"
    proc = _run([BENCH, "--smoke", "--only", "redact", "--json", str(out)])
    assert proc.returncode == 0, proc.stdout
    doc = json.loads(out.read_text(encoding="utf-8"))
    names = [r["name"] for r in doc["results"]]
    assert names and all(n.startswith("redact.") for n in names)


def _load_bench():
    import importlib.util

    spec = importlib.util.spec_from_file_location("_bench_mod", BENCH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_child_env_names_the_package_that_the_harness_measures(
    monkeypatch, tmp_path
):
    # A child benchmark runs in a temp directory, so a relative PYTHONPATH
    # entry would name another directory for it than for the harness, and
    # the subprocess benchmarks would measure another copy of the package.
    # An empty entry is relative too: Python reads it as the current
    # directory.
    bench = _load_bench()
    monkeypatch.chdir(tmp_path)
    here = os.path.abspath(".")
    other = str(tmp_path / "elsewhere")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(["sub", other, ""]))
    monkeypatch.setattr(bench, "_SRC_FALLBACK", None)
    paths = bench._child_env()["PYTHONPATH"].split(os.pathsep)
    assert paths == [os.path.join(here, "sub"), other, here]
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(["", other]))
    paths = bench._child_env()["PYTHONPATH"].split(os.pathsep)
    assert paths == [here, other]
    # the source fallback goes first, as it does on the harness's own path
    monkeypatch.setattr(bench, "_SRC_FALLBACK", "/src")
    paths = bench._child_env()["PYTHONPATH"].split(os.pathsep)
    assert paths == ["/src", here, other]
    # Python ignores a PYTHONPATH that is set to nothing, so it adds no
    # entry here either
    monkeypatch.setenv("PYTHONPATH", "")
    assert bench._child_env()["PYTHONPATH"] == "/src"
    monkeypatch.delenv("PYTHONPATH")
    assert bench._child_env()["PYTHONPATH"] == "/src"
    monkeypatch.setattr(bench, "_SRC_FALLBACK", None)
    assert "PYTHONPATH" not in bench._child_env()


_FAILING_CHILD = (
    "import sys\n"
    "sys.stderr.write('first line\\nthe cause\\n')\n"
    "sys.exit(7)\n"
)


def test_failed_child_skips_with_a_one_line_reason_naming_the_cause():
    # The reason is printed on the benchmark's result line and stored in
    # the result document, and a release that fails on the skip is
    # diagnosed from it.
    bench = _load_bench()
    with pytest.raises(bench.Skip) as caught:
        bench._timed_child(["-c", _FAILING_CHILD, "an-argument"])
    reason = str(caught.value)
    assert "\n" not in reason, reason
    assert reason == (
        "child exited 7: -c <script> an-argument (stderr: the cause)"
    )
    # a one-line script stays in the reason
    with pytest.raises(bench.Skip) as caught:
        bench._timed_child(["-c", "import sys; sys.exit(3)"])
    assert str(caught.value) == "child exited 3: -c import sys; sys.exit(3)"
    assert bench._timed_child(["-c", "pass"]) > 0.0


@pytest.mark.skipif(
    sys.platform == "win32", reason="peak RSS is read through getrusage"
)
def test_failed_rss_child_skips_with_the_target_and_its_cause():
    # The target runs under a wrapper process, which has to pass the
    # target's stderr on.
    bench = _load_bench()
    with pytest.raises(bench.Skip) as caught:
        bench._child_peak_rss_mb(["-c", _FAILING_CHILD, "an-argument"])
    assert str(caught.value) == (
        "child exited 7: -c <script> an-argument (stderr: the cause)"
    )
    assert bench._child_peak_rss_mb(["-c", "pass"]) > 0.0


_HUNG_CHILD = (
    "import sys, time\n"
    "time.sleep(30)\n"
    "open(sys.argv[1], 'w').close()\n"
)


def test_hung_child_is_killed_and_skips(monkeypatch, tmp_path):
    # Without a bound, a child that never exits holds the perf job until
    # the job's own time limit.  The child's sleep outlasts the bound and
    # ends inside the test's time limit.  A child that the kill misses
    # then writes a file, which fails the test.
    bench = _load_bench()
    outlived = tmp_path / "outlived"
    monkeypatch.setattr(bench, "_CHILD_TIMEOUT", 0.25)
    with pytest.raises(bench.Skip, match="child timed out") as caught:
        bench._timed_child(["-c", _HUNG_CHILD, str(outlived)])
    assert "-c <script>" in str(caught.value)
    assert not outlived.exists()


def test_main_names_a_benchmark_before_it_runs(capsys):
    # A result line prints when a benchmark ends, so the log of a run that
    # hangs would otherwise end on the benchmark before the stuck one.
    bench = _load_bench()
    printed = []

    @bench.bench("zznamed.first", "zznamed", repeats=(1, 1, 1))
    def _first():
        printed.append(capsys.readouterr())
        return 0.001

    assert bench.main(["--smoke", "--no-stabilize", "--only", "zznamed"]) == 0
    assert printed[0].err.splitlines()[-1] == "running zznamed.first"
    assert "zznamed.first" not in printed[0].out


@pytest.mark.parametrize(
    "failure, reason",
    [
        ("skip", "child timed out after 360s: -c <script>"),
        ("error", "error: RuntimeError('boom')"),
    ],
)
def test_failure_on_a_later_repeat_is_recorded_and_printed(
    tmp_path, capsys, failure, reason
):
    # The repeats that measured stay the row's values, so the reason of
    # the repeat that failed is the only record of a child that crashed
    # or hung.
    bench = _load_bench()
    calls = []

    @bench.bench("zzshort.later", "zzshort", repeats=(3, 3, 3))
    def _later():
        calls.append(True)
        if len(calls) < 2:
            return 0.002
        if failure == "skip":
            raise bench.Skip("child timed out after 360s: -c <script>")
        raise RuntimeError("boom")

    @bench.bench("zzshort.whole", "zzshort", repeats=(3, 3, 3))
    def _whole():
        return 0.002

    out = tmp_path / "short.json"
    args = ["--smoke", "--no-stabilize", "--only", "zzshort"]
    assert bench.main(args + ["--json", str(out)]) == 0
    printed = capsys.readouterr().out
    doc = json.loads(out.read_text(encoding="utf-8"))
    rows = {r["name"]: r for r in doc["results"]}
    short = rows["zzshort.later"]
    assert not short["skipped"] and short["values"] == [0.002]
    assert (short["runs"], short["repeats"]) == (1, 3)
    assert short["partial_reason"] == reason
    # a result line starts with its benchmark's name
    lines = {
        line.split()[0]: line
        for line in printed.splitlines()
        if line.startswith("zzshort.")
    }
    assert lines["zzshort.later"].endswith(
        "PARTIAL (1 of 3 repeats: %s)" % reason
    )
    assert "2 benchmarks, 0 skipped, 1 partial" in printed
    # a benchmark that ran every repeat carries neither field
    whole = rows["zzshort.whole"]
    assert whole["runs"] == 3
    assert "partial_reason" not in whole and "repeats" not in whole
    assert "PARTIAL" not in lines["zzshort.whole"]


# Each memo a benchmark empties by name before a timed call, as (owner,
# attribute).  bench.py reaches every one through getattr with a default,
# so that it also runs against a release that predates the memo.
_CLEARED_MEMOS = (
    ("dag", "_summaries_memo"),
    ("dag", "_dag_summary_cache"),
    ("cron", "_jobs_response_memo"),
    ("cron", "_metrics_response_memo"),
    ("cron", "_metric_samples_memo"),
    ("cron", "_activity_response_memo"),
)


def test_memos_that_benchmarks_clear_by_name_exist_in_the_package():
    # A memo renamed in the package leaves the benchmark's clear with
    # nothing to reach, and the timed region then measures memo hits.
    from cronstable.cron import Cron

    with open(BENCH, encoding="utf-8") as f:
        source = f.read()
    cron = Cron(
        None,
        config_yaml=(
            "jobs:\n  - name: j\n    command: 'x'\n"
            "    schedule: '0 0 * * *'\n"
        ),
    )
    owners = {"cron": cron, "dag": cron._dag}
    for owner, name in _CLEARED_MEMOS:
        assert '"%s"' % name in source, (
            "bench.py does not reach %s; drop it from _CLEARED_MEMOS" % name
        )
        assert hasattr(owners[owner], name), (
            "%s is gone from the package but bench.py still clears it by "
            "that name" % name
        )
    assert isinstance(cron._dag._summaries_memo, dict)
    assert isinstance(cron._dag._dag_summary_cache, dict)
    for memo in (
        cron._jobs_response_memo,
        cron._metric_samples_memo,
        cron._activity_response_memo,
        *cron._metrics_response_memo.values(),
    ):
        assert memo.cached is None


def test_benchmark_fails_when_its_memo_clear_reaches_nothing(monkeypatch):
    # The harness as it would behave after such a rename: every memo name
    # it reaches for is one the package lacks.  Each benchmark counts the
    # work under its memo and has to raise, where a result would read as
    # a large improvement.
    import types

    from cronstable import cron as cron_mod

    with open(BENCH, encoding="utf-8") as f:
        source = f.read()
    for _owner, name in _CLEARED_MEMOS:
        source = source.replace('"%s"' % name, '"%s_absent"' % name)
    # A product that never ages: loop.stall_jobs_500 passes on one build,
    # which a product that expired during a stalled run would cause.
    monkeypatch.setattr(cron_mod, "_JOBS_RESPONSE_TTL", 3600.0)
    bench = types.ModuleType("_bench_mod_renamed")
    bench.__file__ = BENCH
    exec(compile(source, BENCH, "exec"), bench.__dict__)
    bench._MODE = "smoke"
    specs = {spec["name"]: spec for spec in bench._BENCHMARKS}
    try:
        for name, complaint in (
            ("dag.list_dags_warm_x300", "timed memo hits"),
            ("dag.list_runs_warm", "timed memo hits"),
            ("dag.full_sweep_50x200", "reused cached summaries"),
            ("webapi.jobs_payload_500", "timed memo hits"),
            ("mcp.query_metrics_500", "timed memo hits"),
            ("loop.stall_jobs_500", "gauged an idle loop"),
            ("loop.stall_metrics_2000", "gauged an idle loop"),
        ):
            with pytest.raises(RuntimeError, match=complaint):
                specs[name]["fn"]()
    finally:
        bench._evict_fixtures("renamed")


def test_benchmark_fails_when_its_memo_clear_reaches_an_unused_memo(
    monkeypatch,
):
    # These two skip when their memo's name is gone.  Here the package
    # still has the name and another slot serves the product, so the clear
    # succeeds and the timed requests are memo hits.
    import types

    from cronstable import cron as cron_mod

    with open(BENCH, encoding="utf-8") as f:
        source = f.read()
    for name in ("_activity_response_memo", "_jobs_response_memo"):
        source = source.replace('"%s"' % name, '"%s_unused"' % name)
        monkeypatch.setattr(
            cron_mod.Cron,
            name + "_unused",
            types.SimpleNamespace(cached=None),
            raising=False,
        )
    # a product that never ages, so a stalled run cannot turn a hit into
    # a build
    monkeypatch.setattr(cron_mod, "_ACTIVITY_RESPONSE_TTL", 3600.0)
    monkeypatch.setattr(cron_mod, "_JOBS_RESPONSE_TTL", 3600.0)
    bench = types.ModuleType("_bench_mod_unused")
    bench.__file__ = BENCH
    exec(compile(source, BENCH, "exec"), bench.__dict__)
    # A floor of two on every scaled count: the /pools loop's first /jobs
    # request has no earlier product to hit, and smoke mode makes only one.
    bench._MODE = "smoke"
    scaled = bench._n
    bench._n = lambda base, floor=1: scaled(base, max(floor, 2))
    specs = {spec["name"]: spec for spec in bench._BENCHMARKS}
    try:
        for name in ("webapi.activity_build_500", "webapi.pools_poll_4x130"):
            with pytest.raises(RuntimeError, match="timed memo hits"):
                specs[name]["fn"]()
    finally:
        bench._evict_fixtures("unused")


def _bench_fn(bench, name):
    return next(s["fn"] for s in bench._BENCHMARKS if s["name"] == name)


def test_stream_passthrough_hands_the_mirror_a_batch_per_read(monkeypatch):
    # A pipe delivers a read at a time and the reader hands the mirror one
    # batch per read.  A stream that is whole before the reader starts
    # yields one batch, and the benchmark then pays a per-batch cost once.
    from cronstable import job

    bench = _load_bench()
    monkeypatch.setattr(bench, "_MODE", "quick")
    batches = []
    submit = job._MIRROR.submit

    def counting(job_name, stream_name, text):
        batches.append(len(text))
        return submit(job_name, stream_name, text)

    monkeypatch.setattr(job._MIRROR, "submit", counting)
    try:
        _bench_fn(bench, "job.stream_passthrough_240k")()
    finally:
        bench._evict_fixtures("job")
    # 24k lines are about 1.2 MiB, which is 20 reads of 64 KiB; the bound
    # leaves room for an event loop that orders its callbacks differently
    assert len(batches) >= 10, batches
    assert max(batches) < 4 * 65536, batches


@pytest.mark.parametrize(
    "name, queues",
    [
        ("job.launch_reap_2k", True),
        ("job.launch_fail_retry_2k", True),
        # This one reads the memory a finished batch can pin.  Behind the
        # gate the reaper's last batch is the 4 runs past the 16 permits,
        # and a pinned batch that small stays under the metric's floor.
        ("mem.launch_reap_steady_20x55", False),
    ],
)
def test_launch_herd_timings_queue_on_the_spawn_gate(name, queues):
    # A real spawn suspends, so every launch past the gate's permits waits
    # on it.  A stubbed spawn that returns at once never fills the gate,
    # and the herd then skips the queueing that a real one pays.
    import asyncio

    bench = _load_bench()
    # full mode is the one where each herd is larger than the gate
    bench._MODE = "full"
    waited = []

    class _Gate(asyncio.Semaphore):
        async def acquire(self):
            if self.locked():
                waited.append(True)
            return await super().acquire()

    herd_cron = bench._herd_cron

    def counting_cron():
        cron = herd_cron()
        assert isinstance(cron._spawn_gate, asyncio.Semaphore)
        cron._spawn_gate = _Gate(cron._spawn_gate._value)
        return cron

    bench._herd_cron = counting_cron
    try:
        _bench_fn(bench, name)()
    finally:
        bench._evict_fixtures("job")
    assert bool(waited) == queues, (name, len(waited))


def test_tui_poll_benchmark_keeps_the_payload_decode_off_the_clock(
    monkeypatch,
):
    # The stand-in API's JSON decode is the standard library's work on a
    # body the harness serialized; inside the timed region it would be
    # most of the value and no change to the dashboard could move it.
    import time

    bench = _load_bench()
    monkeypatch.setattr(bench, "_MODE", "smoke")
    events = []

    class _Time:
        def __getattr__(self, name):
            return getattr(time, name)

        def perf_counter(self):
            events.append("clock")
            return time.perf_counter()

    class _Json:
        def __getattr__(self, name):
            return getattr(json, name)

        def loads(self, text):
            events.append("decode")
            return json.loads(text)

    monkeypatch.setattr(bench, "time", _Time())
    monkeypatch.setattr(bench, "json", _Json())
    try:
        _bench_fn(bench, "tui.poll_absorb_5k")()
    finally:
        bench._evict_fixtures("tui")
    assert "decode" in events and "clock" in events
    running = False
    for event in events:
        if event == "clock":
            running = not running
        else:
            assert not running, events
    assert not running, events


def test_seeded_store_fixtures_rebuild_into_a_fresh_directory():
    # The harness drops every fixture at a group boundary and builds one
    # again when a later benchmark asks.  These builders append to their
    # store, so a second build into the same directory doubles it, and
    # each benchmark below checks the size of the store it reads.
    bench = _load_bench()
    bench._MODE = "smoke"
    names = (
        "state.list_records_warm",
        "state.artifact_get_oldest_2k",
        "state.artifact_put_distinct_2k",
        "jobapi.sem_denied_64x10",
        "mem.gc_manifest_scan_128x2k",
    )
    for _build in range(2):
        try:
            for name in names:
                _bench_fn(bench, name)()
        finally:
            bench._evict_fixtures("state")


def test_yaml_parse_is_gated_above_the_quadratic_threshold(monkeypatch):
    # Config parsing was quadratic in the job count for the whole life of the
    # project: strictyaml's vendored CommentedSeq.__deepcopy__ calls
    # copy_attributes INSIDE its element loop, and jobs: is the one big
    # sequence a config has, so it paid the entire bill (8k jobs took 49.6s;
    # the hoisted call takes 3.7s). The suite never saw it, because the only
    # YAML parse metric measured 300 jobs, where the linear term still
    # dominates: the same slip showed there as 0.161s against 0.117s, next to
    # 6.27s against 1.18s at 3k on the same machine.
    #
    # What has to survive is the SIZE, not a benchmark id. Some gated parse
    # benchmark must hand the parser a config large enough that a quadratic
    # term stands out from a runner's own jitter. Shrinking the big one to
    # save CI minutes, or dropping it from expected_gated.txt so its baseline
    # side may skip, retires the tripwire without failing anything else, so
    # this test fails instead.
    #
    # The parse itself is stubbed out: this is a shape check on the workload
    # the harness builds, not a timing test (see tests/test_perf_invariants.py
    # for the same split), and running the real 3k-job parse here would cost
    # the suite seconds for no added signal.
    bench = _load_bench()
    import cronstable.config as config

    seen = []

    def _record(text, path, *args, **kwargs):
        seen.append(text)
        return None

    monkeypatch.setattr(config, "parse_config_string", _record)
    # The sizes asserted below are the ones CI measures, and _n() scales the
    # workload per mode, so a bench module left in quick/smoke mode would
    # check the wrong numbers.
    assert bench._MODE == "full"

    sizes = {}
    for spec in bench._BENCHMARKS:
        if not spec["name"].startswith("config.parse_yaml"):
            continue
        seen.clear()
        spec["fn"]()
        assert seen, "%s never called parse_config_string" % spec["name"]
        sizes[spec["name"]] = seen[-1].count("\n  - name:")
    assert sizes, "no config.parse_yaml* benchmark is registered"

    gated = set(_expected_gated_names())
    big = {
        name: jobs
        for name, jobs in sizes.items()
        if jobs >= 3000 and name in gated
    }
    assert big, (
        "no gated config.parse_yaml* benchmark parses 3000+ jobs, so a "
        "return of the quadratic Seq validation would not be caught: %r"
        % (sizes,)
    )


def test_evict_fixtures_runs_finalizers_and_audits_loop_hygiene():
    # Regression net for the 1.2.31 webui/Playwright incident: a fixture that
    # parks a RUNNING event loop on the harness thread (Playwright's sync API
    # does this between calls) made every later asyncio.run() benchmark skip
    # on BOTH sides of the CI pairing, which the release run then failed as
    # six dead gates. Group eviction is the boundary where the thread must be
    # clean again: fixture finalizers run there, and a parked loop left
    # behind is a hard error naming the offending group, never a silent
    # downstream skip. The park is simulated with asyncio's own thread-local
    # setter because that is precisely the state the sync Playwright session
    # leaves behind between calls.
    import asyncio

    bench = _load_bench()
    loop = asyncio.new_event_loop()
    try:
        # A loop-parking fixture WITH a finalizer (the fixed _web_page
        # shape): eviction runs the finalizer, the audit passes, the cache
        # clears.
        released = []

        def park():
            asyncio.events._set_running_loop(loop)
            return "session"

        def release(value):
            assert value == "session"
            asyncio.events._set_running_loop(None)
            released.append(True)

        assert bench.fixture("parked", park, finalizer=release) == "session"
        bench._evict_fixtures("webui")
        assert released == [True]
        assert bench._FIX == {} and bench._FIX_FINAL == {}

        # The same fixture WITHOUT a finalizer: the audit fails loudly and
        # names the group, while the innocent later groups never run.
        bench.fixture("parked", park)
        with pytest.raises(SystemExit, match="webui"):
            bench._evict_fixtures("webui")
    finally:
        asyncio.events._set_running_loop(None)
        loop.close()

    # The exact downstream call the incident broke must work again once the
    # audit passes.
    asyncio.run(asyncio.sleep(0))


def test_main_audits_every_group_boundary_including_the_last():
    # The companion to the unit test above: that one pins _evict_fixtures'
    # behavior, this one pins that main() actually CALLS it -- at the boundary
    # between groups and again after the final group. Without this, deleting
    # either call site reinstates the 1.2.31 incident with a fully green test
    # suite, since no other test runs a loop-parking fixture through main().
    import asyncio

    bench = _load_bench()
    loop = asyncio.new_event_loop()

    def park():
        asyncio.events._set_running_loop(loop)
        return "session"

    def release(value):
        asyncio.events._set_running_loop(None)

    try:
        # Two synthetic groups; the first parks a loop with NO finalizer, so
        # the boundary audit must stop the suite and name that group.
        @bench.bench("zzpoison.leaky", "zzpoison", repeats=(1, 1, 1))
        def _leaky():
            bench.fixture("leaky_session", park)
            return 0.001

        @bench.bench("zzafter.downstream", "zzafter", repeats=(1, 1, 1))
        def _downstream():
            asyncio.run(asyncio.sleep(0))  # what the incident silently broke
            return 0.001

        with pytest.raises(SystemExit, match="zzpoison"):
            bench.main(
                ["--smoke", "--no-stabilize", "--only", "zzpoison", "--only",
                 "zzafter"]
            )
        asyncio.events._set_running_loop(None)

        # The final-group boundary is audited too: run ONLY the leaky group,
        # so the sole eviction is the post-loop one.
        with pytest.raises(SystemExit, match="zzpoison"):
            bench.main(["--smoke", "--no-stabilize", "--only", "zzpoison"])
        asyncio.events._set_running_loop(None)

        # Positive control: the same fixture WITH a finalizer runs clean and
        # the downstream asyncio.run() benchmark measures instead of skipping.
        bench._BENCHMARKS[:] = [
            s for s in bench._BENCHMARKS
            if not s["name"].startswith(("zzpoison.", "zzafter."))
        ]

        @bench.bench("zzpoison.tidy", "zzpoison", repeats=(1, 1, 1))
        def _tidy():
            bench.fixture("tidy_session", park, finalizer=release)
            return 0.001

        @bench.bench("zzafter.downstream", "zzafter", repeats=(1, 1, 1))
        def _downstream_ok():
            asyncio.run(asyncio.sleep(0))
            return 0.001

        assert (
            bench.main(
                ["--smoke", "--no-stabilize", "--only", "zzpoison", "--only",
                 "zzafter"]
            )
            == 0
        )
    finally:
        asyncio.events._set_running_loop(None)
        loop.close()


def _entry(name, value, gate_pct=25.0, gate_floor=0.010, unit="s"):
    return {
        "name": name,
        "group": name.split(".")[0],
        "detail": "",
        "unit": unit,
        "gate_pct": gate_pct,
        "gate_floor": gate_floor,
        "compare": "min",
        "info": False,
        "skipped": False,
        "reason": None,
        "runs": 1,
        "values": [value],
        "value": value,
        "mean": value,
        "median": value,
        "stdev": 0.0,
        "min": value,
        "max": value,
    }


def _skipped(name, reason="playwright unavailable", gate_pct=25.0):
    """A metric the harness recorded on this side but could not run.

    bench.py still emits a row for a skipped metric (so the report can say the
    coverage was lost), which is why it carries a gate config it never used.
    Pass gate_pct=None for an ``info`` metric, which never gates by design.
    """
    return {
        "name": name,
        "group": name.split(".")[0],
        "detail": "",
        "unit": "s",
        "gate_pct": gate_pct,
        "gate_floor": 0.010,
        "compare": "min",
        "info": gate_pct is None,
        "skipped": True,
        "reason": reason,
    }


def _doc(entries, version="1.0.0"):
    return {
        "schema": 1,
        "mode": "smoke",
        "cronstable_version": version,
        "results": entries,
    }


def _write(path, doc):
    path.write_text(json.dumps(doc), encoding="utf-8")
    return str(path)


def test_compare_gates_regression_and_honors_floor(tmp_path):
    base = _write(
        tmp_path / "base.json",
        _doc(
            [
                _entry("startup.version", 0.100),
                # Big relative but sub-floor absolute change: must not gate.
                _entry("micro.jitter", 0.0001),
            ]
        ),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [
                _entry("startup.version", 0.200),  # +100%: gates
                _entry("micro.jitter", 0.0002),  # +100% but +0.1ms: floor
            ],
            version="1.1.0",
        ),
    )
    md = tmp_path / "out.md"
    svg = tmp_path / "out.svg"
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base,
            "--current",
            cur,
            "--md",
            str(md),
            "--svg",
            str(svg),
        ]
    )
    assert proc.returncode == 1, proc.stdout
    assert "startup.version" in proc.stdout
    assert "micro.jitter" not in proc.stdout.split("gate:")[-1]
    text = md.read_text(encoding="utf-8")
    assert "Regression check: failed" in text
    assert svg.read_text(encoding="utf-8").startswith("<svg")

    # --warn-only and --accept both downgrade the failure to exit 0.
    for flag in ("--warn-only", "--accept"):
        proc = _run([COMPARE, "--baseline", base, "--current", cur, flag])
        assert proc.returncode == 0, (flag, proc.stdout)
        assert "::warning::" in proc.stdout


def test_compare_identical_passes_and_merges_rounds(tmp_path):
    r1 = _write(tmp_path / "r1.json", _doc([_entry("startup.version", 0.120)]))
    r2 = _write(tmp_path / "r2.json", _doc([_entry("startup.version", 0.100)]))
    md = tmp_path / "out.md"
    merged = tmp_path / "merged.json"
    proc = _run(
        [
            COMPARE,
            "--baseline",
            r1,
            "--baseline-label",
            "prev",
            "--current",
            r1,
            r2,
            "--md",
            str(md),
            "--merged-out",
            str(merged),
        ]
    )
    assert proc.returncode == 0, proc.stdout
    assert "Regression check: passed" in md.read_text(encoding="utf-8")
    merged_doc = json.loads(merged.read_text(encoding="utf-8"))
    # Two rounds with compare="min" merge to the faster round.
    assert merged_doc["results"][0]["value"] == 0.100


def _load_compare():
    import importlib.util

    spec = importlib.util.spec_from_file_location("_bench_compare", COMPARE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_compare_startup_subtracts_python_baseline(tmp_path):
    # Interpreter-startup drift must not gate a startup metric: only
    # cronstable's own contribution is compared. Here the raw
    # cronstable --version time grows +27% (0.150 -> 0.190) but ENTIRELY
    # because python_baseline drifted 0.100 -> 0.140; cronstable's own share
    # is a flat 0.050s, so the gate must not fire.
    base = _write(
        tmp_path / "base.json",
        _doc(
            [
                _entry("startup.python_baseline", 0.100, gate_pct=None),
                _entry("startup.version", 0.150),
            ]
        ),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [
                _entry("startup.python_baseline", 0.140, gate_pct=None),
                _entry("startup.version", 0.190),
            ],
            version="1.1.0",
        ),
    )
    proc = _run([COMPARE, "--baseline", base, "--current", cur])
    assert proc.returncode == 0, proc.stdout
    assert "0 gate violation" in proc.stdout

    # But a real regression in cronstable's own share DOES gate: same
    # interpreter floor, version time doubles its cronstable contribution.
    cur2 = _write(
        tmp_path / "cur2.json",
        _doc(
            [
                _entry("startup.python_baseline", 0.100, gate_pct=None),
                _entry("startup.version", 0.200),  # cronstable 0.05 -> 0.10
            ],
            version="1.1.0",
        ),
    )
    proc = _run([COMPARE, "--baseline", base, "--current", cur2])
    assert proc.returncode == 1, proc.stdout
    assert "startup.version" in proc.stdout


def test_compare_gates_own_share_regression_under_the_raw_floor(tmp_path):
    # Regression: the delta was computed from the FLOOR-SUBTRACTED own share
    # but the absolute check compared that subtracted change against the
    # RAW-scale gate_floor constant (10ms), silently re-imposing the very
    # dilution the subtraction exists to remove. startup.import_cronexpr owns
    # only ~9.5ms of its ~41ms total, so DOUBLING cronstable's own import cost
    # moved 9.5ms, failed "9.5ms > 10ms", and passed the gate: the metric was
    # effectively ungated. The floor is now rescaled by the same ratio the
    # values were reduced by, so it guards the number it is actually applied
    # to. (The pre-existing own-share test above uses a 50ms own share, well
    # clear of the raw floor, which is why it never caught this.)
    base = _write(
        tmp_path / "base.json",
        _doc(
            [
                _entry("startup.python_baseline", 0.0314, gate_pct=None),
                _entry("startup.import_cronexpr", 0.0409),  # own share 9.5ms
            ]
        ),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [
                _entry("startup.python_baseline", 0.0314, gate_pct=None),
                # own share 9.5ms -> 19.0ms: +100%, but only a 9.5ms absolute
                # move, i.e. just under the unscaled 10ms floor.
                _entry("startup.import_cronexpr", 0.0504),
            ],
            version="1.1.0",
        ),
    )
    md = tmp_path / "out.md"
    proc = _run(
        [COMPARE, "--baseline", base, "--current", cur, "--md", str(md)]
    )
    assert proc.returncode == 1, proc.stdout
    assert "1 gate violation" in proc.stdout
    assert "startup.import_cronexpr" in proc.stdout
    assert "Regression check: failed" in md.read_text(encoding="utf-8")


def test_startup_gate_cuts_over_at_the_declared_gate_pct(tmp_path):
    # _adjusted_values' docstring says the startup sensitivity is gate_pct of
    # cronstable's OWN share. Before the floor was brought onto the adjusted
    # scale that was false at every percentage (the raw 10ms floor swamped a
    # 9.5ms own share), and a proportional rescale would have left it drifting
    # to roughly +47%. Pin the boundary so the documented number stays the
    # delivered one: 24% passes, 26% gates, on a real-shaped own share.
    def run(own_ms):
        base = _write(
            tmp_path / ("b%s.json" % own_ms),
            _doc(
                [
                    _entry("startup.python_baseline", 0.0314, gate_pct=None),
                    _entry("startup.import_cronexpr", 0.0409),  # own 9.5ms
                ]
            ),
        )
        cur = _write(
            tmp_path / ("c%s.json" % own_ms),
            _doc(
                [
                    _entry("startup.python_baseline", 0.0314, gate_pct=None),
                    _entry(
                        "startup.import_cronexpr", 0.0314 + own_ms / 1000.0
                    ),
                ],
                version="1.1.0",
            ),
        )
        return _run([COMPARE, "--baseline", base, "--current", cur])

    just_under = run(9.5 * 1.24)
    assert just_under.returncode == 0, just_under.stdout
    just_over = run(9.5 * 1.26)
    assert just_over.returncode == 1, just_over.stdout
    assert "startup.import_cronexpr" in just_over.stdout


def test_capped_floor_still_suppresses_a_tiny_change(tmp_path):
    # The counterpart to the tests above: capping the floor must not turn the
    # startup gate into a hair trigger. A metric owning 0.4ms of its ~32ms
    # total can clear its 25% limit on a 0.12ms move, which is still far too
    # small to fail a release over, so the 2ms adjusted floor holds it. This
    # is the band where the own share's relative noise is worst, so the floor
    # is doing real work here rather than just papering over gate_pct.
    base = _write(
        tmp_path / "base.json",
        _doc(
            [
                _entry("startup.python_baseline", 0.0314, gate_pct=None),
                _entry("startup.import_config", 0.0318),  # own share 0.4ms
            ]
        ),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [
                _entry("startup.python_baseline", 0.0314, gate_pct=None),
                _entry("startup.import_config", 0.03192),  # 0.4ms -> 0.52ms
            ],
            version="1.1.0",
        ),
    )
    proc = _run([COMPARE, "--baseline", base, "--current", cur])
    assert proc.returncode == 0, proc.stdout
    assert "0 gate violation" in proc.stdout


def test_adjusted_floor_caps_once_the_floor_is_subtracted():
    compare = _load_compare()
    cur = {"value": 0.0504, "gate_floor": 0.010}
    # adjusted to the own share: the raw 10ms floor, sized for a ~40ms total,
    # is capped so it cannot swamp gate_pct on a single-digit-ms own share.
    assert compare._adjusted_floor(cur, 0.0190) == compare.ADJUSTED_GATE_FLOOR
    # a flat cap, not a proportional rescale: the same own share reached from
    # a much larger raw total gets the same floor, so the effective threshold
    # does not drift with the un-regressable interpreter overhead.
    assert compare._adjusted_floor(
        {"value": 0.5, "gate_floor": 0.010}, 0.0190
    ) == compare.ADJUSTED_GATE_FLOOR
    # never RAISES a floor that was already tighter than the cap.
    assert compare._adjusted_floor(
        {"value": 0.0504, "gate_floor": 0.0005}, 0.0190
    ) == 0.0005
    # no adjustment happened (every non-startup metric): floor untouched.
    assert compare._adjusted_floor(cur, 0.0504) == 0.010
    # no floor declared: nothing to cap.
    assert compare._adjusted_floor({"value": 0.0504}, 0.0190) == 0.0


def test_compare_reports_metrics_skipped_on_both_sides(tmp_path):
    # Regression: a metric skipped on BOTH sides was dropped at the top of
    # _compare and excluded from build_md's dropped list (that list only
    # covers metrics measured on the baseline), so it vanished from the report
    # while the gate still printed an unqualified "Gate: passed." That is how
    # the webui.* metrics could be absent from an entire release's gate
    # without anything saying so.
    entries = [_entry("startup.version", 0.100), _skipped("webui.wallboard")]
    base = _write(tmp_path / "base.json", _doc(entries))
    cur = _write(tmp_path / "cur.json", _doc(entries, version="1.1.0"))
    md = tmp_path / "out.md"
    proc = _run(
        [COMPARE, "--baseline", base, "--current", cur, "--md", str(md)]
    )
    assert proc.returncode == 0, proc.stdout
    # visible in the job log, not only in the rendered report
    assert "::warning::" in proc.stdout
    assert "not compared" in proc.stdout
    assert "webui.wallboard" in proc.stdout
    text = md.read_text(encoding="utf-8")
    assert (
        "Not measured in either version; regression checks "
        "could not run: webui.wallboard." in text
    )
    # The pass line must carry the compared/total count; the bare form claims
    # coverage the run did not have.
    assert "**Regression check: passed.**" not in text
    assert "**Regression check: passed** for 1 of 2 metrics" in text


def test_compare_reports_a_metric_the_baseline_had_and_this_run_lost(tmp_path):
    # The other half of the same defect, and the likelier shape now that both
    # sides install Playwright: the baseline measured a gated metric and this
    # run skipped it. It produces no row and no violation, exactly like the
    # both-sides case, so an unqualified pass here is just as misleading. The
    # first version of this fix only counted both-sides skips and left this
    # path silent.
    base = _write(
        tmp_path / "base.json",
        _doc(
            [
                _entry("startup.version", 0.100),
                _entry("webui.wallboard", 0.500),
            ]
        ),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [_entry("startup.version", 0.100), _skipped("webui.wallboard")],
            version="1.1.0",
        ),
    )
    md = tmp_path / "out.md"
    proc = _run(
        [COMPARE, "--baseline", base, "--current", cur, "--md", str(md)]
    )
    assert proc.returncode == 0, proc.stdout
    assert "::warning::" in proc.stdout
    assert "webui.wallboard" in proc.stdout
    text = md.read_text(encoding="utf-8")
    assert "**Regression check: passed.**" not in text
    assert "**Regression check: passed** for 1 of 2 metrics" in text


def test_gate_coverage_ignores_info_only_metrics(tmp_path):
    # An info metric (gate_pct None) skipping is not lost gate coverage, so it
    # must not appear in the fraction. Counting every skip made the report
    # name metrics as gated that never were: startup.python_baseline is
    # declared info=True and skips whenever the subprocess tier is filtered
    # out, which would have understated coverage on an ordinary run.
    entries = [
        _entry("startup.version", 0.100),
        _skipped("startup.python_baseline", gate_pct=None),
    ]
    base = _write(tmp_path / "base.json", _doc(entries))
    cur = _write(tmp_path / "cur.json", _doc(entries, version="1.1.0"))
    md = tmp_path / "out.md"
    proc = _run(
        [COMPARE, "--baseline", base, "--current", cur, "--md", str(md)]
    )
    assert proc.returncode == 0, proc.stdout
    assert "::warning::" not in proc.stdout
    text = md.read_text(encoding="utf-8")
    # nothing gateable was lost, so the unqualified pass line is honest here
    assert "**Regression check: passed.**" in text


def test_compare_warns_when_a_compared_metric_skipped_or_ran_short(tmp_path):
    # The merge keeps the rounds that measured, so the metric counts as
    # compared and the exit code is the gate's.  The warning is the only
    # sign in the job log that a child crashed or hung.
    name = "startup.daemon_first_pass_2k"
    hung = "child timed out after 360s: -m cronstable"
    short = _entry(name, 1.0)
    short.update(runs=1, repeats=2, partial_reason=hung)
    whole = [
        _write(tmp_path / ("whole%d.json" % i), _doc([_entry(name, 1.0)]))
        for i in (1, 2)
    ]
    broken = [
        _write(tmp_path / "broken1.json", _doc([short])),
        _write(tmp_path / "broken2.json", _doc([_skipped(name, hung)])),
    ]
    expected = tmp_path / "expected.txt"
    expected.write_text(name + "\n", encoding="utf-8")
    merged = tmp_path / "merged.json"
    args = [COMPARE, "--expected-gated", str(expected)]
    args += ["--merged-out", str(merged)]
    rounds = [
        "round 1 ran 1 of 2 repeats (%s)" % hung,
        "round 2 skipped (%s)" % hung,
    ]
    for side, base, cur in (
        ("current", whole, broken),
        ("baseline", broken, whole),
    ):
        proc = _run(args + ["--baseline"] + base + ["--current"] + cur)
        assert proc.returncode == 0, proc.stdout
        assert "compared 1 metrics" in proc.stdout
        assert "0 gate violation(s)" in proc.stdout
        warnings = [
            line
            for line in proc.stdout.splitlines()
            if line.startswith("::warning::")
        ]
        assert warnings == [
            "::warning::perf gate: %s was compared with a short %s side: %s"
            % (name, side, "; ".join(rounds))
        ]
        row = json.loads(merged.read_text(encoding="utf-8"))["results"][0]
        assert row.get("short_rounds") == (
            rounds if side == "current" else None
        )
    # every round ran every repeat, so there is nothing to report
    proc = _run(args + ["--baseline"] + whole + ["--current"] + whole)
    assert proc.returncode == 0, proc.stdout
    assert "::warning::" not in proc.stdout


def test_rel_cov_is_robust_to_one_outlier_round():
    compare = _load_compare()
    steady = {"round_values": [1.00, 1.05, 0.95, 1.02, 0.98]}
    spiked = {"round_values": [1.00, 1.05, 0.95, 1.02, 5.00]}
    cov_steady = compare._rel_cov(steady)
    cov_spiked = compare._rel_cov(spiked)
    # A single throttled round barely moves the robust (MAD-based) band, where
    # a plain stdev/mean would multiply it several-fold and mask regressions.
    assert cov_steady is not None and cov_spiked is not None
    assert cov_spiked < 3 * cov_steady
    naive = statistics.pstdev(spiked["round_values"]) / statistics.fmean(
        spiked["round_values"]
    )
    assert cov_spiked < naive / 5  # far below the naive estimate


def test_adjusted_values_only_touches_startup_with_a_floor():
    compare = _load_compare()
    base = {"value": 0.150}
    cur = {"value": 0.190}
    # startup metric with both floors -> cronstable's own share
    assert compare._adjusted_values(
        "startup.version", base, cur, 0.100, 0.140
    ) == pytest.approx((0.050, 0.050))
    # non-startup metric -> untouched
    assert compare._adjusted_values(
        "cronexpr.parse_simple", base, cur, 0.100, 0.140
    ) == (0.150, 0.190)
    # missing floor -> untouched (older release without the baseline metric)
    assert compare._adjusted_values(
        "startup.version", base, cur, None, None
    ) == (0.150, 0.190)


def test_svg_large_change_labels_stay_within_the_plot():
    # Regression: a large (clamped) bar used to place its percentage label just
    # past the bar end, which for a big FASTER change landed left of the plot,
    # on top of the metric name (e.g. a -94% label unreadable). Large changes
    # must now render their label INSIDE the bar and within the plot bounds.
    import re

    compare = _load_compare()

    def _e(name, value):
        return {
            "name": name,
            "unit": "s",
            "compare": "min",
            "gate_pct": 15.0,
            "gate_floor": 0.01,
            "value": value,
            "round_values": [value],
        }

    base = {"big.win": _e("big.win", 1.0), "big.reg": _e("big.reg", 0.10)}
    cur = {"big.win": _e("big.win", 0.05), "big.reg": _e("big.reg", 0.50)}
    rows, _, _ = compare._compare(base, cur)
    svg = compare.build_svg(rows, "old", "new")

    width, gutter = 860, 230
    pat = re.compile(
        r'<text class="([^"]*num[^"]*)" x="([\d.]+)"[^>]*'
        r'text-anchor="(\w+)">([+\-][\d.]+%[^<]*)</text>'
    )
    inside = []
    for cls, x, anchor, txt in pat.findall(svg):
        if anchor == "middle" or "t3" in cls:
            continue  # axis tick labels, not data labels
        x = float(x)
        w = len(txt) * 6.0 + 3.0
        left, right = (x - w, x) if anchor == "end" else (x, x + w)
        assert left >= gutter - 3, ("spills into name gutter", txt, left)
        assert right <= width - 2, ("spills past right edge", txt, right)
        if "inlabel" in cls:
            inside.append(txt)
    # both the big win and the big gated regression were drawn inside the bar
    assert any(t.startswith("-") for t in inside), inside
    assert any("regression" in t for t in inside), inside


def test_svg_expanded_chart_includes_every_compared_metric():
    # The release chart used to cut to the 16 largest changes and wave at
    # the rest in a footnote. The expanded chart draws one row per compared
    # metric, repeats the % scale at both ends of the (now tall) plot, and
    # the only footnote left is for metrics with no baseline side, so
    # nothing measured is silently absent from the image.
    import re

    compare = _load_compare()

    def _e(name, value):
        return {
            "name": name,
            "unit": "s",
            "compare": "min",
            "gate_pct": 25.0,
            "gate_floor": 0.01,
            "value": value,
            "round_values": [value],
        }

    names = ["suite.metric_%02d" % i for i in range(40)]
    base = {n: _e(n, 1.0) for n in names}
    cur = {n: _e(n, 1.0 + (i - 20) / 100.0) for i, n in enumerate(names)}
    cur["suite.no_baseline"] = _e("suite.no_baseline", 1.0)
    rows, _, _ = compare._compare(base, cur)
    svg = compare.build_svg(rows, "old", "new")

    for name in names:
        assert ">%s</text>" % name in svg, name
    assert "largest changes shown" not in svg
    # a metric with no baseline has no change to draw, so it gets no row;
    # the footnote owns up to it instead of dropping it silently
    assert ">suite.no_baseline</text>" not in svg
    assert "Metrics without a percentage comparison: 1." in svg
    # the chart grew to hold all 40 rows rather than clipping them
    height = int(re.search(r'height="(\d+)"', svg).group(1))
    assert height >= 78 + 40 * 24
    # the % scale reads at both ends of the tall plot (deltas span -20%..
    # +19%, so the limit is 20 and +10% is a tick), and the alternate-row
    # wash that carries a name across to its bar is present
    assert svg.count(">+10%</text>") == 2
    assert 'class="stripe"' in svg


def test_compare_without_baseline_records_first_release(tmp_path):
    cur = _write(tmp_path / "cur.json", _doc([_entry("startup.version", 0.1)]))
    md = tmp_path / "out.md"
    proc = _run([COMPARE, "--current", cur, "--md", str(md)])
    assert proc.returncode == 0, proc.stdout
    text = md.read_text(encoding="utf-8")
    assert "No baseline results are available" in text
    assert "startup.version" in text


def _budgets(path, budgets):
    path.write_text(
        json.dumps({"schema": 1, "budgets": budgets}), encoding="utf-8"
    )
    return str(path)


def test_budget_breach_fails_and_accept_does_not_excuse_it(tmp_path):
    # The relative gate is HEAD vs latest tag only, so a slow multi-release
    # drift ships with a green gate every hop; the absolute budget is the only
    # thing that stops it. Identical sides here: the relative gate passes,
    # the ceiling still fails the run.
    entries = [_entry("schedule.cold_build_100k", 4.5)]
    base = _write(tmp_path / "base.json", _doc(entries))
    cur = _write(tmp_path / "cur.json", _doc(entries, version="1.1.0"))
    budgets = _budgets(
        tmp_path / "budgets.json",
        {"schedule.cold_build_100k": {"max": 4.0, "unit": "s"}},
    )
    md = tmp_path / "out.md"
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base,
            "--current",
            cur,
            "--budgets",
            budgets,
            "--md",
            str(md),
        ]
    )
    assert proc.returncode == 1, proc.stdout
    assert "::error::perf budget:" in proc.stdout
    assert "absolute limit" in proc.stdout
    assert "Absolute performance limits: exceeded" in md.read_text(
        encoding="utf-8"
    )

    # [perf:accept] acknowledges a relative regression; the ceiling has its
    # own ritual (a reviewed edit to budgets.json) and stays failed.
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base,
            "--current",
            cur,
            "--budgets",
            budgets,
            "--accept",
        ]
    )
    assert proc.returncode == 1, proc.stdout

    # --warn-only (ordinary commits) downgrades it like everything else.
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base,
            "--current",
            cur,
            "--budgets",
            budgets,
            "--warn-only",
        ]
    )
    assert proc.returncode == 0, proc.stdout
    assert "::warning::perf budget:" in proc.stdout

    # Under the ceiling: silent pass.
    ok = _budgets(
        tmp_path / "ok.json",
        {"schedule.cold_build_100k": {"max": 5.0, "unit": "s"}},
    )
    proc = _run(
        [COMPARE, "--baseline", base, "--current", cur, "--budgets", ok]
    )
    assert proc.returncode == 0, proc.stdout
    assert "perf budget" not in proc.stdout


@pytest.mark.parametrize("broken", ["nan", "inf", "-inf"])
def test_non_finite_value_breaches_its_budget(tmp_path, broken):
    # NaN compares false against every ceiling, and -inf sits under every
    # one, so neither can exceed a limit by comparison alone.
    base = _write(
        tmp_path / "base.json",
        _doc([_entry("schedule.cold_build_100k", 0.5)]),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [_entry("schedule.cold_build_100k", float(broken))],
            version="1.1.0",
        ),
    )
    budgets = _budgets(
        tmp_path / "budgets.json",
        {"schedule.cold_build_100k": {"max": 4.0, "unit": "s"}},
    )
    args = [COMPARE, "--baseline", base, "--current", cur]
    args += ["--budgets", budgets]
    proc = _run(args)
    assert proc.returncode == 1, proc.stdout
    assert "::error::perf budget: schedule.cold_build_100k" in proc.stdout
    assert _run(args + ["--accept"]).returncode == 1
    proc = _run(args + ["--warn-only"])
    assert proc.returncode == 0, proc.stdout
    assert "::warning::perf budget:" in proc.stdout


def test_budgeted_metric_not_measured_warns_but_does_not_fail(tmp_path):
    # A skipped/absent budgeted metric is lost COVERAGE, which is
    # expected_gated.txt's job to fail on; the budget check only warns so the
    # same loss is not reported as two different failures.
    entries = [_entry("startup.version", 0.1)]
    base = _write(tmp_path / "base.json", _doc(entries))
    cur = _write(tmp_path / "cur.json", _doc(entries, version="1.1.0"))
    budgets = _budgets(
        tmp_path / "budgets.json", {"tui.log_restyle_5k": {"max": 0.14}}
    )
    proc = _run(
        [COMPARE, "--baseline", base, "--current", cur, "--budgets", budgets]
    )
    assert proc.returncode == 0, proc.stdout
    assert "::warning::perf budget:" in proc.stdout
    assert "not measured" in proc.stdout


def test_expected_gated_dead_gate_fails(tmp_path):
    # A metric whose BASELINE side skips lands in coverage['new'], which is
    # never warned about (it looks like a metric added this release), so a
    # gate that died because a private seam drifted was silently ungated
    # forever. The checked-in expected list makes that an integrity failure.
    base = _write(
        tmp_path / "base.json",
        _doc([_entry("startup.version", 0.1), _skipped("tui.drawer")]),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [_entry("startup.version", 0.1), _entry("tui.drawer", 0.09)],
            version="1.1.0",
        ),
    )
    expected = tmp_path / "expected.txt"
    expected.write_text(
        "# comment\nstartup.version\ntui.drawer\n", encoding="utf-8"
    )
    md = tmp_path / "out.md"
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base,
            "--current",
            cur,
            "--expected-gated",
            str(expected),
            "--md",
            str(md),
        ]
    )
    assert proc.returncode == 1, proc.stdout
    assert "::error::perf gate integrity:" in proc.stdout
    assert "tui.drawer" in proc.stdout
    assert "startup.version" not in proc.stdout.split("integrity")[-1]
    assert "Required comparisons: missing" in md.read_text(encoding="utf-8")

    # not [perf:accept]-able; --warn-only still downgrades.
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base,
            "--current",
            cur,
            "--expected-gated",
            str(expected),
            "--accept",
        ]
    )
    assert proc.returncode == 1, proc.stdout
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base,
            "--current",
            cur,
            "--expected-gated",
            str(expected),
            "--warn-only",
        ]
    )
    assert proc.returncode == 0, proc.stdout
    assert "::warning::perf gate integrity:" in proc.stdout

    # Fully compared: clean pass, and no baseline means no check at all (a
    # first release has nothing to compare against).
    base_ok = _write(
        tmp_path / "base_ok.json",
        _doc([_entry("startup.version", 0.1), _entry("tui.drawer", 0.09)]),
    )
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base_ok,
            "--current",
            cur,
            "--expected-gated",
            str(expected),
        ]
    )
    assert proc.returncode == 0, proc.stdout
    assert "integrity" not in proc.stdout
    proc = _run(
        [COMPARE, "--current", cur, "--expected-gated", str(expected)]
    )
    assert proc.returncode == 0, proc.stdout


def _gate_pair(tmp_path, tag, base_value, cur_value, mode="full"):
    """Paths of a baseline and a current document that hold tui.drawer at
    the two values, beside a metric that compares cleanly."""
    paths = []
    for side, value in (("base", base_value), ("cur", cur_value)):
        doc = _doc(
            [_entry("startup.version", 0.1), _entry("tui.drawer", value)]
        )
        doc["mode"] = mode
        paths.append(_write(tmp_path / ("%s-%s.json" % (tag, side)), doc))
    return paths


@pytest.mark.parametrize(
    "tag, base_value, cur_value",
    [
        ("zero-base", 0.0, 0.5),
        ("zero-both", 0.0, 0.0),
        ("negative-base", -0.001, 40.0),
        ("nan-base", float("nan"), 0.09),
        ("nan-cur", 0.09, float("nan")),
        ("inf-cur", 0.09, float("inf")),
    ],
)
def test_value_that_no_limit_can_fire_on_is_a_dead_gate(
    tmp_path, tag, base_value, cur_value
):
    # A percentage change needs a finite pair and a baseline above zero.
    # Without them no limit can fire, whatever the current side reads, so
    # the metric is counted with the uncompared ones and a listed one
    # fails the run.
    base, cur = _gate_pair(tmp_path, tag, base_value, cur_value)
    expected = tmp_path / "expected.txt"
    expected.write_text("startup.version\ntui.drawer\n", encoding="utf-8")
    md = tmp_path / "out.md"
    args = [
        COMPARE,
        "--baseline",
        base,
        "--current",
        cur,
        "--expected-gated",
        str(expected),
    ]
    proc = _run(args + ["--md", str(md)])
    assert proc.returncode == 1, proc.stdout
    assert "::error::perf gate integrity: tui.drawer" in proc.stdout
    assert "compared 1 metrics" in proc.stdout
    text = md.read_text(encoding="utf-8")
    assert "**Regression check: passed** for 1 of 2 metrics" in text
    assert "No percentage comparison available: tui.drawer." in text
    assert "nan%" not in text and "inf%" not in text
    # not [perf:accept]-able; --warn-only still downgrades.
    assert _run(args + ["--accept"]).returncode == 1
    proc = _run(args + ["--warn-only"])
    assert proc.returncode == 0, proc.stdout
    assert "::warning::perf gate integrity: tui.drawer" in proc.stdout


def test_zero_reading_of_a_reduced_workload_is_not_a_dead_gate(tmp_path):
    # --quick and --smoke shrink a workload until it can finish inside one
    # step of its clock (webui.log_count_5k does in --quick), so a zero
    # there is the mode's doing and does not fail a local paired run.
    expected = tmp_path / "expected.txt"
    expected.write_text("startup.version\ntui.drawer\n", encoding="utf-8")
    for mode in ("quick", "smoke"):
        for cur_value in (0.0, 0.0001):
            base, cur = _gate_pair(tmp_path, mode, 0.0, cur_value, mode=mode)
            proc = _run(
                [
                    COMPARE,
                    "--baseline",
                    base,
                    "--current",
                    cur,
                    "--expected-gated",
                    str(expected),
                ]
            )
            assert proc.returncode == 0, (mode, cur_value, proc.stdout)
            assert "integrity" not in proc.stdout
        # every other reading that cannot gate still fails in these modes
        for base_value, cur_value in (
            (-0.001, 0.09),
            (0.09, float("nan")),
            (float("nan"), 0.09),
        ):
            base, cur = _gate_pair(
                tmp_path, mode, base_value, cur_value, mode=mode
            )
            proc = _run(
                [
                    COMPARE,
                    "--baseline",
                    base,
                    "--current",
                    cur,
                    "--expected-gated",
                    str(expected),
                ]
            )
            assert proc.returncode == 1, (mode, base_value, proc.stdout)
            assert "perf gate integrity: tui.drawer" in proc.stdout


def test_non_finite_round_decides_the_merged_value():
    # min() and median() skip or keep a NaN depending on where it sits in
    # the round order, so a broken round must not merge away.
    import math

    compare = _load_compare()
    for estimator in ("min", "median"):
        for position in range(3):
            values = [1.0, 1.1, 0.9]
            values[position] = float("nan")
            docs = []
            for value in values:
                entry = _entry("state.broken", value)
                entry["compare"] = estimator
                docs.append(_doc([entry]))
            merged = compare._merge(docs)["state.broken"]
            assert math.isnan(merged["value"]), (estimator, position)
            # statistics.stdev raises on a NaN, so the noise estimate
            # stands down
            assert compare._rel_cov(merged) is None


def test_incomparable_sides_refuse_to_gate(tmp_path):
    # A one-sided backend turns a backend swap into a fake code regression,
    # or masks a real one; the pairing is invalid either way, so the
    # comparison refuses a verdict entirely: exit 2 even under --warn-only,
    # never a pass or a fail.  The same holds for an interpreter, platform
    # or run mode that differs.
    #
    # The keys are written out here.  A loop over compare.py's own tuple
    # would lose a key together with the tuple.
    same = {
        "mode": "full",
        "python": "3.14.0",
        "platform": "linux",
        "orjson": True,
        "uvloop": False,
        "isal": True,
    }
    other = {
        "mode": "quick",
        "python": "3.13.0",
        "platform": "darwin",
        "orjson": False,
        "uvloop": True,
        "isal": False,
    }
    assert set(_load_compare()._COMPARABILITY_KEYS) == set(same), (
        "compare.py's comparability keys changed; give each one a "
        "mismatch below"
    )

    def stamped(value, version, **stamp):
        doc = _doc([_entry("json.roundtrip_3k", value)], version=version)
        doc.update(same)
        doc.update(stamp)
        return doc

    base = _write(tmp_path / "base.json", stamped(0.7, "1.0.0"))
    cur = _write(tmp_path / "cur.json", stamped(0.7, "1.1.0"))
    # the control: documents that agree on every key do compare
    proc = _run([COMPARE, "--baseline", base, "--current", cur])
    assert proc.returncode == 0, proc.stdout
    for key in same:
        odd = _write(
            tmp_path / ("cur-%s.json" % key),
            stamped(0.7, "1.1.0", **{key: other[key]}),
        )
        for flags in ([], ["--warn-only"]):
            proc = _run(
                [COMPARE, "--baseline", base, "--current", odd] + flags
            )
            assert proc.returncode == 2, (key, flags, proc.stdout)
            assert "not comparable" in proc.stdout
            # the one key that differs is the one named
            assert " on %s (" % key in proc.stdout, (key, proc.stdout)
        # A mismatch WITHIN one side is just as invalid.
        proc = _run([COMPARE, "--baseline", base, "--current", cur, odd])
        assert proc.returncode == 2, (key, proc.stdout)
        assert " on %s (" % key in proc.stdout, (key, proc.stdout)


def test_effective_gate_percentage_is_reported(tmp_path):
    # compare.py ANDs the percentage gate with the absolute floor, so a
    # metric whose value sits near its floor really gates at
    # 100*floor/value, however tight gate_pct reads. That is deliberate
    # harness policy (the floor is the anti-jitter rule); what was missing
    # is anything REPORTING when it binds. The 2ms value against a 10ms
    # floor here gates at an effective 500%, not its declared 15%.
    base = _write(
        tmp_path / "base.json",
        _doc(
            [
                _entry("micro.floor_bound", 0.002, gate_pct=15.0),
                _entry("big.well_sized", 1.0, gate_pct=15.0),
            ]
        ),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [
                _entry("micro.floor_bound", 0.002, gate_pct=15.0),
                _entry("big.well_sized", 1.0, gate_pct=15.0),
            ],
            version="1.1.0",
        ),
    )
    md = tmp_path / "out.md"
    proc = _run(
        [COMPARE, "--baseline", base, "--current", cur, "--md", str(md)]
    )
    assert proc.returncode == 0, proc.stdout
    assert "::notice::perf sizing:" in proc.stdout
    assert "micro.floor_bound" in proc.stdout
    assert "eff. 500%" in proc.stdout
    assert "big.well_sized" not in proc.stdout  # not floor-bound: no notice
    text = md.read_text(encoding="utf-8")
    assert "Regression limit" in text
    assert "**500%** (declared 15%)" in text


def test_suppressed_regression_reaches_the_job_log(tmp_path):
    # A move over the raw limit but inside the noise band deliberately does
    # not gate -- but it must not vanish into a bare "0 gate violation(s)"
    # line either. Craft one: +21% delta on a 15% gate with round scatter
    # wide enough that 2 noise bands exceed it.
    def rounds(path, name, values, version="1.0.0"):
        docs = [
            _write(
                tmp_path / ("%s%d.json" % (path, i)),
                _doc([_entry(name, v, gate_pct=15.0)], version=version),
            )
            for i, v in enumerate(values)
        ]
        return docs

    base_files = rounds("b", "state.wobbly", [1.00, 1.30, 0.70])
    cur_files = rounds("c", "state.wobbly", [0.85, 1.15, 1.45], "1.1.0")
    proc = _run(
        [COMPARE, "--baseline"]
        + base_files
        + ["--current"]
        + cur_files
    )
    assert proc.returncode == 0, proc.stdout
    assert "0 gate violation(s)" in proc.stdout
    assert "::warning::perf gate: state.wobbly" in proc.stdout
    assert "reported, not gated" in proc.stdout
