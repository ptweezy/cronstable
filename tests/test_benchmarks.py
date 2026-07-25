"""Smoke tests for the performance benchmark tooling in benchmarks/.

The CI perf gate runs benchmarks/bench.py against both the current commit and
the previous release and diffs the two with benchmarks/compare.py, so a
harness that crashes (or silently skips everything) would take the release
gate down with it.  These tests run the suite in its minimal --smoke mode and
exercise compare.py's merge, chart, and gate logic on synthetic inputs.
"""

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
    # The headline metrics must never silently skip -- including the terminal
    # UI and the branch-win backend metrics (the web UI ones legitimately skip
    # in smoke, since they would launch a browser).  Every 2026-07 addition
    # that leans on a private seam is in this net BY REQUIREMENT (see
    # benchmarks/expected_gated.txt): this test is what catches a drifted
    # seam in the ordinary test run instead of at release time.  Not listed:
    # json.roundtrip_orjson_3k (skips wherever the optional orjson is not
    # installed, e.g. the tox envs) and the webui.* browser metrics.
    for name in (
        "startup.version",
        "schedule.cold_build_100k",
        "config.parse_yaml_300",
        "state.append_1k",
        "state.artifact_list_churn",
        "state.depends_on_past_gate",
        "dag.finish_fanin_1k",
        "dag.list_dags_warm",
        "tui.log_restyle_5k",
        # 2026-07 additions, round 1 (bench-additions.md)
        "loop.stall_jobs_500",
        "cronexpr.next_dst_2k",
        "redact.adversarial_10k",
        "schedule.due_pass_100k",
        "schedule.lint_250_zoned",
        "state.lease_renew_200",
        "state.gc_sweep_2k_streams",
        "dag.mapped_drain_256",
        "dag.adopt_scan_500",
        "dag.advance_quiescent_1k",
        "cluster.job_owner_2k",
        "cluster.parse_summaries_6k",
        "config.reload_warm_50",
        "config.interp_2k",
        "prometheus.render_500",
        "statsd.emit_2k",
        "webapi.jobs_payload_500",
        "webapi.auth_scope_20k",
        "mcp.handle_200",
        "job.stream_capture_120k",
        "json.roundtrip_3k",
        # 2026-07 additions, round 2 (bench-additions-2.md)
        "state.fanout_gather_100",
        "state.boot_rehydrate_populated",
        "state.list_documents_600",
        "tui.drawer_paint_5k",
        "webapi.jobs_bytes_500",
        "schedule.pressure_20k_48h",
    ):
        assert not results[name]["skipped"], results[name]


def test_bench_only_filter(tmp_path):
    out = tmp_path / "only.json"
    proc = _run([BENCH, "--smoke", "--only", "redact", "--json", str(out)])
    assert proc.returncode == 0, proc.stdout
    doc = json.loads(out.read_text(encoding="utf-8"))
    names = [r["name"] for r in doc["results"]]
    assert names and all(n.startswith("redact.") for n in names)


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
    assert "Gate: FAILED" in text
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
    assert "Gate: passed" in md.read_text(encoding="utf-8")
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
    assert "Gate: FAILED" in md.read_text(encoding="utf-8")


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
    assert "Not measured on either side (ungated): webui.wallboard." in text
    # The pass line must carry the compared/total count; the bare form claims
    # coverage the run did not have.
    assert "**Gate: passed.**" not in text
    assert "**Gate: passed** over 1 of 2 gated metrics" in text


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
    assert "**Gate: passed.**" not in text
    assert "**Gate: passed** over 1 of 2 gated metrics" in text


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
    assert "**Gate: passed.**" in text


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
    assert any("gate" in t for t in inside), inside


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
    assert "1 metric(s) have no baseline to compare" in svg
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
    assert "No previous release baseline" in text
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
    assert "absolute budget" in proc.stdout
    assert "Absolute budget: FAILED" in md.read_text(encoding="utf-8")

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
    assert "Gate integrity: FAILED" in md.read_text(encoding="utf-8")

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


def test_incomparable_sides_refuse_to_gate(tmp_path):
    # A one-sided backend (orjson here) turns a backend swap into a fake code
    # regression, or masks a real one; the pairing is invalid either way, so
    # the comparison refuses a verdict entirely -- exit 2 even under
    # --warn-only, never a pass or a fail.
    base_doc = _doc([_entry("json.roundtrip_3k", 0.7)])
    base_doc["orjson"] = False
    cur_doc = _doc([_entry("json.roundtrip_3k", 0.4)], version="1.1.0")
    cur_doc["orjson"] = True
    base = _write(tmp_path / "base.json", base_doc)
    cur = _write(tmp_path / "cur.json", cur_doc)
    for flags in ([], ["--warn-only"]):
        proc = _run([COMPARE, "--baseline", base, "--current", cur] + flags)
        assert proc.returncode == 2, (flags, proc.stdout)
        assert "not comparable" in proc.stdout
        assert "orjson" in proc.stdout
    # Mixed run modes WITHIN one side are just as invalid.
    cur_quick = _doc([_entry("json.roundtrip_3k", 0.1)], version="1.1.0")
    cur_quick["orjson"] = True
    cur_quick["mode"] = "quick"
    cur2 = _write(tmp_path / "cur2.json", cur_quick)
    proc = _run([COMPARE, "--baseline", base, "--current", cur, cur2])
    assert proc.returncode == 2, proc.stdout


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
    assert "Gate (eff.)" in text
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
