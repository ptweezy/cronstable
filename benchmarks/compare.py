#!/usr/bin/env python3
"""Compare two benchmark runs, render the release chart, gate regressions.

Consumes JSON documents written by benchmarks/bench.py.  Both sides accept
several files (the CI job runs the suites in interleaved rounds on one
runner); per metric the rounds are merged with the metric's own estimator
("min" for time, "median" for memory) so a single noisy round cannot fake or
mask a regression.

Outputs:
  --md PATH          markdown summary (release-notes section)
  --svg PATH         diverging bar chart of every compared metric
  --merged-out PATH  the merged current-side results as one JSON document

Gating: a metric fails when its slowdown exceeds all three limits at once:
the declared gate percentage, the absolute floor, and a couple of its
measured noise bands (the round-to-round scatter of the two sides, so
measurement noise on a jittery metric never gates).  Failures exit 1 unless
--warn-only (ordinary commits) or --accept (an acknowledged, intentional
regression).

Usage:
    python benchmarks/compare.py \
        --baseline perf/old.*.json --current perf/new.*.json \
        --md perf-summary.md --svg perf-chart.svg
"""

import argparse
import json
import math
import statistics
import sys

# Chart palette: the validated reference dataviz palette (diverging blue/red
# pair, ink and chrome tokens), light and dark, selected per mode.  The dark
# block is applied by prefers-color-scheme inside the SVG; renderers without
# media-query support fall back to the self-contained light card.
_LIGHT = {
    "surface": "#fcfcfb",
    "border": "rgba(11,11,11,0.10)",
    "grid": "#e1e0d9",
    "baseline": "#c3c2b7",
    "stripe": "rgba(11,11,11,0.035)",
    "ink1": "#0b0b0b",
    "ink2": "#52514e",
    "muted": "#898781",
    "faster": "#2a78d6",
    "slower": "#e34948",
}
_DARK = {
    "surface": "#1a1a19",
    "border": "rgba(255,255,255,0.10)",
    "grid": "#2c2c2a",
    "baseline": "#383835",
    "stripe": "rgba(255,255,255,0.045)",
    "ink1": "#ffffff",
    "ink2": "#c3c2b7",
    "muted": "#898781",
    "faster": "#3987e5",
    "slower": "#e66767",
}

# Significance guard for the gate.  A metric's round-to-round scatter is
# estimated as the coefficient of variation of its per-round estimator values;
# the two sides' CoVs combine in quadrature into a noise band.  A regression
# gates only when it also exceeds _SIG_SIGMA noise bands, so measurement noise
# alone never trips the gate and the gate percentage can be tight without
# generating false positives.  When a side has fewer than two rounds the noise
# cannot be estimated, and the guard stands down: the percentage and floor
# tests then decide on their own.
_SIG_SIGMA = 2.0
_MIN_ROUNDS_FOR_NOISE = 2


def _fmt(value, unit):
    if unit == "MB":
        return "%.2f MB" % value
    if unit == "KB":
        return "%.2f KB" % value
    if value < 0.001:
        return "%.1f us" % (value * 1e6)
    if value < 1.0:
        return "%.2f ms" % (value * 1e3)
    return "%.3f s" % value


def _load(paths):
    docs = []
    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            docs.append(json.load(f))
    return docs


# A paired comparison is only meaningful when every result document was
# produced under the same conditions.  These are the doc-level fields that
# change what a metric measures rather than how the code performs: a
# different interpreter, platform, run mode, or optional-backend state
# (orjson swaps the whole JSON hot path, uvloop the event loop) on one side
# turns a backend difference into a fake code regression.  bench.py stamps
# all of them into every result document.
_COMPARABILITY_KEYS = ("mode", "python", "platform", "orjson", "uvloop")


def _comparability_failures(baseline_docs, current_docs):
    """Fields from _COMPARABILITY_KEYS that differ across the result docs.

    Every document of both sides must agree on every key; the first document
    is the reference.  Returns human-readable mismatch strings (empty when
    comparable).  Missing keys compare as None on both sides, so documents
    from an older bench.py (which did not stamp them) stay comparable with
    each other.
    """
    ref = None
    ref_src = None
    failures = []
    for side, docs in (("baseline", baseline_docs), ("current", current_docs)):
        for doc in docs:
            sig = {key: doc.get(key) for key in _COMPARABILITY_KEYS}
            if ref is None:
                ref, ref_src = sig, side
                continue
            diffs = [k for k in _COMPARABILITY_KEYS if sig[k] != ref[k]]
            if diffs:
                failures.append(
                    "%s-side document disagrees with the first %s-side "
                    "document on %s"
                    % (
                        side,
                        ref_src,
                        ", ".join(
                            "%s (%r vs %r)" % (k, sig[k], ref[k])
                            for k in diffs
                        ),
                    )
                )
    return failures


def _load_budgets(path):
    """The absolute-ceiling document (benchmarks/budgets.json).

    Relative gating against the previous release cannot stop slow multi-hop
    drift: a metric taking the maximum legal regression on each of several
    quick releases compounds with a green gate every time.  The checked-in
    budget file pins an absolute ceiling for a handful of headline metrics;
    raising one takes a deliberate, reviewed edit to that file and cannot
    happen as a side effect of accepting a relative regression.
    """
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    return doc.get("budgets", {})


def _budget_failures(budgets, current):
    """Budget breaches, and budgeted metrics this run did not measure."""
    breaches = []
    unmeasured = []
    for name in sorted(budgets):
        spec = budgets[name]
        entry = current.get(name)
        if entry is None or entry.get("skipped"):
            unmeasured.append(name)
            continue
        if entry["value"] > spec["max"]:
            breaches.append(
                "%s measured %s, over its absolute budget of %s (raising "
                "the budget is a deliberate edit to benchmarks/budgets.json)"
                % (
                    name,
                    _fmt(entry["value"], entry.get("unit", "s")),
                    _fmt(spec["max"], spec.get("unit", "s")),
                )
            )
    return breaches, unmeasured


def _load_expected_gated(path):
    """Metric ids that must be compared (measured on both sides) every run.

    A gate whose baseline side skips is silently ungated forever: the
    comparator files it under first-release 'new' coverage, which is never
    warned about, so a gate that died because a private seam drifted is
    indistinguishable from a metric added this release.  This checked-in
    list makes the coverage claim falsifiable: a listed metric that was not
    compared counts as an integrity failure rather than a footnote.
    """
    names = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                names.append(line)
    return names


def _merge(docs):
    """Merge several rounds into one {name: entry} map.

    The entry's stats are recomputed over every round's raw repeats; `value`
    uses the metric's declared estimator across rounds.
    """
    merged = {}
    for doc in docs:
        for entry in doc.get("results", []):
            name = entry["name"]
            slot = merged.setdefault(name, {"entry": None, "round_values": []})
            if entry.get("skipped"):
                slot.setdefault("skip_reason", entry.get("reason"))
                # Keep the skipped row's declared config. bench.py stamps
                # gate_pct/gate_floor/info on a skipped result too, and that
                # is the only thing downstream can use to tell lost gate
                # coverage from an info-only metric that never gates; without
                # it every skipped metric looks ungateable by design.
                slot.setdefault("skipped_entry", entry)
                continue
            if slot["entry"] is None:
                slot["entry"] = dict(entry)
            slot["round_values"].append(entry["value"])
    out = {}
    for name, slot in merged.items():
        if slot["entry"] is None:
            declared = slot.get("skipped_entry") or {}
            out[name] = {
                "name": name,
                "skipped": True,
                "reason": slot.get("skip_reason", "skipped"),
                "gate_pct": declared.get("gate_pct"),
                "gate_floor": declared.get("gate_floor"),
                "info": declared.get("info"),
                "unit": declared.get("unit"),
            }
            continue
        entry = slot["entry"]
        vals = slot["round_values"]
        entry["value"] = (
            min(vals)
            if entry.get("compare") == "min"
            else statistics.median(vals)
        )
        entry["round_values"] = vals
        out[name] = entry
    return out


def _delta_pct(base, cur):
    if base <= 0:
        return None
    return (cur - base) / base * 100.0


def _rel_cov(entry):
    """A side's round-to-round scatter as a coefficient of variation.

    Uses the per-round estimator values that _merge preserved in
    ``round_values``; needs at least two rounds to estimate scatter, else
    None.  Returned as a fraction of the metric's center.

    From three rounds up the scatter is a robust estimate: the median absolute
    deviation scaled to a standard-deviation equivalent (x1.4826).  A single
    throttled/GC-stalled round then cannot inflate the band and so cannot mask
    a real regression behind it, which is the failure mode a plain stdev
    has on a handful of noisy samples.  (A degenerate MAD of 0, e.g. a tied
    median on few points, falls back to stdev so the band is never falsely
    zero.)  Two rounds carry too little shape for a robust estimator, so
    they keep stdev.
    """
    rounds = entry.get("round_values") or []
    if len(rounds) < _MIN_ROUNDS_FOR_NOISE:
        return None
    center = statistics.median(rounds)
    if center <= 0:
        return None
    if len(rounds) >= 3:
        med = statistics.median(rounds)
        mad = statistics.median([abs(x - med) for x in rounds])
        scatter = 1.4826 * mad or statistics.stdev(rounds)
    else:
        scatter = statistics.stdev(rounds)
    return scatter / center


def _baseline_of(side_map):
    """The interpreter-startup floor (``startup.python_baseline``) measured on
    one side, or None if it was not run (older release, filtered run)."""
    entry = side_map.get("startup.python_baseline")
    if entry is None or entry.get("skipped"):
        return None
    return entry.get("value")


def _adjusted_values(name, base, cur, base_floor, cur_floor):
    """Startup metrics minus the co-measured interpreter-startup floor.

    ``cronstable --version`` and the import timings are dominated by Python's
    own process spawn + interpreter init, which cronstable cannot regress.
    Subtracting each side's ``startup.python_baseline`` isolates cronstable's
    own contribution, so a regression in import cost is measured against the
    ~9ms cronstable actually owns rather than being diluted by ~30ms of
    un-regressable interpreter overhead.  Returns the pair of adjusted values,
    or the raw pair when the floor is unknown or would leave a non-positive
    remainder (a subtraction artefact, not a real measurement).

    The gate's sensitivity after this is ``gate_pct`` of the own share (25%
    for the startup benches), floored by :func:`_adjusted_floor`.  That is a
    relative figure, not a flat millisecond one: a +2ms move is under 25% of
    every measured own share, so no startup metric can promise to catch it.
    """
    if not name.startswith("startup.") or name == "startup.python_baseline":
        return base["value"], cur["value"]
    if base_floor is None or cur_floor is None:
        return base["value"], cur["value"]
    ab, ac = base["value"] - base_floor, cur["value"] - cur_floor
    if ab <= 0 or ac <= 0:
        return base["value"], cur["value"]
    return ab, ac


# Absolute floor for a startup metric once its interpreter-startup share has
# been subtracted.  The raw 10ms default (bench.py) is calibrated against the
# ~40ms totals; applied to an own share of a few ms it swamps gate_pct
# entirely.  2ms sits below gate_pct for every own share big enough to gate on
# a percentage, so gate_pct is what actually binds; a sub-2ms move on a tiny
# metric still passes, because a small own share has the worst relative
# noise.
ADJUSTED_GATE_FLOOR = 0.002


def _adjusted_floor(cur, adj_cur):
    """``gate_floor`` brought onto the scale :func:`_adjusted_values` left.

    ``gate_floor`` is an absolute "too small to bother failing a release over"
    threshold, calibrated against a metric's raw value.  When the startup
    adjustment strips the interpreter floor, the quantity being gated shrinks
    (often to a small fraction of the raw total) while the constant does not,
    so an unscaled floor silently re-imposes the very dilution the
    subtraction exists to remove: ``startup.import_cronexpr`` owns ~9.5ms of
    its ~41ms total against a 10ms default floor, so even a +100% regression
    in cronstable's own import cost could not clear it and the metric would
    be effectively ungated.

    Deliberately a flat cap rather than the same ratio the values were reduced
    by: a proportional floor still scales with the interpreter overhead,
    which lets the effective threshold drift to roughly +47% instead of the
    declared ``gate_pct``.  Capping makes ``gate_pct`` the binding constraint
    wherever a percentage is meaningful, so metrics really do gate at the
    percentage they declare.

    Returns ``gate_floor`` unchanged whenever no adjustment happened: every
    non-startup metric, plus any startup metric whose interpreter floor was
    unavailable (an older baseline, or a run filtered to the in-process tier,
    so ``startup.python_baseline`` is missing on one side) or whose
    subtraction left a non-positive remainder.  Those keep the raw floor and
    stay hard to gate, for the same reason they keep raw values: there is no
    measured own share to judge them on.
    """
    floor = cur.get("gate_floor") or 0.0
    raw = cur.get("value")
    if not floor or not raw or adj_cur >= raw:
        return floor
    return min(floor, ADJUSTED_GATE_FLOOR)


def _noise_band(base, cur):
    """Combined round-to-round noise band as a fraction, or None if unknown.

    The two sides' coefficients of variation add in quadrature; None whenever
    either side has too few rounds to estimate its own scatter.
    """
    noise_base = _rel_cov(base)
    noise_cur = _rel_cov(cur)
    if noise_base is None or noise_cur is None:
        return None
    return math.hypot(noise_base, noise_cur)


def _declared_gate_pct(baseline, current, name):
    """``gate_pct`` for ``name`` from whichever side declares one.

    A skipped entry still carries the benchmark's declared gate, so this
    distinguishes a metric that would gate from an ``info=True`` metric that
    never gates by design (``gate_pct`` is None for those, see bench.py).
    """
    for side in (current, baseline):
        entry = side.get(name)
        if entry is not None and entry.get("gate_pct") is not None:
            return entry["gate_pct"]
    return None


def _gate_coverage(baseline, current):
    """How much of the declared gate this run actually compared.

    A gate can only fire on a metric measured on both sides, and a metric can
    fall out three ways: skipped on both sides, skipped (or absent) now after
    the baseline measured it, or measured only now.  None of the three
    produces a row that can gate, and the first produces no row at all, so a
    report that does not count them presents a pass over a shrunken set as a
    pass over the whole one.  Only metrics that declare a ``gate_pct`` are
    counted; an ``info``-only metric skipping is not lost coverage.
    """
    coverage = {
        "compared": 0,
        "compared_names": [],
        "both": [],
        "dropped": [],
        "new": [],
    }
    for name in sorted(set(baseline) | set(current)):
        if _declared_gate_pct(baseline, current, name) is None:
            continue
        cur, base = current.get(name), baseline.get(name)
        cur_ok = cur is not None and not cur.get("skipped")
        base_ok = base is not None and not base.get("skipped")
        if cur_ok and base_ok:
            coverage["compared"] += 1
            coverage["compared_names"].append(name)
        elif not cur_ok and not base_ok:
            coverage["both"].append(name)
        elif not cur_ok:
            coverage["dropped"].append(name)
        else:
            coverage["new"].append(name)
    return coverage


def _compare(baseline, current):
    """Per-metric comparison rows, gate violations, and gate coverage."""
    rows = []
    violations = []
    coverage = _gate_coverage(baseline, current)
    base_floor = _baseline_of(baseline)
    cur_floor = _baseline_of(current)
    for name, cur in current.items():
        if cur.get("skipped"):
            continue
        base = baseline.get(name)
        if base is None or base.get("skipped"):
            rows.append(
                {
                    "name": name,
                    "entry": cur,
                    "base_value": None,
                    "delta_pct": None,
                    "noise_pct": None,
                    "significant": None,
                    "gated": False,
                }
            )
            continue
        # Gate on cronstable's own contribution: startup metrics have their
        # interpreter-startup floor subtracted first. The displayed base/cur
        # values stay the raw totals; only delta_pct (what gates) is adjusted.
        adj_base, adj_cur = _adjusted_values(
            name, base, cur, base_floor, cur_floor
        )
        delta = _delta_pct(adj_base, adj_cur)
        noise = _noise_band(base, cur)
        # Unknown noise (too few rounds) never suppresses: the guard is only
        # allowed to make the gate more conservative, never to hide a
        # regression when it lacks the data to prove it is noise.
        significant = noise is None or (
            delta is not None and abs(delta) / 100.0 > _SIG_SIGMA * noise
        )
        gated = False
        gate_pct = cur.get("gate_pct")
        # The floor is brought onto the same scale as the values: comparing
        # an adjusted delta against the raw-scale constant leaves the
        # small-own-share startup metrics ungateable.
        floor_used = _adjusted_floor(cur, adj_cur)
        # The gate a metric actually delivers rather than the one it
        # declares: the percentage and absolute tests are ANDed, so on a
        # metric whose value sits near (or under) its floor the floor binds
        # and the real sensitivity is 100*floor/value, however tight gate_pct
        # reads.  The floor itself is deliberate harness policy (jitter on a
        # tiny metric must never gate); this figure is what shows when a
        # workload is undersized against the harness's own 50ms+ sizing rule.
        effective_pct = gate_pct
        if gate_pct is not None and adj_base > 0:
            effective_pct = max(gate_pct, 100.0 * floor_used / adj_base)
        over_gate = (
            delta is not None
            and gate_pct is not None
            and delta > gate_pct
            and (adj_cur - adj_base) > floor_used
        )
        if over_gate and significant:
            gated = True
            violations.append(
                "%s regressed %+.1f%% (%s to %s, gate %.0f%%, noise band "
                "+-%s)"
                % (
                    name,
                    delta,
                    _fmt(base["value"], cur["unit"]),
                    _fmt(cur["value"], cur["unit"]),
                    gate_pct,
                    "%.1f%%" % (noise * 100) if noise is not None else "n/a",
                )
            )
        rows.append(
            {
                "name": name,
                "entry": cur,
                "base_value": base["value"],
                "delta_pct": delta,
                "noise_pct": noise * 100 if noise is not None else None,
                "significant": significant,
                "effective_pct": effective_pct,
                # a change that cleared the raw gate but not the noise band:
                # reported, but explicitly not gated.
                "suppressed": over_gate and not significant,
                "gated": gated,
            }
        )
    return rows, violations, coverage


def _floor_bound(rows, slack=1.05):
    """Compared rows whose effective gate exceeds their declared one.

    ``slack`` ignores hair-width excesses: a metric 5% over its declared
    percentage is measurement dust, not an undersized workload.
    """
    out = []
    for row in rows:
        declared = row["entry"].get("gate_pct")
        effective = row.get("effective_pct")
        if (
            declared is not None
            and effective is not None
            and effective > declared * slack
        ):
            out.append(row)
    out.sort(key=lambda r: -(r["effective_pct"] / r["entry"]["gate_pct"]))
    return out


# ---------------------------------------------------------------------------
# SVG chart
# ---------------------------------------------------------------------------


def _nice_limit(max_abs):
    for candidate in (5, 10, 15, 20, 30, 40, 50):
        if max_abs <= candidate:
            return candidate
    return 50


def _bar_path(x0, y, length, height, rightward):
    """A bar from the zero baseline: square there, 4px rounded data-end."""
    r = min(4.0, abs(length))
    h = height
    if rightward:
        return (
            "M%.1f,%.1f h%.1f a%.1f,%.1f 0 0 1 %.1f,%.1f v%.1f "
            "a%.1f,%.1f 0 0 1 -%.1f,%.1f h-%.1f z"
            % (
                x0,
                y,
                length - r,
                r,
                r,
                r,
                r,
                h - 2 * r,
                r,
                r,
                r,
                r,
                length - r,
            )
        )
    return (
        "M%.1f,%.1f h-%.1f a%.1f,%.1f 0 0 0 -%.1f,%.1f v%.1f "
        "a%.1f,%.1f 0 0 0 %.1f,%.1f h%.1f z"
        % (x0, y, length - r, r, r, r, r, h - 2 * r, r, r, r, r, length - r)
    )


def build_svg(rows, base_label, cur_label):
    """Diverging horizontal bars: % runtime change per metric, vs baseline.

    Every compared metric gets a row and the chart grows to fit, so the
    release image is the whole suite rather than a top-N cut of it.  A
    metric measured on only one side has no change to draw; those are
    counted in a footnote and their numbers stay in the release-notes
    table.
    """
    shown = [r for r in rows if r["delta_pct"] is not None]
    shown.sort(key=lambda r: -r["delta_pct"])
    uncompared = len(rows) - len(shown)

    width = 860
    gutter = 230
    plot_right = width - 96
    center = gutter + (plot_right - gutter) / 2.0
    half = (plot_right - gutter) / 2.0 - 4
    row_h = 24
    bar_h = 12
    top = 78
    plot_bottom = top + len(shown) * row_h
    height = plot_bottom + (46 if uncompared > 0 else 30)

    limit = _nice_limit(max((abs(r["delta_pct"]) for r in shown), default=5.0))
    scale = half / limit

    css = (
        "svg{color-scheme:light dark;"
        "font-family:system-ui,-apple-system,'Segoe UI',sans-serif}"
        ".surface{fill:%(surface)s;stroke:%(border)s}"
        ".t1{fill:%(ink1)s}.t2{fill:%(ink2)s}.t3{fill:%(muted)s}"
        ".num{font-variant-numeric:tabular-nums}"
        ".grid{stroke:%(grid)s;stroke-width:1}"
        ".zero{stroke:%(baseline)s;stroke-width:1}"
        ".stripe{fill:%(stripe)s}"
        ".fast{fill:%(faster)s}.slow{fill:%(slower)s}"
        # a label drawn inside a clamped bar: near-black reads on both the
        # blue and the red fill, in either theme (>=4.5:1), where white would
        # fail on the red; so it needs no per-theme override.
        ".inlabel{fill:#000000}" % _LIGHT
    )
    dark_css = (
        "@media(prefers-color-scheme:dark){"
        ".surface{fill:%(surface)s;stroke:%(border)s}"
        ".t1{fill:%(ink1)s}.t2{fill:%(ink2)s}.t3{fill:%(muted)s}"
        ".grid{stroke:%(grid)s}"
        ".zero{stroke:%(baseline)s}"
        ".stripe{fill:%(stripe)s}"
        ".fast{fill:%(faster)s}.slow{fill:%(slower)s}}" % _DARK
    )

    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
        'viewBox="0 0 %d %d" role="img" '
        'aria-label="Performance change per benchmark, %s vs %s">'
        % (width, height, width, height, cur_label, base_label),
        "<style>%s%s</style>" % (css, dark_css),
        '<rect class="surface" x="0.5" y="0.5" width="%d" height="%d" rx="8"/>'
        % (width - 1, height - 1),
        '<text class="t1" x="20" y="30" font-size="14" font-weight="600">'
        "cronstable %s: performance vs %s</text>" % (cur_label, base_label),
        '<text class="t2" x="20" y="50" font-size="11">'
        "% change in runtime and memory for every compared benchmark. "
        "Lower is better; bars left of zero are faster than the previous "
        "release.</text>",
        # Legend: identity for the two directions, text in ink tokens.
        '<rect class="fast" x="%d" y="21" width="10" height="10" rx="2"/>'
        % (width - 180),
        '<text class="t2" x="%d" y="30" font-size="11">faster</text>'
        % (width - 165),
        '<rect class="slow" x="%d" y="21" width="10" height="10" rx="2"/>'
        % (width - 112),
        '<text class="t2" x="%d" y="30" font-size="11">slower</text>'
        % (width - 97),
    ]

    # Alternate-row wash, drawn under the grid and bars: at full-suite
    # height a reader has to carry a metric name across the gutter to its
    # bar, and the stripe does that without adding data-weight ink.
    for i in range(1, len(shown), 2):
        parts.append(
            '<rect class="stripe" x="12" y="%d" width="%d" height="%d"/>'
            % (top + i * row_h, width - 24, row_h)
        )

    # The tick labels sit at both ends of the plot: with every metric shown
    # the bottom axis can be a full screen below the title.
    for tick in (-limit, -limit / 2.0, limit / 2.0, limit):
        x = center + tick * scale
        parts.append(
            '<line class="grid" x1="%.1f" y1="%d" x2="%.1f" y2="%d"/>'
            % (x, top - 6, x, plot_bottom + 4)
        )
        for y in (top - 12, plot_bottom + 18):
            parts.append(
                '<text class="t3 num" x="%.1f" y="%d" font-size="10" '
                'text-anchor="middle">%+g%%</text>' % (x, y, tick)
            )
    parts.append(
        '<line class="zero" x1="%.1f" y1="%d" x2="%.1f" y2="%d"/>'
        % (center, top - 6, center, plot_bottom + 4)
    )
    for y in (top - 12, plot_bottom + 18):
        parts.append(
            '<text class="t3 num" x="%.1f" y="%d" font-size="10" '
            'text-anchor="middle">0</text>' % (center, y)
        )

    for i, row in enumerate(shown):
        y_mid = top + i * row_h + row_h / 2.0
        y_bar = y_mid - bar_h / 2.0
        delta = row["delta_pct"]
        clamped = max(-limit, min(limit, delta))
        length = abs(clamped) * scale
        parts.append(
            '<text class="t2" x="%d" y="%.1f" font-size="11" '
            'text-anchor="end">%s</text>'
            % (gutter - 10, y_mid + 4, row["name"])
        )
        label = "%+.1f%%" % delta
        if row["gated"]:
            label += " (gate)"
        rightward = delta > 0
        if length >= 0.75:
            cls = "slow" if rightward else "fast"
            parts.append(
                '<path class="%s" d="%s"/>'
                % (cls, _bar_path(center, y_bar, length, bar_h, rightward))
            )
        # Place the percentage just past the bar's data-end, unless a large
        # (clamped) bar would push it off the plot: past the right edge, or
        # left into the metric-name gutter, where a -94% label would land on
        # top of the name.  Then draw it inside the bar's end instead, so the
        # number stays readable rather than colliding or clipping.
        pad = 6.0
        # rough font-size-10 advance; err high so a borderline label goes
        # inside (always legible) rather than half-off the edge.
        label_w = len(label) * 6.0 + 3.0
        end = center + length if rightward else center - length
        if rightward:
            if end + pad + label_w <= width - 10.0:
                lx, anchor, lcls = end + pad, "start", "t2 num"
            else:
                lx, anchor, lcls = end - pad, "end", "inlabel num"
        else:
            if end - pad - label_w >= gutter + 4.0:
                lx, anchor, lcls = end - pad, "end", "t2 num"
            else:
                lx, anchor, lcls = end + pad, "start", "inlabel num"
        parts.append(
            '<text class="%s" x="%.1f" y="%.1f" font-size="10" '
            'text-anchor="%s">%s</text>' % (lcls, lx, y_mid + 4, anchor, label)
        )

    if uncompared > 0:
        parts.append(
            '<text class="t3" x="20" y="%d" font-size="10">'
            "%d metric(s) have no baseline to compare; first numbers in "
            "the release-notes table.</text>" % (plot_bottom + 36, uncompared)
        )
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


# ---------------------------------------------------------------------------
# Markdown summary
# ---------------------------------------------------------------------------


def _pct(value):
    return "%.4g%%" % value


def build_md(
    rows,
    violations,
    base_label,
    cur_label,
    baseline_missing,
    current,
    baseline,
    img_url=None,
    accept=False,
    coverage=None,
    budget_breaches=None,
    expected_missing=None,
):
    lines = ["### Performance vs %s" % (base_label or "(no baseline)")]
    lines.append("")
    if baseline_missing:
        lines.append(
            "There is no previous release to compare against, so this run "
            "only records its numbers. The next release diffs against them."
        )
        lines.append("")
    if img_url:
        lines.append(
            "![Performance change per benchmark, %s vs %s](%s)"
            % (cur_label, base_label, img_url)
        )
        lines.append("")
    if not baseline_missing:
        if violations and accept:
            lines.append(
                "**Gate: regressions accepted** (a `[perf:accept]` marker "
                "acknowledged them):"
            )
            lines.extend("- %s" % v for v in violations)
        elif violations:
            lines.append("**Gate: FAILED**")
            lines.extend("- %s" % v for v in violations)
        else:
            # Qualified by how many metrics the gate could actually compare.
            # An unqualified pass over a set that silently lost its webui
            # metrics reads as coverage the run did not have.
            cov = coverage or {}
            missed = (
                len(cov.get("both", ()))
                + len(cov.get("dropped", ()))
                + len(cov.get("new", ()))
            )
            if missed:
                lines.append(
                    "**Gate: passed** over %d of %d gated metrics."
                    % (cov.get("compared", 0), cov.get("compared", 0) + missed)
                )
            else:
                lines.append(
                    "**Gate: passed.** Every metric stayed inside its "
                    "regression limit."
                )
        if budget_breaches:
            lines.append("")
            lines.append(
                "**Absolute budget: FAILED.** These metrics are over the "
                "ceilings in `benchmarks/budgets.json`, and raising a ceiling "
                "takes an edit to that file:"
            )
            lines.extend("- %s" % b for b in budget_breaches)
        if expected_missing:
            lines.append("")
            lines.append(
                "**Gate integrity: FAILED.** `benchmarks/expected_gated.txt` "
                "lists these metrics, but the run never compared them, so "
                "their gates are dead rather than passing: %s."
                % ", ".join(expected_missing)
            )
        lines.append("")
        lines.append(
            "Both versions ran interleaved on one runner. Time metrics "
            "compare the best round of each and memory metrics the median. "
            "A negative change means faster or smaller."
        )
        lines.append("")
        lines.append(
            "A regression gates only when it exceeds both its declared limit "
            "and %.0f noise bands. The +- column is that band: the "
            "round-to-round scatter of the two sides, combined in "
            "quadrature." % _SIG_SIGMA
        )
        lines.append("")
        suppressed = [r for r in rows if r.get("suppressed")]
        if suppressed:
            lines.append(
                "Some moves cleared their raw limit but stayed inside the "
                "noise band, so they are reported without gating: %s."
                % ", ".join(
                    "%s (%+.1f%%, noise +-%.1f%%)"
                    % (r["name"], r["delta_pct"], r["noise_pct"] or 0.0)
                    for r in sorted(suppressed, key=lambda r: -r["delta_pct"])
                )
            )
            lines.append("")

        comparable = [r for r in rows if r["delta_pct"] is not None]
        comparable.sort(key=lambda r: -abs(r["delta_pct"]))
        lines.append("<details>")
        lines.append(
            "<summary>All benchmark results (%d metrics)</summary>"
            % len(comparable)
        )
        lines.append("")
        lines.append(
            "| Benchmark | %s | %s | Change | Noise +- | Gate (eff.) |"
            % (base_label, cur_label)
        )
        lines.append("|---|---:|---:|---:|---:|---:|")
        for row in comparable:
            entry = row["entry"]
            if row["gated"]:
                mark = " **(gate)**"
            elif row.get("suppressed"):
                mark = " (within noise)"
            else:
                mark = ""
            noise = row.get("noise_pct")
            noise_cell = "%.1f%%" % noise if noise is not None else "n/a"
            declared = entry.get("gate_pct")
            effective = row.get("effective_pct")
            # The gate the metric delivers: gate_pct where the percentage
            # binds, 100*floor/value where the absolute floor does.  A
            # floor-bound cell names both numbers so an undersized workload
            # is visible in the release table rather than only in the
            # harness's source.
            if declared is None:
                gate_cell = "info"
            elif effective is not None and effective > declared * 1.05:
                gate_cell = "**%s** (declared %s)" % (
                    _pct(effective),
                    _pct(declared),
                )
            else:
                gate_cell = _pct(declared)
            lines.append(
                "| %s | %s | %s | %+.1f%%%s | %s | %s |"
                % (
                    row["name"],
                    _fmt(row["base_value"], entry["unit"]),
                    _fmt(entry["value"], entry["unit"]),
                    row["delta_pct"],
                    mark,
                    noise_cell,
                    gate_cell,
                )
            )
        lines.append("")
        lines.append("</details>")

        new_metrics = [r["name"] for r in rows if r["delta_pct"] is None]
        if new_metrics:
            lines.append("")
            lines.append(
                "New in this release (no baseline yet): %s."
                % ", ".join(sorted(new_metrics))
            )
        dropped = sorted(
            name
            for name, entry in baseline.items()
            if not entry.get("skipped")
            and (name not in current or current[name].get("skipped"))
        )
        if dropped:
            lines.append("")
            lines.append(
                "Measured in %s but not in this run: %s."
                % (base_label, ", ".join(dropped))
            )
        both_sides = (coverage or {}).get("both", ())
        if both_sides:
            # Skipped on both sides, so absent from every list above (the
            # dropped list only covers metrics the baseline measured). These
            # are ungated by construction: say so rather than let a clean
            # gate line imply they passed.
            lines.append("")
            lines.append(
                "Not measured on either side (ungated): %s."
                % ", ".join(sorted(both_sides))
            )
    else:
        lines.append("| Benchmark | %s |" % cur_label)
        lines.append("|---|---:|")
        for name in sorted(current):
            entry = current[name]
            if entry.get("skipped"):
                continue
            lines.append(
                "| %s | %s |" % (name, _fmt(entry["value"], entry["unit"]))
            )
    lines.append("")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--baseline", nargs="*", default=[])
    parser.add_argument("--current", nargs="+", required=True)
    parser.add_argument("--baseline-label", default=None)
    parser.add_argument("--current-label", default=None)
    parser.add_argument("--md")
    parser.add_argument("--svg")
    parser.add_argument("--merged-out")
    parser.add_argument("--img-url")
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="report gate failures as warnings, never exit nonzero",
    )
    parser.add_argument(
        "--accept",
        action="store_true",
        help="acknowledge regressions ([perf:accept]): report but pass",
    )
    parser.add_argument(
        "--budgets",
        help="benchmarks/budgets.json: absolute ceilings for headline "
        "metrics; a breach fails even when the relative gate passes, and "
        "[perf:accept] does not excuse it (the only override is a reviewed "
        "edit to the file)",
    )
    parser.add_argument(
        "--expected-gated",
        help="benchmarks/expected_gated.txt: metric ids that must be "
        "compared on both sides; a listed metric that was not compared is "
        "an integrity failure that [perf:accept] cannot excuse",
    )
    args = parser.parse_args(argv)

    current_docs = _load(args.current)
    current = _merge(current_docs)
    cur_label = args.current_label or current_docs[0].get(
        "cronstable_version", "current"
    )

    baseline_missing = not args.baseline
    baseline = {}
    baseline_docs = []
    base_label = args.baseline_label
    if not baseline_missing:
        baseline_docs = _load(args.baseline)
        baseline = _merge(baseline_docs)
        base_label = base_label or baseline_docs[0].get(
            "cronstable_version", "baseline"
        )

    # An incomparable pair is a broken measurement rather than a pass or a
    # fail: refuse to render a verdict from it at all (exit 2, even under
    # --warn-only).  A best-effort install that lands on one side only would
    # otherwise report a backend swap as a large code regression, or mask a
    # real one.
    incomparable = _comparability_failures(baseline_docs, current_docs)
    if incomparable:
        for failure in incomparable:
            print("::error::perf compare: sides not comparable: %s" % failure)
        return 2

    rows, violations, coverage = _compare(baseline, current)

    budget_breaches, budget_unmeasured = [], []
    if args.budgets:
        budget_breaches, budget_unmeasured = _budget_failures(
            _load_budgets(args.budgets), current
        )

    expected_missing = []
    if args.expected_gated and not baseline_missing:
        expected = _load_expected_gated(args.expected_gated)
        compared_names = set(coverage["compared_names"])
        expected_missing = [n for n in expected if n not in compared_names]

    if args.merged_out:
        merged_doc = dict(current_docs[0])
        merged_doc["results"] = [current[name] for name in sorted(current)]
        merged_doc["merged_from"] = len(current_docs)
        with open(args.merged_out, "w", encoding="utf-8") as f:
            json.dump(merged_doc, f, indent=1, sort_keys=True)
            f.write("\n")

    if args.svg and not baseline_missing:
        with open(args.svg, "w", encoding="utf-8") as f:
            f.write(build_svg(rows, base_label, cur_label))

    if args.md:
        with open(args.md, "w", encoding="utf-8") as f:
            f.write(
                build_md(
                    rows,
                    violations,
                    base_label,
                    cur_label,
                    baseline_missing,
                    current,
                    baseline,
                    img_url=args.img_url,
                    accept=args.accept,
                    coverage=coverage,
                    budget_breaches=budget_breaches,
                    expected_missing=expected_missing,
                )
            )

    comparable = sum(1 for r in rows if r["delta_pct"] is not None)
    print(
        "compared %d metrics (%s vs %s): %d gate violation(s)"
        % (comparable, cur_label, base_label or "no baseline", len(violations))
    )
    # Run-level sizing feedback, printed with the run summary (before any
    # verdict lines): which metrics' declared gate_pct overstates the real
    # sensitivity because the absolute floor binds instead.
    floor_bound = _floor_bound(rows)
    if floor_bound:
        print(
            "::notice::perf sizing: %d metric(s) are floor-bound (the "
            "absolute floor sets the real sensitivity, not gate_pct; the "
            "workload is undersized against the harness's 50ms+ rule): %s"
            % (
                len(floor_bound),
                ", ".join(
                    "%s (eff. %.4g%% vs %.4g%% declared)"
                    % (
                        r["name"],
                        r["effective_pct"],
                        r["entry"]["gate_pct"],
                    )
                    for r in floor_bound
                ),
            )
        )
    # Visible in the job log too, not only the rendered report: a metric that
    # declared a gate but was not compared produces no violation, so silence
    # here reads as coverage. "new" is expected for a benchmark added this
    # release and is already reported as such, so it is not warned about.
    lost = sorted(coverage["both"]) + sorted(coverage["dropped"])
    if lost:
        print(
            "::warning::perf gate: %d declared-gate metric(s) not compared, "
            "so ungated this run: %s" % (len(lost), ", ".join(lost))
        )
    for violation in violations:
        if args.accept or args.warn_only:
            print("::warning::perf gate: %s" % violation)
        else:
            print("::error::perf gate: %s" % violation)
    # A regression that cleared its raw limit but sat inside the noise band
    # is deliberately not gated, but it must reach the job log and not only
    # the rendered report: a +18% move printing as "0 gate violation(s)"
    # with no other output reads as a clean pass.
    for row in rows:
        if row.get("suppressed"):
            print(
                "::warning::perf gate: %s regressed %+.1f%% (over its "
                "%.4g%% limit) but within the noise band (+-%.1f%%); "
                "reported, not gated"
                % (
                    row["name"],
                    row["delta_pct"],
                    row["entry"].get("gate_pct") or 0.0,
                    row.get("noise_pct") or 0.0,
                )
            )
    for name in budget_unmeasured:
        print(
            "::warning::perf budget: %s has an absolute budget but was not "
            "measured this run" % name
        )
    for breach in budget_breaches:
        if args.warn_only:
            print("::warning::perf budget: %s" % breach)
        else:
            print("::error::perf budget: %s" % breach)
    for name in expected_missing:
        if args.warn_only:
            print(
                "::warning::perf gate integrity: %s is in expected_gated.txt "
                "but was not compared this run (dead gate)" % name
            )
        else:
            print(
                "::error::perf gate integrity: %s is in expected_gated.txt "
                "but was not compared this run (dead gate)" % name
            )

    # --accept excuses relative regressions only.  A budget breach or a dead
    # gate is not a perf trade-off a commit subject can acknowledge; each
    # takes its own reviewed edit (budgets.json / expected_gated.txt).
    failed = bool(violations and not args.accept)
    failed = failed or bool(budget_breaches) or bool(expected_missing)
    if failed and not args.warn_only:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
