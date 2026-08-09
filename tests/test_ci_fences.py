"""release.yml keeps its silent-skip fences pointed at live matrix cells.

The web engine differential (tests/test_web_engine_parity.py) self-skips
wherever Chromium is missing, so exactly one tox cell installs the browser
and then re-drives the file, requiring that nothing skipped.  Both steps
gate on hardcoded matrix literals in their ``if:``; if the tox matrix moves
on, those literals go stale, GitHub skips the steps without a word, and the
differential quietly runs nowhere again.  This pins the literals to the
matrix they select from, and pins the enforcement to junitxml counts
instead of a stdout grep.
"""

import os
import re

# strictyaml's VENDORED ruamel (the same import cronstable.backends.kubernetes
# uses): guaranteed present wherever cronstable is, unlike the standalone
# ruamel.yaml distribution, which is NOT a dependency.
from strictyaml.ruamel import YAML

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "release.yml")
INSTALL_STEP = "Install Chromium for the web engine differential"
ENFORCE_STEP = "Enforce the web engine differential ran (no silent skip)"


def _workflow():
    with open(WORKFLOW, encoding="utf-8") as fobj:
        return YAML(typ="safe").load(fobj.read())


def _tox_job():
    return _workflow()["jobs"]["tox"]


def _named_steps(job):
    return {step["name"]: step for step in job["steps"] if "name" in step}


def test_parity_fence_steps_gate_on_a_live_matrix_cell():
    job = _tox_job()
    steps = _named_steps(job)
    assert INSTALL_STEP in steps, "the Chromium install step is gone"
    assert ENFORCE_STEP in steps, "the enforcement step is gone"
    install = steps[INSTALL_STEP]
    enforce = steps[ENFORCE_STEP]
    # one cell, one pair: if the two conditions diverge, one of the steps
    # silently stops running.
    assert install["if"] == enforce["if"]
    os_match = re.search(r"matrix\.os == '([^']+)'", install["if"])
    py_match = re.search(r"matrix\.python == '([^']+)'", install["if"])
    assert os_match and py_match, install["if"]
    matrix = job["strategy"]["matrix"]
    cells = {(str(o), str(p)) for o in matrix["os"] for p in matrix["python"]}
    cells |= {
        (str(row["os"]), str(row["python"]))
        for row in matrix.get("include", [])
    }
    assert (os_match.group(1), py_match.group(1)) in cells, (
        "the parity fence gates on a matrix cell that no longer exists; "
        "update the two `if:` literals in release.yml"
    )


def test_parity_enforcement_reads_junitxml_counts():
    run = _named_steps(_tox_job())[ENFORCE_STEP]["run"]
    assert "--junitxml" in run
    assert "skipped" in run
    # the old fence grepped stdout for the literal "1 passed", which a
    # second test in the file flips to a false red and which "11 passed"
    # also satisfies.
    assert '"1 passed"' not in run


# --------------------------------------------------------------------------
# Actions cache writers: one 10 GB pool per repository
# --------------------------------------------------------------------------


def _cache_writers():
    """Every step in the workflow that can WRITE to the Actions cache.

    Three shapes: an explicit ``actions/cache/save``, a bare
    ``actions/cache@`` (restore plus an implicit post-job save), and a
    buildx ``cache-to`` naming the ``type=gha`` backend.  Yields
    ``(job_name, label, gate)`` where ``gate`` is the step's ``if:``
    (``""`` when it has none).
    """
    out = []
    for job_name, job in _workflow()["jobs"].items():
        for step in job.get("steps", []):
            uses = str(step.get("uses", ""))
            with_block = step.get("with") or {}
            cache_to = str(with_block.get("cache-to", ""))
            writes = (
                "actions/cache/save" in uses
                or re.match(r"actions/cache@", uses) is not None
                or "type=gha" in cache_to
            )
            if writes:
                label = step.get("name") or uses
                gate = str(step.get("if", "")) + cache_to
                out.append((job_name, label, gate))
    return out


def test_every_actions_cache_write_is_branch_gated():
    # The Actions cache is ONE quota per repository (10 GB), shared by the
    # buildx layer exporter and every other writer, and it evicts
    # least-recently-used across the whole pool. binaries-container alone
    # is 13 matrix rows. A writer that runs on every branch and every fork
    # PR evicts the docker layer cache the build depends on, which is how
    # 1.2.31 broke: the release build hit a `not_found` on an evicted
    # blob. Restores stay ungated on purpose; every branch reads through
    # to what develop and main stored.
    writers = _cache_writers()
    assert writers, "no cache writers found: the detector went stale"
    ungated = [
        (job, label)
        for job, label, gate in writers
        if not ("refs/heads/develop" in gate and "refs/heads/main" in gate)
    ]
    assert not ungated, (
        "these Actions cache writers are not gated to develop/main, so "
        "they write from every branch and fork PR against a shared 10 GB "
        "quota: {}".format(ungated)
    )


def test_every_browser_backed_test_module_is_fenced():
    # The enforcement step is the ONLY thing standing between a
    # browser-backed module and running nowhere: these all self-skip
    # without Chromium, which is every environment but the one cell that
    # provisions it. Listing the files by hand let the heat E2E ship
    # outside the fence, so derive the set from disk instead: any module
    # that importorskips playwright must be named in the step.
    tests_dir = os.path.join(ROOT, "tests")
    browser_backed = set()
    myself = os.path.basename(__file__)  # this file names the probe string
    for name in os.listdir(tests_dir):
        if not name.startswith("test_") or not name.endswith(".py"):
            continue
        if name == myself:
            continue
        with open(os.path.join(tests_dir, name), encoding="utf-8") as fobj:
            if 'importorskip("playwright' in fobj.read():
                browser_backed.add(name)
    assert browser_backed, "the playwright detector went stale"
    run = _named_steps(_tox_job())[ENFORCE_STEP]["run"]
    missing = sorted(n for n in browser_backed if n not in run)
    assert not missing, (
        "these browser-backed modules self-skip everywhere and are not "
        "re-driven by the enforcement step, so nothing proves they ever "
        "ran: {}".format(missing)
    )
