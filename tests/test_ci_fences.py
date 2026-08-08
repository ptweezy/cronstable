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


def _tox_job():
    with open(WORKFLOW, encoding="utf-8") as fobj:
        return YAML(typ="safe").load(fobj.read())["jobs"]["tox"]


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
