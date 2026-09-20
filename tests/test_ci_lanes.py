"""Check that CI jobs reference the expected files and configuration.

Workflow YAML and tox.ini maintain explicit lists of property tests,
mutation targets, and live servers. Verify these lists against the working
tree so missing tests, unconfigured mutation targets, or skipped server
checks cannot go unnoticed.
"""

import configparser
import os
import re

import pytest
from strictyaml.ruamel import YAML

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_workflow(name):
    path = os.path.join(ROOT, ".github", "workflows", name)
    with open(path, encoding="utf-8") as fobj:
        return YAML(typ="safe").load(fobj.read())


def _tox():
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(os.path.join(ROOT, "tox.ini"), encoding="utf-8")
    return parser


def _pyproject():
    tomllib = pytest.importorskip("tomllib")  # Python 3.11+
    with open(os.path.join(ROOT, "pyproject.toml"), "rb") as fobj:
        return tomllib.load(fobj)


def test_the_deep_env_searches_every_property_file():
    on_disk = sorted(
        "tests/" + name
        for name in os.listdir(os.path.join(ROOT, "tests"))
        if name.startswith("test_properties_") and name.endswith(".py")
    )
    assert on_disk, "the property-file detector went stale"
    thorough = _tox()["testenv:deep"]["commands"].strip().splitlines()[0]
    assert sorted(re.findall(r"tests/test_properties_\w+\.py", thorough)) == (
        on_disk
    )
    # the thorough pass is the one that lifts the per-test timeout, and the
    # profile it selects is one tests/conftest.py registers
    assert "--timeout=0" in thorough
    setenv = _tox()["testenv:deep"]["setenv"]
    assert "CRONSTABLE_HYPOTHESIS_PROFILE = thorough" in setenv
    with open(os.path.join(ROOT, "tests", "conftest.py")) as fobj:
        assert '"thorough"' in fobj.read()


def test_the_deep_env_shuffles_with_randomly_switched_on():
    commands = _tox()["testenv:deep"]["commands"].strip().splitlines()
    shuffled = [line for line in commands[1:] if "-p randomly" in line]
    assert len(shuffled) >= 3
    assert all("no:randomly" not in line for line in shuffled)


def test_every_mutation_cell_is_a_module_mutmut_mutates():
    sources = _pyproject()["tool"]["mutmut"]["source_paths"]
    configured = sorted(
        os.path.splitext(os.path.basename(path))[0] for path in sources
    )
    for path in sources:
        assert os.path.isfile(os.path.join(ROOT, path)), path
    nightly = _load_workflow("nightly.yml")
    cells = sorted(nightly["jobs"]["mutation"]["strategy"]["matrix"]["module"])
    assert cells == configured


def test_every_mutation_test_selection_exists():
    config = _pyproject()["tool"]["mutmut"]
    for path in config["pytest_add_cli_args_test_selection"]:
        assert os.path.isfile(os.path.join(ROOT, path)), path


def test_the_nightly_runs_on_a_schedule_and_never_on_push():
    # PyYAML-family loaders read the bare key `on` as boolean True
    nightly = _load_workflow("nightly.yml")
    triggers = nightly.get("on", nightly.get(True))
    assert set(triggers) == {"schedule", "workflow_dispatch"}
    assert set(nightly["jobs"]) == {"deep", "devmode", "mutation"}
    assert "tox -e deep" in str(nightly["jobs"]["deep"]["steps"])


def test_the_live_backend_lane_cannot_pass_hollow():
    job = _load_workflow("release.yml")["jobs"]["backends-live"]
    run = next(
        step
        for step in job["steps"]
        if step.get("name") == "Run the live backend tests"
    )
    env = run["env"]
    assert env["CRONSTABLE_LIVE_REQUIRED"] == "1"
    # every server the live file knows how to reach is provisioned
    with open(os.path.join(ROOT, "tests", "test_backend_live.py")) as fobj:
        source = fobj.read()
    switches = set(
        re.findall(r"CRONSTABLE_LIVE_(?:ETCD_AUTH|ETCD|K8S)\b", source)
    )
    assert switches == {
        "CRONSTABLE_LIVE_ETCD",
        "CRONSTABLE_LIVE_ETCD_AUTH",
        "CRONSTABLE_LIVE_K8S",
    }
    assert switches <= set(env)
    assert "tests/test_backend_live.py" in run["run"]
    assert 'totals["skipped"] == 0' in run["run"]


def test_the_mindeps_lane_installs_the_generated_floor_pins():
    env = _tox()["testenv:mindeps"]
    assert "-rrequirements_min.txt" in env["deps"]
    assert "-rrequirements_dev.txt" in env["deps"]
    job = _load_workflow("release.yml")["jobs"]["tox-mindeps"]
    assert "tox -e mindeps" in str(job["steps"])
    # the oldest supported Python, where every pinned floor has a wheel
    project = _pyproject()["project"]
    oldest = re.search(r">=\s*([\d.]+)", project["requires-python"])[1]
    setup = next(
        step for step in job["steps"] if "setup-python" in step.get("uses", "")
    )
    assert setup["with"]["python-version"] == oldest


def test_minimum_pins_cover_every_runtime_dependency():
    project = _pyproject()["project"]
    with open(os.path.join(ROOT, "requirements_min.txt")) as fobj:
        pins = {
            line.split("==")[0]
            for line in fobj.read().splitlines()
            if "==" in line
        }
    for line in project["dependencies"]:
        name = re.match(r"[\w.-]+", line)[0].lower()
        assert name in pins, (
            "{} declares no `>=` floor, so the mindeps lane cannot prove "
            "one".format(name)
        )


def test_the_unit_matrix_covers_every_shipped_desktop_os():
    matrix = _load_workflow("release.yml")["jobs"]["tox"]["strategy"]["matrix"]
    assert {"ubuntu-latest", "windows-latest", "macos-latest"} <= set(
        matrix["os"]
    )
    included = {row["os"] for row in matrix["include"]}
    assert {"windows-11-arm", "ubuntu-24.04-arm"} <= included

