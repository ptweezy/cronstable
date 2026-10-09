"""Check that CI jobs reference the expected files and configuration.

Workflow YAML and tox.ini maintain explicit lists of property tests,
mutation targets, and live servers. Verify these lists against the working
tree so missing tests, unconfigured mutation targets, or skipped server
checks cannot go unnoticed.
"""

import configparser
import itertools
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


def _expand(pattern):
    """Expand a tox generative name such as ``py3{14,15}t{,-posix}``."""
    parts = re.split(r"\{([^}]*)\}", pattern)
    choices = [
        part.split(",") if index % 2 else [part]
        for index, part in enumerate(parts)
    ]
    return {"".join(combo) for combo in itertools.product(*choices)}


def test_the_test_envs_install_the_dev_extra():
    # configparser reads each section alone, so `[testenv]` here is the
    # base that every env without its own `extras` inherits.
    tox = _tox()
    assert tox["testenv"]["extras"].split() == ["dev"]
    # orjson has no free-threaded build, so the free-threaded envs swap the
    # extra for the generated list without it. Their section sets both
    # keys: dropping `extras` installs orjson there, and dropping `deps`
    # leaves the envs with no test deps.
    (section,) = [
        name for name in tox.sections() if re.match(r"testenv:py3.*t\{", name)
    ]
    assert tox[section]["extras"].strip() == ""
    assert tox[section]["deps"].split() == [
        "-rrequirements/dev-freethreaded.txt"
    ]
    assert os.path.exists(
        os.path.join(ROOT, "requirements", "dev-freethreaded.txt")
    )
    # mypy installs the package to resolve the runtime deps and nothing
    # else; inheriting the extra would type-check against the dev tools.
    assert tox["testenv:mypy"]["extras"].strip() == ""


def test_every_free_threaded_ci_cell_has_a_tox_env_without_orjson():
    # The shared tox steps derive the env from the matrix version (3.15t
    # runs py315t-posix). A version with no env in the free-threaded
    # section would fall back to [testenv], whose dev extra includes orjson.
    (section,) = [
        name
        for name in _tox().sections()
        if re.match(r"testenv:py3.*t\{", name)
    ]
    envs = _expand(section.partition(":")[2])
    jobs = _load_workflow("release.yml")["jobs"]
    cells = [
        version
        for job in ("tox", "tox-experimental")
        for version in jobs[job]["strategy"]["matrix"]["python"]
        if version.endswith("t")
    ]
    assert cells, "no free-threaded cell is left in the test matrix"
    for version in cells:
        assert "py{}-posix".format(version.replace(".", "")) in envs, version
    run = next(
        step["run"]
        for step in jobs["tox"]["steps"]
        if step.get("name", "").startswith("Test free-threaded Python")
    )
    assert run == 'tox -e "py${PYTHON_VERSION//./}-posix"'


def test_the_unit_matrix_covers_every_python_the_metadata_declares():
    # A classifier is a support claim, so each declared version needs a
    # gating row on every desktop OS and a place in a bare `tox`.
    declared = {
        classifier.rpartition(" ")[2]
        for classifier in _pyproject()["project"]["classifiers"]
        if re.fullmatch(
            r"Programming Language :: Python :: 3\.\d+", classifier
        )
    }
    assert declared, "the classifier detector went stale"
    matrix = _load_workflow("release.yml")["jobs"]["tox"]["strategy"]["matrix"]
    assert set(matrix["python"]) == declared
    # split the envlist on the commas outside its braces
    in_tox = set()
    for entry in re.split(r",(?![^{]*\})", _tox()["tox"]["envlist"]):
        in_tox |= _expand(entry.strip())
    for version in declared:
        for arm in ("windows", "posix"):
            env = "py{}-{}".format(version.replace(".", ""), arm)
            assert env in in_tox, env
    oldest = re.search(
        r">=\s*([\d.]+)", _pyproject()["project"]["requires-python"]
    )[1]
    assert oldest == min(
        declared, key=lambda version: tuple(map(int, version.split(".")))
    )


def test_the_chromium_steps_install_the_playwright_the_dev_extra_names():
    # The workflows install Chromium through a host `pip install
    # playwright`, which matches the tox env's playwright only while the
    # dev extra leaves it unpinned. A pin there belongs in these steps too.
    dev = _pyproject()["project"]["optional-dependencies"]["dev"]
    (line,) = [dep for dep in dev if dep.startswith("playwright")]
    assert line.partition(";")[0].strip() == "playwright"
    for name in ("release.yml", "nightly.yml"):
        path = os.path.join(ROOT, ".github", "workflows", name)
        with open(path, encoding="utf-8") as fobj:
            assert "pip install playwright\n" in fobj.read(), name


def test_the_mindeps_lane_installs_the_generated_floor_pins():
    env = _tox()["testenv:mindeps"]
    assert env["deps"].split() == ["-rrequirements/min.txt"]
    # as constraints, the pins also bind the resolve of the dev extra
    assert env["constraints"].split() == ["requirements/min.txt"]
    assert "extras" not in env
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
    with open(os.path.join(ROOT, "requirements", "min.txt")) as fobj:
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
    assert {"ubuntu-26.04", "windows-latest", "macos-latest"} <= set(
        matrix["os"]
    )
    included = {row["os"] for row in matrix["include"]}
    assert {"windows-11-arm", "ubuntu-26.04-arm"} <= included
