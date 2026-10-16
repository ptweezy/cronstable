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
import sys
import types
import xml.etree.ElementTree as ET

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


def _cgroup_lane_step(name):
    """The ``run`` script of the cgroups-live step called ``name``."""
    job = _load_workflow("release.yml")["jobs"]["cgroups-live"]
    assert job["runs-on"].startswith("ubuntu-")
    return next(
        step["run"] for step in job["steps"] if step.get("name") == name
    )


def _live_readers():
    """Every test file that names the CRONSTABLE_LIMITS_LIVE switch."""
    tests = os.path.join(ROOT, "tests")
    myself = os.path.basename(__file__)  # this file names the switch
    readers = []
    for name in sorted(os.listdir(tests)):
        if not name.endswith(".py") or name == myself:
            continue
        with open(os.path.join(tests, name), encoding="utf-8") as fobj:
            source = fobj.read()
        if re.search(r"[\"']CRONSTABLE_LIMITS_LIVE[\"']", source):
            readers.append("tests/" + name)
    assert len(readers) >= 2, "the live-switch detector went stale"
    return readers


def test_the_live_cgroup_lane_cannot_pass_hollow():
    run = _cgroup_lane_step(
        "Run the live resource limit tests in a delegated cgroup"
    )
    # the tests run as a transient service in a cgroup delegated to the
    # runner user, and systemd-run hands that service none of the step's
    # environment, so both switches ride on its command line
    command = " ".join(run.split("\\\n"))
    assert re.search(r"\bsudo systemd-run\b", command)
    for flag in ("--wait", "--pipe", "-p Delegate=yes", "--uid=", "--gid="):
        assert flag in command, flag
    for switch in ("CRONSTABLE_LIMITS_LIVE", "CRONSTABLE_LIVE_REQUIRED"):
        assert "--setenv={}=1".format(switch) in command, switch
    # every test file that names the switch runs in the lane
    for path in _live_readers():
        assert path in command, path
    assert "--junitxml=" in command
    assert 'totals["skipped"] == 0' in run


def _cpu_skips(tmp_path, monkeypatch):
    """The live files' skip messages where only cpu is not delegated."""
    from cronstable import limits
    from cronstable._cgroups import _CPU_NOT_DELEGATED
    from tests import test_cgroups_live, test_limits_live

    cap = limits.Capability(
        True,
        "cgroup-v2",
        frozenset({"memory", "processes"}),
        _CPU_NOT_DELEGATED,
        None,
    )
    (tmp_path / "cgroup.controllers").write_text("memory pids\n")
    manager = types.SimpleNamespace(
        setup=lambda retry=False: cap,
        backend=types.SimpleNamespace(_base=str(tmp_path)),
    )
    messages = []
    with monkeypatch.context() as patch:
        patch.setattr(sys, "platform", "linux")
        patch.setattr(test_limits_live, "IS_WINDOWS", False)
        for module in (test_cgroups_live, test_limits_live):
            patch.setattr(module, "_LIVE", True)
            patch.setattr(module, "_REQUIRED", False)
            patch.setitem(module._STATE, "manager", manager)
        for needs_cpu in (
            test_cgroups_live._cpu_manager,
            lambda: next(test_limits_live._installed(test_limits_live._ALL)),
        ):
            with pytest.raises(pytest.skip.Exception) as caught:
                needs_cpu()
            messages.append(caught.value.msg)
    return messages


def _junit(path, cases):
    """Write ``cases``, (name, outcome, message) each, as pytest does."""
    counts = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    tags = {"failures": "failure", "errors": "error", "skipped": "skipped"}
    suite = ET.Element("testsuite", name="pytest")
    for name, outcome, message in cases:
        counts["tests"] += 1
        case = ET.SubElement(suite, "testcase", classname="tests", name=name)
        if outcome != "passed":
            counts[outcome] += 1
            tag = ET.SubElement(case, tags[outcome], message=message)
            tag.text = "tests/test_live.py:1: " + message
    for key, value in counts.items():
        suite.set(key, str(value))
    root = ET.Element("testsuites", name="pytest tests")
    root.append(suite)
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def test_the_memory_pids_lane_skips_only_the_tests_that_need_cpu(
    tmp_path, monkeypatch
):
    run = _cgroup_lane_step(
        "Run the live resource limit tests with Delegate=memory pids"
    )
    # the packaged unit's delegation, in a slice that no Delegate=yes
    # sibling shares, so cpu is not offered
    command = " ".join(run.split("\\\n"))
    assert re.search(r"\bsudo systemd-run\b", command)
    for flag in ("--wait", "--pipe", '-p "Delegate=memory pids"', "--uid="):
        assert flag in command, flag
    assert re.search(r"--slice=(?!system\.slice)\S+\.slice\s", command)
    assert "--setenv=CRONSTABLE_LIMITS_LIVE=1" in command
    assert "CRONSTABLE_LIVE_REQUIRED" not in command  # the cpu tests skip
    for path in _live_readers():
        assert path in command, path
    junit = re.search(r"--junitxml=(\S+)", command).group(1)
    checker = compile(
        run.split("<<'EOF'\n", 1)[1].split("\nEOF", 1)[0], "check", "exec"
    )
    skips = _cpu_skips(tmp_path, monkeypatch)
    cpu = [("test_cpu", "skipped", message) for message in skips]
    passed = ("test_memory", "passed", None)
    monkeypatch.chdir(tmp_path)

    def check(*cases):
        _junit(junit, cases)
        exec(checker, {})

    check(passed, *cpu)
    check(passed)
    for hollow in (
        (),
        tuple(cpu),
        (passed, ("test_scope", "skipped", "no user scope: no bus")),
        (passed, ("test_hog", "failures", "the hog lived")),
        (passed, ("test_hog", "errors", "no fixture")),
    ):
        with pytest.raises(AssertionError):
            check(*hollow)


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
