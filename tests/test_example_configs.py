"""Every example config in example/ still parses with the real parser.

The 41 YAML files under ``example/`` are the documentation people copy from,
and they ride along in the sdist (MANIFEST.in keeps ``recursive-include
example *``), yet nothing validated them: a schema key renamed in
cronstable/config.py left the examples teaching the old spelling, and the
first report came from whoever pasted one into a live config.  Every file
that is a cronstable config is now parsed through :func:`parse_config`, the
same entry point ``cronstable -c`` uses, cross-section validation included.

Not every YAML file here is a cronstable config, so the walk classifies by
RULE rather than by a hand-kept skip list (a list of names goes stale the
moment someone adds an example, which is the one moment this test exists
for):

* ``docker-compose.yml`` / ``docker-compose.yaml`` is Compose's file name,
  fixed by Compose itself, and every example directory that ships a stack
  uses it.  Compose's own schema, and its anchors and flow sequences, are
  not cronstable's.
* a top-level ``apiVersion:`` line marks a Kubernetes manifest (the two
  deployment examples).  ``apiVersion`` is not a cronstable config key and
  never can be without a schema change, so this cannot swallow a real
  config, and it also catches the multi-document manifests that strictyaml
  refuses on principle.

Anything else must parse.  Add ``example/whatever/cronstable.yaml`` with a
typo in it and this test fails; add a new kind of non-cronstable YAML and
the fix is another rule here rather than a skip list.

Two examples are container-shaped: their jobs name an ``env_file`` at
``/config/*.env``, the path the Compose file bind-mounts the example
directory to.  That path does not exist in a checkout, so the parse would
die on the mount layout rather than on anything about the config.  The
fixture below resolves those container paths back to the example directory,
which is exactly what the bind mount does at runtime, and leaves every other
part of the parse alone.
"""

import os

import pytest

from cronstable import config as config_module
from cronstable.config import CronstableConfig, parse_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLE_ROOT = os.path.join(ROOT, "example")

#: where the example Compose files bind-mount the example directory
CONTAINER_CONFIG_DIR = "/config/"

#: floors, not exact counts: the corpus grows, but a walk that silently
#: stops finding files (a renamed directory, a glob that no longer matches)
#: would otherwise turn this whole module into zero tests that all pass.
#: (lowered when example/acme-platform and example/adhoc.cronstable.d were
#: retired; acme's unique jobs live on in grand-tour)
_MIN_CONFIGS = 20
_MIN_COMPOSE = 7
_MIN_JOBS = 80


def _yaml_files():
    out = []
    for dirpath, dirnames, filenames in os.walk(EXAMPLE_ROOT):
        dirnames[:] = sorted(dirnames)
        for filename in sorted(filenames):
            if filename.endswith((".yaml", ".yml")):
                out.append(os.path.join(dirpath, filename))
    return out


def _kind(path):
    """One of "compose", "kubernetes", "cronstable" (see the docstring)."""
    if os.path.basename(path) in ("docker-compose.yml", "docker-compose.yaml"):
        return "compose"
    with open(path, encoding="utf-8") as f:
        for line in f:
            # top level only: a nested apiVersion (a job that pipes a manifest
            # into kubectl, say) is indented and stays a cronstable config
            if line.startswith("apiVersion:"):
                return "kubernetes"
    return "cronstable"


def _by_kind():
    grouped = {"compose": [], "kubernetes": [], "cronstable": []}
    for path in _yaml_files():
        grouped[_kind(path)].append(path)
    return grouped


_GROUPED = _by_kind()


def _ident(path):
    return os.path.relpath(path, EXAMPLE_ROOT).replace(os.sep, "/")


@pytest.fixture
def container_env_files(monkeypatch):
    """Resolve ``/config/...`` env_file paths back to the example directory.

    ``parse_environment_file`` is looked up as a module global at its one
    call site, so replacing it here covers every job and DAG task of the
    file under test.  Only the container prefix is rewritten; a path that
    does not start with it is passed through untouched, so a genuinely
    missing env_file still fails the test the way it should.
    """
    real = config_module.parse_environment_file
    state = {"directory": os.getcwd()}

    def resolve(path):
        if path.startswith(CONTAINER_CONFIG_DIR):
            path = path[len(CONTAINER_CONFIG_DIR) :]
        if not os.path.isabs(path):
            path = os.path.join(state["directory"], path)
        return real(path)

    monkeypatch.setattr(config_module, "parse_environment_file", resolve)
    return state


@pytest.mark.parametrize(
    "path",
    _GROUPED["cronstable"],
    ids=[_ident(p) for p in _GROUPED["cronstable"]],
)
def test_example_config_parses(path, container_env_files):
    container_env_files["directory"] = os.path.dirname(path)
    parsed = parse_config(path)
    assert isinstance(parsed, CronstableConfig)


def test_the_example_corpus_is_still_being_found():
    assert len(_GROUPED["cronstable"]) >= _MIN_CONFIGS, _GROUPED["cronstable"]
    assert len(_GROUPED["compose"]) >= _MIN_COMPOSE, _GROUPED["compose"]


def test_the_examples_actually_define_jobs(container_env_files):
    # Parsing is only half the claim. An example that defines no jobs
    # documents nothing, and a parser regression that quietly dropped every
    # job would still leave every parse above "valid".
    total = 0
    for path in _GROUPED["cronstable"]:
        container_env_files["directory"] = os.path.dirname(path)
        total += len(parse_config(path).jobs)
    assert total >= _MIN_JOBS, total
