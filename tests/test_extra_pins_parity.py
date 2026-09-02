"""Optional-extra version floors pinned in CI/Docker match pyproject.

The runtime extras (speedups, push, discovery, kubernetes) have one
canonical floor each, declared in ``[project.optional-dependencies]``.  The
release workflow's binary lanes, the Dockerfiles and the pyinstaller helper
scripts install those packages by hand, spelling the floor out again at
every site.  Nothing tied the copies together: bump the floor in pyproject
and every lane keeps building against the old one, silently, until an API
mismatch surfaces in a shipped artifact.  This walks every hand-spelled
``pkg>=floor`` pin in those files and demands it equal pyproject's.
"""

import ast
import glob
import os
import re

import pytest

tomllib = pytest.importorskip("tomllib")  # py3.11+; the other cells enforce
requirements = pytest.importorskip("packaging.requirements")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: the files that hand-install optional extras outside pip's resolver
_SCANNED = [
    ".github/workflows/release.yml",
    "Dockerfile",
    "pyinstaller/install_orjson.sh",
] + sorted(
    os.path.relpath(p, ROOT).replace(os.sep, "/")
    for p in glob.glob(os.path.join(ROOT, "docker", "Dockerfile.*"))
)

_PIN = re.compile(r"\b([A-Za-z][A-Za-z0-9_.\-]*)>=([0-9][A-Za-z0-9.\-]*)")


def _canonical_floors():
    """{package: ">=floor"} from every runtime extra (dev has its own test).

    Only the ``>=`` clause counts: a package can appear on more than one
    marker-split line with the same floor and a cap on one of them
    (cryptography's capped Intel macOS and 32-bit Windows line), and every
    line must agree on the floor for the pin scan below to mean anything.
    """
    with open(os.path.join(ROOT, "pyproject.toml"), "rb") as f:
        pyproject = tomllib.load(f)
    floors = {}
    for extra, lines in pyproject["project"]["optional-dependencies"].items():
        if extra == "dev":
            continue
        for line in lines:
            req = requirements.Requirement(line)
            name = req.name.lower()
            floor = [
                ">=" + spec.version
                for spec in req.specifier
                if spec.operator == ">="
            ]
            assert len(floor) == 1, "%s has no single >= floor" % (line,)
            assert floors.get(name, floor[0]) == floor[0], (
                "%s appears with two different floors in pyproject" % (name,)
            )
            floors[name] = floor[0]
    return floors


def test_hand_spelled_extra_pins_match_pyproject_floors():
    floors = _canonical_floors()
    found = set()
    mismatches = []
    for rel in _SCANNED:
        with open(
            os.path.join(ROOT, rel.replace("/", os.sep)), encoding="utf-8"
        ) as f:
            text = f.read()
        for match in _PIN.finditer(text):
            name = match.group(1).lower()
            if name not in floors:
                continue
            found.add(name)
            spelled = ">=" + match.group(2)
            if spelled != floors[name]:
                mismatches.append(
                    "%s pins %s%s but pyproject's extra floor is %s%s"
                    % (rel, name, spelled, name, floors[name])
                )
    # self-check: the five extras the lanes actually bundle must be visible
    # to the scan, or the pin idiom changed and the regex needs updating
    assert {
        "uvloop",
        "pynacl",
        "zeroconf",
        "orjson",
        "cryptography",
    } <= found, (
        "the extra-pin scan no longer sees the pins it was built on "
        "(found only %r); update _PIN or _SCANNED" % (sorted(found),)
    )
    assert not mismatches, (
        "hand-spelled optional-extra floors have drifted from pyproject "
        "(bump them together; pyproject is canonical):\n"
        + "\n".join(mismatches)
    )


def test_docker_inline_orjson_probe_matches_verify_extra():
    """The image builds' inline orjson probe round-trips verify_extra's sample.

    docker/install_orjson.sh cannot delegate to pyinstaller/verify_extra.py
    (the builder layers never COPY it), so it spells the probe inline: the
    same sample dict, round-tripped through OPT_SORT_KEYS. If _verify_orjson
    grows a case (say a new orjson option cronstable._json starts using) and
    the inline probe keeps the old sample, the container lanes silently
    verify a weaker contract than the binary lanes, and a QEMU-miscompiled
    orjson failing only the new case ships in the images. Decode both
    samples and demand equality.
    """
    with open(
        os.path.join(ROOT, "docker", "install_orjson.sh"), encoding="utf-8"
    ) as f:
        script = f.read()
    probe = re.search(r"python -c '([^']*)'", script)
    assert probe, "inline python -c probe not found in docker/install_orjson.sh"
    # the probe rides through docker build argv as-is; it stays ASCII on
    # purpose (its own comment) so no build-stage locale can mangle it
    assert probe.group(1).isascii(), "the inline probe is no longer ASCII"
    sample = re.search(r"s=(\{.*?\});", probe.group(1))
    assert sample, "sample dict not found in the inline probe"
    docker_sample = ast.literal_eval(sample.group(1))
    assert "orjson.OPT_SORT_KEYS" in probe.group(1), (
        "the inline probe no longer exercises the OPT_SORT_KEYS path "
        "cronstable._json depends on"
    )

    with open(
        os.path.join(ROOT, "pyinstaller", "verify_extra.py"), encoding="utf-8"
    ) as f:
        verify_tree = ast.parse(f.read())
    canonical = None
    for node in ast.walk(verify_tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_verify_orjson":
            for stmt in node.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and isinstance(stmt.targets[0], ast.Name)
                    and stmt.targets[0].id == "sample"
                ):
                    canonical = ast.literal_eval(stmt.value)
    assert canonical is not None, (
        "_verify_orjson's sample assignment not found in verify_extra.py; "
        "update this scan alongside it"
    )
    assert docker_sample == canonical, (
        "docker/install_orjson.sh's inline probe sample %r has drifted from "
        "verify_extra.py's _verify_orjson sample %r (keep the two probes in "
        "step; the container lanes must verify the same contract as the "
        "binary lanes)" % (docker_sample, canonical)
    )
