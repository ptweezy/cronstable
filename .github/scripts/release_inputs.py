"""Resolve and download sources once, without running build hooks."""

import hashlib
import json
import os
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path

import tomllib
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version


def fetch(url):
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                return response.read()
        except (urllib.error.URLError, TimeoutError):
            if attempt == 2:
                raise
            time.sleep(2**attempt)


def select_source(data, specifier, python):
    candidates = []
    for number, files in data["releases"].items():
        version = Version(number)
        if (
            version.is_prerelease
            or version.is_devrelease
            or version not in SpecifierSet(specifier)
        ):
            continue
        for item in files:
            if item["packagetype"] != "sdist" or item.get("yanked"):
                continue
            if python not in SpecifierSet(item.get("requires_python") or ""):
                continue
            candidates.append((version, item))
    if not candidates:
        raise ValueError("No compatible source distribution")
    version, item = max(candidates, key=lambda pair: pair[0])
    return str(version), item


def resolve(name, specifier, python, directory):
    data = json.loads(fetch(f"https://pypi.org/pypi/{name}/json"))
    version, item = select_source(data, specifier, python)
    filename = item["filename"]
    if Path(filename).name != filename or not filename.endswith(".tar.gz"):
        raise ValueError("Unexpected source filename")
    content = fetch(item["url"])
    digest = hashlib.sha256(content).hexdigest()
    if digest != item["digests"]["sha256"]:
        raise ValueError(f"Source digest mismatch: {name}")
    (directory / filename).write_bytes(content)
    return {"version": version, "sha256": digest, "filename": filename}


def requirement_for(name, project):
    for lines in project["project"]["optional-dependencies"].values():
        for line in lines:
            req = Requirement(line)
            if req.name.lower() != name:
                continue
            if req.marker is None or req.marker.evaluate(
                {
                    "sys_platform": "linux",
                    "platform_system": "Linux",
                    "os_name": "posix",
                    "platform_machine": "x86_64",
                }
            ):
                return str(req.specifier)
    raise ValueError(f"No Linux requirement for {name} in pyproject")


def main():
    root = Path("release-inputs")
    source = root / "third-party"
    source.mkdir(parents=True, exist_ok=True)
    # All shipped interpreters are at least 3.11 (including the ARMv6 lane).
    project = tomllib.loads(Path("pyproject.toml").read_text())
    zeroconf = resolve(
        "zeroconf", requirement_for("zeroconf", project), "3.11", source
    )
    crypto = resolve(
        "cryptography", requirement_for("cryptography", project), "3.11", root
    )
    shutil.copy("cronstable/licenses/THIRD-PARTY-NOTICES.txt", source)
    (root / "constraints.txt").write_text(f"zeroconf=={zeroconf['version']}\n")
    (root / "resolved.json").write_text(
        json.dumps({"zeroconf": zeroconf, "cryptography": crypto}, indent=2)
        + "\n"
    )
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"zeroconf={zeroconf['version']}\n")
        output.write(f"cryptography={crypto['version']}\n")
        output.write(f"crypto-sha={crypto['sha256']}\n")


if __name__ == "__main__":
    main()
