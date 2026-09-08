"""Expand each distro into its default multi-arch image and explicit v3 tag.

Both gate and publish consume this output. v3 deliberately has its own tag:
OCI's amd64 platform alone does not ensure the pulling CPU supports v3.
"""

import json
import os
from pathlib import Path


def expand(distros):
    rows = []
    for distro in distros:
        rows.append(dict(distro, python_variant="baseline"))
        if "linux/amd64" in distro["platforms"].split(","):
            rows.append(
                dict(
                    distro,
                    distro=distro["distro"] + "-amd64v3",
                    suffix=distro["suffix"] + "-amd64v3",
                    platforms="linux/amd64",
                    python_variant="amd64v3",
                )
            )
    return rows


def platforms(distros):
    """One independently scheduled build per platform and CPU variant."""
    rows = []
    foreign = {
        "linux/386": "i686",
        "linux/arm/v7": "armv7",
        "linux/ppc64le": "ppc64le",
        "linux/s390x": "s390x",
        "linux/riscv64": "riscv64",
    }
    for distro in expand(distros):
        libc = "musl" if distro["dockerfile"].endswith(".alpine") else "glibc"
        for platform in distro["platforms"].split(","):
            arch = foreign.get(platform)
            wheel = bool(
                arch
                and (libc == "musl" or arch in {"i686", "s390x", "riscv64"})
            )
            rows.append(
                dict(
                    distro,
                    platforms=platform,
                    platform_id=platform.replace("/", "-"),
                    runner="ubuntu-24.04-arm"
                    if platform == "linux/arm64"
                    else "ubuntu-24.04",
                    qemu=platform
                    not in {"linux/amd64", "linux/arm64", "linux/386"},
                    wheel_group=libc if wheel else "none",
                    wheel=f"pq-wheel-{libc}-{arch}" if wheel else "",
                )
            )
    return rows


if __name__ == "__main__":
    source = Path(__file__).resolve().parents[1] / "docker-matrix.json"
    distros = json.loads(source.read_text(encoding="utf-8"))
    print(json.dumps(expand(distros)))
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            for group in ("none", "glibc", "musl"):
                rows = [
                    r for r in platforms(distros) if r["wheel_group"] == group
                ]
                output.write(f"docker-{group}={json.dumps(rows)}\n")
