"""Expand each distro into its default multi-arch image and explicit v3 tag.

Both gate and publish consume this output. v3 deliberately has its own tag:
OCI's amd64 platform alone does not ensure the pulling CPU supports v3.
"""

import json
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


if __name__ == "__main__":
    source = Path(__file__).resolve().parents[1] / "docker-matrix.json"
    print(json.dumps(expand(json.loads(source.read_text(encoding="utf-8")))))
