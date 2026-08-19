"""Render the Scoop manifest for a release from its SHA256SUMS.

    python .github/scripts/render_scoop.py <version> <SHA256SUMS> <out.json>

Scoop is a pull channel: the manifest lives in the ScoopInstaller/Extras bucket
and its Excavator bot re-reads `checkver` and `autoupdate` every four hours,
bumping version and hashes on its own from the SHA256SUMS asset this release
already publishes.  So this file exists to be submitted ONCE; after that it
maintains itself, which is the property worth having here -- the push channels
(Homebrew tap, winget) each broke on their own when a release was withdrawn.

Rendering it from SHA256SUMS rather than checking a copy into the tree keeps it
from going stale: the hashes are always the ones this release actually shipped,
and a Windows architecture added or dropped shows up here without an edit.
"""

import json
import sys

REPO = "https://github.com/ptweezy/cronstable"
# Scoop architecture name -> the asset that serves it.
ARCHES = {
    "64bit": "cronstable-windows-amd64.exe",
    "arm64": "cronstable-windows-arm64.exe",
    "32bit": "cronstable-windows-i686.exe",
}


def read_sums(path):
    sums = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            parts = line.split()
            if len(parts) == 2:
                sums[parts[1]] = parts[0]
    return sums


def main(argv):
    if len(argv) != 4:
        return "usage: render_scoop.py <version> <SHA256SUMS> <out.json>"
    version, sums_path, out_path = argv[1:]
    sums = read_sums(sums_path)

    architecture = {}
    autoupdate = {}
    for arch, asset in ARCHES.items():
        digest = sums.get(asset)
        if digest is None:
            # An architecture this release did not build simply does not appear;
            # a manifest naming an asset that 404s is what breaks an install.
            continue
        # The #/ fragment renames the download, so `bin` finds one stable name
        # whichever architecture installed it.
        architecture[arch] = {
            "url": "{}/releases/download/{}/{}#/cronstable.exe".format(
                REPO, version, asset
            ),
            "hash": digest,
        }
        autoupdate[arch] = {
            "url": "{}/releases/download/$version/{}#/cronstable.exe".format(
                REPO, asset
            )
        }

    if not architecture:
        return "{}: no Windows assets found".format(sums_path)

    manifest = {
        "version": version,
        "description": "Cron daemon with a schedule model you can inspect",
        "homepage": REPO,
        "license": "MIT",
        "architecture": architecture,
        "bin": "cronstable.exe",
        "checkver": {"github": REPO},
        "autoupdate": {
            "architecture": autoupdate,
            # Scoop reads the digest straight out of the published sums file,
            # matching the line that names the asset.
            "hash": {"url": "{}/releases/download/$version/SHA256SUMS".format(REPO)},
        },
    }

    with open(out_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, indent=4)
        handle.write("\n")
    print("{}: {} at {}".format(out_path, ", ".join(sorted(architecture)), version))
    return None


if __name__ == "__main__":
    sys.exit(main(sys.argv))
