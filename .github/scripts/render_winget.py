"""Render MSI manifests from metadata read from the published installers.

The renderer sets the installer type and scope independently of the manifest
in winget-pkgs. Product codes and hashes come from the release MSIs.
"""

import json
import re
import sys
from pathlib import Path

from strictyaml.ruamel import YAML

PACKAGE = "ptweezy.cronstable"
REPO = "https://github.com/ptweezy/cronstable"
ARCHES = {"amd64": "x64", "arm64": "arm64"}


def render(version, metadata, output):
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError(f"Invalid release version: {version!r}")
    if set(metadata) != set(ARCHES):
        raise ValueError("Expected metadata for amd64 and arm64")
    installers = []
    for arch, architecture in ARCHES.items():
        item = metadata[arch]
        if item["ProductVersion"] != version:
            raise ValueError(f"{arch}: MSI version differs from release")
        if item["Architecture"] != architecture:
            raise ValueError(f"{arch}: MSI architecture mismatch")
        if not re.fullmatch(r"[A-Fa-f0-9]{64}", item["Sha256"]):
            raise ValueError(f"{arch}: Invalid SHA256")
        for field in ("ProductCode", "UpgradeCode"):
            if not re.fullmatch(
                r"\{[A-Fa-f0-9]{8}(?:-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12}\}",
                item[field],
            ):
                raise ValueError(f"{arch}: Invalid {field}")
        installers.append(
            {
                "Architecture": architecture,
                "InstallerUrl": (
                    f"{REPO}/releases/download/{version}/"
                    f"cronstable-windows-{arch}.msi"
                ),
                "InstallerSha256": item["Sha256"].upper(),
                "ProductCode": item["ProductCode"],
                "AppsAndFeaturesEntries": [
                    {
                        "DisplayName": item["ProductName"],
                        "Publisher": item["Manufacturer"],
                        "DisplayVersion": item["ProductVersion"],
                        "ProductCode": item["ProductCode"],
                        "UpgradeCode": item["UpgradeCode"],
                        "InstallerType": "msi",
                    }
                ],
            }
        )
    common = {"PackageIdentifier": PACKAGE, "PackageVersion": version}
    manifests = {
        "installer": {
            "InstallerType": "wix",
            "Scope": "machine",
            "ElevationRequirement": "elevationRequired",
            "UpgradeBehavior": "install",
            "Commands": ["cronstable"],
            "Installers": installers,
            "ManifestType": "installer",
        },
        "locale.en-US": {
            "PackageLocale": "en-US",
            "Publisher": "ptweezy",
            "PublisherUrl": "https://github.com/ptweezy",
            "PublisherSupportUrl": f"{REPO}/issues",
            "PackageName": "cronstable",
            "PackageUrl": REPO,
            "License": "MIT",
            "LicenseUrl": f"{REPO}/blob/{version}/LICENSE",
            "ShortDescription": "A distributed cron replacement.",
            "Moniker": "cronstable",
            "Tags": ["cron", "crontab", "scheduler", "job-scheduler"],
            "ReleaseNotesUrl": f"{REPO}/releases/tag/{version}",
            "Documentations": [
                {
                    "DocumentLabel": "Wiki",
                    "DocumentUrl": f"{REPO}/wiki",
                }
            ],
            "ManifestType": "defaultLocale",
        },
        "": {"DefaultLocale": "en-US", "ManifestType": "version"},
    }
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    yaml = YAML()
    for suffix, body in manifests.items():
        filename = f"{PACKAGE}{'.' + suffix if suffix else ''}.yaml"
        with (output / filename).open(
            "w", encoding="utf-8", newline="\n"
        ) as f:
            yaml.dump({**common, **body, "ManifestVersion": "1.12.0"}, f)


if __name__ == "__main__":
    version, metadata_path, output = sys.argv[1:]
    render(
        version, json.loads(Path(metadata_path).read_text("utf-8-sig")), output
    )
