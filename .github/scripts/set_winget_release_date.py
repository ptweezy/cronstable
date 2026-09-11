"""Set the WinGet release date from GitHub's publication timestamp."""

import sys
from datetime import datetime, timezone
from pathlib import Path

from strictyaml.ruamel import YAML


def set_release_date(version, published_at, manifest_path):
    published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    if published.tzinfo is None:
        raise ValueError("Publication timestamp must include a timezone")
    release_date = published.astimezone(timezone.utc).date()
    path = Path(manifest_path)
    yaml = YAML()
    manifest = yaml.load(path.read_text("utf-8"))
    if (
        manifest["PackageIdentifier"] != "ptweezy.cronstable"
        or manifest["PackageVersion"] != version
        or manifest["ManifestType"] != "installer"
    ):
        raise ValueError("Installer manifest does not match the release")
    manifest["ReleaseDate"] = release_date
    with path.open("w", encoding="utf-8", newline="\n") as output:
        yaml.dump(manifest, output)


if __name__ == "__main__":
    set_release_date(*sys.argv[1:])
