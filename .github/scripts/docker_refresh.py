"""Select released source and tags for scheduled container rebuilds."""

import argparse
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from urllib.parse import quote


def api(path):
    result = subprocess.run(
        ["gh", "api", f"repos/{os.environ['GITHUB_REPOSITORY']}/{path}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def release_version(release):
    tag = release["tag_name"]
    if release.get("draft") or release.get("prerelease"):
        raise ValueError("Docker refresh requires a published stable release")
    if not re.fullmatch(
        r"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", tag
    ):
        raise ValueError("Release tag must be X.Y.Z or vX.Y.Z")
    return tag.removeprefix("v")


def plan(release, revision, run_id, attempt, date):
    version = release_version(release)
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("Release revision must be a commit SHA")
    if not all(
        re.fullmatch(r"[0-9]+", value) for value in (run_id, attempt, date)
    ):
        raise ValueError("Build identifiers must contain only digits")
    return {
        "version": version,
        "tag": release["tag_name"],
        "release-id": str(release["id"]),
        "revision": revision,
        "build": f"{version}-rebuild-{date}-{run_id}-{attempt}",
    }


def is_current(release, revision, expected):
    release_version(release)
    return (
        str(release["id"]) == expected["release-id"]
        and release["tag_name"] == expected["tag"]
        and revision == expected["revision"]
    )


def tags(version, build, distro, suffix):
    prefixes = (build, version, "latest")
    result = [prefix + suffix for prefix in prefixes]
    if distro in {"debian", "debian-amd64v3"}:
        result += [prefix + "-debian" + suffix for prefix in prefixes]
    if any(
        not re.fullmatch(r"[\w][\w.-]{0,127}", tag, re.ASCII) for tag in result
    ):
        raise ValueError("Invalid Docker tag")
    return result


def write_outputs(values):
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("select", "check", "tags"))
    args = parser.parse_args()
    if args.command == "tags":
        print(
            "\n".join(
                tags(
                    os.environ["VERSION"],
                    os.environ["BUILD"],
                    os.environ["DISTRO"],
                    os.environ["SUFFIX"],
                )
            )
        )
        return
    release = api("releases/latest")
    release_version(release)
    revision = api("commits/" + quote(release["tag_name"], safe=""))["sha"]
    if args.command == "select":
        if bool(os.environ.get("DOCKERHUB_USERNAME")) != bool(
            os.environ.get("DOCKERHUB_TOKEN")
        ):
            raise ValueError(
                "Docker Hub requires both username and token or neither"
            )
        write_outputs(
            plan(
                release,
                revision,
                os.environ["GITHUB_RUN_ID"],
                os.environ["GITHUB_RUN_ATTEMPT"],
                datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
        )
    else:
        expected = {
            "release-id": os.environ["RELEASE_ID"],
            "tag": os.environ["RELEASE_TAG"],
            "revision": os.environ["REVISION"],
        }
        current = is_current(release, revision, expected)
        write_outputs({"current": str(current).lower()})
        if not current:
            print(
                "::notice::The release changed during the rebuild; "
                "publication is skipped."
            )


if __name__ == "__main__":
    main()
