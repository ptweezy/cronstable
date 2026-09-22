"""Bound only this workflow's compiler caches; never remove image artifacts."""

import json
import os
import subprocess

PREFIXES = ("pq-work-", "armv6-work-", "slow-pip-")
BUDGET = 2 * 1024**3


def victims(caches, budget=BUDGET):
    owned = sorted(
        (
            c
            for c in caches
            if c["key"].startswith(PREFIXES) and c["ref"] == "refs/heads/main"
        ),
        key=lambda c: c["last_accessed_at"],
    )
    size = sum(c["size_in_bytes"] for c in owned)
    result = []
    for cache in owned:
        if size <= budget:
            break
        result.append(cache["id"])
        size -= cache["size_in_bytes"]
    return result


def main():
    endpoint = f"repos/{os.environ['GITHUB_REPOSITORY']}/actions/caches"
    result = subprocess.run(
        ["gh", "api", endpoint + "?per_page=100", "--paginate", "--slurp"],
        check=True,
        capture_output=True,
        text=True,
    )
    caches = [
        c for page in json.loads(result.stdout) for c in page["actions_caches"]
    ]
    for cache_id in victims(caches):
        subprocess.run(
            ["gh", "api", f"{endpoint}/{cache_id}", "--method", "DELETE"],
            check=True,
        )
        print(f"Removed compiler cache {cache_id} (2 GiB total budget)")


if __name__ == "__main__":
    main()
