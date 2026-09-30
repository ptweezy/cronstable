"""Block a refreshed image only on vulnerabilities its published image lacks.

A finding that the published image also carries needs an upstream fix, and
holding the refresh back would leave that older image live. Both scans use
the same vulnerability database, so a difference comes from the image alone.
"""

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

# Matches the refreshed-image scan in build-docker.yml.
SEVERITY = "HIGH,CRITICAL"
ATTEMPTS = 4


def findings(report):
    """Map each (type, package, vulnerability) to a printable row."""
    rows = {}
    for result in report.get("Results") or []:
        for vuln in result.get("Vulnerabilities") or []:
            key = (
                result.get("Type", ""),
                vuln["PkgName"],
                vuln["VulnerabilityID"],
            )
            rows[key] = (
                vuln["PkgName"],
                vuln.get("InstalledVersion", ""),
                vuln.get("FixedVersion", ""),
                vuln.get("Severity", ""),
                vuln["VulnerabilityID"],
            )
    return rows


def scan(image, platform, cache_dir, output):
    """Scan a registry image with the refreshed scan's database."""
    command = [
        "trivy",
        "image",
        "--image-src",
        "remote",
        "--platform",
        platform,
        "--cache-dir",
        str(cache_dir),
        "--skip-db-update",
        "--skip-java-db-update",
        "--skip-version-check",
        "--scanners",
        "vuln",
        "--severity",
        SEVERITY,
        "--ignore-unfixed",
        "--format",
        "json",
        "--output",
        str(output),
        "--timeout",
        "15m",
        image,
    ]
    for attempt in range(ATTEMPTS):
        if subprocess.run(command, check=False).returncode == 0:
            return json.loads(output.read_text(encoding="utf-8"))
        if attempt + 1 < ATTEMPTS:
            delay = 15 * 2**attempt
            print(
                f"::warning::Scanning {image} failed; retrying in {delay}s",
                flush=True,
            )
            time.sleep(delay)
    return None


def describe(rows, keys):
    return ", ".join(f"{rows[k][0]} {rows[k][4]}" for k in keys)


def report(refreshed, new, image, platform):
    """Print every finding and add it to the job summary."""
    lines = [
        f"### Vulnerabilities in {platform}, compared with `{image}`",
        "",
        "| Status | Package | Installed | Fixed | Severity | ID |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for key in sorted(refreshed):
        status = "new" if key in new else "also published"
        print("\t".join((status, *refreshed[key])), flush=True)
        lines.append("| " + " | ".join((status, *refreshed[key])) + " |")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--published", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    refreshed = findings(json.loads(args.report.read_text(encoding="utf-8")))
    if not refreshed:
        print("No fixable high or critical vulnerabilities", flush=True)
        return 0
    baseline = scan(
        args.published, args.platform, args.cache_dir, args.baseline
    )
    if baseline is None:
        print(
            f"::warning::{args.published} could not be scanned, so every "
            "finding blocks publication",
            flush=True,
        )
        baseline = {}
    published = findings(baseline)
    new = sorted(key for key in refreshed if key not in published)
    carried = sorted(key for key in refreshed if key in published)
    report(refreshed, set(new), args.published, args.platform)
    if carried:
        print(
            f"::warning title=Waiting on upstream fixes::{len(carried)} "
            f"finding(s) also in {args.published}: "
            + describe(refreshed, carried),
            flush=True,
        )
    if new:
        print(
            f"::error title=New vulnerabilities::{len(new)} finding(s) "
            f"absent from {args.published}: " + describe(refreshed, new),
            flush=True,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
