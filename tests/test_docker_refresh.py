"""Check release selection, refresh coverage, and publication safeguards."""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from strictyaml.ruamel import YAML

ROOT = Path(__file__).resolve().parents[1]
REVISION = "a" * 40
RELEASE = {"id": 42, "tag_name": "v1.2.3", "draft": False, "prerelease": False}
WORKFLOW_SHA = "b" * 40


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / ".github/scripts" / (name + ".py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def workflow(name):
    return YAML(typ="safe").load(
        (ROOT / ".github/workflows" / (name + ".yml")).read_text()
    )


def ancestors(jobs, name):
    needs = jobs[name].get("needs", [])
    if isinstance(needs, str):
        needs = [needs]
    return set(needs).union(*(ancestors(jobs, n) for n in needs))


@pytest.mark.parametrize("status", ["ahead", "identical"])
def test_refresh_source_must_belong_to_trusted_workflow_history(
    monkeypatch, status
):
    refresh = load("docker_refresh")
    calls = []

    def compare(path):
        calls.append(path)
        return {
            "status": status,
            "base_commit": {"sha": REVISION},
            "merge_base_commit": {"sha": REVISION},
        }

    monkeypatch.setattr(refresh, "api", compare)
    assert refresh.trusted_revision(REVISION, WORKFLOW_SHA) == REVISION
    assert calls == [f"compare/{REVISION}...{WORKFLOW_SHA}"]


@pytest.mark.parametrize(
    "comparison",
    [
        {"status": "behind"},
        {"status": "diverged"},
        {"status": "ahead", "merge_base_commit": {"sha": "c" * 40}},
        {"status": "ahead", "base_commit": {"sha": "c" * 40}},
        {},
    ],
)
def test_unmerged_or_unverifiable_source_is_rejected(monkeypatch, comparison):
    refresh = load("docker_refresh")
    response = {
        "base_commit": {"sha": REVISION},
        "merge_base_commit": {"sha": REVISION},
        **comparison,
    }
    monkeypatch.setattr(refresh, "api", lambda path: response)
    with pytest.raises(ValueError, match="outside"):
        refresh.trusted_revision(REVISION, WORKFLOW_SHA)


@pytest.mark.parametrize(
    "revision", ["main", "v1.2.3", "a" * 39, REVISION + "\n"]
)
def test_mutable_or_malformed_source_never_reaches_github(
    monkeypatch, revision
):
    refresh = load("docker_refresh")
    monkeypatch.setattr(refresh, "api", lambda path: pytest.fail(path))
    with pytest.raises(ValueError, match="commit SHAs"):
        refresh.trusted_revision(revision, WORKFLOW_SHA)


@pytest.mark.parametrize(
    "refresh_mode,ref",
    [("false", "refs/heads/main"), ("true", "refs/heads/feature")],
)
def test_source_override_cannot_escape_main_refresh_jobs(
    monkeypatch, refresh_mode, ref
):
    refresh = load("docker_refresh")
    monkeypatch.setattr(sys, "argv", ["docker_refresh.py", "validate-ref"])
    monkeypatch.setenv("REFRESH", refresh_mode)
    monkeypatch.setenv("GITHUB_REF", ref)
    monkeypatch.setattr(refresh, "api", lambda path: pytest.fail(path))
    with pytest.raises(ValueError, match="main refreshes"):
        refresh.main()


@pytest.mark.parametrize("tag", ["1.2.3", "v1.2.3"])
def test_refresh_pins_released_source_and_distinguishes_reruns(tag):
    refresh = load("docker_refresh")
    release = dict(RELEASE, tag_name=tag)
    first = refresh.plan(release, REVISION, "100", "1", "20260917")
    retry = refresh.plan(release, REVISION, "100", "2", "20260917")
    assert first == {
        "version": "1.2.3",
        "tag": tag,
        "release-id": "42",
        "revision": REVISION,
        "build": "1.2.3-rebuild-20260917-100-1",
    }
    assert retry["build"] != first["build"]


@pytest.mark.parametrize(
    "changes",
    [
        {"draft": True},
        {"prerelease": True},
        {"tag_name": "v1.2.3-rc1"},
        {"tag_name": "main"},
        {"tag_name": "1.02.3"},
        {"tag_name": "1.2.3\nrevision=main"},
    ],
)
def test_unreleased_or_invalid_tags_cannot_enter_refresh(changes):
    with pytest.raises(ValueError):
        load("docker_refresh").plan(
            dict(RELEASE, **changes), REVISION, "100", "1", "20260917"
        )


@pytest.mark.parametrize(
    "revision,run_id", [("main", "100"), (REVISION, "100\ntag=latest")]
)
def test_refresh_rejects_unpinned_source_and_invalid_build_ids(
    revision, run_id
):
    with pytest.raises(ValueError):
        load("docker_refresh").plan(RELEASE, revision, run_id, "1", "20260917")


@pytest.mark.parametrize(
    "changes,revision",
    [
        ({"id": 43, "tag_name": "v1.2.4"}, REVISION),
        ({"id": 43}, REVISION),
        ({"tag_name": "v1.2.4"}, REVISION),
        ({}, "b" * 40),
    ],
)
def test_superseded_releases_and_moved_tags_cannot_publish(changes, revision):
    refresh = load("docker_refresh")
    expected = refresh.plan(RELEASE, REVISION, "100", "1", "20260917")
    assert refresh.is_current(RELEASE, REVISION, expected)
    assert not refresh.is_current(dict(RELEASE, **changes), revision, expected)


@pytest.mark.parametrize("current", [True, False])
def test_publication_check_emits_a_machine_readable_decision(
    tmp_path, monkeypatch, current
):
    refresh = load("docker_refresh")
    output = tmp_path / "outputs"
    for key, value in {
        "GITHUB_OUTPUT": str(output),
        "RELEASE_ID": "42",
        "RELEASE_TAG": "v1.2.3",
        "REVISION": REVISION,
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["docker_refresh.py", "check"])
    responses = {
        "releases/latest": RELEASE,
        "commits/v1.2.3": {"sha": REVISION if current else "b" * 40},
    }
    monkeypatch.setattr(refresh, "api", responses.__getitem__)
    refresh.main()
    assert output.read_text() == f"current={str(current).lower()}\n"


@pytest.mark.parametrize(
    "artifacts,ready",
    [
        ([{"name": "docker-refresh-alpine", "expired": False}], True),
        ([{"name": "docker-refresh-alpine", "expired": True}], False),
        ([{"name": "docker-refresh-alpine-amd64v3", "expired": False}], False),
        ([], False),
    ],
)
def test_publication_waits_for_its_distro_to_pass_every_gate(
    tmp_path, monkeypatch, artifacts, ready
):
    refresh = load("docker_refresh")
    output = tmp_path / "outputs"
    for key, value in {
        "GITHUB_OUTPUT": str(output),
        "GITHUB_RUN_ID": "100",
        "ARTIFACT": "docker-refresh-alpine",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["docker_refresh.py", "ready"])
    calls = []

    def listing(path):
        calls.append(path)
        return {"total_count": len(artifacts), "artifacts": artifacts}

    monkeypatch.setattr(refresh, "api", listing)
    refresh.main()
    assert calls == ["actions/runs/100/artifacts?name=docker-refresh-alpine"]
    assert output.read_text() == f"ready={str(ready).lower()}\n"


@pytest.mark.parametrize("failures", [0, 2, 4])
def test_github_api_calls_retry_transient_failures(monkeypatch, failures):
    refresh = load("docker_refresh")
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    results = [subprocess.CompletedProcess([], 1, "", "HTTP 502\n")] * failures
    results.append(subprocess.CompletedProcess([], 0, '{"id": 42}', ""))
    commands, sleeps = [], []

    def gh(command, **kwargs):
        commands.append(command)
        return results[len(commands) - 1]

    monkeypatch.setattr(refresh.subprocess, "run", gh)
    monkeypatch.setattr(refresh.time, "sleep", sleeps.append)
    if failures < refresh.ATTEMPTS:
        assert refresh.api("releases/latest") == {"id": 42}
    else:
        with pytest.raises(subprocess.CalledProcessError):
            refresh.api("releases/latest")
    assert commands[0] == ["gh", "api", "repos/o/r/releases/latest"]
    assert len(commands) == min(failures + 1, refresh.ATTEMPTS)
    assert sleeps == [10, 20, 40][: len(commands) - 1]


def test_every_image_gets_unique_build_and_existing_release_aliases():
    refresh = load("docker_refresh")
    matrix = load("docker_matrix")
    rows = matrix.expand(
        json.loads((ROOT / ".github/docker-matrix.json").read_text())
    )
    all_tags = []
    build = "1.2.3-rebuild-20260917-100-1"
    for row in rows:
        tags = refresh.tags("1.2.3", build, row["distro"], row["suffix"])
        assert build + row["suffix"] in tags
        assert "1.2.3" + row["suffix"] in tags
        assert "latest" + row["suffix"] in tags
        if row["distro"] == "debian-amd64v3":
            assert "latest-debian-amd64v3" in tags
        all_tags.extend(tags)
    assert len(all_tags) == len(set(all_tags))
    assert "latest-debian" in all_tags


def test_matrix_uses_released_recipes_and_covers_required_wheels(tmp_path):
    source = tmp_path / "matrix.json"
    source.write_text(
        json.dumps(
            [
                {
                    "distro": "alpine",
                    "dockerfile": "docker/Dockerfile.alpine",
                    "suffix": "-alpine",
                    "platforms": "linux/amd64,linux/s390x",
                }
            ]
        )
    )
    output = tmp_path / "outputs"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / ".github/scripts/docker_matrix.py"),
            str(source),
        ],
        env=dict(os.environ, GITHUB_OUTPUT=str(output)),
        check=True,
        capture_output=True,
        text=True,
    )
    assert {r["distro"] for r in json.loads(result.stdout)} == {
        "alpine",
        "alpine-amd64v3",
    }
    values = {
        k: json.loads(v)
        for k, v in (
            line.split("=", 1) for line in output.read_text().splitlines()
        )
    }
    assert len(values["docker-musl"]) == 1
    assert values["docker-glibc"] == []
    matrix = load("docker_matrix")
    rows = matrix.platforms(
        json.loads((ROOT / ".github/docker-matrix.json").read_text())
    )
    wheel_names = {
        f"pq-wheel-{r['libc']}-{r['arch']}"
        for group in ("glibc", "musl")
        for r in values["pq-" + group]
    }
    assert {r["wheel"] for r in rows if r["wheel"]} <= wheel_names


def test_scheduled_refresh_gates_each_distro_before_its_publication():
    refresh = workflow("rehydrate-docker")
    assert refresh["on"]["schedule"] == [{"cron": "23 6 * * *"}]
    assert "workflow_dispatch" in refresh["on"]
    assert refresh["concurrency"]["cancel-in-progress"] is False
    jobs = refresh["jobs"]
    assert {
        "prepare",
        "test",
        "pq-wheels",
        "pq-wheels-musl",
        "docker",
        "docker-glibc",
        "docker-musl",
        "assemble",
    } <= ancestors(jobs, "publish")
    for name in (
        "docker",
        "docker-glibc",
        "docker-musl",
        "pq-wheels",
        "pq-wheels-musl",
    ):
        assert jobs[name]["with"]["refresh"] is True
        assert (
            jobs[name]["with"]["ref"]
            == "${{ needs.prepare.outputs.revision }}"
        )
    # A failed platform holds back only its distro, never the release
    # tests or the source selection that every distro depends on.
    for name in ("docker-glibc", "docker-musl"):
        assert "!cancelled()" in jobs[name]["if"]
        assert "needs.prepare.result == 'success'" in jobs[name]["if"]
    for name in ("assemble", "publish"):
        assert "test" in jobs[name]["needs"]
        for gate in (
            "!cancelled()",
            "needs.prepare.result == 'success'",
            "needs.test.result == 'success'",
        ):
            assert gate in jobs[name]["if"]
    publish = jobs["publish"]
    assert (
        publish["concurrency"]
        == workflow("release")["jobs"]["docker-push"]["concurrency"]
    )
    steps = publish["steps"]
    ready = next(s for s in steps if s.get("id") == "ready")
    assert ready["run"].endswith("docker_refresh.py ready")
    assert (
        ready["env"]["ARTIFACT"] == "docker-refresh-${{ matrix.distro }}"
    )
    assert publish["permissions"]["actions"] == "read"
    download = next(
        s
        for s in steps
        if s.get("uses", "").startswith("actions/download-artifact")
    )
    assert download["with"]["name"] == ready["env"]["ARTIFACT"]
    current = next(s for s in steps if s.get("id") == "current")
    for step in (download, current):
        assert steps.index(ready) < steps.index(step)
        assert step["if"] == "steps.ready.outputs.ready == 'true'"
    step = next(
        s
        for s in steps
        if s.get("name") == "Publish the validated images"
    )
    assert step["if"] == "steps.current.outputs.current == 'true'"
    assert "retry 6 skopeo copy --all --preserve-digests" in step["run"]
    assert "docker/build-push-action" not in str(publish)


def test_refresh_retries_transient_failures_before_failing():
    jobs = workflow("rehydrate-docker")["jobs"]
    tests = next(
        s
        for s in jobs["test"]["steps"]
        if s.get("name") == "Test the release with fresh dependencies"
    )
    assert "python -m pytest -q --last-failed" in tests["run"]
    assert "--last-failed-no-failures none" in tests["run"]
    for name in ("assemble", "publish"):
        run = str(jobs[name]["steps"])
        assert run.count("sudo apt-get") == 2
        assert run.count("retry 5 sudo apt-get") == 2
    steps = workflow("build-docker")["jobs"]["build"]["steps"]
    first, again = (
        s
        for s in steps
        if s.get("uses", "").startswith("docker/build-push-action")
    )
    assert first["id"] == "build"
    # Release builds fail at once; only a refresh spends a second attempt.
    assert first["continue-on-error"] == "${{ inputs.refresh }}"
    retried = "inputs.refresh && steps.build.outcome == 'failure'"
    assert again["if"] == retried
    assert again["with"] == first["with"]
    wait = steps[steps.index(again) - 1]
    assert wait["if"] == retried
    assert wait["run"].startswith("sleep ")


def test_refresh_bypasses_all_image_and_compiler_caches_and_checks_bytes():
    jobs = workflow("build-docker")["jobs"]
    steps = jobs["build"]["steps"]
    build = next(
        s
        for s in steps
        if s.get("uses", "").startswith("docker/build-push-action")
    )
    for option in ("pull", "no-cache"):
        assert build["with"][option] == "${{ inputs.refresh }}"
    assert "inputs.ref || github.sha" in build["with"]["labels"]
    assert "!inputs.refresh" in build["with"]["cache-to"]
    scan = next(
        s for s in steps if s.get("name") == "Scan the refreshed image"
    )
    assert scan["with"]["input"] == "${{ runner.temp }}/scan-image"
    assert scan["env"]["TRIVY_PLATFORM"] == "${{ matrix.platforms }}"
    assert not scan.get("continue-on-error")
    names = [s.get("name") for s in steps]
    assert (
        names.index("Test the refreshed runtime")
        < names.index("Prepare the OCI image for scanning")
        < names.index("Scan the refreshed image")
    )
    contexts = build["with"]["build-contexts"]
    assert "dependency-sources=" in contexts
    assert "refresh-tools=" in contexts
    smoke = next(
        s for s in steps if s.get("name") == "Test the refreshed runtime"
    )
    for option in ("--override-os", "--override-arch", "--override-variant"):
        assert option in smoke["run"]
    assert 'check --distro "$DISTRO"' in smoke["run"]
    wheel = workflow("build-pq-wheels")["jobs"]["wheel"]
    assert wheel["continue-on-error"] == "${{ !inputs.refresh }}"
    for step in wheel["steps"]:
        if step.get("uses", "").startswith("actions/cache/"):
            assert "!inputs.refresh" in step["if"]


def test_only_findings_new_to_the_refresh_block_publication():
    steps = workflow("build-docker")["jobs"]["build"]["steps"]
    scan = next(
        s for s in steps if s.get("name") == "Scan the refreshed image"
    )
    # The published image is scanned with the same filters and database.
    assert scan["with"]["exit-code"] == "0"
    assert scan["with"]["severity"] == load("scan_gate").SEVERITY
    assert scan["with"]["ignore-unfixed"] is True
    assert scan["with"]["cache-dir"] == "${{ github.workspace }}/.cache/trivy"
    gate = next(s for s in steps if "scan_gate.py" in s.get("run", ""))
    assert gate["if"] == "inputs.refresh"
    assert not gate.get("continue-on-error")
    for fragment in (
        '"$RUNNER_TEMP/release-inputs/refresh-tools/scan_gate.py"',
        '--cache-dir "$GITHUB_WORKSPACE/.cache/trivy"',
        '--published "ghcr.io/${REPO,,}:$VERSION$SUFFIX"',
        '--platform "$PLATFORM"',
    ):
        assert fragment in gate["run"]
    upload = next(
        s
        for s in steps
        if s.get("with", {}).get("name", "").startswith("image-")
    )
    assert "if" not in upload
    assert steps.index(scan) < steps.index(gate) < steps.index(upload)


OPENSSL = ("debian", "libssl3t64", "CVE-2026-75804")
MSGPACK = ("python-pkg", "msgpack", "GHSA-6v7p-g79w-8964")


def trivy_report(*findings):
    return {
        "Results": [
            {
                "Type": kind,
                "Vulnerabilities": [
                    {
                        "PkgName": package,
                        "InstalledVersion": "1",
                        "FixedVersion": "2",
                        "Severity": "HIGH",
                        "VulnerabilityID": vulnerability,
                    }
                ],
            }
            for kind, package, vulnerability in findings
        ]
    }


def run_gate(tmp_path, monkeypatch, refreshed, published):
    """Run the gate; published holds each attempt's findings or None."""
    gate = load("scan_gate")
    report = tmp_path / "vulnerabilities.json"
    report.write_text(json.dumps(trivy_report(*refreshed)))
    baseline = tmp_path / "published.json"
    commands, sleeps = [], []

    def trivy(command, check):
        commands.append(command)
        findings = published[len(commands) - 1]
        if findings is None:
            return subprocess.CompletedProcess(command, 1)
        baseline.write_text(json.dumps(trivy_report(*findings)))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(gate.subprocess, "run", trivy)
    monkeypatch.setattr(gate.time, "sleep", sleeps.append)
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    code = gate.main(
        [
            "--report",
            str(report),
            "--baseline",
            str(baseline),
            "--published",
            "ghcr.io/o/r:1.2.3-distroless",
            "--platform",
            "linux/arm64",
            "--cache-dir",
            str(tmp_path / "cache"),
        ]
    )
    text = summary.read_text() if summary.exists() else ""
    return code, commands, sleeps, text


def test_scan_gate_skips_the_published_scan_for_a_clean_image(
    tmp_path, monkeypatch
):
    code, commands, _, summary = run_gate(tmp_path, monkeypatch, (), [])
    assert (code, commands, summary) == (0, [], "")


def test_scan_gate_publishes_findings_the_published_image_shares(
    tmp_path, monkeypatch, capsys
):
    code, commands, sleeps, summary = run_gate(
        tmp_path, monkeypatch, [OPENSSL], [[OPENSSL, MSGPACK]]
    )
    assert (code, sleeps) == (0, [])
    (command,) = commands
    for option in (
        ["--image-src", "remote"],
        ["--platform", "linux/arm64"],
        ["--cache-dir", str(tmp_path / "cache")],
        ["--severity", "HIGH,CRITICAL"],
    ):
        start = command.index(option[0])
        assert command[start : start + 2] == option
    for flag in ("--skip-db-update", "--ignore-unfixed"):
        assert flag in command
    assert command[-1] == "ghcr.io/o/r:1.2.3-distroless"
    assert "| also published | libssl3t64 |" in summary
    out = capsys.readouterr().out
    assert "::warning title=Waiting on upstream fixes::1 finding(s)" in out
    assert "::error" not in out


def test_scan_gate_blocks_findings_new_to_the_refresh(
    tmp_path, monkeypatch, capsys
):
    code, _, _, summary = run_gate(
        tmp_path, monkeypatch, [OPENSSL, MSGPACK], [[OPENSSL]]
    )
    assert code == 1
    assert "| new | msgpack |" in summary
    assert "| also published | libssl3t64 |" in summary
    out = capsys.readouterr().out
    assert (
        "::error title=New vulnerabilities::1 finding(s) absent from "
        "ghcr.io/o/r:1.2.3-distroless: msgpack GHSA-6v7p-g79w-8964" in out
    )


@pytest.mark.parametrize(
    "published,code,sleeps",
    [
        ([None, [OPENSSL]], 0, [15]),
        ([None, None, None, None], 1, [15, 30, 60]),
    ],
)
def test_scan_gate_retries_the_published_scan_then_blocks_everything(
    tmp_path, monkeypatch, published, code, sleeps
):
    result = run_gate(tmp_path, monkeypatch, [OPENSSL], published)
    assert result[0] == code
    assert len(result[1]) == len(published)
    assert result[2] == sleeps


def test_runtime_refresh_uses_tools_from_the_workflow_checkout():
    prepare = workflow("rehydrate-docker")["jobs"]["prepare"]["steps"]
    inputs = next(s for s in prepare if s.get("id") == "inputs")
    assert inputs["working-directory"] == "source"
    assert (
        "cp ../.github/scripts/refresh_runtime.py "
        "../.github/scripts/scan_gate.py \\\n"
        "  release-inputs/refresh-tools/" in inputs["run"]
    )
    steps = workflow("build-docker")["jobs"]["build"]["steps"]
    render = next(s for s in steps if " recipe " in s.get("run", ""))
    assert render["if"] == "inputs.refresh"
    assert (
        "$RUNNER_TEMP/release-inputs/refresh-tools/refresh_runtime.py"
        in (render["run"])
    )
    assert render["env"]["DOCKERFILE"] == "${{ matrix.dockerfile }}"
    assert render["env"]["DISTRO"] == "${{ matrix.distro }}"


@pytest.mark.parametrize(
    "row",
    load("docker_matrix").expand(
        json.loads((ROOT / ".github/docker-matrix.json").read_text())
    ),
    ids=lambda row: row["distro"],
)
def test_runtime_refresh_preserves_the_released_recipe_and_user(row):
    source = (ROOT / row["dockerfile"]).read_text()
    rendered = load("refresh_runtime").recipe(source, row["distro"])
    assert rendered.startswith(source.rstrip() + "\n")
    added = rendered[len(source.rstrip()) :]
    lines = added.strip().splitlines()
    assert lines[0] == "USER 0:0"
    assert lines[2] == "USER 65534:65534"
    command = json.loads(lines[1][lines[1].index("[") :])
    assert command == [
        "/opt/venv/bin/python",
        "/tmp/refresh-tools/refresh_runtime.py",
        "refresh",
        "--distro",
        row["distro"].removesuffix("-amd64v3"),
    ]
    assert "COPY --from=dependency-sources" in lines[3]
    assert "ENTRYPOINT" not in added
    assert "CMD" not in added


def test_runtime_refresh_preserves_a_custom_runtime_user():
    source = "FROM builder\nUSER build\nFROM runtime\nUSER app:group\n"
    assert "\nUSER app:group\nCOPY" in load("refresh_runtime").recipe(
        source, "debian"
    )


@pytest.mark.parametrize(
    "source,distro",
    [
        ("FROM builder\nUSER build\nFROM runtime\n", "debian"),
        ("FROM runtime\nUSER app\n", "unknown"),
    ],
)
def test_runtime_refresh_rejects_unsupported_recipes(source, distro):
    with pytest.raises(ValueError):
        load("refresh_runtime").recipe(source, distro)
