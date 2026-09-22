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


def test_scheduled_refresh_gates_all_images_before_any_publication():
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
    publish = jobs["publish"]
    assert (
        publish["concurrency"]
        == workflow("release")["jobs"]["docker-push"]["concurrency"]
    )
    step = next(
        s
        for s in publish["steps"]
        if s.get("name") == "Publish the validated images"
    )
    assert step["if"] == "steps.current.outputs.current == 'true'"
    assert "skopeo copy --all --preserve-digests" in step["run"]
    assert "docker/build-push-action" not in str(publish)


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
    assert scan["with"]["exit-code"] == "1"
    assert scan["with"]["ignore-unfixed"] is True
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


def test_runtime_refresh_uses_tools_from_the_workflow_checkout():
    prepare = workflow("rehydrate-docker")["jobs"]["prepare"]["steps"]
    inputs = next(s for s in prepare if s.get("id") == "inputs")
    assert inputs["working-directory"] == "source"
    assert (
        "cp ../.github/scripts/refresh_runtime.py "
        "release-inputs/refresh-tools/" in inputs["run"]
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
