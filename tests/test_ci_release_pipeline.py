"""Exercise failures between preparation, builds and publication."""

import hashlib
import importlib.util
import io
import json
import tarfile
from pathlib import Path

import pytest
from strictyaml.ruamel import YAML

pytest.importorskip("tomllib")  # CI helpers execute on Python 3.14.

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / ".github/scripts" / (name + ".py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def workflow(name="release"):
    return YAML(typ="safe").load(
        (ROOT / f".github/workflows/{name}.yml").read_text()
    )


def ancestors(jobs, name):
    needs = jobs[name].get("needs", [])
    if isinstance(needs, str):
        needs = [needs]
    return set(needs).union(*(ancestors(jobs, n) for n in needs))


def test_every_required_build_and_preparation_gates_publication():
    jobs = workflow()["jobs"]
    gate = ancestors(jobs, "release")
    assert {
        n
        for n in jobs
        if n.startswith("binaries-") and "experimental" not in n
    } <= gate
    assert {
        "binaries",
        "tox",
        "tox-static",
        "licenses",
        "preflight",
        "dist",
        "packages",
        "sign-windows",
        "perf",
        "docker",
        "docker-glibc",
        "docker-musl",
        "docker-assemble",
        "release-prepare",
    } <= gate
    assert (
        not {
            "tox-experimental",
            "binaries-macos-experimental",
            "compiler-cache-budget",
        }
        & gate
    )
    assert "pq-wheel-armv6" not in ancestors(jobs, "binaries-armel")
    assert "pq-wheels" not in ancestors(jobs, "docker")
    assert "pq-wheels-musl" not in ancestors(jobs, "docker-glibc")
    assert "pq-wheels" not in ancestors(jobs, "docker-musl")


def test_publication_consumes_only_prepared_artifacts():
    jobs = workflow()["jobs"]
    steps = jobs["release"]["steps"]
    names = [s.get("name") for s in steps]
    assert (
        names.index("Download prepared release")
        < names.index("Verify prepared artifact checksums")
        < names.index("Publish to PyPI (Trusted Publishing)")
    )
    assert "prepared-release" in str(steps)
    for name in ("release", "docker-push", "homebrew"):
        text = str(jobs[name]["steps"])
        assert "docker/build-push-action" not in text
        assert "build_packages.sh" not in text
        assert "pip download" not in text
    push = str(jobs["docker-push"]["steps"])
    assert "skopeo copy --all --preserve-digests" in push
    assert "docker-release-${{ matrix.distro }}" in push
    assert "oci-archive:image.tar:release" in push
    assert jobs["docker-push"]["strategy"]["fail-fast"] is False


def test_each_docker_platform_has_one_build_and_only_its_required_wheel():
    matrix = load("docker_matrix")
    source = json.loads((ROOT / ".github/docker-matrix.json").read_text())
    expected = {
        (r["distro"], p)
        for r in matrix.expand(source)
        for p in r["platforms"].split(",")
    }
    rows = matrix.platforms(source)
    assert len(rows) == len(expected)
    assert {(r["distro"], r["platforms"]) for r in rows} == expected
    for row in rows:
        if row["platforms"] in ("linux/amd64", "linux/arm64"):
            assert row["wheel"] == "" and row["wheel_group"] == "none"
            assert not row["qemu"]
        if row["platforms"] == "linux/arm64":
            assert row["runner"] == "ubuntu-24.04-arm"
        if row["wheel"]:
            assert row["wheel"].startswith(
                "pq-wheel-" + row["wheel_group"] + "-"
            )
    build = workflow("build-docker")["jobs"]["build"]
    step = next(
        s
        for s in build["steps"]
        if s.get("uses", "").startswith("docker/build-push-action")
    )
    assert step["with"]["push"] is False
    assert step["with"]["outputs"].startswith("type=oci,")
    assert "matrix.platform_id" in step["with"]["cache-from"]


def test_source_offer_resolution_reaches_every_bundled_artifact():
    jobs = workflow()["jobs"]
    for name, job in jobs.items():
        if not name.startswith("binaries"):
            continue
        assert "preflight" in ancestors(jobs, name)
        run = str(job["steps"])
        assert "zeroconf==${{ needs.preflight.outputs.zeroconf }}" in run
        assert "Check every declared binary artifact" in run
    build = workflow("build-docker")["jobs"]["build"]
    assert "ZEROCONF_VERSION=${{ inputs.zeroconf }}" in str(build["steps"])


def test_partial_upload_is_rejected(tmp_path):
    check = load("check_artifacts").check
    (tmp_path / "one.exe").write_bytes(b"exe")
    with pytest.raises(ValueError, match="Missing or empty"):
        check([str(tmp_path / "one.exe"), str(tmp_path / "two.msi")])
    (tmp_path / "two.msi").write_bytes(b"msi")
    check([str(tmp_path / "one.exe"), str(tmp_path / "two.msi")])


@pytest.fixture
def release_env():
    return dict.fromkeys(load("release_preflight").REQUIRED, "configured")


@pytest.mark.parametrize(
    "version",
    ["v1.2.3", "01.2.3", "256.1.1", "1.256.1", "1.1.65535", "1.2.3.dev1"],
)
def test_release_preflight_rejects_versions_before_building(
    version, release_env
):
    with pytest.raises(ValueError):
        load("release_preflight").validate(version, release_env)


@pytest.mark.parametrize("secret", load("release_preflight").REQUIRED)
def test_missing_release_secret_blocks_fanout(secret, release_env):
    del release_env[secret]
    with pytest.raises(ValueError, match=secret):
        load("release_preflight").validate("1.2.3", release_env)


def test_optional_channels_require_consistent_configuration(release_env):
    pre = load("release_preflight")
    pre.validate("1.2.3", release_env)
    with pytest.raises(ValueError, match="macOS"):
        pre.validate("1.2.3", dict(release_env, MACOS_CERT_P12_BASE64="cert"))
    with pytest.raises(ValueError, match="Docker Hub"):
        pre.validate("1.2.3", dict(release_env, DOCKERHUB_USERNAME="user"))


@pytest.mark.parametrize("token", [None, "", "custom-release-token"])
def test_release_token_override_is_optional(monkeypatch, release_env, token):
    pre = load("release_preflight")
    env = dict(release_env, GITHUB_REPOSITORY="ptweezy/cronstable")
    if token is not None:
        env["RELEASE_TOKEN"] = token
    calls = []

    def github(path, credential):
        calls.append((path, credential))
        return {"permissions": {"push": True}}, {
            "X-OAuth-Scopes": "repo, workflow"
        }

    monkeypatch.setattr(pre, "github", github)
    pre.validate("1.2.51", env)
    pre.authenticate(env)
    assert ("repos/ptweezy/homebrew-tap", env["HOMEBREW_TAP_TOKEN"]) in calls
    assert ("user", env["WINGET_TOKEN"]) in calls
    release_calls = [c for c in calls if c[0] == "repos/ptweezy/cronstable"]
    assert release_calls == (
        [("repos/ptweezy/cronstable", token)] if token else []
    )
    release = workflow()["jobs"]["release"]
    assert release["permissions"]["contents"] == "write"
    tag = next(
        s for s in release["steps"] if s.get("name") == "Tag the release"
    )
    assert 'git push origin "refs/tags/$NEW"' in tag["run"]


@pytest.mark.parametrize(
    "push, scopes", [(False, "repo, workflow"), (True, "repo")]
)
def test_configured_release_token_still_requires_access(
    monkeypatch, release_env, push, scopes
):
    pre = load("release_preflight")

    def github(path, credential):
        if credential == "invalid-release-token":
            return {"permissions": {"push": push}}, {"X-OAuth-Scopes": scopes}
        return {"permissions": {"push": True}}, {"X-OAuth-Scopes": "repo"}

    monkeypatch.setattr(pre, "github", github)
    with pytest.raises(ValueError, match="RELEASE_TOKEN"):
        pre.authenticate(
            dict(
                release_env,
                GITHUB_REPOSITORY="ptweezy/cronstable",
                RELEASE_TOKEN="invalid-release-token",
            )
        )


def test_pypi_preflight_exchanges_identity_without_publishing(monkeypatch):
    pre = load("release_preflight")
    calls = []

    def request(req, timeout):
        calls.append(req)
        if len(calls) == 1:
            return io.BytesIO(b'{"value":"identity"}')
        return io.BytesIO(b'{"token":"discarded-pypi-token"}')

    monkeypatch.setattr(pre.urllib.request, "urlopen", request)
    pre.authenticate_pypi(
        {
            "ACTIONS_ID_TOKEN_REQUEST_URL": "https://example.invalid/oidc?job=1",
            "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "request-token",
        }
    )
    assert calls[0].full_url.endswith("&audience=pypi")
    assert calls[1].full_url == "https://pypi.org/_/oidc/mint-token"
    assert json.loads(calls[1].data) == {"token": "identity"}
    assert len(calls) == 2


def test_source_download_digest_failure_blocks_builders(monkeypatch, tmp_path):
    inputs = load("release_inputs")
    metadata = {
        "releases": {
            "48.0.1": [
                {
                    "packagetype": "sdist",
                    "filename": "cryptography-48.0.1.tar.gz",
                    "url": "https://example.invalid/source",
                    "digests": {"sha256": "0" * 64},
                }
            ]
        }
    }
    monkeypatch.setattr(
        inputs,
        "fetch",
        lambda url: (
            json.dumps(metadata).encode() if url.endswith("/json") else b"bad"
        ),
    )
    with pytest.raises(ValueError, match="Source digest mismatch"):
        inputs.resolve("cryptography", ">=48", "3.11", tmp_path)
    assert not list(tmp_path.iterdir())


def test_input_resolution_ignores_yanked_prerelease_and_incompatible_sources():
    source = {
        "packagetype": "sdist",
        "requires_python": ">=3.11",
        "filename": "source.tar.gz",
    }
    data = {
        "releases": {
            "48.0.0": [dict(source)],
            "48.0.1": [dict(source)],
            "49.0.0": [dict(source, requires_python=">=3.12")],
            "49.0.1": [dict(source, yanked=True)],
            "50.0.0rc1": [dict(source)],
        }
    }
    assert (
        load("release_inputs").select_source(data, ">=48", "3.11")[0]
        == "48.0.1"
    )


def test_source_constraints_come_from_pyproject():
    import tomllib
    from packaging.requirements import Requirement

    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    inputs = load("release_inputs")
    discovery = project["project"]["optional-dependencies"]["discovery"]
    assert inputs.requirement_for("zeroconf", project) == str(
        Requirement(discovery[0]).specifier
    )
    assert inputs.requirement_for("cryptography", project) == ">=48"


def test_prepared_release_rejects_changed_bytes(tmp_path):
    verifier = load("verify_release_checksums")
    (tmp_path / "binaries").mkdir()
    binary = tmp_path / "binaries/cronstable-linux-amd64"
    binary.write_bytes(b"validated")
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    (tmp_path / "SHA256SUMS").write_text(digest + "  " + binary.name + "\n")
    verifier.verify(tmp_path)
    binary.write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum mismatch"):
        verifier.verify(tmp_path)


def image_archive(
    tmp_path,
    platform="linux/amd64",
    version="1.2.3",
    revision="abc",
    corrupt=False,
):
    oci = load("oci_images")
    files = {}

    def blob(value, media):
        data = json.dumps(value).encode()
        digest = hashlib.sha256(data).hexdigest()
        files["blobs/sha256/" + digest] = data
        return {
            "mediaType": media,
            "digest": "sha256:" + digest,
            "size": len(data),
        }

    os_name, arch, *variant = platform.split("/")
    config = {
        "os": os_name,
        "architecture": arch,
        "config": {
            "Labels": {
                "org.opencontainers.image.version": version,
                "org.opencontainers.image.revision": revision,
            }
        },
    }
    if variant:
        config["variant"] = variant[0]
    descriptor = blob(
        {
            "schemaVersion": 2,
            "config": blob(config, "application/vnd.oci.image.config.v1+json"),
            "layers": [],
        },
        oci.MANIFEST,
    )
    files["index.json"] = json.dumps(
        {"schemaVersion": 2, "manifests": [descriptor]}
    ).encode()
    files["oci-layout"] = b'{"imageLayoutVersion":"1.0.0"}'
    if corrupt:
        files[next(n for n in files if n.startswith("blobs/"))] = b"tampered"
    folder = tmp_path / ("image-debian-" + platform.replace("/", "-"))
    folder.mkdir()
    archive = folder / "image.tar"
    with tarfile.open(archive, "w") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return archive


def test_oci_merge_keeps_all_platforms_and_original_manifest_digests(tmp_path):
    oci = load("oci_images")
    expected = []
    for platform in ("linux/amd64", "linux/arm64", "linux/arm/v7"):
        archive = image_archive(tmp_path, platform)
        with tarfile.open(archive) as tar:
            expected.append(
                json.load(tar.extractfile("index.json"))["manifests"][0][
                    "digest"
                ]
            )
    output = tmp_path / "merged"
    oci.merge(
        tmp_path,
        "debian",
        "linux/amd64,linux/arm64,linux/arm/v7",
        output,
        "1.2.3",
        "abc",
    )
    top = json.loads((output / "index.json").read_text())["manifests"][0]
    assert top["annotations"]["org.opencontainers.image.ref.name"] == "release"
    merged = json.loads(oci.blob(output, top).read_text())
    assert [d["digest"] for d in merged["manifests"]] == expected
    assert merged["manifests"][2]["platform"]["variant"] == "v7"


@pytest.mark.parametrize("flat", [False, True])
def test_oci_merge_single_platform_download(tmp_path, flat):
    oci = load("oci_images")
    archive = image_archive(tmp_path)
    if flat:
        archive = archive.replace(tmp_path / "image.tar")
    with tarfile.open(archive) as tar:
        original = json.load(tar.extractfile("index.json"))["manifests"][0]
    output = tmp_path / "merged"
    oci.merge(tmp_path, "debian", "linux/amd64", output, "1.2.3", "abc")
    top = json.loads((output / "index.json").read_text())["manifests"][0]
    merged = json.loads(oci.blob(output, top).read_text())
    assert len(merged["manifests"]) == 1
    assert merged["manifests"][0]["digest"] == original["digest"]
    assert merged["manifests"][0]["platform"] == {
        "os": "linux",
        "architecture": "amd64",
    }


@pytest.mark.parametrize(
    "case",
    ["corrupt", "version", "revision", "missing", "wrong-arch", "duplicate"],
)
@pytest.mark.parametrize("flat", [False, True])
def test_oci_assembly_blocks_invalid_release_images(tmp_path, case, flat):
    oci = load("oci_images")
    archive = image_archive(
        tmp_path,
        version="9.9.9" if case == "version" else "1.2.3",
        revision="wrong" if case == "revision" else "abc",
        corrupt=case == "corrupt",
    )
    platforms = "linux/amd64"
    if case == "missing":
        platforms += ",linux/arm64"
    if case == "duplicate":
        platforms += ",linux/amd64"
    if flat:
        archive.replace(tmp_path / "image.tar")
    if case == "wrong-arch":
        if not flat:
            archive.parent.rename(tmp_path / "image-debian-linux-arm64")
        platforms = "linux/arm64"
    with pytest.raises((ValueError, FileNotFoundError)):
        oci.merge(
            tmp_path, "debian", platforms, tmp_path / "merged", "1.2.3", "abc"
        )


def test_oci_archive_rejects_path_traversal(tmp_path):
    archive = tmp_path / "bad.tar"
    with tarfile.open(archive, "w") as tar:
        info = tarfile.TarInfo("../escape")
        info.size = 1
        tar.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="Unexpected OCI archive member"):
        load("oci_images").unpack(archive, tmp_path / "layout")
    assert not (tmp_path / "escape").exists()


def test_cache_budget_does_not_delete_other_cache_families_or_branches():
    prune = load("prune_ci_caches")
    base = {
        "ref": "refs/heads/main",
        "size_in_bytes": 100,
        "last_accessed_at": "2026-09-08",
    }
    caches = [
        dict(base, id=1, key="pq-work-musl-s390x"),
        dict(base, id=2, key="slow-pip-loong64"),
        dict(base, id=3, key="docker-cache"),
        dict(base, id=4, key="pq-work-other", ref="refs/heads/feature"),
    ]
    assert prune.victims(caches, budget=100) == [1]


@pytest.mark.parametrize("job", ["binaries-mips", "binaries-loong64"])
def test_source_wheel_cache_is_owned_before_pip_runs(job):
    steps = workflow()["jobs"][job]["steps"]
    build = next(
        step["run"]
        for step in steps
        if step.get("name", "").startswith("Build binary")
    )
    # A cache miss leaves no directory, and a hit restores runner ownership.
    # Both must be handled inside the root container before its first pip.
    create = build.index("mkdir -p /src/.pip-cache")
    own = build.index("chown -R root:root /src/.pip-cache")
    install = build.index("pip install")
    assert build.index("sh -euc '") < create < own < install


def test_compiler_cache_changes_with_toolchain_flags_and_openssl(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    compiler = load("compiler_fingerprint")
    monkeypatch.setattr(compiler.shutil, "which", lambda name: name == "rustc")
    toolchain = {"version": b"compiler version one"}
    monkeypatch.setattr(
        compiler.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=toolchain["version"]),
    )
    monkeypatch.setenv("OPENSSL_DIR", str(tmp_path))
    lib = tmp_path / "lib"
    lib.mkdir()
    archive = lib / "libcrypto.a"
    archive.write_bytes(b"OpenSSL one")
    first = compiler.fingerprint()
    assert compiler.fingerprint() == first
    toolchain["version"] = b"compiler version two"
    second = compiler.fingerprint()
    assert second != first
    monkeypatch.setenv("RUSTFLAGS", "-C target-cpu=arm1176jzf-s")
    third = compiler.fingerprint()
    assert third != second
    archive.write_bytes(b"OpenSSL two")
    assert compiler.fingerprint() != third
