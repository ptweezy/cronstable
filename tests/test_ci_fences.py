"""release.yml keeps its silent-skip fences pointed at live matrix cells.

The web engine differential (tests/test_web_engine_parity.py) self-skips
wherever Chromium is missing, so exactly one tox cell installs the browser
and then re-drives the file, requiring that nothing skipped.  Both steps
gate on hardcoded matrix literals in their ``if:``; if the tox matrix moves
on, those literals go stale, GitHub skips the steps without a word, and the
differential quietly runs nowhere again.  This pins the literals to the
matrix they select from, and pins the enforcement to junitxml counts
instead of a stdout grep.
"""

import fnmatch
import os
import re

# strictyaml's VENDORED ruamel (the same import cronstable.backends.kubernetes
# uses): guaranteed present wherever cronstable is, unlike the standalone
# ruamel.yaml distribution, which is NOT a dependency.
from strictyaml.ruamel import YAML

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "release.yml")
INSTALL_STEP = "Install Chromium for the web engine differential"
ENFORCE_STEP = "Enforce the web engine differential ran (no silent skip)"


def _workflow():
    with open(WORKFLOW, encoding="utf-8") as fobj:
        return YAML(typ="safe").load(fobj.read())


def _tox_job():
    return _workflow()["jobs"]["tox"]


def _named_steps(job):
    return {step["name"]: step for step in job["steps"] if "name" in step}


def test_parity_fence_steps_gate_on_a_live_matrix_cell():
    job = _tox_job()
    steps = _named_steps(job)
    assert INSTALL_STEP in steps, "the Chromium install step is gone"
    assert ENFORCE_STEP in steps, "the enforcement step is gone"
    install = steps[INSTALL_STEP]
    enforce = steps[ENFORCE_STEP]
    # one cell, one pair: if the two conditions diverge, one of the steps
    # silently stops running.
    assert install["if"] == enforce["if"]
    os_match = re.search(r"matrix\.os == '([^']+)'", install["if"])
    py_match = re.search(r"matrix\.python == '([^']+)'", install["if"])
    assert os_match and py_match, install["if"]
    matrix = job["strategy"]["matrix"]
    cells = {(str(o), str(p)) for o in matrix["os"] for p in matrix["python"]}
    cells |= {
        (str(row["os"]), str(row["python"]))
        for row in matrix.get("include", [])
    }
    assert (os_match.group(1), py_match.group(1)) in cells, (
        "the parity fence gates on a matrix cell that no longer exists; "
        "update the two `if:` literals in release.yml"
    )


def test_parity_enforcement_reads_junitxml_counts():
    run = _named_steps(_tox_job())[ENFORCE_STEP]["run"]
    assert "--junitxml" in run
    assert "skipped" in run
    # the old fence grepped stdout for the literal "1 passed", which a
    # second test in the file flips to a false red and which "11 passed"
    # also satisfies.
    assert '"1 passed"' not in run


# --------------------------------------------------------------------------
# Actions cache writers: one 10 GB pool per repository
# --------------------------------------------------------------------------


def _cache_writers():
    """Every step in the workflow that can WRITE to the Actions cache.

    Three shapes: an explicit ``actions/cache/save``, a bare
    ``actions/cache@`` (restore plus an implicit post-job save), and a
    buildx ``cache-to`` naming the ``type=gha`` backend.  Yields
    ``(job_name, label, gate)`` where ``gate`` is the step's ``if:``
    (``""`` when it has none).
    """
    out = []
    for job_name, job in _workflow()["jobs"].items():
        for step in job.get("steps", []):
            uses = str(step.get("uses", ""))
            with_block = step.get("with") or {}
            cache_to = str(with_block.get("cache-to", ""))
            writes = (
                "actions/cache/save" in uses
                or re.match(r"actions/cache@", uses) is not None
                or "type=gha" in cache_to
            )
            if writes:
                label = step.get("name") or uses
                gate = str(step.get("if", "")) + cache_to
                out.append((job_name, label, gate))
    return out


def test_every_actions_cache_write_is_branch_gated():
    # The Actions cache is ONE quota per repository (10 GB), shared by the
    # buildx layer exporter and every other writer, and it evicts
    # least-recently-used across the whole pool. binaries-container alone
    # is 13 matrix rows. A writer that runs on every branch and every fork
    # PR evicts the docker layer cache the build depends on, which is how
    # 1.2.31 broke: the release build hit a `not_found` on an evicted
    # blob. Restores stay ungated on purpose; every branch reads through
    # to what main stored.
    writers = _cache_writers()
    assert writers, "no cache writers found: the detector went stale"
    ungated = [
        (job, label)
        for job, label, gate in writers
        if "refs/heads/main" not in gate
    ]
    assert not ungated, (
        "these Actions cache writers are not gated to main, so they write "
        "from every branch and fork PR against a shared 10 GB quota: "
        "{}".format(ungated)
    )


def test_workflow_names_no_retired_branch():
    # The repository develops on a single trunk. A leftover
    # `refs/heads/develop` condition is dead but silent: it evaluates false
    # forever, so the cache write or wiki publish it guards simply stops
    # happening.
    with open(WORKFLOW, encoding="utf-8") as fobj:
        body = fobj.read()
    assert "refs/heads/develop" not in body, (
        "release.yml still gates on the retired `develop` branch; those "
        "conditions are now permanently false"
    )


def test_release_runs_are_never_cancelled():
    # The release test is spelled twice, in `group` and again in
    # `cancel-in-progress`. Let them drift and a release lands in the
    # cancellable group, where the next push kills it mid-publish. A queued
    # run loses its slot too (cancel-in-progress false protects only a run
    # already in flight), so the split group is the whole protection.
    con = _workflow()["concurrency"]
    group = str(con["group"])
    cancel = str(con["cancel-in-progress"])
    for probe in ("workflow_dispatch", "'[release'", "commits[19]"):
        assert probe in group, "concurrency.group lost {}".format(probe)
        assert probe in cancel, "cancel-in-progress lost {}".format(probe)
    # and it must split the group on that test, so check for both arms
    assert "'release'" in group and "'ci'" in group, group


def test_every_browser_backed_test_module_is_fenced():
    # The enforcement step is the ONLY thing standing between a
    # browser-backed module and running nowhere: these all self-skip
    # without Chromium, which is every environment but the one cell that
    # provisions it. Listing the files by hand let the heat E2E ship
    # outside the fence, so derive the set from disk instead: any module
    # that importorskips playwright must be named in the step.
    tests_dir = os.path.join(ROOT, "tests")
    browser_backed = set()
    myself = os.path.basename(__file__)  # this file names the probe string
    for name in os.listdir(tests_dir):
        if not name.startswith("test_") or not name.endswith(".py"):
            continue
        if name == myself:
            continue
        with open(os.path.join(tests_dir, name), encoding="utf-8") as fobj:
            if 'importorskip("playwright' in fobj.read():
                browser_backed.add(name)
    assert browser_backed, "the playwright detector went stale"
    run = _named_steps(_tox_job())[ENFORCE_STEP]["run"]
    missing = sorted(n for n in browser_backed if n not in run)
    assert not missing, (
        "these browser-backed modules self-skip everywhere and are not "
        "re-driven by the enforcement step, so nothing proves they ever "
        "ran: {}".format(missing)
    )


def test_release_hashes_exactly_what_it_attaches():
    # The SHA256SUMS cp list and the gh-release files: list are both
    # deliberately explicit (a glob would fold the CI-only macos26 assets
    # in), and the attach is one-shot under immutable-release protection,
    # so a name present in one list and missing from the other ships an
    # unhashed asset or a hashed-but-unattached one with no red anywhere.
    steps = _named_steps(_workflow()["jobs"]["release"])
    hashed = set(
        re.findall(r"binaries/\S+", steps["Generate SHA256SUMS"]["run"])
    )
    attached = set(
        re.findall(
            r"binaries/\S+", steps["Create GitHub Release"]["with"]["files"]
        )
    )
    assert hashed == attached, (
        "SHA256SUMS and the release files: list disagree; hashed-only: "
        "{}, attached-only: {}".format(
            sorted(hashed - attached), sorted(attached - hashed)
        )
    )


def test_msi_build_recipe_is_shared_and_pinned():
    # The MSI is built in TWO jobs (the gate build, whose msiexec smoke
    # proves the .wxs semantics, and the signed rebuild). The recipe
    # must be one code path or a release ships a signed MSI built
    # differently from the one the gate proved: both jobs must invoke
    # the shared script without regrowing a private wix invocation, and
    # the script's tool and extension pins must match. A docs-only PR
    # never runs the Windows lane, so pin it here.
    script_path = os.path.join(ROOT, ".github", "scripts", "build_msi.sh")
    with open(script_path, encoding="utf-8") as fobj:
        script = fobj.read()
    tool = re.search(r"^WIX_TOOL_VERSION=(\S+)$", script, re.M)
    ext = re.search(r"^WIX_UTIL_VERSION=(\S+)$", script, re.M)
    assert tool is not None, "build_msi.sh lost its wix tool pin"
    assert ext is not None, "build_msi.sh lost its Util extension pin"
    assert tool.group(1) == ext.group(1), (
        "wix pins moved apart: tool {}, extension {}".format(
            tool.group(1), ext.group(1)
        )
    )
    for job_name, step_name in [
        ("binaries-windows", "Build MSI"),
        ("sign-windows", "Rebuild the MSIs from the signed payload"),
    ]:
        run = _named_steps(_workflow()["jobs"][job_name])[step_name]["run"]
        assert ".github/scripts/build_msi.sh" in run, (
            "{}: no longer builds through the shared script".format(job_name)
        )
        assert "wix build" not in run, (
            "{}: grew a private wix invocation beside the shared "
            "script".format(job_name)
        )


def test_msi_smoke_steps_share_the_msiexec_helpers():
    # The msiexec incantation (MSYS2_ARG_CONV_EXCL, exit 3010 counted
    # as the reboot-required success it is, the log tail on failure)
    # lives once in msi_smoke.sh. A step spelling msiexec by hand
    # regrows the duplicate and its 3010 flake, which in sign-windows
    # fails the release.
    for job_name, step_name in [
        ("binaries-windows", "Smoke-test MSI (install, verify, uninstall)"),
        (
            "binaries-windows",
            "Smoke-test MSI upgrade (remembered properties, restart, reload)",
        ),
        ("sign-windows", "Smoke-test the signed amd64 MSI (install, uninstall)"),
    ]:
        run = _named_steps(_workflow()["jobs"][job_name])[step_name]["run"]
        assert ".github/scripts/msi_smoke.sh" in run, step_name
        assert "msiexec /i" not in run, step_name
        assert "msiexec /x" not in run, step_name


# --------------------------------------------------------------------------
# Windows signing: the signed set must be what actually ships
# --------------------------------------------------------------------------

SIGN_JOB = "sign-windows"
UPLOAD_STEP = "Upload the signed set"
OVERLAY_STEP = "Overlay the signed Windows artifacts"


def test_signed_upload_name_dodges_the_broad_release_download():
    # The release job merges every cronstable-* artifact into one
    # directory, so a signed artifact matching that pattern would leave
    # signed and unsigned copies of the same filenames to fight over
    # download order. The overlay step must also name exactly the
    # artifact the sign job uploads.
    upload = _named_steps(_workflow()["jobs"][SIGN_JOB])[UPLOAD_STEP]
    name = str(upload["with"]["name"])
    release_steps = _named_steps(_workflow()["jobs"]["release"])
    broad = str(release_steps["Download release binaries"]["with"]["pattern"])
    assert not fnmatch.fnmatch(name, broad), (
        "signed artifact name {!r} matches the broad download pattern "
        "{!r}".format(name, broad)
    )
    assert str(release_steps[OVERLAY_STEP]["with"]["name"]) == name


def test_release_overlays_the_signed_set_before_hashing():
    # The overlay must sit after the broad download (or the unsigned
    # set wins) and before SHA256SUMS (or the sums describe bytes that
    # are not attached). Its gate reads the sign job's output, which
    # requires sign-windows in needs; without that the expression is
    # silently empty and a signed release ships unsigned.
    job = _workflow()["jobs"]["release"]
    assert SIGN_JOB in job["needs"]
    names = [step.get("name") for step in job["steps"]]
    assert (
        names.index("Download release binaries")
        < names.index(OVERLAY_STEP)
        < names.index("Generate SHA256SUMS")
    )
    overlay = _named_steps(job)[OVERLAY_STEP]
    assert "needs.sign-windows.outputs.signed" in str(overlay["if"])


def test_signed_set_covers_every_windows_release_asset():
    # A Windows asset added to the release lists must go through the
    # sign job too, and one that stops shipping must leave it, or a new
    # asset ships unsigned beside its signed siblings.
    upload = _named_steps(_workflow()["jobs"][SIGN_JOB])[UPLOAD_STEP]
    signed = {
        os.path.basename(line.strip())
        for line in str(upload["with"]["path"]).splitlines()
        if line.strip()
    }
    release_steps = _named_steps(_workflow()["jobs"]["release"])
    attached = set(
        re.findall(
            r"binaries/(cronstable-windows-\S+)",
            release_steps["Create GitHub Release"]["with"]["files"],
        )
    )
    assert signed == attached, "signed-only: {}, attached-only: {}".format(
        sorted(signed - attached), sorted(attached - signed)
    )


def test_decide_step_env_covers_every_signing_secret():
    # The decide step's env block is the single enumeration of the
    # signing secrets (its shell derives the all-or-none check from the
    # AZURE_* env vars). A secret referenced by a later step but absent
    # from the env block is not gated: the job claims signed=true and
    # then fails mid-release.
    job = _workflow()["jobs"][SIGN_JOB]
    decide = _named_steps(job)["Decide whether to sign"]
    env_keys = set(decide.get("env") or {})
    referenced = set()
    for step in job["steps"]:
        referenced |= set(re.findall(r"secrets\.(AZURE_\w+)", str(step)))
    assert referenced, "no AZURE_ secret references: the detector went stale"
    missing = sorted(referenced - env_keys)
    assert not missing, (
        "signing secrets referenced by sign-windows steps but not gated "
        "by the decide step's env block: {}".format(missing)
    )


def test_refusal_message_assets_are_release_assets():
    # The one-file refusal names the zips, and the names are owned by
    # the release attach list: a rename there must move the message too,
    # or it sends users hunting for a filename no release carries.
    from cronstable import winservice

    release_steps = _named_steps(_workflow()["jobs"]["release"])
    attached = set(
        re.findall(
            r"binaries/(cronstable-windows-\S+)",
            release_steps["Create GitHub Release"]["with"]["files"],
        )
    )
    for asset in winservice.ONEDIR_RELEASE_ASSETS:
        assert asset in attached, (
            "the refusal message names {}, which the release does not "
            "attach".format(asset)
        )


def _sign_action():
    path = os.path.join(ROOT, ".github", "actions", "sign-artifacts", "action.yml")
    with open(path, encoding="utf-8") as fobj:
        return YAML(typ="safe").load(fobj.read())


def _sign_attempts():
    """The signing calls inside the local retrying action, in order."""
    return [
        step
        for step in _sign_action()["runs"]["steps"]
        if "artifact-signing-action" in str(step.get("uses", ""))
    ]


def test_every_signing_step_carries_a_timestamp():
    # Artifact Signing rotates its leaf certificates within days and
    # the signing action does not timestamp by default, so an
    # untimestamped signature dies with its certificate. Every attempt
    # must pin both timestamp inputs.
    attempts = _sign_attempts()
    assert attempts, "no signing steps found: the detector went stale"
    for step in attempts:
        with_block = step.get("with") or {}
        assert with_block.get("timestamp-rfc3161"), step.get("name")
        assert with_block.get("timestamp-digest"), step.get("name")


def test_signing_goes_through_the_retrying_action():
    # A call site that reaches for the vendor action directly gets no
    # retry, so one gallery blip fails the release again.
    for step in _workflow()["jobs"][SIGN_JOB]["steps"]:
        assert "artifact-signing-action" not in str(step.get("uses", "")), (
            "{}: signs outside the retrying local action".format(
                step.get("name")
            )
        )
    call_sites = [
        step
        for step in _workflow()["jobs"][SIGN_JOB]["steps"]
        if str(step.get("uses", "")) == "./.github/actions/sign-artifacts"
    ]
    assert call_sites, "no signing call sites: the detector went stale"


def test_signing_retries_a_transient_failure():
    # The vendor action reinstalls its module from the PowerShell
    # gallery on every call, and a gallery blip fails the step before a
    # byte is signed (a 2026-08-17 release run died on "Unable to find
    # repository 'PSGallery'" and a plain re-run signed fine). So the
    # ladder must keep more than one attempt, every attempt but the
    # last must be non-fatal, and the last must NOT be: an exhausted
    # ladder has to fail the release rather than ship unsigned bytes
    # under a green check.
    attempts = _sign_attempts()
    assert len(attempts) > 1, "the signing retry ladder lost its retries"
    for step in attempts[:-1]:
        assert step.get("continue-on-error") is True, step.get("name")
    assert not attempts[-1].get("continue-on-error"), (
        "the last signing attempt swallows its own failure, so an "
        "exhausted ladder would ship unsigned"
    )
    # Each retry runs only when the attempt before it failed, so a
    # first attempt that signs costs nothing.
    for previous, step in zip(attempts, attempts[1:]):
        gate = "steps.{}.outcome".format(previous["id"])
        assert gate in str(step.get("if", "")), step.get("name")


def test_signing_retries_repeat_the_same_call():
    # One call written three times: a `with` or a version that drifts
    # between attempts means the retry signs on terms the first attempt
    # was never given.
    attempts = _sign_attempts()
    for step in attempts[1:]:
        assert step["uses"] == attempts[0]["uses"], step.get("name")
        assert step["with"] == attempts[0]["with"], step.get("name")


def _matrix_images():
    """(job, image) for every container base image the matrices name."""
    out = []
    for name, job in _workflow()["jobs"].items():
        for entry in (
            job.get("strategy", {}).get("matrix", {}).get("include", []) or []
        ):
            if "image" in entry:
                out.append((name, entry["image"]))
    return out


def test_every_container_base_image_is_pinned_to_a_distro_release():
    # The libc floor of a container-built binary is a property of the base
    # image, so a floating tag makes it drift on its own: `python:3.14-alpine`
    # walked from Alpine 3.19 to 3.24 and took the musl floor from 1.2.4 to
    # 1.2.6 with it, with CI green throughout because the smoke test runs on
    # the same image that raised it.  Every base must therefore name a distro
    # release, not just a Python version.
    floating = []
    for job, image in _matrix_images():
        _, _, tag = image.rpartition(":")
        if tag == image or tag in ("latest", ""):
            floating.append((job, image))
            continue
        if image.startswith("python:") and not re.search(
            r"(alpine\d+\.\d+|slim-[a-z]+)$", tag
        ):
            floating.append((job, image))
    assert not floating, (
        "these base images float, so the libc floor moves without an edit: "
        "{}".format(sorted(floating))
    )


def _declared_floors():
    """arch -> the glibc floor its build lane declares."""
    floors = {}
    for job in _workflow()["jobs"].values():
        for entry in (
            job.get("strategy", {}).get("matrix", {}).get("include", []) or []
        ):
            if entry.get("floor") and entry.get("arch"):
                floors[entry["arch"]] = str(entry["floor"])
    return floors


def test_package_dependencies_declare_the_floor_the_lane_enforces():
    # The .deb/.rpm `libc6 >= X` dependency is the only thing that stops a
    # package installing on a host the binary cannot start on, and it is
    # spelled in a second place: build_packages.sh's table.  elf_floor.py
    # enforces the workflow's number against the actual bytes, so if the two
    # disagree the packages promise something nothing checks.
    script = os.path.join(ROOT, ".github", "scripts", "build_packages.sh")
    with open(script, encoding="utf-8") as fobj:
        body = fobj.read()
    rows = re.search(r'ROWS="\n(.*?)\n"', body, re.S)
    assert rows, "build_packages.sh no longer carries a ROWS table"
    packaged = {}
    for line in rows.group(1).splitlines():
        fields = line.split()
        if len(fields) == 4:
            packaged[fields[0]] = fields[3]
    assert packaged, "the ROWS table parsed to nothing"

    declared = _declared_floors()
    mismatched = {
        arch: (floor, declared.get(arch))
        for arch, floor in packaged.items()
        if declared.get(arch) != floor
    }
    assert not mismatched, (
        "packaged floor != the floor the build lane declares and elf_floor.py "
        "enforces, as {arch: (packaged, lane)}: {}".format(mismatched)
    )
