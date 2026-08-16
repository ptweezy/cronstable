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
            "Smoke-test MSI upgrade (remembered properties, restart)",
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


def test_every_signing_step_carries_a_timestamp():
    # Artifact Signing rotates its leaf certificates within days and
    # the signing action does not timestamp by default, so an
    # untimestamped signature dies with its certificate. Every signing
    # step must pin both timestamp inputs.
    steps = [
        step
        for step in _workflow()["jobs"][SIGN_JOB]["steps"]
        if "artifact-signing-action" in str(step.get("uses", ""))
    ]
    assert steps, "no signing steps found: the detector went stale"
    for step in steps:
        with_block = step.get("with") or {}
        assert with_block.get("timestamp-rfc3161"), step.get("name")
        assert with_block.get("timestamp-digest"), step.get("name")
