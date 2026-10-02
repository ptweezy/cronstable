# Release pipeline

This page documents cronstable's CI and release pipeline: what runs on every commit, how a release is triggered, what it publishes, and the procedures for code signing, WinGet submission, and Intel Mac support. Its instructions are for the maintainer, who holds the release credentials. Contributors can open a pull request without reading it; that workflow is in [CONTRIBUTING.md](https://github.com/ptweezy/cronstable/blob/main/CONTRIBUTING.md).

## CI for every commit

The `.github/workflows/release.yml` workflow (named `CI`) runs on every `push` (any branch) and every `pull_request`. The separate `nightly` workflow runs scheduled checks. The required test matrix starts when the static checks pass. Once package checks and source preflight pass, binary and image builds run alongside those tests. Publication waits for every required test and artifact check. Advisory Nix builds and experimental coverage wait for the required Python matrix to finish, preserving workers for supported-platform tests without serializing the release builds behind the full suite. They still run when a required test fails, so independent failures remain visible, but stop on cancellation or a failed preflight. Only a release (described later) proceeds to publish.

Linux jobs use Ubuntu 26.04, except the emulated FreeBSD ARM64 host, which uses Ubuntu 24.04. The FreeBSD ARM64 guest fails to boot under the Ubuntu 26 QEMU and firmware stack, before any build command runs. The single `binaries-freebsd` runner expression records this exception. Remove it after verifying the ARM guest boots and builds on Ubuntu 26; the FreeBSD version and published target do not change.

Native `linux/386` binary, wheel, and image jobs use `.github/actions/setup-docker` before starting containers or BuildKit. It replaces runner Docker versions older than 29.4.3 with a pinned patched engine: 29.4.2's `socketcall` restriction breaks 32-bit networking. Newer runner engines pass through unchanged. Keep container security profiles enabled. APT index refreshes use `APT::Update::Error-Mode=any`, so a partial DNS/download failure reaches the existing retry logic instead of producing misleading package-not-found errors.

Container refreshes accept only immutable release commits that are ancestors of the trusted workflow commit on `main`. Selection and the reusable builders validate this before checking out or executing the released source. A free-form ref, an unmerged branch, or an override outside a refresh fails closed. Validation code runs from the workflow checkout; refresh jobs still avoid saving compiler and image caches.

The `tox-static` job runs `tox -e lint,mypy,bandit,openapi` on Ubuntu. The `tox` matrix runs Python 3.10–3.14 on Linux, Windows, and macOS. It also tests Linux ARM64 with Python 3.10 and 3.14, and Windows ARM64 with Python 3.14. Each runner selects both OS profiles with `tox -e py-windows,py-posix`; the profile that doesn't match the OS skips. Experimental Linux jobs test Python 3.15 and the free-threaded Python 3.14 build without blocking releases. Separate `tox-mindeps` and `backends-live` jobs check minimum dependency versions and real backend servers.

`pyproject.toml` owns dependency declarations. After a dependency update, run `python scripts/generate_build_files.py`; `tox-static` checks that the derived files match before dependent builds start. When Dependabot updates `requirements/min.txt`, first promote the accepted pins to the corresponding declarations in `pyproject.toml`, including matching extras, then regenerate. The Nix flake reads its build and runtime dependencies from the same declarations. Commit `flake.lock` so reruns use the same reviewed toolchains; refresh inputs with `nix flake update`, then evaluate all systems and build on both native CI targets. Its small set of source overrides in `nix/python-packages.nix` covers packages that nixpkgs has not updated yet, keeps the upstream build recipes without their test suites, and automatically yields to newer nixpkgs versions. Intel macOS uses the supported 26.05 toolchain and borrows newer upstream Python recipes when required by those declarations. The advisory `nix` matrix waits for both `preflight` and the required `tox` matrix, so it cannot occupy a macOS worker ahead of a required Python test. It evaluates all four advertised systems, builds on Linux and Intel macOS, checks runtime requirements and imports, and smoke-tests the installed CLI.

Alongside the tests, the same run builds every release artifact at the computed version (all the PyInstaller binaries, the wheel + sdist) and does a **build-only pass over every Docker image** (the `docker` job, all 8 distros at their full published arch sets, no push), so a broken `Dockerfile` fails CI before a release. On an ordinary commit the version is the natural `setuptools_scm` dev version. No **software** is published, pushed, tagged, or signed. The lone exception is documentation: the `wiki` job publishes `wiki/` to the GitHub wiki whenever it changes on `main` (see [editing the wiki](https://github.com/ptweezy/cronstable/blob/main/CONTRIBUTING.md#editing-the-wiki)). See [production and container deployment](Production-Deployment).

## Releasing

`.github/workflows/release.yml` fully automates releases. You never edit a version by hand; `setuptools_scm` derives the version from git tags (`version_file = "cronstable/version.py"`).

### Triggering a release

A release runs when **either**:

1. A **push to `main`** in which **any** commit introduced by the push has a release marker at the **start of its subject line**, not only the tip commit. The scanned range is `BEFORE..AFTER` (the commits new in the push). On a brand-new branch where `BEFORE` is all-zeros (or unresolvable), it falls back to the tip commit only.
2. A **manual `workflow_dispatch`** run, choosing the bump level (`patch` default, `minor`, `major`) or an exact `tag` (X.Y.Z), which wins over the bump. The same form has a `perf` dropdown (`gate` default, `accept`, `ignore`) that overrides the performance gate; see [performance benchmarks](Performance-Benchmarks#overriding-the-gate).

Valid markers (case-insensitive; the bump level is optional):

| Marker | Bump | 1.0.5 → |
| --- | --- | --- |
| `[release]` | minor | 1.1.0 |
| `[release:major]` | major | 2.0.0 |
| `[release:minor]` | minor | 1.1.0 |
| `[release:patch]` | patch | 1.0.6 |

If several pushed commits carry a marker, the **latest such commit wins**. A bare `[release]` counts as minor.

The `version` job's decide step performs the marker match with `grep -oiE '^\[release(:(major|minor|patch))?\]'` over the commit **subject lines** (`git log --pretty=%s`), taking the newest matching commit.

> **Why subjects only, anchored:** a substring match over whole commit messages would let a commit *body* that only discusses the bare `[release]` marker out-bump an explicit `[release:patch]`. The trigger scans subject lines only, and a marker counts only when it begins the subject line. File contents are never scanned (this page can name the markers freely).

### What the pipeline does

The `release.yml` jobs run in dependency order. Top-level `permissions` default to `contents: read`. Only these jobs opt up to the write scopes they need: the `release` job (`contents: write` + `id-token: write`), the `preflight` and `sign-windows` jobs (`id-token: write`, for OIDC to PyPI/Azure), the best-effort `compiler-cache-budget` job (`actions: write`, limited by its script to its own cache families), the `docker-push` job (`packages: write`), and the `wiki` job (`contents: write`). `version` and the whole build+test gate run on **every** event. Only the signing, preparation, and publish jobs (`sign-windows`, `release-prepare`, `release`, `docker-push`, `homebrew`, and `winget`) are guarded by `needs.version.outputs.release == 'true'`. The `wiki` job (10) is the one exception to "no software is published on an ordinary commit": it publishes documentation, so it is gated on the branch rather than on a release, and on nothing else.

1. **`version`, the decide step**: determines `release` (true/false) and `bump`. Trigger logic lives in a real shell script rather than a fuzzy `contains()` expression. It releases **only** on a `workflow_dispatch` or a push to `main` carrying a marker (a marker on any other branch, or in a PR, never releases). The same scan resolves the perf gate override (`perf`: `gate`, `accept` or `ignore`) from the `perf` dispatch input and the `[perf:accept]` / `[perf:ignore]` subject markers, strongest request winning, so the `perf` and `release` jobs read one answer.
2. **`version`, the compute step**: computes the version once, so every builder (and the publish job) use the same number. On a release it uses the dispatch `tag` input when set. Otherwise it finds the latest tag matching `^[0-9]+\.[0-9]+\.[0-9]+$` (with `git tag -l | … | sort -V | tail -n1`, defaulting to `0.0.0`) and applies the bump. Either way it **refuses with an error if the tag already exists** (`refs/tags/$new`). Otherwise it emits the natural `setuptools_scm` dev version for the build-only run. The job also expands the one Docker distro matrix (`.github/docker-matrix.json`) into platform build groups and the distro manifests used for assembly and publication.
3. **Checks before builds**: `tox-static` runs lint, mypy, bandit and OpenAPI validation concurrently, plus actionlint for the workflows. Once static checks pass, stable **`tox`**, **`tox-mindeps`**, **`backends-live`**, and **`dist`** run in parallel. Every required test row finishes even if a sibling fails, so one run reports the platform failures together. `dist`'s success (with `tox-static`) unlocks **`preflight`**, which checks release version/MSI limits, required credentials, GitHub token access, PyPI Trusted Publishing token exchange, Azure login and optional Docker Hub login without publishing software. Release preflight requires `HOMEBREW_TAP_TOKEN` (push access to `ptweezy/homebrew-tap`), `WINGET_TOKEN` (a classic PAT with `public_repo`), and the six Azure signing secrets. `RELEASE_TOKEN`, macOS signing, and Docker Hub (`DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN`) are optional but must be complete if set; without `RELEASE_TOKEN`, the publishing job uses its `GITHUB_TOKEN`. On every event, preflight resolves fresh compatible zeroconf and cryptography sources from the canonical `pyproject.toml` requirements, checks their PyPI SHA256 digests, exercises the real package recipes with small fixture payloads, and renders sample Homebrew/Scoop manifests. Every binary and Docker image receives the same zeroconf version as the source offer. The dependency-license gate uses the resolved source version. Advisory Python 3.15 and free-threaded coverage runs separately in **`tox-experimental`** and cannot delay publication.
4. **Binary builds** (these and the image builds need only `preflight` and run alongside the test matrix, whose rows gate `release`; the publish jobs need them, so a broken build fails the run instead of producing a half-finished release). Each job installs PyInstaller from the pin in `pyinstaller/build-requirements.txt`, installs the project to bake `SETUPTOOLS_SCM_PRETEND_VERSION` (the computed version) into `cronstable/version.py`, runs `pyinstaller pyinstaller/cronstable.spec`, and smoke-tests the bundle with `dist/cronstable --version`. The **runner-native** jobs (`binaries-macos`, `binaries-windows`) install with **uv** (`uv venv` + `uv pip install` + `uv run pyinstaller`, using `astral-sh/setup-uv`). Every **container** job stays on **pip** inside its `docker run` containers, because uv's official image is amd64/arm64 only and it publishes no musl `ppc64le`/`s390x` wheels; pip is the arch-portable choice there:
   - **`binaries`**: the Linux **glibc** rows, built **inside a manylinux container** against a [python-build-standalone](https://github.com/astral-sh/python-build-standalone) interpreter, which is what puts the glibc floor at 2.17 rather than at the runner's libc. `amd64`/`amd64v3` and `arm64` build natively on `ubuntu-26.04` and `ubuntu-26.04-arm`; `ppc64le`, `s390x` and `armv7` build under QEMU. The container is load-bearing: pip derives manylinux compatibility from the **running** glibc, so on a bare runner it selects newer wheels and pins the floor there whatever the interpreter was built against. The image's own `/opt/python` interpreters are unusable, being configured `--disable-shared`, which PyInstaller cannot freeze from. `armv7` has no manylinux2014 image and builds on `manylinux_2_31_armv7l`, declaring glibc 2.31. `ppc64le` builds on `manylinux_2_28` (floor 2.28), because its only cryptography wheel carries that tag. Artifacts `cronstable-linux-<arch>`.
   - **`binaries-container`**: the **musl** rows plus the two glibc rows manylinux cannot serve, one matrix row each, built **inside a `docker run --platform` container** (PyInstaller is not a cross-compiler and the runners are glibc; checkout/upload stay on the host). The musl rows build on `python:3.14-alpine3.23` and cover `amd64`, `amd64v3`, `arm64`, `i686`, `armv7`, `armv6`, `ppc64le`, `s390x` and `riscv64` (artifacts `cronstable-linux-<arch>-musl`). The glibc rows are `i686` on `python:3.14-slim-bookworm` (python-build-standalone publishes no i686 Linux target) and `riscv64` on `python:3.14-slim-trixie` (bookworm has no riscv64 port). `armv6` is **musl-only** (Debian/glibc ships no arm32v6).

     Every base image is pinned to an explicit distro release rather than a floating `python:3.14-slim`/`-alpine` tag, because the libc floor of a container-built binary is a property of the base: a floating Alpine tag that moves from 3.19 to 3.24 raises the binary's musl requirement from 1.2.4 to 1.2.6. `tests/test_ci_fences.py` holds every base to a dated tag.

     `amd64`/`amd64v3`/`i686` run natively on `ubuntu-26.04` and `arm64` on `ubuntu-26.04-arm`; the rest run under QEMU (`docker/setup-qemu-action`). Each row installs its libc's C toolchain plus libffi/zlib headers (the spec strips on POSIX; the headers cover the deps that ship no wheel for that libc/arch and compile from sdist, notably the i686 aiohttp stack and the whole C-ext stack on `armv6`), and persists pip's cache per arch and libc with `actions/cache`, so the slow QEMU source builds carry over between runs instead of recompiling on every push.
   - **`binaries-arm-legacy`** and **`binaries-armel`**: separate jobs for the two legacy 32-bit ARM ABIs, glibc hard-float `armv6` (Raspberry Pi 1, Zero, Zero W) on `tianon/raspbian:bookworm-slim` and glibc soft-float `armel` (Kirkwood: SheevaPlug, QNAP TS-x1x, DNS-320, NSA325, Pogoplug) on a digest-pinned `arm32v5/debian:bookworm-slim`. Only the ARMv6 job waits for its optional cryptography wheel; armel starts independently. Neither ABI has a base image in any family the other lanes use: Debian's armhf port is ARMv7, so `library/debian` and `python:3.14-*` cannot supply an ARMv6 base at all, and python-build-standalone publishes no ARMv6 target either, so Python comes from apt (3.11) as it does on MIPS. Both rows set `_PYTHON_HOST_PLATFORM` (without it, pip installs `armv7l` wheels, because an emulated ARMv6 container reports `armv7l` from `uname`) and pin `QEMU_CPU` to the real target core, so an instruction the hardware lacks faults during the build. The `armel` base is frozen: the bookworm official-images definition has no arm32v5 entry, and `bookworm-security` carries no armel component, so that row builds on an OS layer nobody will patch again.
   - **`binaries-mips`** and **`binaries-loong64`**: the two ports with no `python:3.14-*` image of their own, each compiled from source under emulation. MIPS builds on `mips64le/debian:bookworm-slim`, pinned to a `snapshot.debian.org` slice because bookworm's LTS phase carries no mips64el and its live index will be deleted on an unannounced date. LoongArch builds both libcs from third-party images (`ghcr.io/loong64/python` and `ghcr.io/loong64/alpine`), targeting the new-world ABI upstream Debian and Alpine use.
   - **`binaries-openbsd`**, **`binaries-netbsd`**, **`binaries-illumos`**: amd64 and amd64v3 binaries, built in a KVM-accelerated virtual machine of that system (`vmactions/*-vm`), since no runner offers one. OpenBSD is release-locked (7.9), so its `release:` input needs bumping every six months. illumos builds on OmniOS r151054 LTS and the result runs anywhere the illumos ABI does.
   - **`binaries-macos`**: macOS, `arm64` on `macos-15` (Apple Silicon) and `amd64`/`amd64v3` on `macos-15-intel`. Built on Python 3.14. After the smoke test it asserts the native arch with `file`, so Rosetta cannot let a mislabeled x86_64 build pass on the arm64 runner. Artifacts `cronstable-macos-arm64`, `cronstable-macos-amd64`, and `cronstable-macos-amd64v3` are the **shipped** trio (signed + notarized on a release, described later). A separate `binaries-macos-experimental` job builds macOS 26 (Tahoe) `arm64`/`amd64`/`amd64v3` rows as **CI-only** coverage (`ship: false`, `continue-on-error` so a flaky Tahoe build never blocks a release). Those upload as `cronstable-macos26-{arch}` and are neither signed nor attached to the Release.
   - **`binaries-windows`**: Windows, `amd64`/`amd64v3` and `i686` on `windows-latest` and `arm64` on the `windows-11-arm` runner (all native; PyInstaller is not a cross-compiler). Built on Python 3.14 with the same PyInstaller pin and `dist/cronstable.exe --version` smoke test as the others. Any C-extension dep lacking a `win_arm64` wheel compiles from sdist with the runner's Visual Studio ARM64 toolchain.

     The same invocation also emits the one-directory layout (`CRONSTABLE_BUNDLE=both`, one shared Analysis). The job smoke-tests `dist/cronstable/cronstable.exe`, proves it against the real SCM (`service install`, `status`, `remove` under a CI-scoped name; the hosted runners are elevated, and the zip proof never starts a service), and zips the directory as `cronstable-windows-<arch>.zip`. It then builds the MSI from `packaging/msi/cronstable.wxs` with the shared `.github/scripts/build_msi.sh` (WiX v6 as a pinned .NET tool, not preinstalled on either runner), smoke-tests it with a real `msiexec` install and uninstall, asserting the registered service's exact ImagePath and recovery actions in between, then drives a real major upgrade with a custom `CONFIGDIR`, asserting the directory survives and the upgrade starts the service.

     On a release, the `sign-windows` job signs and repackages all twelve artifacts (see [Windows signing](#windows-signing-azure-artifact-signing)). Release preflight requires the signing secrets; ordinary CI builds are unsigned. Artifacts `cronstable-windows-{amd64,amd64v3,arm64,i686}.exe`, `.zip` and `.msi`. See [running on Windows](Running-on-Windows) and [Windows MSI](Windows-MSI).

   Every glibc row ends with `.github/scripts/elf_floor.py`, which unpacks the frozen bundle, parses `.gnu.version_r` across the bootloader and every embedded shared library, and fails the job when the highest `GLIBC_x.y` exceeds the floor that row declares. On 32-bit ARM it additionally reads each object's float ABI from `e_flags` and its `Tag_CPU_arch` and `Tag_FP_arch` from `.ARM.attributes`, and fails on the wrong ABI or on anything needing a newer core than the row targets. No functional test can catch either failure. An `armv6` bundle that carries ARMv7, VFPv3 object code from `armv7l` wheels passes every functional test, because emulators execute those instructions and only the target hardware faults. The smoke test also runs on a libc newer than the binary needs, so a dependency that starts publishing a higher-tagged wheel would raise the floor with CI green throughout. The same step rejects an executable stack, which ships fine and then dies on SELinux-hardened hosts. The `mips64le` row is exempt from the exec-stack check. Debian's mips64el port configures glibc for an executable stack, because the kernel FPU emulator runs floating-point branch delay slots out of line on the user stack. Every object on that port declares one, so no build there passes. `tests/test_ci_fences.py` holds these floors to the ones the `.deb`/`.rpm` dependencies promise.

5. **`perf`**: the paired performance benchmark (see [performance benchmarks](Performance-Benchmarks)). It installs this commit and the latest release tag into separate virtualenvs, runs the suite in `benchmarks/` against both, interleaved on one runner, and diffs the two with `benchmarks/compare.py`. On a release, a regression past a metric's declared limit fails the gate; on an ordinary commit or PR the same comparison only warns. A `[perf:accept]` marker at the start of a pushed commit subject acknowledges an intentional regression (reported, not gating). Under `perf=ignore` (the dispatch input or a `[perf:ignore]` marker) the comparison is warnings only and the job runs with `continue-on-error`, so nothing it finds, and no failure of its own, holds the release; the notes then open with a line naming the override, and a report that never uploaded is noted rather than fatal. The job's `perf-report` artifact holds `perf-chart.svg`, `perf-summary.md` and `perf-results.json`; the `release` job appends the summary to the notes and attaches `perf-summary.md` and `perf-results.json` as assets.
6. **Preparation and `release`**: `packages` builds and validates the real Linux `.deb`, `.rpm` and `.apk` files as soon as its Linux inputs finish. `release-prepare` gathers the wheel/sdist and all shipped binaries, overlays the signed Windows files, downloads the packages, generates `SHA256SUMS`, renders Scoop and Homebrew manifests, includes the already-downloaded zeroconf source offer, and prepares the release notes and performance report. The `dist` job also installs its wheel into a clean virtualenv outside the checkout and checks the CLI and packaged resources. Each binary producer requires every declared artifact to exist and be nonempty before uploading, including native packages such as FreeBSD `.pkg`.

   `release` waits for preparation, every required build/test/performance gate, and `docker-assemble`. Experimental macOS/Python jobs, optional Nix coverage and cache maintenance are outside its dependency graph. Release matrices cancel remaining rows on failure; ordinary CI retains complete diagnostics. The job downloads the prepared release and rechecks its checksums, publishes the wheel/sdist to PyPI with Trusted Publishing (`skip-existing: true`), creates and pushes the annotated tag using optional `RELEASE_TOKEN` or the job’s `GITHUB_TOKEN` (`contents: write`), then creates the GitHub Release with the complete prepared asset set. No packaging or dependency-source resolution remains after the PyPI upload. External publishing services can still fail at this stage.
7. **`docker-push`**: after `release`, copies the validated OCI images to GHCR and optional Docker Hub without rebuilding. Publish matrices set `fail-fast: false` so a registry failure does not cancel other destinations.
8. **`homebrew`**: after `release`, downloads the prepared formula and pushes it to the tap.
9. **`winget`**: after `release` and `sign-windows`, downloads the published installers and `SHA256SUMS`, verifies them against the scanned hashes, and submits the validated manifests to winget-pkgs with `wingetcreate submit`. See [WinGet submission and Defender failures](#winget-submission-and-defender-failures).
10. **`wiki`**: publishes [`wiki/`](https://github.com/ptweezy/cronstable/tree/main/wiki) to this repository's GitHub wiki. It is **not** a release job and **not** gated on the build+test matrix: a wiki page is not a build artifact, so it neither waits for a release nor lets a flaky emulated arch delay a typo fix. It runs on **every push to `main`** and on nothing else. See [editing the wiki](https://github.com/ptweezy/cronstable/blob/main/CONTRIBUTING.md#editing-the-wiki).

Because no file is committed back to *this* repo, a release never re-triggers the workflow. Four jobs do push elsewhere. On a release, `homebrew` pushes to the tap, `winget` submits manifests to winget-pkgs with `wingetcreate submit`, and `docker-push` copies images to the container registries. On a `main` commit, `wiki` pushes to the wiki. None of these targets is this repository, so a push to any of them raises no event here. Because the tag is created **after** publishing, a failed publish leaves no orphan tag and a re-run cleanly retries the same version.

### macOS signing and notarization

The macOS binaries are Developer ID signed (hardened runtime) and notarized **when the signing secrets are configured**. If all are absent, the release ships unsigned macOS binaries. A partially configured set fails preflight. When configured, each shipped macOS lane imports the certificate, signs a small probe with the configured identity, and checks notarization authentication before compiling. The secrets are `MACOS_CERT_P12_BASE64`, `MACOS_CERT_PASSWORD`, `MACOS_SIGN_IDENTITY`, `MACOS_NOTARY_KEY_BASE64`, `MACOS_NOTARY_KEY_ID`, `MACOS_NOTARY_ISSUER_ID`.

Signing imports the cert into a throwaway randomly-keyed keychain, signs with `codesign --options runtime --timestamp --entitlements pyinstaller/entitlements.plist`, verifies, then notarizes with `xcrun notarytool submit … --wait`. Because a one-file binary cannot be stapled, notarization publishes the ticket online and Gatekeeper validates on first run, so end users do not need `xattr -d com.apple.quarantine`.

`pyinstaller/entitlements.plist` enables the three hardened-runtime entitlements a PyInstaller one-file binary needs (`com.apple.security.cs.allow-unsigned-executable-memory`, `…allow-jit`, `…disable-library-validation`) so the unpacked CPython runtime can load and execute its embedded `.so`/`.dylib` files.

### Windows signing (Azure Artifact Signing)

On a release, the `sign-windows` job Authenticode-signs the Windows assets with Azure Artifact Signing. Release preflight requires all signing secrets and checks Azure login before the expensive builds start. The secrets are `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_SUBSCRIPTION_ID` (OIDC federation with `azure/login`; no client secret exists anywhere), plus `AZURE_SIGNING_ENDPOINT`, `AZURE_SIGNING_ACCOUNT` and `AZURE_SIGNING_PROFILE`. All six are required together.

The job runs on the x64 runner because the signing client does not support Windows ARM runners. Authenticode is architecture-agnostic, so one runner signs every Windows variant. It signs the one-file exes and each zip's inner `cronstable.exe`, re-zips, rebuilds all MSIs from the signed payload with the same shared build script the gate used (`.github/scripts/build_msi.sh`), signs those, verifies every signature, and installs and uninstalls the signed amd64 MSI for real. The `release-prepare` job then overlays the signed set before `SHA256SUMS`, so the sums, the Release assets, and the winget manifests describe the signed bytes. Every signature carries an RFC 3161 timestamp because Artifact Signing rotates its leaf certificates within days. `tests/test_ci_fences.py` pins the wiring.

The job wraps the signed amd64, amd64v3, and arm64 MSIs in setup executables
using `.github/scripts/build_setup.sh`. It signs each detached Burn engine,
reattaches it, and signs the complete bundle. The engine signature identifies
the publisher during elevation, including repair and uninstall. The signing
job also installs and uninstalls both signed x64 setup executables. WinGet
uses the baseline amd64 bundle and the arm64 bundle.

A signing failure that survives three attempts fails the release. Repair the credentials or resolve the signing-service failure before retrying; removing a secret fails preflight and cannot bypass the signed-installer requirement.

### WinGet submission and Defender failures

Configure Windows signing before you submit a release to WinGet. On signed
releases, `sign-windows` validates the amd64 and arm64 MSIs and setup bundles
as soon as Windows builds and signing finish, while other platform builds run. It
records the signed files' SHA256 hashes, verifies their timestamped Authenticode
signatures, and updates Microsoft Defender's signatures before scanning each
MSI, bundle, and their extracted payloads. WiX extracts both architectures as
data and uses the build script's version pin. The inner `cronstable.exe` and
the detached Burn engine also require valid timestamped signatures. Each
bundle's embedded MSI must match the standalone release MSI byte for byte.

The signing job updates Defender signatures with `MpCmdRun.exe -SignatureUpdate -MMPC`,
which downloads from Microsoft's update service. It makes up to three attempts,
waiting 5 seconds before the second attempt and 10 seconds before the third.
Each attempt checks Defender's readiness. If all attempts fail, the job stops
before scanning. You can inspect the update commands, exit codes, and retry
messages in `winget-validation/defender.log`.

The scan uses `-DisableRemediation` to preserve detected files for inspection.
Detections fail the job. Missing Defender, a signature update failure, or a scan
error also fails the signing job and blocks release publication. The other
build jobs continue independently. The build and signing jobs test MSI
installation and uninstallation. Each native Windows build also launches the
installed executable with no arguments in a temporary, unconfigured profile.
The signing job repeats this check for its x64 MSIs. The check requires setup
guidance, usage help, and exit `0`, with no stderr output or configuration
creation. It also verifies that `--validate-config` exits `1` for that missing
configuration. Each process has a 30-second timeout.

The manifest renderer reads product identities and hashes from the validated
bundle and MSI metadata. It sets the installer type to `burn` and the scope to
`machine`. Each manifest includes both installed package types so WinGet can
upgrade an MSI installation through the setup executable.
`sign-windows` runs `winget validate` and saves the manifests with the scan
metadata. You can find these files and the scan and extraction logs in the
`winget-validation` Actions artifact for 14 days, including on failure.

After publication, the `winget` job downloads the release MSIs, setup bundles,
and `SHA256SUMS` and verifies them against the scanned hashes. It submits the
validated manifests with `wingetcreate submit`. Release downloads, GitHub submission,
and Microsoft's upstream validation depend on published assets and run at
this stage. Missing signing credentials fail preflight; the `winget` job also
requires signed installers as a final check.

For `Validation-Executable-Error`, open the validation artifact linked from
the PR's **Validation Completed** check. Inspect `ExeRunInfo` in
`InstallationVerification_Result.json` for the executable's output and exit
code. WinGet launches installed executables without arguments; a fresh
cronstable installation shows setup help and exits successfully. The MSI
smoke checks exercise this launch before publication.

Defender can flag a signed MSI or its payload. If validation fails, follow the
[Microsoft validation guide](https://github.com/microsoft/winget-pkgs/blob/master/doc/ValidationFailureGuide.md).
Record the asset URL, SHA256, detection name, and scan engine and signature
versions. Submit the exact flagged files to
[Microsoft's file analysis portal](https://www.microsoft.com/en-us/wdsi/filesubmission)
for false-positive analysis.
Request revalidation on the affected PR after Microsoft resolves the detection.
If you cannot reproduce the detection, ask the WinGet maintainers to investigate.

Keep Defender enabled and preserve the files at each release URL. If you change
a binary, publish a new release with matching manifest hashes. Rebuilding to
change a hash leaves the detection unresolved. A clean local scan provides
evidence for your report; the upstream PR requires revalidation or a validated
MSI manifest replacement.

### Release notes

The "Build release notes from HISTORY.md" step extracts this version's section from `HISTORY.md` into `release-notes.md`: everything between its `## X.Y.Z (…)` header and the next `## ` header, with leading blank lines stripped. If there is no matching section it warns and the body is auto-generated only. The Release uses that section as `body_path` with `generate_release_notes: true` (the curated notes are prepended above GitHub's auto-generated "What's Changed" / compare link). Head [HISTORY.md](https://github.com/ptweezy/cronstable/blob/main/HISTORY.md) entries `## X.Y.Z (date)`: the matcher (`$1 == "##" && $2 == ver`) accepts `## X.Y.Z` with or without a date and keeps 1.2.1 from matching 1.2.10.

### amd64v3 runtimes

Downloads present `amd64v3` as **Recommended for compatible CPUs** and `amd64`
as the **Compatibility build**. The labels are presentation only. The
`amd64` asset URLs, the Docker tags without an `-amd64v3` suffix, and the
Homebrew, Scoop, and winget defaults all resolve to the baseline build.

Every amd64 binary and package also ships as `amd64v3`. These rows run the
same build, smoke, architecture, package, signing, and publication steps as
baseline `amd64`.
The Linux glibc row uses the same manylinux2014 base and enforced GLIBC_2.17
floor as baseline amd64. The musl row installs the matching v3 runtime inside
its pinned Alpine base before resolving dependencies.

`docker/python_runtime.py` owns the pinned CPython version, source digest and
Linux v3 runtime digests. On Linux it installs python-build-standalone; on
macOS, FreeBSD, OpenBSD, NetBSD and illumos it builds a shared CPython with
`-O3 -march=x86-64-v3`; on Windows it builds x64 CPython with `/arch:AVX2` in
the common compiler property sheet and checks the core compiler log. The
helper verifies the runtime's target, shared library and essential modules
before exposing its path. No build uses `-march=native`. The upstream pins are
linked in the helper; update the manylinux matrix pin alongside it (a test
checks that they agree).

Source builds use each platform's native compiler and development libraries.
They run separately from baseline builds; OpenBSD and NetBSD v3 environments
do not reuse extension packages built for the system Python's different minor
version. A source-build failure gates publication just like any other binary
failure. macOS 26 v3 is CI-only and doesn't gate a release, like the other
macOS 26 rows.

### Release assets

The GitHub Release (`softprops/action-gh-release@v3`) attaches:

- `dist/*.whl`, `dist/*.tar.gz`
- `SHA256SUMS`: SHA256 checksums of the wheel, sdist, binaries, packages, and Windows installers
- `THIRD-PARTY-NOTICES.txt`: the notices and license texts for bundled third-party code, including the LGPL-2.1 text for zeroconf
- `zeroconf-<version>.tar.gz`: the python-zeroconf source archive, the LGPL source offer for the copy that binaries and images bundle
- `cronstable-linux-{amd64,amd64v3,arm64,i686,armv7,ppc64le,s390x,riscv64}` (glibc)
- the same eight variants with a `-musl` suffix, such as `cronstable-linux-amd64-musl` … `cronstable-linux-riscv64-musl`, **plus** `cronstable-linux-armv6-musl` (armv6 is musl-only)
- `cronstable-linux-mips64le` (glibc only; built on Debian bookworm under emulation, Python 3.11, no `orjson`)
- `cronstable-linux-loong64` and `cronstable-linux-loong64-musl` (LoongArch, new-world ABI, compiled from source under emulation)
- `cronstable-linux-armv6` and `cronstable-linux-armel` (glibc; ARMv6 hard-float and ARMv5 soft-float, compiled from source under an emulator pinned to the target core)
- `cronstable-linux-{amd64,amd64v3,arm64,i686,armv7,ppc64le,s390x,riscv64}.deb` and the same eight as `.rpm`, built with nfpm from the glibc binaries above (no emulation: nfpm never executes the payload)
- `cronstable-linux-{amd64,amd64v3,arm64,i686,armv7,armv6,ppc64le,s390x,riscv64,loong64}.apk`, built with nfpm from the **musl** binaries and carrying an OpenRC service instead of the systemd unit. Unsigned, so `apk add` needs `--allow-untrusted`
- `cronstable-freebsd-{amd64,amd64v3,arm64}.pkg`, built by FreeBSD's own `pkg create` inside the build VM, which is also where each one is installed and started before it ships
- `cronstable-macos-amd64`, `cronstable-macos-amd64v3`, `cronstable-macos-arm64`
- `cronstable-freebsd-amd64`, `cronstable-freebsd-amd64v3`, `cronstable-freebsd-arm64` (built in a FreeBSD 14 VM; arm64 under full-system emulation)
- `cronstable-openbsd-amd64`, `cronstable-openbsd-amd64v3`, `cronstable-netbsd-amd64`, `cronstable-netbsd-amd64v3`, `cronstable-illumos-amd64`, `cronstable-illumos-amd64v3` (each built in a VM of that system)
- `cronstable-windows-amd64.exe`, `cronstable-windows-amd64v3.exe`, `cronstable-windows-arm64.exe`, `cronstable-windows-i686.exe`
- `cronstable-windows-amd64.zip`, `cronstable-windows-amd64v3.zip`, `cronstable-windows-arm64.zip`, `cronstable-windows-i686.zip` (one-directory builds, the shape that hosts the [Windows service](Windows-Service))
- `cronstable-windows-amd64.msi`, `cronstable-windows-amd64v3.msi`, `cronstable-windows-arm64.msi`, `cronstable-windows-i686.msi` (machine-wide installers; see [Windows MSI](Windows-MSI))
- `cronstable-windows-amd64-setup.exe`, `cronstable-windows-amd64v3-setup.exe`, `cronstable-windows-arm64-setup.exe` (setup bundles with the MSI embedded and cronstable's icon in the administrator prompt)
- `cronstable.json`: the [Scoop](https://scoop.sh) manifest for this release, rendered from `SHA256SUMS` by `.github/scripts/render_scoop.py`. Users install it by URL (`scoop install https://github.com/ptweezy/cronstable/releases/latest/download/cronstable.json`). Its `checkver`/`autoupdate` blocks read the same `SHA256SUMS` asset, so the manifest also works in a bucket without changes.
- `perf-summary.md`, `perf-results.json`: the performance comparison against the previous release (see [performance benchmarks](Performance-Benchmarks); the diff chart `perf-chart.svg` ships in the run's `perf-report` artifact)

The download-artifact pattern `cronstable-*` must stay broad enough to match all of them: a too-narrow pattern silently drops artifacts it misses rather than erroring.

## Container image release

`.github/scripts/docker_matrix.py` expands each row of
`.github/docker-matrix.json` into the multi-arch image and a separate
amd64v3 image built from the same Dockerfile. Platform builders, OCI assembly and
publication consume expansions of this same matrix. `PYTHON_VARIANT=amd64v3` selects the shared optimized runtime
before either venv is created, and the runtime is copied into the final stage.
A final-stage smoke test checks that its interpreter and libraries survived
the copy, including on distroless. Each variant has its own cache scope and
`-amd64v3` tag suffix, so baseline tags never select v3-only instructions.

The single `release.yml` pipeline builds and publishes the official images from the top-level `Dockerfile` and per-distro `docker/Dockerfile.*`:

- **`docker`, `docker-glibc`, `docker-musl`** call the shared `build-docker.yml` workflow on every event. Each distro/platform builds independently; ARM64 uses native `ubuntu-26.04-arm` runners, amd64/386 use x64 runners, and other platforms use QEMU. Platforms with upstream wheels start immediately. Foreign platforms that need optional cryptography wheels wait only for their libc group. Each build exports an OCI artifact with the computed version and commit labels.
- **`docker-assemble`** verifies every expected platform, label, blob digest and manifest, merges each distro's index without changing image manifests, and exercises the same Skopeo OCI transport used for publication. It gates the release and saves the complete OCI archives and their checksums.
- **`docker-push`** runs after `release`, verifies those archives and copies every platform with `skopeo copy --all --preserve-digests`. It publishes GHCR version/latest tags (Debian owns the bare tags, other distros carry a `-<distro>` suffix) and optional Docker Hub tags. It performs no image build or dependency resolution.

BuildKit caches are scoped per distro/platform and written only on trunk. Release dependency layers refresh on each run. Optional cryptography builders always resolve fresh sources and reuse compiler work keyed by the source, base image and installed toolchain. The compiler and source-wheel caches save only entries below 512 MiB; best-effort maintenance limits these cache families on trunk to 2 GiB. Cache loss affects speed, while release publication uses immutable run artifacts. Platform OCI artifacts expire after two days for CI and seven days for releases.

The image build passes the computed version with `--build-arg VERSION=X.Y.Z`. A plain local `docker build .` leaves it empty, and `setuptools_scm` reads the version from `.git`. See [production and container deployment](Production-Deployment).

## The PyInstaller build

`pyinstaller/cronstable.spec` produces the self-contained binaries. The spec analyzes the entry script `pyinstaller/cronstable` (which calls `cronstable.__main__:main`) and by default emits a single-file console executable named `cronstable` with `upx=False`, `debug=False`, `console=True`, stripped on POSIX only (`STRIP = sys.platform != "win32"`; the GNU `strip` that ships with git bash corrupts the bundled PE DLLs). Setting `CRONSTABLE_BUNDLE=onedir` switches the same Analysis to `EXE(exclude_binaries=True)` plus `COLLECT`, emitting the one-directory `dist/cronstable/` layout the Windows zip and MSI assets carry. The default stays one-file because every other build lane consumes the single-file path. PyInstaller is pinned in `pyinstaller/build-requirements.txt` (`pyinstaller==6.22.3`), which the release jobs and the local Dockerfile install from.

Installing the package under `SETUPTOOLS_SCM_PRETEND_VERSION` before running PyInstaller records the version, so the bundled `cronstable/version.py` carries the release version (verified by the `--version` smoke test). PyInstaller is not a cross-compiler, so each architecture/libc is built on a matching native runner or container.

### Building a binary locally

`pyinstaller/Dockerfile` builds a glibc binary reproducibly on `ubuntu:26.04`. It installs build deps, uses `pyenv` to install CPython `3.14.7` (the release the CI lanes freeze) with `--enable-shared`, creates a venv, installs PyInstaller from `pyinstaller/build-requirements.txt` and the package with **uv** (copied in with `COPY --from=ghcr.io/astral-sh/uv`), runs the entry script (`python pyinstaller/cronstable --version`), runs `pyinstaller pyinstaller/cronstable.spec`, and smoke-tests `dist/cronstable --version`. This amd64-only local build makes the image-copy pattern arch-safe here, unlike the multi-arch release `Dockerfile`.

`pyinstaller/Makefile` wraps that: `make` (target `all`) builds the image, copies `dist/cronstable` out of the container, and runs `dist/cronstable --version`.

> The standalone binaries unpack their embedded runtime to a temp directory at startup. The temp directory must be writable and executable. See [installation](Installation) and [troubleshooting and FAQ](Troubleshooting).

## Intel Mac support

Keep Intel Macs supported for as long as you can build and validate a secure, usable release. An upstream wheel, runner, or nixpkgs support deadline is a maintenance trigger, not an automatic removal date. Prefer a patched source build, a maintained compatibility toolchain, or a replacement native runner. Never lower a security floor to keep a platform green. Retiring any delivery channel is an explicit project decision; record the last supported version and migration path in `HISTORY.md` and Installation.

The compatibility boundary is deliberately small:

- The `amd64` and `amd64v3` rows in `binaries-macos` build on macOS 15, run native acceptance tests, and gate publication alongside Apple Silicon. They share the same build steps; macOS 26 rows provide additional advisory coverage. Keep the oldest viable build host to preserve the binary's OS compatibility.
- The macOS matrix's `pq_src` switch selects a patched cryptography source build using Rust and OpenSSL. A verified X-Wing seal enables post-quantum push; an unsuccessful source build leaves working X25519 push. The security requirement is the same as on every other platform. `pyproject.toml` excludes unavailable Intel wheels from ordinary pip installs without imposing an old cryptography cap.
- `nix/intel-darwin.nix` is the sole adapter for Intel Nix recipe backports. `flake.nix` selects the final Intel-capable 26.05 input only for `x86_64-darwin`; shared PyPI updates live in `nix/python-packages.nix`. Both paths enforce the same `pyproject.toml` floors. Backports and overrides build and pass their import checks without running upstream test suites. No binary cache holds them, so CI would rerun those suites on every build, and their timing tests fail on the slow Intel host. Review the pinned toolchain when upstream maintenance ends; do not silently freeze vulnerable dependencies or remove the target.

When retirement becomes necessary, remove the compatibility pieces in one reviewed change. For Nix alone, remove `nix/intel-darwin.nix`, the adapter's import/selection, the `x86_64-darwin` system, `nixpkgs-intel-darwin` input and lock entry, and the Intel `nix` matrix row. Keep the shared package overrides. For binary retirement, also remove the Intel rows from both macOS matrices, their expected and published assets in `release.yml`, and the macOS Intel branch/checksum substitution in `packaging/homebrew/{cronstable.rb.tmpl,render-formula.sh}`. Remove the macOS source-toolchain block once no macOS row needs it; Windows source builds are independent. Update Installation, Push Notifications, the release-policy tests, and the package marker commentary together. Search `macos-amd64`, `macos26-amd64`, `macos-15-intel`, `macos-26-intel`, and `x86_64-darwin` to verify no stale promise or release prerequisite remains. Python source installs may continue independently of binary or Nix support.

## Related pages

- [CONTRIBUTING.md](https://github.com/ptweezy/cronstable/blob/main/CONTRIBUTING.md)
- [Performance Benchmarks](Performance-Benchmarks)
- [Installation](Installation)
- [Running on Windows](Running-on-Windows)
- [Windows MSI](Windows-MSI)
- [Production and Container Deployment](Production-Deployment)
- [Architecture and Internals](Architecture-and-Internals)
