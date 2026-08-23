# Contributing and releasing

This page covers the cronstable developer workflow (environment, tests, linters, type checks, pre-commit) and the fully automated GitHub Actions release pipeline that builds, publishes, tags, and containerizes each version. `setuptools_scm` derives version numbers from git tags; they are never hand-edited.

## Development environment

The project targets **Python 3.10+**; 3.10, 3.11, 3.12, 3.13, 3.14 and 3.15 are the tested interpreters (`pyproject.toml` `requires-python = ">=3.10"`, classifiers for 3.10 through 3.15).

You can run cronstable **natively on Windows, Linux, and macOS** (WSL is not required). `cronstable/platform.py` isolates all OS-specific behavior and guards `grp`/`pwd` instead of importing them unconditionally at load time on Windows, so the package and its full test suite run natively on every supported OS, and `pip install cronstable` works on Windows. For the platform-specific details, see [running on Windows](Running-on-Windows).

Linting and type checking do not import the package and run on any platform. mypy is pinned to the `linux` platform (`pyproject.toml` `[tool.mypy]` `platform = "linux"`), so type-checking is identical on every OS. mypy type-checks the POSIX API surface, and the Windows branches are runtime-guarded.

Clone and install the editable package with the `dev` extra. For a fast dev loop,
cronstable uses [uv](https://docs.astral.sh/uv/) (`tox` also runs through uv with
`tox-uv`, and uv can fetch the 3.10–3.15 interpreters the matrix needs):

```sh
git clone https://github.com/ptweezy/cronstable
cd cronstable
uv venv                                         # create .venv (uv picks a suitable Python)
uv pip install -e ".[dev]"                      # editable install with the dev extra
```

The classic virtualenv+pip path still works unchanged:

```sh
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # or: pip install -r requirements_dev.txt
```

The editable dev install (`pip install -e ".[dev]"`) and the checks (`pytest`, `ruff`, `mypy`) all run natively on Windows too. Use `.venv\Scripts\activate` to enter the venv as shown earlier.

The `dev` optional-dependency group (`pyproject.toml`) and the equivalent `requirements_dev.txt` both pull in two sets. The check tooling is `bandit`, `mypy`, `mypy-extensions`, `openapi-spec-validator`, `pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`, `tox`, and `tox-uv`, the plugin that makes `tox` provision and install with uv. The optional-feature libraries the test suite must really exercise are `orjson`, `pynacl`, `zeroconf`, `cryptography`, and `playwright`, some gated by platform markers. `requirements_dev.txt` documents why each is there, and `tests/test_dev_deps_parity.py` pins the two lists equal.

Installing `playwright` gets the library only. The browser it drives is a separate download (`python -m playwright install chromium`), and the tests that need one self-skip without it. The editable install adds the console entry point `cronstable = cronstable.__main__:main` (see [command-line reference](CLI-Reference)).

## Running the checks

`tox` drives all CI checks (`tox.ini`). The default `envlist` is `py3{10,11,12,13,14,15}-{windows,posix}, lint, mypy, bandit, openapi`.

```sh
tox                # all envs: py310-py315 in both OS arms, lint, mypy, bandit, openapi
tox -e lint        # ruff check + ruff format --check
tox -e mypy        # mypy
tox -e bandit      # bandit security lint (medium+ severity)
tox -e py          # pytest on the current interpreter, POSIX coverage profile
tox -e py-windows  # Windows hosts: the Windows coverage profile explicitly
tox -e py-posix    # POSIX hosts: the POSIX coverage profile explicitly
```

Each interpreter row exists twice, once per OS coverage profile (see [per-OS coverage profiles](#per-os-coverage-profiles) later), and `tox.ini`'s `platform` key makes the arm that does not match the running OS skip. A skip counts as a pass as long as the invocation also names an arm that does match. An invocation whose only env skips exits 1.

So a bare `tox` measures the profile for the machine you are on, and CI names both arms on every runner with `tox -e py-windows,py-posix`. Naming the single wrong arm for your box fails. On Windows, `tox -e py-posix` prints `py-posix: skipped because platform win32 does not match (?!win32).*` followed by `evaluation failed :(`.

An unfactored env such as `tox -e py` or `tox -e py312` still runs the whole suite, at the POSIX profile. On Windows, use a bare `tox` or `tox -e py-windows` instead. The POSIX profile hides the Windows branches you are editing, and counts the POSIX ones you cannot run as missed against the same `--cov-fail-under`.

| Env | Installs package | What it runs |
| --- | --- | --- |
| `py313-posix`, `py315-windows`, ... | yes (`-rrequirements_dev.txt`, `PYTHONPATH={toxinidir}`) | `pytest --color=yes -vv` under coverage, gated at `--cov-fail-under={env:CRONSTABLE_COV_FLOOR}` |
| `lint` | no (`skip_install = true`) | `ruff check cronstable` then `ruff format --check cronstable` |
| `mypy` | yes | `mypy -p cronstable` |
| `bandit` | no (`skip_install = true`) | `bandit -c pyproject.toml -r cronstable --severity-level=medium` |
| `openapi` | no (`skip_install = true`) | `python .github/scripts/check_openapi.py` |

The interpreter rows carry two environment variables the others do not. `CRONSTABLE_COVERAGE_SKIP` picks the coverage profile, and `CRONSTABLE_COV_FLOOR` is the `--cov-fail-under` number, one per arm so the two cells can be ratcheted independently. The command line itself is deliberately not factor-conditional. A factored `commands` resolves to nothing in an unfactored env, so `tox -e py312` would build a venv, run no tests, and report success.

`tox.ini` declares `requires = tox-uv`, so `tox` provisions its environments and installs dependencies with uv automatically (much faster; behavior-identical). If you ever need the legacy virtualenv+pip path, force it with `tox --runner virtualenv`.

The `lint` and `bandit` envs deliberately skip installing the package. ruff and bandit analyze the source tree directly, so they avoid imposing the project's `requires-python` on those interpreters. The `mypy` env does install it, so the runtime dependencies resolve for real. The imports that legitimately cannot resolve (optional extras, untyped libraries) are enumerated per-module in `pyproject.toml`'s `[[tool.mypy.overrides]]` tables rather than blanket-ignored.

### Tool configuration

`pyproject.toml` configures the tooling:

- **ruff**: `target-version = "py310"`, `line-length = 79`. Lint rule sets selected: `B`, `B9` (bugbear), `C` (mccabe complexity), `E` (pycodestyle errors), `F` (pyflakes), `W` (pycodestyle warnings), `I` (import sorting). `pyupgrade` (`UP`) is present but commented out. `max-complexity = 20`.
- **mypy**: `no_implicit_optional = true`, `warn_no_return = true`, `warn_return_any = true`, `strict_optional = true`.
- **pytest**: `asyncio_mode = "auto"`, `testpaths = ["tests"]`.
- **bandit**: `exclude_dirs = ["tests"]` and `skips = ["B104"]` (B104's only matches are non-bind wildcard-listen host constants in `config.py`). CI runs it at medium severity with `tox -e bandit`.
- **coverage**: `source = ["cronstable"]`, `branch = true`, `omit = ["cronstable/version.py"]`, `show_missing = true`, plus the `exclude_lines` block described next.

### Per-OS coverage profiles

`# pragma: no cover` comes in three forms:

| Form | Hidden on | Measured on |
| --- | --- | --- |
| `# pragma: no cover` | every OS | nowhere |
| `# pragma: no cover (windows)` | POSIX | Windows |
| `# pragma: no cover (posix)` | Windows | POSIX |

There is more than one because a single vocabulary scored the wrong file on the Windows rows. Every Windows branch carried a bare pragma, so the code that really ran on those cells was invisible to `--cov-fail-under`, while the POSIX code that can never run there stayed in the denominator and counted as missed. Retagging `cronstable/platform.py` moves it from 149 measured statements at 69% to 250 at 87% on a Windows run, and changes nothing on Linux.

Use the bare form only for code that no CI row can reach: defensive branches, unreachable raises, the etcd/kubernetes network glue, and `tui.py`'s macOS branch (the matrix has no macOS row, so a third token would have no profile to be measured in).

A branch guarded by `IS_WINDOWS` or `sys.platform == "win32"` takes a token where its clause genuinely cannot run on the other OS, because it reaches for something that exists only there: `msvcrt`, `fcntl`, `grp`/`pwd`, `os.nice`, `os.killpg`, `ctypes.windll`. Where it does, the other side of the branch takes the other token.

Not every platform branch qualifies. The rest stay untagged on purpose. Much of `state.py`'s Windows handling is plain Python whose arms the tests drive from either box by monkeypatching `IS_WINDOWS`, so both really are measured on both profiles, and hiding either would remove real coverage. `tests/test_coverage_profiles.py` enforces the half a test can reach: a branch tagged on one side has to be tagged on the other.

Tagging an `if` header excludes that clause only, so an `else` needs its own tag, and a fall-through tail (code after the `if` block rather than inside an `else`) has no header to tag at all. `cronstable/platform.py` therefore spells its POSIX arms out as explicit `else` clauses. The token may sit anywhere after `cover`, so a site can keep the trailing prose that explains it. The guard also has to keep the spelling its tests drive. Arms exercised from Linux by monkeypatching `platform.IS_WINDOWS` break if the guard is rewritten to `sys.platform == "win32"`, which no monkeypatch can reach, and the test then takes the POSIX arm for real.

`CRONSTABLE_COVERAGE_SKIP` selects the profile. It names the platform whose branches **cannot** run in that environment: `posix` on a Windows run, `windows` on a POSIX one. `pyproject.toml` reads it with coverage's own `${VAR-default}` substitution, so there is one coverage configuration instead of two files to keep in step. The default (`windows`) is the POSIX-shaped measurement that a bare `pytest --cov` or an IDE run has always reported. `tests/test_coverage_profiles.py` pins the vocabulary, both profiles, and the tox wiring.

One consequence to expect rather than diagnose: the merged Codecov number and the README badge read lower with the profiles in place, without anything having regressed. The published figure is a union across two different exclusion sets, so a `(windows)` line that no Windows test happens to hit is excluded from the POSIX reports and counted as missed in the merged view. `.github/codecov.yml` keeps `informational: true` on both the project and the patch status, so that drop annotates a PR and cannot fail one. The tox gate stays the only pass/fail.

### CI for every commit

There is **one** workflow, `.github/workflows/release.yml` (named `CI`), and it runs on every `push` (any branch) and every `pull_request`. On an ordinary commit it builds and tests the whole product in parallel and stops there. Only a release (described later) proceeds to publish.

The test half is a `tox-static` job (`tox -e lint,mypy,bandit,openapi`) on `ubuntu-latest`, plus a `tox` matrix running `tox -e py-windows,py-posix` (`fail-fast: false`) across `os` `[ubuntu-latest, windows-latest]` × Python `3.10`–`3.15`. The matrix adds exactly one more row, `windows-11-arm`/`3.14` for **Windows ARM64**; that row stays on 3.14 because aiohttp publishes `win_arm64` wheels only through cp314. Every row gates, 3.15 included. Both arms are named on every runner because the one that does not match the OS skips.

Alongside the tests, the same run builds every release artifact at the computed version (all the PyInstaller binaries, the wheel + sdist) and does a **build-only pass over every Docker image** (the `docker` job, all 8 distros at their full published arch sets, no push), so a broken `Dockerfile` fails CI before a release. On an ordinary commit the version is the natural `setuptools_scm` dev version. No **software** is published, pushed, tagged, or signed. The lone exception is documentation: the `wiki` job publishes `wiki/` to the GitHub wiki whenever it changes on `main` (see [editing the wiki](#editing-the-wiki)). See [production and container deployment](Production-Deployment).

## Releasing

`.github/workflows/release.yml` fully automates releases. You never edit a version by hand; `setuptools_scm` derives the version from git tags (`version_file = "cronstable/version.py"`).

### Triggering a release

A release runs when **either**:

1. A **push to `main`** in which **any** commit introduced by the push has a release marker at the **start of its subject line**, not only the tip commit. The scanned range is `BEFORE..AFTER` (the commits new in the push). On a brand-new branch where `BEFORE` is all-zeros (or unresolvable), it falls back to the tip commit only.
2. A **manual `workflow_dispatch`** run, choosing the bump level from a dropdown (`minor` default, or `major` / `patch`).

Valid markers (case-insensitive; the bump level is optional):

| Marker | Bump | 1.0.5 → |
| --- | --- | --- |
| `[release]` | minor | 1.1.0 |
| `[release:major]` | major | 2.0.0 |
| `[release:minor]` | minor | 1.1.0 |
| `[release:patch]` | patch | 1.0.6 |

If several pushed commits carry a marker, the **latest such commit wins**. A bare `[release]` counts as minor.

The `version` job's decide step performs the marker match with `grep -oiE '^\[release(:(major|minor|patch))?\]'` over the commit **subject lines** (`git log --pretty=%s`), taking the newest matching commit.

> **Why subjects only, anchored:** the original trigger substring-matched whole commit messages, so a commit *body* that only discussed the bare `[release]` marker out-bumped an explicit `[release:patch]` and shipped 1.3.0 instead of 1.2.15. The trigger no longer scans bodies at all, and a marker only counts when it begins the subject line. File contents are never scanned (this page can name the markers freely).

### What the pipeline does

The `release.yml` jobs run in dependency order. Top-level `permissions` default to `contents: read`. Only these jobs opt up to the write scopes they need: the `release` job (`contents: write` + `id-token: write`), the `sign-windows` job (`id-token: write`, for OIDC to Azure), the `docker-push` job (`packages: write`), and the `wiki` job (`contents: write`). `version` and the whole build+test gate run on **every** event. Only the publish jobs (`release`, `docker-push`, `homebrew`, and the best-effort `winget`) are guarded by `needs.version.outputs.release == 'true'`. The `wiki` job (9) is the one exception to "no software is published on an ordinary commit": it publishes documentation, so it is gated on the branch rather than on a release, and on nothing else.

1. **`version`, the decide step**: determines `release` (true/false) and `bump`. Trigger logic lives in a real shell script rather than a fuzzy `contains()` expression. It releases **only** on a `workflow_dispatch` or a push to `main` carrying a marker (a marker on any other branch, or in a PR, never releases).
2. **`version`, the compute step**: computes the version once, so every builder (and the publish job) use the same number. On a release it finds the latest tag matching `^[0-9]+\.[0-9]+\.[0-9]+$` (with `git tag -l | … | sort -V | tail -n1`, defaulting to `0.0.0`), applies the bump, and **refuses with an error if the computed tag already exists** (`refs/tags/$new`). Otherwise it emits the natural `setuptools_scm` dev version for the build-only run. The job also emits the one Docker distro matrix (`.github/docker-matrix.json`) that both the `docker` gate and `docker-push` expand from.
3. **Tests** (`tox-static`, which runs lint, mypy, bandit, and the OpenAPI check in one env list, and the `tox` matrix) run **in parallel with** the binary and Docker builds, not before them: the whole matrix together is the gate. A red anywhere means no release.
4. **Binary builds** (run in parallel with the tests; the publish jobs need them, so a broken build fails the run instead of producing a half-finished release). Each job pins `pyinstaller==6.22.1`, installs the project to bake `SETUPTOOLS_SCM_PRETEND_VERSION` (the computed version) into `cronstable/version.py`, runs `pyinstaller pyinstaller/cronstable.spec`, and smoke-tests the bundle with `dist/cronstable --version`. The **runner-native** jobs (`binaries-macos`, `binaries-windows`) install with **uv** (`uv venv` + `uv pip install` + `uv run pyinstaller`, using `astral-sh/setup-uv`). Every **container** job stays on **pip** inside its `docker run` containers, because uv's official image is amd64/arm64 only and it publishes no musl `ppc64le`/`s390x` wheels; pip is the arch-portable choice there:
   - **`binaries`**: the Linux **glibc** rows, built **inside a manylinux container** against a [python-build-standalone](https://github.com/astral-sh/python-build-standalone) interpreter, which is what puts the glibc floor at 2.17 rather than at the runner's libc. The interpreter is the 3.15.0rc1 build, pinned by URL and digest per row; the pins move to the 3.15.0 build when it ships on 2026-10-01. On 3.15 every row also compiles aiohttp and its stack from sdist (no cp315 wheels yet) and builds orjson from source, because its cp315 wheels are tagged `manylinux_2_39`, above this lane's floor. `amd64` and `arm64` build natively on `ubuntu-24.04` and `ubuntu-24.04-arm`; `ppc64le`, `s390x` and `armv7` build under QEMU. The container is load-bearing: pip derives manylinux compatibility from the **running** glibc, so on a bare runner it selects newer wheels and pins the floor there whatever the interpreter was built against. The image's own `/opt/python` interpreters are unusable, being configured `--disable-shared`, which PyInstaller cannot freeze from. `armv7` has no manylinux2014 image and builds on `manylinux_2_31_armv7l`, declaring glibc 2.31. Artifacts `cronstable-linux-<arch>`.
   - **`binaries-container`**: the **musl** rows plus the two glibc rows manylinux cannot serve, one matrix row each, built **inside a `docker run --platform` container** (PyInstaller is not a cross-compiler and the runners are glibc; checkout/upload stay on the host). The musl rows build on `python:3.15-rc-alpine3.23` and cover `amd64`, `arm64`, `i686`, `armv7`, `armv6`, `ppc64le`, `s390x` and `riscv64` (artifacts `cronstable-linux-<arch>-musl`). The glibc rows are `i686` on `python:3.15-rc-slim-bookworm` (python-build-standalone publishes no i686 Linux target) and `riscv64` on `python:3.15-rc-slim-trixie` (bookworm has no riscv64 port). The Python half of each tag is `-rc-`, which stops being updated when 3.15.0 ships on 2026-10-01; swap them to `python:3.15-alpine3.23` and `python:3.15-slim-*` then. `armv6` is **musl-only** (Debian/glibc ships no arm32v6).

     Every base image is pinned to an explicit distro release rather than a floating `python:3.14-slim`/`-alpine` tag, because the libc floor of a container-built binary is a property of the base: the floating Alpine tag moved from 3.19 to 3.24 on its own and took the musl requirement from 1.2.4 to 1.2.6 with it. `tests/test_ci_fences.py` holds every base to a dated tag.

     `amd64`/`i686` run natively on `ubuntu-24.04` and `arm64` on `ubuntu-24.04-arm`; the rest run under QEMU (`docker/setup-qemu-action`). Each row installs its libc's C toolchain plus libffi/zlib headers (the spec strips on POSIX; the headers cover the deps that compile from sdist, which on 3.15 is every row rather than the usual i686 aiohttp stack and whole C-ext stack on `armv6`, since aiohttp publishes no cp315 wheel for any arch yet), and persists pip's cache per arch and libc with `actions/cache`, so the slow QEMU source builds carry over between runs instead of recompiling on every push.
   - **`binaries-arm-legacy`**: the two legacy 32-bit ARM ABIs, glibc hard-float `armv6` (Raspberry Pi 1, Zero, Zero W) on `tianon/raspbian:bookworm-slim` and glibc soft-float `armel` (Kirkwood: SheevaPlug, QNAP TS-x1x, DNS-320, NSA325, Pogoplug) on a digest-pinned `arm32v5/debian:bookworm-slim`. Neither ABI has a base image in any family the other lanes use: Debian's armhf port is ARMv7, so `library/debian` and `python:3.14-*` cannot supply an ARMv6 base at all, and python-build-standalone publishes no ARMv6 target either, so Python comes from apt (3.11) as it does on MIPS. Both rows set `_PYTHON_HOST_PLATFORM` (without it, pip installs `armv7l` wheels, because an emulated ARMv6 container reports `armv7l` from `uname`) and pin `QEMU_CPU` to the real target core, so an instruction the hardware lacks faults during the build. The `armel` base is frozen: arm32v5 was dropped from the bookworm official-images definition, and `bookworm-security` carries no armel component, so that row builds on an OS layer nobody will patch again.
   - **`binaries-mips`** and **`binaries-loong64`**: the two ports with no `python:3.14-*` image of their own, each compiled from source under emulation. MIPS builds on `mips64le/debian:bookworm-slim`, pinned to a `snapshot.debian.org` slice because bookworm's LTS phase carries no mips64el and its live index will be deleted on an unannounced date. LoongArch builds both libcs from third-party images (`ghcr.io/loong64/python` and `ghcr.io/loong64/alpine`), targeting the new-world ABI upstream Debian and Alpine use.
   - **`binaries-openbsd`**, **`binaries-netbsd`**, **`binaries-illumos`**: one amd64 binary each, built in a KVM-accelerated virtual machine of that system (`vmactions/*-vm`), since no runner offers one. OpenBSD is release-locked (7.9), so its `release:` input needs bumping every six months. illumos builds on OmniOS r151054 LTS and the result runs anywhere the illumos ABI does.
   - **`binaries-macos`**: macOS, `arm64` on `macos-15` (Apple Silicon) and `amd64` on `macos-15-intel`. Built on Python 3.15. After the smoke test it asserts the native arch with `file`, so Rosetta cannot let a mislabeled x86_64 build pass on the arm64 runner. Artifacts `cronstable-macos-arm64` and `cronstable-macos-amd64` are the **shipped** pair (signed + notarized on a release, described later). The same job also builds macOS 26 (Tahoe) `arm64`/`amd64` rows as **CI-only** coverage (`ship: false`, `continue-on-error` so a flaky Tahoe build never blocks a release). Those upload as `cronstable-macos26-{arch}` and are neither signed nor attached to the Release.
   - **`binaries-windows`**: Windows, `amd64` on `windows-latest` and `arm64` on the `windows-11-arm` runner (both native; PyInstaller is not a cross-compiler). Built on Python 3.15 with the same `pyinstaller==6.22.1` pin and `dist/cronstable.exe --version` smoke test as the others. Any C-extension dep lacking a wheel compiles from sdist with the runner's Visual Studio toolchain, which on 3.15 means aiohttp and zeroconf on every arch and the rest of the aiohttp stack on `arm64`.

     The same invocation also emits the one-directory layout (`CRONSTABLE_BUNDLE=both`, one shared Analysis). The job smoke-tests `dist/cronstable/cronstable.exe`, proves it against the real SCM (`service install`, `status`, `remove` under a CI-scoped name; the hosted runners are elevated, and the zip proof never starts a service), and zips the directory as `cronstable-windows-<arch>.zip`. It then builds the MSI from `packaging/msi/cronstable.wxs` with the shared `.github/scripts/build_msi.sh` (WiX v6 as a pinned .NET tool, not preinstalled on either runner), smoke-tests it with a real `msiexec` install and uninstall, asserting the registered service's exact ImagePath and recovery actions in between, then drives a real major upgrade with a custom `CONFIGDIR`, asserting the directory survives and the upgrade starts the service.

     On a release, the `sign-windows` job signs and repackages these six artifacts (see [Windows signing](#windows-signing-azure-artifact-signing)). Without the signing secrets they ship unsigned, like the Linux binaries. Artifacts `cronstable-windows-{amd64,arm64}.exe`, `.zip` and `.msi`. See [running on Windows](Running-on-Windows) and [Windows MSI](Windows-MSI).
   Every glibc row ends with `.github/scripts/elf_floor.py`, which unpacks the frozen bundle, parses `.gnu.version_r` across the bootloader and every embedded shared library, and fails the job when the highest `GLIBC_x.y` exceeds the floor that row declares. On 32-bit ARM it additionally reads each object's float ABI from `e_flags` and its `Tag_CPU_arch` and `Tag_FP_arch` from `.ARM.attributes`, and fails on the wrong ABI or on anything needing a newer core than the row targets. That check exists because the 1.2.41 `armv6` binary shipped 27 members of ARMv7, VFPv3 object code from `armv7l` wheels and passed every functional test: emulators execute those instructions happily, and only the target hardware does not. No functional test can catch this: the smoke test runs on a libc newer than the binary needs, so a dependency that starts publishing a higher-tagged wheel would raise the floor with CI green throughout. The same step rejects an executable stack, which ships fine and then dies on SELinux-hardened hosts. The `mips64le` row is exempt from the exec-stack check. Debian's mips64el port configures glibc for an executable stack, because the kernel FPU emulator runs floating-point branch delay slots out of line on the user stack. Every object on that port declares one, so no build there passes. `tests/test_ci_fences.py` holds these floors to the ones the `.deb`/`.rpm` dependencies promise.

5. **`perf`**: the paired performance benchmark (see [performance benchmarks](Performance-Benchmarks)). It installs this commit and the latest release tag into separate virtualenvs, runs the suite in `benchmarks/` against both, interleaved on one runner, and diffs the two with `benchmarks/compare.py`. On a release, a regression past a metric's declared limit fails the gate; on an ordinary commit or PR the same comparison only warns. A `[perf:accept]` marker at the start of a pushed commit subject acknowledges an intentional regression (reported, not gating). The job's `perf-report` artifact holds `perf-chart.svg`, `perf-summary.md` and `perf-results.json`; the `release` job appends the summary to the notes and attaches `perf-summary.md` and `perf-results.json` as assets.
6. **`release`**: runs with `permissions: contents: write` and `id-token: write`, and only after the **entire** gate succeeds: every test job, every binary job (`binaries`, `binaries-container`, `binaries-mips`, `binaries-loong64`, `binaries-macos`, `binaries-freebsd`, `binaries-openbsd`, `binaries-netbsd`, `binaries-illumos`, `binaries-windows`), the `docker` build-only job **and** the `perf` benchmark gate. The `nix` job is deliberately not among them: everything it builds comes from a nixpkgs branch that moves outside this repository, so a red there is information about nixpkgs rather than a reason to hold a release that ships no Nix artifact. In order:
   - Downloads the `dist` artifact (the wheel + sdist the `dist` job already built and `twine check`ed) and **publishes it to PyPI** with Trusted Publishing / OIDC (`pypa/gh-action-pypi-publish`, `skip-existing: true`), no API token.
   - **Only after a successful publish**: creates an annotated tag `X.Y.Z` and pushes it (with `RELEASE_TOKEN` so the tag can point at a commit that touches `.github/workflows/`), downloads every per-arch binary artifact (pattern `cronstable-*`, `merge-multiple: true`), overlays the signed Windows set from `sign-windows` when signing ran (see [Windows signing](#windows-signing-azure-artifact-signing)), builds the `.deb` and `.rpm` packages from those same bytes with `.github/scripts/build_packages.sh`, generates one `SHA256SUMS` over the shipped set, renders the Scoop manifest from it, and extracts the release notes. It then creates the GitHub Release with **all** binaries + `SHA256SUMS` attached in a single step (no separate later attach step, so nothing ever collides with immutable-release protection).
7. **`docker-push`**: after `release` (so the tag it checks out exists, and it is thus gated on the whole gate including the `docker` build), builds and pushes every distro's multi-arch image at the released version (described later).
8. **`homebrew`**: after `release`, re-renders and pushes the tap formula from the published `SHA256SUMS`.
9. **`wiki`**: publishes [`wiki/`](https://github.com/ptweezy/cronstable/tree/main/wiki) to this repository's GitHub wiki. It is **not** a release job and **not** gated on the build+test matrix: a wiki page is not a build artifact, so it neither waits for a release nor lets a flaky emulated arch delay a typo fix. It runs on **every push to `main`** and on nothing else. See [editing the wiki](#editing-the-wiki).

Because no file is committed back to *this* repo, a release never re-triggers the workflow. Two jobs do push elsewhere: `homebrew` to the tap on a release, and `wiki` to the wiki on a `main` commit. Both targets are separate repositories, so a push to either raises no event here. Because the tag is created **after** publishing, a failed publish leaves no orphan tag and a re-run cleanly retries the same version.

### macOS signing and notarization

The macOS binaries are Developer ID signed (hardened runtime) and notarized **when the signing secrets are configured**. If absent, the "Sign and notarize" step warns and exits 0, shipping an unsigned binary (a release is never blocked on signing setup). The secrets are `MACOS_CERT_P12_BASE64`, `MACOS_CERT_PASSWORD`, `MACOS_SIGN_IDENTITY`, `MACOS_NOTARY_KEY_BASE64`, `MACOS_NOTARY_KEY_ID`, `MACOS_NOTARY_ISSUER_ID`.

Signing imports the cert into a throwaway randomly-keyed keychain, signs with `codesign --options runtime --timestamp --entitlements pyinstaller/entitlements.plist`, verifies, then notarizes with `xcrun notarytool submit … --wait`. Because a one-file binary cannot be stapled, notarization publishes the ticket online and Gatekeeper validates on first run, so end users do not need `xattr -d com.apple.quarantine`.

`pyinstaller/entitlements.plist` enables the three hardened-runtime entitlements a PyInstaller one-file binary needs (`com.apple.security.cs.allow-unsigned-executable-memory`, `…allow-jit`, `…disable-library-validation`) so the unpacked CPython runtime can load and execute its embedded `.so`/`.dylib` files.

### Windows signing (Azure Artifact Signing)

On a release, the `sign-windows` job Authenticode-signs the Windows assets with Azure Artifact Signing **when the signing secrets are configured**. If any are absent, the job warns and the release ships them unsigned (a release is never blocked on signing setup, the same rule as macOS). The secrets are `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_SUBSCRIPTION_ID` (OIDC federation with `azure/login`; no client secret exists anywhere), plus `AZURE_SIGNING_ENDPOINT`, `AZURE_SIGNING_ACCOUNT` and `AZURE_SIGNING_PROFILE`. All six are required together.

The job runs on the x64 runner because the signing client does not support Windows ARM runners. Authenticode is architecture-agnostic, so one runner signs both arches. It signs the one-file exes and each zip's inner `cronstable.exe`, re-zips, rebuilds both MSIs from the signed payload with the same shared build script the gate used (`.github/scripts/build_msi.sh`), signs those, verifies every signature, and installs and uninstalls the signed amd64 MSI for real. The `release` job then overlays the signed set before `SHA256SUMS`, so the sums, the Release assets, and the winget manifests describe the signed bytes. Every signature carries an RFC 3161 timestamp because Artifact Signing rotates its leaf certificates within days. `tests/test_ci_fences.py` pins the wiring.

With the secrets present, a signing failure that survives three attempts fails the release. To ship unsigned in an emergency (say an Azure outage on release day), remove one of the six secrets and re-run.

### Release notes

The "Build release notes from HISTORY.md" step extracts this version's section from `HISTORY.md` into `release-notes.md`: everything between its `## X.Y.Z (…)` header and the next `## ` header, with leading blank lines stripped. If there is no matching section it warns and the body is auto-generated only. The Release uses that section as `body_path` with `generate_release_notes: true` (the curated notes are prepended above GitHub's auto-generated "What's Changed" / compare link). Keep [HISTORY.md](https://github.com/ptweezy/cronstable/blob/main/HISTORY.md) entries headed exactly `## X.Y.Z (date)` so the matcher (`index($0, "## " ver " ") == 1`) finds them.

### Release assets

The GitHub Release (`softprops/action-gh-release@v3`) attaches:

- `dist/*.whl`, `dist/*.tar.gz`
- `cronstable-linux-{amd64,arm64,i686,armv7,ppc64le,s390x,riscv64}` (glibc)
- the same seven arches with a `-musl` suffix, such as `cronstable-linux-amd64-musl` … `cronstable-linux-riscv64-musl`, **plus** `cronstable-linux-armv6-musl` (armv6 is musl-only)
- `cronstable-linux-mips64le` (glibc only; built on Debian bookworm under emulation, Python 3.11, no `orjson`)
- `cronstable-linux-loong64` and `cronstable-linux-loong64-musl` (LoongArch, new-world ABI, compiled from source under emulation)
- `cronstable-linux-armv6` and `cronstable-linux-armel` (glibc; ARMv6 hard-float and ARMv5 soft-float, compiled from source under an emulator pinned to the target core)
- `cronstable-linux-{amd64,arm64,i686,armv7,ppc64le,s390x,riscv64}.deb` and the same seven as `.rpm`, built with nfpm from the glibc binaries above (no emulation: nfpm never executes the payload)
- `cronstable-linux-{amd64,arm64,i686,armv7,armv6,ppc64le,s390x,riscv64,loong64}.apk`, built with nfpm from the **musl** binaries and carrying an OpenRC service instead of the systemd unit. Unsigned, so `apk add` needs `--allow-untrusted`
- `cronstable-freebsd-{amd64,arm64}.pkg`, built by FreeBSD's own `pkg create` inside the build VM, which is also where each one is installed and started before it ships
- `cronstable-macos-amd64`, `cronstable-macos-arm64`
- `cronstable-freebsd-amd64`, `cronstable-freebsd-arm64` (built in a FreeBSD 14 VM; arm64 under full-system emulation)
- `cronstable-openbsd-amd64`, `cronstable-netbsd-amd64`, `cronstable-illumos-amd64` (each built in a VM of that system)
- `cronstable-windows-amd64.exe`, `cronstable-windows-arm64.exe`, `cronstable-windows-i686.exe`
- `cronstable-windows-amd64.zip`, `cronstable-windows-arm64.zip`, `cronstable-windows-i686.zip` (one-directory builds, the shape that hosts the [Windows service](Windows-Service))
- `cronstable-windows-amd64.msi`, `cronstable-windows-arm64.msi`, `cronstable-windows-i686.msi` (machine-wide installers; see [Windows MSI](Windows-MSI))
- `cronstable.json`: the [Scoop](https://scoop.sh) manifest for this release, rendered from `SHA256SUMS` by `.github/scripts/render_scoop.py`. It is submitted to `ScoopInstaller/Extras` once; after that its Excavator bot re-reads `checkver`/`autoupdate` every four hours and bumps the manifest from the same `SHA256SUMS` asset, so nothing here pushes to it again.
- `perf-summary.md`, `perf-results.json`: the performance comparison against the previous release (see [performance benchmarks](Performance-Benchmarks); the diff chart `perf-chart.svg` ships in the run's `perf-report` artifact)

The download-artifact pattern `cronstable-*` must stay broad enough to match all of them: a too-narrow pattern silently drops artifacts it misses rather than erroring.

## Container image release

The single `release.yml` pipeline (the former standalone `docker.yml` is folded into it) builds and pushes the official images from the top-level `Dockerfile` and the per-distro `docker/Dockerfile.*`. Two jobs cover them:

- **`docker` (build-only gate)** runs on **every** push and PR: it builds all 8 distro images at their full published arch sets (non-`amd64` arches under QEMU) and does **not** push. This is part of the gate, so an arch-specific `Dockerfile` or dependency breakage fails CI *before* anything is published, and it warms the per-distro GHA build cache.
- **`docker-push`** runs **only on a release**, after `release` has published PyPI and pushed the tag (so it is transitively gated on the whole build+test gate, the `docker` build included). It checks out the tag and pushes each distro's multi-arch image to GHCR as `ghcr.io/ptweezy/cronstable:<version>` and `:latest` (the Debian base owns the bare tags; each variant gets a `-<distro>` suffix: `-alpine`, `-ubuntu`, `-rhel`, `-fedora`, `-opensuse`, `-amazonlinux`, `-distroless`), and to Docker Hub when `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` are set.

The image build passes the computed version with `--build-arg VERSION=X.Y.Z`. A plain local `docker build .` leaves it empty, and `setuptools_scm` reads the version from `.git`. See [production and container deployment](Production-Deployment).

## The PyInstaller build

`pyinstaller/cronstable.spec` produces the self-contained binaries. The spec analyzes the entry script `pyinstaller/cronstable` (which calls `cronstable.__main__:main`) and by default emits a single-file console executable named `cronstable` with `upx=False`, `debug=False`, `console=True`, stripped on POSIX only (`STRIP = sys.platform != "win32"`; the GNU `strip` that ships with git bash corrupts the bundled PE DLLs). Setting `CRONSTABLE_BUNDLE=onedir` switches the same Analysis to `EXE(exclude_binaries=True)` plus `COLLECT`, emitting the one-directory `dist/cronstable/` layout the Windows zip and MSI assets carry. The default stays one-file because every other build lane consumes the single-file path. PyInstaller is **pinned to `6.22.1`** consistently across the release jobs and the local Dockerfile.

Installing the package under `SETUPTOOLS_SCM_PRETEND_VERSION` before running PyInstaller bakes the version in, so the bundled `cronstable/version.py` carries the release version (verified by the `--version` smoke test). PyInstaller is not a cross-compiler, so each architecture/libc is built on a matching native runner or container.

### Building a binary locally

`pyinstaller/Dockerfile` builds a glibc binary reproducibly on `ubuntu:26.04`. It installs build deps and `upx-ucl`, uses `pyenv` to install CPython `3.13.15` with `--enable-shared`, creates a venv, installs `pyinstaller==6.22.1` and the package with **uv** (copied in with `COPY --from=ghcr.io/astral-sh/uv`), runs the entry script (`python pyinstaller/cronstable --version`), runs `pyinstaller pyinstaller/cronstable.spec`, and smoke-tests `dist/cronstable --version`. This amd64-only local build makes the image-copy pattern arch-safe here, unlike the multi-arch release `Dockerfile`.

`pyinstaller/Makefile` wraps that: `make` (target `all`) builds the image, copies `dist/cronstable` out of the container, and runs `dist/cronstable --version`.

> The standalone binaries unpack their embedded runtime to a temp directory at startup. The temp directory must be writable and executable. See [installation](Installation) and [troubleshooting and FAQ](Troubleshooting).

## Editing the wiki

**Edit [`wiki/`](https://github.com/ptweezy/cronstable/tree/main/wiki) in the repo, not the wiki in the browser.** The wiki you are reading is a *published copy*: every push to `main` runs the `wiki` job, which mirrors `wiki/*.md` onto it. The pages are ordinary Markdown, one file per page, named exactly as the page's URL (`Web-Dashboard.md` → `/wiki/Web-Dashboard`), plus `Home.md`, `_Sidebar.md` and `_Footer.md`.

Two consequences worth knowing:

- **The mirror is authoritative, so it deletes.** A page edited or created from the wiki's *web UI* survives only until the next push to `main`, which reverts or removes it. There is no merge and no warning: the job makes the wiki's tree equal `wiki/`, and prints every add/modify/delete it makes to the run log. If you edit in the browser, copy your change into `wiki/` before it is overwritten.
- **Links resolve only after publishing.** Pages link to each other with bare wiki links (`[Installation](Installation)`), which GitHub resolves relative to the wiki. They are *expected* to be dead when you browse `wiki/*.md` in the repo; that is not a bug to fix. Screenshots are absolute `raw.githubusercontent.com/.../main/docs/img/*.png` URLs for the same reason.

The job publishes from `main`, the repository's only long-lived branch. That is where `wiki/` is edited, and the pages' `main`-pinned links and images mean the wiki documents the trunk by construction. It needs no secret: `GITHUB_TOKEN` with `contents: write` can push a repository's own wiki.

## Related pages

- [Installation](Installation)
- [Running on Windows](Running-on-Windows)
- [Command-Line Reference](CLI-Reference)
- [Production and Container Deployment](Production-Deployment)
- [Architecture and Internals](Architecture-and-Internals)
- [Performance Benchmarks](Performance-Benchmarks)
