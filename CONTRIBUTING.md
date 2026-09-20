# Contributing to cronstable

This guide explains how to develop, test, and release cronstable.

To report a security vulnerability, follow the private reporting process
in [SECURITY.md](SECURITY.md).

## Signing off your commits (DCO)

The project uses the [Developer Certificate of Origin](DCO) (DCO), a
lightweight, sign-off-based alternative to a contributor license agreement
(CLA). By signing off, you certify that you wrote the patch, or otherwise have
the right to submit it under the project's license. The full text is in the
[DCO](DCO) file.

Add a sign-off to each commit with `-s`:

```sh
git commit -s -m "Fix retry scheduling"
```

This appends a trailer with the name and email from your Git configuration:

```text
Signed-off-by: Your Name <you@example.com>
```

The `dco` job in continuous integration (CI) checks that every commit in a pull
request includes this trailer. To add missing sign-offs to a branch, run:

```sh
git rebase --signoff origin/main
git push --force-with-lease
```

## Development setup

The project targets **Python 3.10+** (3.10, 3.11, 3.12, 3.13, and 3.14 are
tested) and runs on **Linux, macOS, and Windows**. The test suite runs on all
three in CI, including Windows ARM64.

cronstable uses [uv](https://docs.astral.sh/uv/) for local development.
The `tox-uv` plugin also lets tox use uv to create environments and install
dependencies. uv can install the Python 3.10–3.14 interpreters used by the
test matrix. After installing uv, run:

```sh
git clone https://github.com/ptweezy/cronstable
cd cronstable
uv venv                                         # create .venv (uv picks a suitable Python)
uv pip install -e ".[dev]"                      # editable install with the dev extra
```

To use Python's built-in `venv` module and pip, run:

```sh
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # or: pip install -r requirements_dev.txt
```

> **Note:** OS-specific behavior lives in
> [`cronstable/platform.py`](cronstable/platform.py) (default shell, default
> configuration location, Unix socket support, and shutdown handlers).
> The POSIX-only `user`/`group` feature imports `grp`/`pwd` lazily and is
> rejected on Windows. mypy is pinned to the `linux` platform. It type-checks
> the POSIX API surface, and the Windows branches are runtime-guarded, so
> type checking is identical on every OS. The coverage check measures
> Windows-specific branches on Windows when they use
> `# pragma: no cover (windows)`. A bare `# pragma: no cover` excludes them
> on every OS. See
> [the pragma vocabulary](#coverage-pragmas).

## Generated build files

Edit `pyproject.toml` to change development dependencies or minimum versions
for optional dependencies. The build generator uses it to produce
`requirements_dev.txt`, `requirements_min.txt`, and
`pyinstaller/requirements/*.txt`. For `tox -e mindeps`,
`requirements_min.txt` pins minimum runtime and test dependency versions.
Binary build jobs select their own optional dependencies, failure policies,
and platform-specific version limits.

The generated `requirements_dev_freethreaded.txt` omits only `orjson`, which
does not support free-threaded Python. With Python 3.14t installed, run
`tox -e py314t-posix` to test the standard-library JSON fallback with the
same test suite and coverage floor.

The eight Dockerfiles share `docker/templates/Dockerfile`; distro-specific
base images, packages, and runtime settings live in `docker/images.toml`.
The supported image paths and platforms remain in `.github/docker-matrix.json`.
After changing these inputs, regenerate with Python 3.11 or newer:

```sh
python scripts/generate_build_files.py
python scripts/generate_build_files.py --check
```

Commit the generated files with their inputs. CI checks freshness before its
static checks. Builds consume the checked-in files directly, including on
Python 3.10; they do not run the generator.

Dependabot updates `pyproject.toml` and excludes generated requirements.
Regenerate the files in dependency update pull requests before merging.

Simple job options declare their default, YAML validator, and fingerprint
policy together in `cronstable.config.JOB_SCALAR_FIELDS`. Normalization and
secret-bearing values keep explicit code paths. An added identity field must
preserve the existing v1 digests when its value is the default; the fingerprint
tests protect persisted retries and reboot markers from accidental changes.

## Branching

The project develops on a single branch, `main`. Open your pull request
against it.

A new push cancels the previous CI run, whether that run is active or queued.
To fully test a particular commit, wait for its checks to finish before
pushing again. Release runs use a separate concurrency group and are not
canceled by later pushes.

## Running the checks

For tests of the actual packaged executable, see
[binary acceptance](acceptance/README.md). That separate suite drives real
scheduled jobs, state CLI calls, pending retries across restart, and graceful
shutdown on native Linux, macOS, and Windows binary builds. It requires an
explicit binary path and runs independently of the source coverage suite.

`tox` drives the source tests and static checks:

```sh
tox                # all envs: py310-py314 (each in a windows and a posix arm),
                   # lint, mypy, bandit, openapi
tox -e lint        # ruff check + ruff format --check
tox -e mypy        # mypy
tox -e bandit      # bandit security lint (medium+ severity)
tox -e py          # pytest on the current interpreter, POSIX coverage profile
tox -e py-windows  # Windows hosts: the Windows coverage profile explicitly
tox -e py-posix    # POSIX hosts: the POSIX coverage profile explicitly
```

Two additional environments run only when selected explicitly:

```sh
tox -e mindeps     # the suite on the oldest dependency versions pyproject.toml allows
tox -e deep        # thorough property tests, then three randomized full-suite runs
```

Each Python version has a Windows environment and a POSIX environment.
The `platform` setting in `tox.ini` skips environments that don't match
your OS. If every selected environment is skipped, tox exits with code 1.

Run `tox` to select the matching environments automatically, or use
`tox -e py-windows,py-posix` as CI does. On Windows, selecting only
`py-posix` fails because that environment is skipped.

Environments without an OS suffix, such as `py` and `py312`, run the full
suite with the POSIX coverage profile. On Windows, use `tox` or
`tox -e py-windows` so coverage measures Windows branches and excludes
POSIX-specific branches.

### How the suite runs

The root `pyproject.toml` configures the source test suite. Install the
development dependencies before running it. The suite uses these rules:

- **Warnings are errors by default.** Pytest treats warnings as errors
  unless `filterwarnings` contains an exception. Document a reason for each
  exception.
- **Test order is random.** `pytest-randomly` shuffles the order on every
  run and prints the seed in the header. To replay a failing order, run
  `pytest -p randomly --randomly-seed=N`. To run in file order, pass
  `-p no:randomly`.
- **Each test has a 270-second timeout.** If a test exceeds the timeout,
  `pytest-timeout` reports the failure and prints every thread's stack.
  When the timeout mechanism supports recovery, the test run continues.

A warning from an object's finalizer, such as a warning about an unclosed
socket or event loop, can fail a later test when garbage collection runs.
To investigate the leak, enable the leak-reporting plugin. It collects
garbage after each test and appends the test ID, error message, and
available allocation trace to `leaks.txt`:

```sh
python -X tracemalloc=25 -m pytest -p tests._leakfinder -p no:randomly
```

Set `CRONSTABLE_LEAK_OUT` to change the report path. The recorded test ID
identifies when collection ran; it doesn't prove which test caused the leak.

If a helper opens a resource that the test can't close with a context
manager, register a cleanup callback with `close_at_test_end` in
`tests/_helpers.py`. To start a state store, call `start_state(cron, cfg)`
from the same module. It registers cleanup for the state backend and its
loopback job API listener.

Property-based tests are in `tests/test_properties_*.py` and generate inputs
with the Hypothesis strategies in `tests/_strategies.py`. When a test fails,
copy its `@reproduce_failure` decorator onto the test to reproduce the input.
If a property test finds a bug, fix the code and add the failing input as a
regression test in the same module. Set
`CRONSTABLE_HYPOTHESIS_PROFILE=thorough` to generate about 30 times as many
examples.

`tests/test_backend_live.py` runs the etcd and Kubernetes backends against
real servers. Configure the servers with `CRONSTABLE_LIVE_ETCD`,
`CRONSTABLE_LIVE_ETCD_AUTH`, or `CRONSTABLE_LIVE_K8S`; the module docstring
explains each variable. Tests skip servers that aren't configured.
The `backends-live` CI job provisions all three configurations and sets
`CRONSTABLE_LIVE_REQUIRED=1` to fail if any test would skip.

The scheduled `nightly` workflow runs the `deep` environment, the suite with
`PYTHONDEVMODE=1`, and mutation tests for modules whose behavior depends on
their inputs. Mutation testing introduces small code changes and checks
whether the tests detect them. `[tool.mutmut]` in `pyproject.toml` selects the
modules and tests. To test mutations in one module locally:

```sh
pip install mutmut
mutmut run "cronstable.redact*"
mutmut results
```

### Running release pipeline and Pro tests

Release pipeline tests are in `.github/tests` and also run in the full test
suite. To run them independently, use Python 3.11 or newer and run these
commands from the repository root:

```sh
python -m pip install packaging pytest strictyaml
python -m pytest .github/tests -q
```

Pytest discovers the configuration for each suite from its directory.
For Pro setup and test commands, see
[Develop and test Pro](pro/README.md#develop-and-test).

### Coverage pragmas

Choose a coverage annotation based on where the code can run:

| Form | Hidden on | Measured on |
| --- | --- | --- |
| `# pragma: no cover` | every OS | nowhere |
| `# pragma: no cover (windows)` | POSIX | Windows |
| `# pragma: no cover (posix)` | Windows | POSIX |

Use the bare form only for code that no CI test can exercise, such as
unreachable defensive branches. The POSIX profile also runs on macOS;
don't exclude code solely because it is specific to macOS.

Annotate a branch guarded by `IS_WINDOWS` or `sys.platform == "win32"` when
it uses an API unavailable on the other OS, such as `msvcrt`, `fcntl`,
`grp`/`pwd`, `os.nice`, `os.killpg`, or `ctypes.windll`. If both branches use
platform-specific APIs, annotate both with their respective OS labels.

Leave platform branches unannotated when tests can exercise them on either
OS by monkeypatching `IS_WINDOWS`. When you add annotations:

- Tagging an `if` header excludes that clause only. An `else` needs its own
  tag. A fall-through tail (code after the `if` block rather than inside an
  `else`) has no header to tag at all, which is why `cronstable/platform.py`
  spells its POSIX arms out as explicit `else` clauses.
- The token may sit anywhere after `cover`, so a site can keep the trailing
  prose that explains it.
- Keep the guard spelled the way the tests drive it. They exercise several
  Windows arms from Linux by monkeypatching `platform.IS_WINDOWS`, which cannot
  patch `sys.platform`. Rewriting such a guard to `sys.platform == "win32"`
  sends the test down the POSIX arm for real.

`tests/test_coverage_profiles.py` holds the vocabulary and both profiles, and
fails a branch that is tagged on one side only.

`tox.ini` declares `requires = tox-uv`, so `tox` provisions its environments and
installs dependencies with uv automatically.
To use virtualenv and pip instead, run
`tox --runner virtualenv`.

## Performance benchmarks

CI benchmarks every commit against the latest release: startup time, schedule
math at 100k-job scale, configuration parsing, state I/O, memory footprint, and
more. On a release, it fails the pipeline if a metric regresses past its
declared limit. The release notes then carry a per-metric diff chart. Check
your own changes locally with:

```sh
python benchmarks/bench.py --quick --json before.json
# make the change
python benchmarks/bench.py --quick --json after.json
python benchmarks/compare.py --baseline before.json --current after.json --md diff.md
```

To ship an intentional, measured regression, start a pushed commit's subject
with `[perf:accept]` (subjects only, like the `[release]` marker). To publish
regardless of what the perf job finds, use `[perf:ignore]` instead, or the
`perf` dropdown of a manual run: the comparison still runs and is attached to
the release, but nothing in it gates. The full harness reference, including
how to add a benchmark, is in [benchmarks/README.md](benchmarks/README.md).

## Releasing

The single [`CI`](.github/workflows/release.yml) GitHub Actions pipeline
**automates** releases: one workflow builds and tests everything on every
commit and, on a release, publishes it. Version numbers come from git tags with
`setuptools_scm`; you never edit a version by hand.

<a id="cutting-a-release"></a>

### Creating a release

A release happens when **any commit in a push to `main`** has a release marker
at the **start of its subject line** (the first line of the commit message):

```text
[release:minor] Add retry backoff to the HTTP reporter
```

It does not need to be the latest commit in the push. But only subject lines
are scanned, and only a marker that begins the subject counts. Prose that
mentions a marker in a commit body (or anywhere else in a subject) never
triggers or escalates a release.

Valid markers (the bump level is optional; case is ignored):

| Marker             | Bump  | 1.0.5 → |
| ------------------ | ----- | ------- |
| `[release]`        | minor | 1.1.0   |
| `[release:major]`  | major | 2.0.0   |
| `[release:minor]`  | minor | 1.1.0   |
| `[release:patch]`  | patch | 1.0.6   |

If more than one commit in the push carries a marker, the **latest** such
commit wins. (File contents like this document are never scanned; only commit
subjects are.)

You can also release manually without a marker: **Actions → release → Run
workflow**, then pick the bump level from the dropdown. The same form has a
`perf` dropdown that overrides the perf gate: `accept` reports regressions
without gating on them, `ignore` publishes whatever the perf job finds.

### What the pipeline does

The same pipeline runs on every commit and pull request; only the publish
steps are gated behind the release check. The lone exception is the `wiki` job,
which publishes documentation on every push to `main` (see [editing the
wiki](#editing-the-wiki)). On a release it, in order:

1. **decides** whether to release and at what level (the strict marker check,
   which only fires on a push to `main` or a manual dispatch);
2. **computes** the next version from the latest `X.Y.Z` tag (refusing if that
   tag already exists);
3. **builds and tests everything in parallel**, all at the computed version:

   - `tox` (py310–py314 on Linux, Windows, and macOS, plus Linux and
     Windows on arm64; lint, mypy);
   - `tox-mindeps` (the suite with the oldest supported dependency versions)
     and `backends-live` (backend tests against an etcd server, a second etcd
     server with TLS and authentication, and a kind cluster);
   - the wheel + sdist;
   - the self-contained PyInstaller binaries for Linux (`amd64`, `arm64`,
     `i686`, `armv7`, `armv6`, `ppc64le`, `s390x` and `riscv64`, glibc and
     musl), macOS (`arm64` + `amd64`) and Windows (`amd64` + `arm64`), each
     smoke-tested with `--version`;
   - a build-only pass over every Docker image.

   This whole matrix is the **gate**: a red anywhere (a failed test, a broken
   binary, or a broken `Dockerfile`) means no release.

4. **only after the entire gate is green**, publishes the wheel + sdist to PyPI
   through [Trusted Publishing with OpenID Connect
   (OIDC)](https://docs.pypi.org/trusted-publishers/): there is no API token to
   manage or leak;
5. **after a successful publish**, creates and pushes the `X.Y.Z` tag and a
   GitHub Release, then pushes the multi-arch container images and updates the
   Homebrew tap.

   The GitHub Release carries the wheel, sdist, and all the binaries
   (`cronstable-linux-{amd64,arm64,i686,armv7,ppc64le,s390x,riscv64}`, their
   `-musl` variants plus `cronstable-linux-armv6-musl`,
   `cronstable-macos-{arm64,amd64}`, and
   `cronstable-windows-{amd64,arm64}.exe`, `.zip` and `.msi`), plus a single
   `SHA256SUMS`.

Because no file is committed back to *this* repo, a release never re-triggers
the workflow. (Two jobs do push elsewhere: the Homebrew tap on a release, and
the wiki on a `main` commit. But both targets are separate repositories, and a
push to either raises no event here.)

Because the tag is created *after* publishing, a failed publish leaves no
orphan tag and a re-run cleanly retries the same version.

## Container image

The single [`CI`](.github/workflows/release.yml) pipeline builds and publishes
the official image from the top-level [`Dockerfile`](Dockerfile) (and the
per-distro `docker/Dockerfile.*`):

- **On every commit and pull request** it builds every image *without* pushing
  (the `docker` gate job), across their full published arch sets, so a broken
  `Dockerfile` fails CI before a release.
- **On a release**, after the whole gate is green, the `docker-push` job builds
  and pushes each distro's multi-arch image, tagged `<version>` and `:latest`,
  to both `ghcr.io/ptweezy/cronstable` and `docker.io/ptweezy/cronstable`. The
  job authenticates to the GitHub Container Registry (GHCR) with the built-in
  `GITHUB_TOKEN`, and to Docker Hub with the `DOCKERHUB_USERNAME` and
  `DOCKERHUB_TOKEN` repository secrets (skipped if unset). The Debian base owns
  the bare tags; variants get a `-<distro>` suffix.

Build it locally the same way CI does (the version is read from git, or pass
`--build-arg VERSION=X.Y.Z`):

```sh
docker build -t cronstable .
docker run --rm -v "$PWD/example/docker/cronstable.yaml:/etc/cronstable.d/cronstable.yaml:ro" cronstable
```

## Editing the wiki

Edit [`wiki/`](wiki) in this repo, not the wiki in the browser. The
[GitHub wiki](https://github.com/ptweezy/cronstable/wiki) is a published copy:
every push to `main` runs the pipeline's `wiki` job, which mirrors
`wiki/*.md` onto it (one file per page, named as the page's URL:
`Web-Dashboard.md` → `/wiki/Web-Dashboard`).

The mirror is authoritative, so it **deletes**: on the next push to `main`, the
`wiki` job reverts a page created or edited from the wiki's web UI. The job
prints every add/modify/delete to the run log.

Pages link to each other with bare wiki links, `[Installation](Installation)`,
which only resolve after publishing. Expect those links to be dead when you
browse `wiki/*.md` here; that is not a bug.
