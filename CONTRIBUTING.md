# Contributing to cronstable

This guide is for anyone changing cronstable's code or documentation. It
covers the path from a fork to a merged pull request:

1. [Agree on the approach](#before-you-start) in an issue, unless the change
   is a small fix.
2. [Set up](#development-setup) your fork and a development environment.
3. [Make the change](#making-a-change) on its own branch, with tests and
   documentation.
4. [Run the checks](#running-the-checks).
5. [Sign off](#signing-off-your-commits-dco) each commit.
6. [Open a pull request](#opening-a-pull-request) against `main`.

One person maintains cronstable. The maintainer reviews every pull request and
cuts every release, so you never release anything yourself. The
[Release Pipeline](https://github.com/ptweezy/cronstable/wiki/Release-Pipeline)
wiki page documents continuous integration (CI) and the release process.

To report a security vulnerability, follow the private reporting process
in [SECURITY.md](SECURITY.md).

## AI use

cronstable's development relies on AI agents, both to help write code and to
review and test it. Every change goes through the same checks, whether a person
or an AI tool wrote it: the test suite and its coverage floor, the performance
benchmarks, automated code scanning, and the maintainer's review.

Contributions written with AI help are welcome and held to the same standards
as any other change. You don't need to say whether or how you used AI. You're
responsible for everything you submit, so make sure you understand a change
before you open a pull request. The
[sign-off](#signing-off-your-commits-dco) on each commit certifies that you
have the right to submit it, and that applies to code an AI tool wrote.

Judge a contribution by the work itself, whatever tools produced it. Keep
review discussion focused on the change, and treat every contributor with
respect.

## Before you start

- To report a bug,
  [open an issue](https://github.com/ptweezy/cronstable/issues/new). The issue
  template lists the details to include.
- For a new feature or a large change, open an issue to agree on the approach
  before you write the code.
- A small fix, such as a typo or a contained bug fix, can go straight to a
  pull request.

## Development setup

The project targets **Python 3.10+** (3.10, 3.11, 3.12, 3.13, and 3.14 are
tested) and runs on **Linux, macOS, and Windows**. The test suite runs on all
three in CI, including Windows ARM64.

cronstable uses [uv](https://docs.astral.sh/uv/) for local development.
The `tox-uv` plugin also lets tox use uv to create environments and install
dependencies. uv can install the Python 3.10–3.14 interpreters used by the
test matrix.

Install uv and fork
[ptweezy/cronstable](https://github.com/ptweezy/cronstable) on GitHub. Then
clone your fork, add the main repository as the `upstream` remote, install
the package, and activate the environment:

```sh
git clone https://github.com/<your-username>/cronstable
cd cronstable
git remote add upstream https://github.com/ptweezy/cronstable
uv venv                                         # create .venv (uv picks a suitable Python)
uv pip install -e ".[dev]"                      # editable install with the dev extra
. .venv/bin/activate                            # Windows: .venv\Scripts\activate
```

To use Python's built-in `venv` module and pip instead of uv, run:

```sh
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # editable install with the dev extra
```

The browser tests need Chromium, which Playwright downloads separately.
Without it, those tests are skipped:

```sh
python -m playwright install chromium
```

Start each change on its own branch from the latest `main`:

```sh
git fetch upstream
git switch -c fix-retry-scheduling upstream/main
```

## Making a change

### Tests

Cover new or changed behavior with tests. For a bug fix, include a test that
fails without the fix. Each test run enforces the coverage floor that
`tox.ini` sets for its coverage profile, so new code needs tests that
exercise it. The Codecov statuses on a pull request are informational and
can't fail it. Codecov merges both profiles, so its figure can read lower
than either tox run.
[Running the checks](#running-the-checks) explains the profiles and how the
suite runs.

### Documentation

Update the documentation in the same pull request as the behavior it
describes:

- Each feature has its own page in [`wiki/`](wiki) and its own section in
  `README.md`. For a new feature, add both, and list the new page in
  `wiki/_Sidebar.md` and `wiki/Home.md`.
- Follow the
  [Google developer documentation style guide](https://developers.google.com/style).
- Describe what cronstable does, as if it had always worked that way. Don't
  write documentation as a change log, such as "now supports" or "no longer
  requires".
- Leave `HISTORY.md` to the maintainer, who writes the release notes there.
  If your change affects users, describe the effect in the pull request
  description.

### Editing the wiki

Edit [`wiki/`](wiki) in this repo, not the wiki in the browser. The
[GitHub wiki](https://github.com/ptweezy/cronstable/wiki) is a published copy:
every push to `main` runs the pipeline's `wiki` job, which mirrors
`wiki/*.md` onto it (one file per page, named as the page's URL:
`Web-Dashboard.md` → `/wiki/Web-Dashboard`).

The `wiki/` directory is the source of truth. On the next push to `main`, the
`wiki` job overwrites an edit made in the wiki's web UI and **deletes** a page
created there. The job prints every page it adds, changes, or deletes to the
run log.

Pages link to each other with bare wiki links, such as
`[Installation](Installation)`, which resolve only on the published wiki.
Those links are dead when you browse `wiki/*.md` in the repository, so leave
them as they are. Link images by their absolute
`raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/` URL, which
resolves in both places.

### Platform-specific code

OS-specific behavior lives in
[`cronstable/platform.py`](cronstable/platform.py): the default shell, the
default configuration location, Unix socket support, and shutdown handlers.
The POSIX-only `user`/`group` feature imports `grp`/`pwd` lazily and is
rejected on Windows.

mypy is pinned to the `linux` platform, so type checking gives the same result
on every OS and covers the POSIX-only APIs, such as `grp`, `pwd`, and
`os.setuid`. mypy skips a branch guarded by `sys.platform == "win32"`, so
only the Windows test runs exercise that code.

Coverage is measured separately on Windows and POSIX. When a branch can run on
only one of them, annotate it as [Coverage pragmas](#coverage-pragmas)
describes.

### Job options

To add a simple job option, add one entry to
`cronstable.config.JOB_SCALAR_FIELDS`. The entry declares the option's
default, its YAML validator, and its identity policy.

The identity policy controls whether the option's value goes into the job's
digest, a hash of the job's effective configuration. cronstable stores a job's
pending retries and its `@reboot` markers with that digest, and drops a pending
retry or reruns an `@reboot` job when the stored digest differs from the job's.
The policy has three values:

- `always`: the value is always hashed.
- `nondefault`: the value is hashed only when it differs from the default.
- `exclude`: the value is never hashed.

Use `nondefault` for a new option that affects how a job runs. A job that
leaves the option at its default then keeps the same digest after an upgrade,
so its stored retries and markers stay valid. Use `exclude` only when a
change to the option's value should keep a job's stored retries and markers
valid. Leave `always` to the options that already use it, because on a new
option it changes every job's digest. The tests in
`tests/test_fingerprint.py` compare digests against fixed values and fail when
one changes.

Options that need normalization or that hold secrets are handled outside the
table, by their own code in `cronstable/config.py` and
`cronstable/fingerprint.py`.

### Generated build files

Some checked-in files are generated. Edit their inputs and leave the generated
files to the generator:

- `pyproject.toml` declares the development dependencies in its `dev` extra
  and the minimum versions for optional dependencies. The `dev` extra is what
  pip, uv, and tox install. From `pyproject.toml`, the generator writes
  `requirements/min.txt`, `requirements/dev-freethreaded.txt`, and
  `pyinstaller/requirements/*.txt`.
- `docker/templates/Dockerfile` is the template for all eight Dockerfiles.
  `docker/images.toml` holds each distro's base images, packages, and runtime
  settings, and `.github/docker-matrix.json` lists the supported image paths
  and platforms.

After changing an input, regenerate with Python 3.11 or newer. The second
command verifies that every generated file is current:

```sh
python scripts/generate_build_files.py
python scripts/generate_build_files.py --check
```

Commit the generated files with their inputs. CI runs the same verification
before its static checks. Builds use the checked-in files directly, including
on Python 3.10, and never run the generator.

`requirements/min.txt` pins the minimum runtime and test dependency versions
for `tox -e mindeps`. Each file in `pyinstaller/requirements/` holds one
optional dependency's minimum version, and the binary build jobs choose which
of them each platform installs.

`requirements/dev-freethreaded.txt` lists the `dev` extra without `orjson`,
which does not support free-threaded Python. With Python 3.14t installed, run
`tox -e py314t-posix` to test the standard-library JSON fallback with the
same test suite and coverage floor.

## Running the checks

`tox` drives the source tests and static checks. Before you open a pull
request, run the static checks and the test suite on your current
interpreter, with the same commands that CI uses:

```sh
tox -e lint,mypy,bandit,openapi
tox -e py-windows,py-posix
```

A test run measures coverage under one of two profiles, Windows or POSIX. A
profile excludes the branches that can't run on its OS and sets its own
coverage floor. Each test environment exists twice, once per profile, and tox
skips the one that doesn't match your OS, so the second command works on
every OS.

The rest of this section is reference for running one check or testing a
specific kind of change. To run everything, or one check at a time:

```sh
tox                # all envs: py310-py314 (each with a windows and a posix
                   # profile), lint, mypy, bandit, openapi
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

The environment you select determines the profile:

- A bare `tox` runs every interpreter with the profile that matches your OS.
  The `platform` setting in `tox.ini` skips the other one.
- If every selected environment is skipped, tox exits with code 1. On
  Windows, `tox -e py-posix` alone fails for that reason.
- Environments without an OS suffix, such as `py` and `py312`, run the full
  suite with the POSIX coverage profile, and so does a bare `pytest --cov`
  or a run from an IDE. On Windows, use `tox` or
  `tox -e py-windows` so coverage measures the Windows branches and excludes
  the POSIX-only ones.

`tox.ini` declares `requires = tox-uv`, so `tox` provisions its environments
and installs dependencies with uv automatically. To use virtualenv and pip
instead, run `tox --runner virtualenv`.

### How the suite runs

The root `pyproject.toml` configures the source test suite. Install the
development dependencies before running it. The suite uses these rules:

- **Warnings are errors by default.** Pytest treats warnings as errors
  unless `filterwarnings` contains an exception. Add a comment with the
  reason for each exception.
- **Test order is random.** `pytest-randomly` shuffles the order on every
  run and prints the seed in the header. To replay a failing order, run
  `pytest -p randomly --randomly-seed=N`. To run in file order, pass
  `-p no:randomly`.
- **Each test has a 270-second timeout.** If a test exceeds the timeout,
  `pytest-timeout` reports the failure and prints every thread's stack.
  On Linux and macOS, the run then continues with the next test. On Windows,
  the run stops.

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
explains each variable. Tests skip servers that aren't configured, so a
local run needs none of them. The `backends-live` CI job provisions all three
configurations and sets `CRONSTABLE_LIVE_REQUIRED=1`, which turns a skip into
a failure.

The scheduled `nightly` workflow runs only on `main`. It runs the `deep`
environment, the suite with `PYTHONDEVMODE=1`, and mutation tests for modules
with no timing-dependent behavior, such as the cron expression parser.
Mutation testing introduces small code changes and checks whether the tests
detect them. `[tool.mutmut]` in `pyproject.toml` selects the modules and
tests. To test mutations in one module locally:

```sh
pip install mutmut
mutmut run "cronstable.redact*"
mutmut results
```

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

- An annotation on an `if` line excludes that clause only, so an `else` needs
  its own annotation. Code that follows the `if` block without an `else` has
  no line to annotate. Write the POSIX branch as an explicit `else` clause, as
  `cronstable/platform.py` does.
- The OS label can sit anywhere after `cover`, so the comment can go on to
  explain the exclusion: `# pragma: no cover (windows) - Windows-only path`.
- Keep each guard in the form its tests use. Several tests run Windows
  branches on Linux by monkeypatching `platform.IS_WINDOWS`, which leaves
  `sys.platform` unchanged. If you rewrite such a guard as
  `sys.platform == "win32"`, those tests run the POSIX branch instead.

`tests/test_coverage_profiles.py` checks the annotation forms and both
profiles, and fails when only one side of a branch is annotated.

### Release pipeline tests

Release pipeline tests are in `.github/tests`, and the full test suite
includes them. To run only these tests, for example after you change a
workflow, use Python 3.11 or newer and run these commands from the repository
root:

```sh
python -m pip install packaging pytest strictyaml
python -m pytest .github/tests -q
```

Pytest discovers this suite's configuration in `.github/tests/pytest.ini`.

### Binary acceptance tests

The [binary acceptance](acceptance/README.md) suite tests the actual packaged
executable. It drives real scheduled jobs, state CLI calls, pending retries
across restart, and graceful shutdown on native Linux, macOS, and Windows
binary builds. It requires an explicit binary path and runs independently of
the source coverage suite. CI runs it against each native binary that it
builds, including on a pull request.

### Building the container image

The top-level [`Dockerfile`](Dockerfile) and the per-distro
`docker/Dockerfile.*` files build the official images;
[Generated build files](#generated-build-files) explains where they come from.
Every pull request builds every image without pushing it, so a broken
`Dockerfile` fails CI.

Build and run the image locally the same way CI does (the version is read from
git, or pass `--build-arg VERSION=X.Y.Z`):

```sh
docker build -t cronstable .
docker run --rm -v "$PWD/example/docker/cronstable.yaml:/etc/cronstable.d/cronstable.yaml:ro" cronstable
```

### Performance benchmarks

CI benchmarks every commit, including each pull request, against the latest
release: startup time, schedule math at 100k-job scale, configuration parsing,
state I/O, memory footprint, and more. On a pull request, a regression past a
metric's declared limit shows as a warning. Check your own changes locally
with:

```sh
python benchmarks/bench.py --quick --json before.json
# make the change
python benchmarks/bench.py --quick --json after.json
python benchmarks/compare.py --baseline before.json --current after.json --md diff.md
```

If your change is slower on purpose, explain why in the pull request
description and include the comparison. The full harness reference, including
how to add a benchmark, is in [benchmarks/README.md](benchmarks/README.md).

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

The `dco` CI job checks that every commit in a pull request includes this
trailer. To add missing sign-offs to a branch, run:

```sh
git rebase --signoff upstream/main
git push --force-with-lease
```

## Commit subjects

Pull requests merge with a merge commit, so each of your commits lands on
`main` as you wrote it. Give each commit a short subject line that says what it
does.

Don't start a commit subject with `[release]`, `[release:major]`,
`[release:minor]`, `[release:patch]`, `[perf:accept]`, or `[perf:ignore]`.
The pipeline reads these markers from the subjects of commits that reach
`main`, so a marked commit in your pull request cuts a release or changes the
performance gate when it merges.

## Opening a pull request

Push your branch to your fork, then open a pull request against `main`, the
project's only long-lived branch:

```sh
git push -u origin fix-retry-scheduling
```

In the description, explain what the change does and why, and describe any
effect on users.

After you open the pull request:

1. The `CI` workflow runs the full build and test pipeline on your branch,
   with the signing and publishing jobs skipped. Until you've had a pull
   request merged, each run waits for the maintainer to approve it. A new push
   cancels the previous run, whether that run is active or queued. To fully
   test a particular commit, wait for its checks to finish before pushing
   again.
2. GitHub Copilot reviews each push to a pull request that isn't a draft.
   CodeQL code scanning and GitHub code quality analysis also run. A new alert
   at warning level or above, or a security alert of medium severity or
   higher, blocks the merge.
3. The maintainer reviews the pull request. A push after approval dismisses
   the approval, and every review conversation, including Copilot's, must be
   resolved before the pull request can merge.
4. The branch must be up to date with `main` before it merges. When `main`
   moves ahead, select **Update branch** on the pull request, or merge
   `upstream/main` into your branch. The DCO check skips merge commits.
5. The maintainer merges the pull request with a merge commit.

A CI failure that reaches `main` blocks the next release, so fix failures
before the merge.

## Releasing

The maintainer cuts releases from `main`, and a merged change ships in the
next one. Version numbers come from git tags, so never edit a version by hand.
The [Release Pipeline](https://github.com/ptweezy/cronstable/wiki/Release-Pipeline)
wiki page documents how releases are triggered, built, signed, and published.
