# Contributing to cronstable

Thanks for working on cronstable. This document covers local development and
how releases are cut.

**Security problems do not go in the issue tracker.**
[SECURITY.md](SECURITY.md) has the private reporting route and what to
include.

## Signing off your commits (DCO)

The project uses the [Developer Certificate of Origin](DCO) (DCO), a
lightweight, sign-off-based alternative to a contributor license agreement
(CLA). By signing off, you certify that you wrote the patch, or otherwise have
the right to submit it under the project's license. The full text is in the
[DCO](DCO) file.

Add a sign-off to each commit with `-s`:

```sh
git commit -s -m "Fix the thing"
```

That appends a trailer with the name and email from your git config:

```text
Signed-off-by: Your Name <you@example.com>
```

The `dco` job in continuous integration (CI) checks that every commit in a pull
request carries this trailer. If you forgot, sign off the whole branch and
force-push:

```sh
git rebase --signoff origin/main
git push --force-with-lease
```

## Development setup

The project targets **Python 3.10+** (3.10, 3.11, 3.12, 3.13, 3.14, and 3.15
are tested) and runs on **Linux, macOS, and Windows**. The test suite runs on
all three in CI, including Windows ARM64.

For a fast development loop, cronstable uses [uv](https://docs.astral.sh/uv/).
`tox` also runs through uv with `tox-uv`, and uv can fetch the 3.10–3.15
interpreters the test matrix needs. With uv installed:

```sh
git clone https://github.com/ptweezy/cronstable
cd cronstable
uv venv                                         # create .venv (uv picks a suitable Python)
uv pip install -e ".[dev]"                      # editable install with the dev extra
```

If you prefer stock tooling, the classic path still works unchanged:

```sh
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"                         # or: pip install -r requirements_dev.txt
```

> **Note:** all OS-specific behavior lives in
> [`cronstable/platform.py`](cronstable/platform.py) (default shell, default
> configuration location, unix-socket support, and shutdown-signal wiring).
> The POSIX-only `user`/`group` feature imports `grp`/`pwd` lazily and is
> rejected on Windows. mypy is pinned to the `linux` platform. It type-checks
> the POSIX API surface, and the Windows branches are runtime-guarded, so
> type-checking is identical on every OS. The coverage gate is the one check
> that reads those Windows branches, and it only does so when they are tagged:
> write `# pragma: no cover (windows)`, not a bare `# pragma: no cover`. See
> [the pragma vocabulary](#coverage-pragmas).

## Branching

The project develops on a single branch, `main`. Open your pull request
against it.

CI collapses superseded runs: pushing again cancels the earlier run, whether it
had already started or was still waiting in the queue, so an intermediate
commit can land with its run cut short. Push a commit you want fully tested on
its own. Releases are the exception. They sit in their own concurrency group,
where they start at once and always run to completion.

## Running the checks

`tox` drives everything CI runs:

```sh
tox                # all envs: py310-py315 (each in a windows and a posix arm),
                   # lint, mypy, bandit, openapi
tox -e lint        # ruff check + ruff format --check
tox -e mypy        # mypy
tox -e bandit      # bandit security lint (medium+ severity)
tox -e py          # pytest on the current interpreter, POSIX coverage profile
tox -e py-windows  # Windows hosts: the Windows coverage profile explicitly
tox -e py-posix    # POSIX hosts: the POSIX coverage profile explicitly
```

Each interpreter row exists twice, once per OS profile, and `tox.ini`'s
`platform` key makes the arm that does not match the machine skip. A skip
counts as a pass as long as the invocation also names an arm that does match;
an invocation whose only env skips exits 1.

So a bare `tox` works everywhere, and CI names both arms in one command
(`tox -e py-windows,py-posix`), but naming the single wrong arm for your
machine fails. On Windows, `tox -e py-posix` prints `py-posix: skipped because
platform win32 does not match (?!win32).*` and then `evaluation failed :(`.

`tox -e py`, `tox -e py312` and the other unfactored envs still run the whole
suite, at the POSIX profile that the coverage numbers have always used. On
Windows, prefer a bare `tox` or `tox -e py-windows`: the POSIX profile hides
the Windows branches you are editing and counts the POSIX ones you cannot run
as missed, all against the same `--cov-fail-under`.

### Coverage pragmas

`# pragma: no cover` comes in three forms, and picking the wrong one is how a
branch stops being gated:

| Form | Hidden on | Measured on |
| --- | --- | --- |
| `# pragma: no cover` | every OS | nowhere |
| `# pragma: no cover (windows)` | POSIX | Windows |
| `# pragma: no cover (posix)` | Windows | POSIX |

Use the bare form only for code no CI row can reach: defensive branches,
unreachable raises, the etcd/kubernetes network glue, and `tui.py`'s macOS
branch (the matrix has no macOS row, so a third token would have no profile to
be measured in).

A branch guarded by `IS_WINDOWS` or `sys.platform == "win32"` takes a token
where its clause genuinely cannot run on the other OS, because it uses
something that only exists there: `msvcrt`, `fcntl`, `grp`/`pwd`, `os.nice`,
`os.killpg`, `ctypes.windll`. Where it does, the other side of the branch takes
the other token, and that half is the one people forget.

Plenty of platform branches here are plain Python that the tests drive from
either machine by monkeypatching `IS_WINDOWS`. Those stay untagged on purpose,
because they are measured on both. A few things to get right when you tag a
branch:

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
installs dependencies with uv automatically (much faster; behavior-identical).
If you ever need the legacy virtualenv+pip path, force it with
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

### Cutting a release

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

   - `tox` (py310–py315, lint, mypy);
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
