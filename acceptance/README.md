# Binary acceptance tests

These tests launch a packaged executable and observe its public CLI, HTTP
API, and job outputs. They never import cronstable. The driver needs Python
3.10+, pytest, and psutil; the workloads need only the executable and the
platform's native shell and utilities. Nothing uses a real account or sends
external notifications.

## Run locally

Use an existing release binary or build one with the repository's PyInstaller
spec. Install driver dependencies into your test environment:

```sh
python -m pip install -r acceptance/requirements.txt
python -m pytest -c acceptance/pytest.ini acceptance \
  --cronstable-bin /absolute/path/to/cronstable \
  --acceptance-output acceptance-results \
  --junitxml acceptance-results/junit.xml
```

On Windows, pass the `.exe` path. The executable can have its release asset
name; the driver stages it under `cronstable[.exe]`. For a one-directory
bundle, pass its inner `cronstable.exe` and keep `_internal/` beside it.
The driver copies that directory too. Add `--expected-version X.Y.Z` to
require an exact version (CI always does), or `-k retry` to run one scenario.
An omitted binary, a source/venv console script, no collected tests, or any
skipped test fails the run. Run tests serially; xdist is not configured.

The executable and working directories live in a fresh temporary directory,
outside the checkout. The working path contains spaces and a non-ASCII
character. Job PATH contains only the staged binary and system utilities,
and Python source-path overrides are removed. This exercises the frozen
bootloader when a scheduled job invokes the CLI by name, including the
PyInstaller environment-inheritance regression previously covered by
`.github/scripts/cli_job_smoke.sh`.

Each run records the supplied executable's SHA-256, version expectation,
platform, and layout in `acceptance-results/run-*/artifact.json`. Each test
retains daemon/CLI logs, last API snapshots, configuration, and a copy of its
work and synthetic state. Logs are written directly into the evidence
directory while the daemon is running. Waits and requests have deadlines;
teardown tracks and kills surviving descendants across process groups.

## Current scenarios

| Test | Evidence required |
| --- | --- |
| Startup and public surfaces | Exact version when specified; valid config accepted and invalid config rejected; authenticated API; unauthenticated request denied; bundled dashboard served. |
| Scheduled CLI state and restart | At least two successful scheduled runs; a CLI read sees a prior run's random KV value; history survives restart; subsequent read-only jobs recover that value without writing it again. |
| Pending retry and restart | A scheduled `@reboot` run fails with exit 23; a pending retry is visible; the daemon stops before retrying; restart alone produces exactly one successful retry at or after its saved deadline and retains both outcomes. |
| Graceful drain | A real job blocks on a file controlled by the driver; shutdown acknowledges the drain; releasing the file lets the job finish; the daemon exits successfully and closes its listener. |

The retry's 30-second delay leaves time for shutdown and restart. Other
scenarios synchronize on observable events, not assumed startup durations.
The drain workload also has its own deadline so it cannot wait forever if
the driver disappears. Forced termination, crash recovery, and service
manager behavior are separate scenarios to add; a graceful restart does
not establish those guarantees.

## CI coverage

The existing release workflow runs this suite after freezing, before its
binary jobs succeed. Driver dependencies are installed after the build:

* Linux glibc: native manylinux amd64, amd64v3, arm64, plus container i686.
* Linux musl: native amd64, amd64v3, arm64, and i686 inside Alpine.
* macOS: all existing native rows; on releases, acceptance follows signing
  and notarization. Experimental macOS rows retain their non-gating policy.
* Windows: every existing architecture row, for both one-file and
  one-directory layouts. These are the unsigned build artifacts; the
  existing separate signing job still verifies signatures and the signed
  MSI installation. This suite does not yet cover signed Windows payloads.

Failures block the existing release dependency chain. Each lane uploads
JUnit and evidence as `acceptance-*` even after failure. Emulated targets
and BSD/illumos retain their existing smoke and ABI checks. The source
suite remains separate under `tests/`; acceptance does not inflate source
coverage or run implicitly in tox.

## Extend toward continuous dogfooding

Add focused scenarios here for pool pressure, DAG recovery, timeouts and
process trees, real dashboard interactions, and reporting to local sinks.
Run the same assertions through service/package and container adapters.
Keep native and emulated coverage explicit rather than skipping a required
capability when it is missing.

A later soak workflow can run a reduced `example/grand-tour` workload with
an independent observer tracking expected outputs and recovery deadlines.
Give that workflow its own non-publishing trigger: dispatching the current
release workflow requests a release. Neither the soak environment nor
cross-release upgrades are part of this first suite.
