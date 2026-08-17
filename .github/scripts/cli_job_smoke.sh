#!/usr/bin/env bash
# Frozen-build regression smoke for release.yml's binary lanes: run the
# built binary as a daemon with a scheduled job whose COMMAND LINE shells
# out to the cronstable CLI, and assert the durable KV side effect, never
# the exit status.
#
# What it guards: a frozen daemon's environ carries PyInstaller's _PYI_*
# process-linkage vars. Inherited through the job's shell, they made the
# frozen CLI's bootloader refuse to start ("parent process has different
# executable"), so every state/cursor/lock/xcom/secret call from a job died
# before the subcommand ran, while the job still exited 0 behind the usual
# `|| true`. The daemon scrubs the vars from job environments
# (cronstable/job.py fixup_pyinstaller_env); only a real frozen binary
# exercises the real bootloader, which is why this lives in the binary
# lanes and not the test suite.
#
# The job runs `state get` BEFORE `state set`, so the asserted value can
# only come from a PRIOR run's write surviving in the store: two CLI round
# trips and daemon-side persistence, proven by one grep.
#
# Usage (bash on every lane, git bash on the Windows runners):
#     bash .github/scripts/cli_job_smoke.sh dist/cronstable[.exe]
set -euo pipefail

BIN_DIR="$(cd "$(dirname "$1")" && pwd)"
BIN="$BIN_DIR/$(basename "$1")"
PORT=18131

WORK="$(mktemp -d)"
DAEMON_PID=""
cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "cli_job_smoke: FAILED; daemon log tail:" >&2
    tail -n 40 "$WORK/daemon.log" >&2 || true
    echo "cli_job_smoke: job stderr:" >&2
    cat "$WORK/err.log" >&2 || true
  fi
  if [ -n "$DAEMON_PID" ]; then
    kill "$DAEMON_PID" 2>/dev/null || true
    # let the daemon release the state dir before the rm (Windows locks)
    for _ in $(seq 1 10); do
      kill -0 "$DAEMON_PID" 2>/dev/null || break
      sleep 1
    done
  fi
  rm -rf "$WORK"
  exit "$rc"
}
trap cleanup EXIT

# The job invokes the CLI by bare name, the way real configs do; the build
# under test must be both the daemon AND what that name resolves to.
export PATH="$BIN_DIR:$PATH"

# get-then-set under the platform's default job shell. cmd.exe chains with
# an unconditional `&`; `state get` exits 4 on the first run, when the key
# does not exist yet, and the chain continues either way.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) SEP='&' ;;
  *) SEP=';' ;;
esac

# Ephemeral, loopback-only, dies with the runner: exists because POST
# /shutdown refuses unauthenticated callers by design, and a graceful drain
# releases the state dir before the cleanup rm (Windows holds the lock).
TOKEN="cli-job-smoke-$$"

cd "$WORK"
cat > smoke.yaml <<EOF
state:
  path: ./state
  jobApi:
    enabled: true
web:
  listen:
    - http://127.0.0.1:$PORT
  authToken:
    value: $TOKEN
jobs:
  - name: clitest
    schedule:
      second: "*/3"
    command: 'cronstable state get beats >> get.log 2>> err.log $SEP cronstable state set beats ok 2>> err.log'
EOF

"$BIN" -c smoke.yaml > daemon.log 2>&1 &
DAEMON_PID=$!

up=""
for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null -H "Authorization: Bearer $TOKEN" \
      "http://127.0.0.1:$PORT/status" 2>/dev/null; then
    up=yes
    break
  fi
  sleep 1
done
if [ -z "$up" ]; then
  echo "cli_job_smoke: daemon never answered on 127.0.0.1:$PORT" >&2
  exit 1
fi

# Two completed runs suffice (a set, then a get that reads it back); the
# budget is generous because a one-file binary re-extracts itself on every
# CLI invocation.
ok=""
for _ in $(seq 1 60); do
  if [ -f get.log ] && tr -d '\r' < get.log | grep -qx ok; then
    ok=yes
    break
  fi
  sleep 1
done

curl -fsS -o /dev/null -X POST -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$PORT/shutdown" || true

if [ -z "$ok" ]; then
  echo "cli_job_smoke: no job run ever read back the KV write" >&2
  exit 1
fi
# The exact failure signature this smoke exists for, wherever it landed.
if grep -qi "security validation" err.log daemon.log 2>/dev/null; then
  echo "cli_job_smoke: bootloader security validation failure in logs" >&2
  exit 1
fi
echo "cli_job_smoke: OK (a job read back a prior run's KV write)"
