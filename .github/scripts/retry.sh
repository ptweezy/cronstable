# Shared retry helper for the CI steps that touch the network.
#
# Sourced, not executed:
#     . .github/scripts/retry.sh          # on the runner
#     . /src/.github/scripts/retry.sh     # inside the build containers, which
#                                         # mount the checkout at /src
#
# Usage: retry <attempts> <command> [args...]
#
#     retry 5 docker pull python:3.14-alpine
#     retry 5 apk add --no-cache build-base
#
# Only wrap the NETWORK operation, never a long build. The emulated foreign-arch
# builds run for the better part of an hour, so retrying the whole `docker run`
# would turn one registry blip into hours of re-emulation; pull first (cheap,
# retryable), then run once against the local image.
#
# POSIX sh: this is sourced by ash (Alpine) and dash (Debian slim) inside the
# containers as well as by bash on the runner, so no bashisms and no pipefail.
# Underscore-prefixed locals because POSIX sh has no `local`.
retry() {
  _retry_max=$1
  shift
  _retry_n=1
  _retry_delay=5
  while :; do
    if "$@"; then
      return 0
    fi
    if [ "$_retry_n" -ge "$_retry_max" ]; then
      echo "::error::retry: '$*' still failing after ${_retry_max} attempts" >&2
      return 1
    fi
    echo "::warning::retry: attempt ${_retry_n}/${_retry_max} of '$*' failed; retrying in ${_retry_delay}s" >&2
    sleep "$_retry_delay"
    _retry_n=$((_retry_n + 1))
    _retry_delay=$((_retry_delay * 2))
  done
}
