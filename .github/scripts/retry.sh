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
# Set RETRY_SOFT=1 when the CALLER can still recover from an exhausted retry
# (pull_base falls through to a mirror): the give-up line then annotates as a
# warning instead of an error, so a run that recovered and went green does not
# carry a red ::error:: in its log. Unset it again once the caller runs out of
# fallbacks.
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
      if [ -n "${RETRY_SOFT:-}" ]; then
        echo "::warning::retry: '$*' still failing after ${_retry_max} attempts" >&2
      else
        echo "::error::retry: '$*' still failing after ${_retry_max} attempts" >&2
      fi
      return 1
    fi
    echo "::warning::retry: attempt ${_retry_n}/${_retry_max} of '$*' failed; retrying in ${_retry_delay}s" >&2
    sleep "$_retry_delay"
    _retry_n=$((_retry_n + 1))
    _retry_delay=$((_retry_delay * 2))
  done
}

# Usage: pull_base <image[:tag]> <platform>
#
#     pull_base python:3.14-alpine linux/arm/v6
#
# Pull a Docker OFFICIAL image with registry failover: Docker Hub first, then
# the two public full mirrors of the official library, ECR Public (Docker Inc
# pushes every official-image update there) and mirror.gcr.io (Google's
# anonymous Docker Hub cache). Both serve the same manifest-list digests as
# Docker Hub, so the bytes are identical wherever the pull lands; a mirror hit
# is retagged to the canonical docker.io name so the build step that follows
# never cares which registry answered. Motivated by the 2026-07-25 release run,
# where a Docker Hub outage outlived retry's five attempts and took the armv6
# musl binary down with it.
#
# Official `library/` images only: the mirror paths hardcode that namespace, so
# a namespaced image (org/name) would be rewritten to a path that does not
# exist. Both call sites pass a hardcoded python:3.14-* tag.
pull_base() {
  _pull_image=$1
  _pull_platform=$2
  # Soft while a fallback remains: an exhausted hop that a mirror then rescues
  # is a warning, not a failure of the run.
  RETRY_SOFT=1
  if retry 3 docker pull --platform "$_pull_platform" "$_pull_image"; then
    unset RETRY_SOFT
    return 0
  fi
  for _pull_mirror in public.ecr.aws/docker/library mirror.gcr.io/library; do
    echo "::warning::pull_base: docker.io failed; trying ${_pull_mirror}/${_pull_image}" >&2
    if retry 3 docker pull --platform "$_pull_platform" "${_pull_mirror}/${_pull_image}"; then
      unset RETRY_SOFT
      # Retag to the canonical name, then drop the mirror-named reference so
      # the two names cannot both linger in the local store. A failure here
      # means the build step below would run the wrong (or no) image, so it
      # is fatal rather than ignored.
      if ! docker tag "${_pull_mirror}/${_pull_image}" "$_pull_image"; then
        echo "::error::pull_base: could not retag ${_pull_mirror}/${_pull_image} to ${_pull_image}" >&2
        return 1
      fi
      docker rmi "${_pull_mirror}/${_pull_image}" >/dev/null 2>&1 || true
      return 0
    fi
  done
  unset RETRY_SOFT
  echo "::error::pull_base: '${_pull_image}' unavailable from docker.io and both mirrors" >&2
  return 1
}
