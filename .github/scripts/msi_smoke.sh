# Shared msiexec helpers for release.yml's MSI smoke steps. The
# incantation is easy to get subtly wrong twice, so it lives once:
#
#   * MSYS2_ARG_CONV_EXCL='*' stops git bash from turning /i /qn into
#     paths.
#   * Exit code 3010 (ERROR_SUCCESS_REBOOT_REQUIRED) is a success that
#     scheduled a file-in-use rename (e.g. Defender holding a fresh
#     DLL). Treating it as failure turns timing into a spurious red,
#     and in sign-windows a spurious red fails the release.
#   * On failure the verbose log's tail is the only diagnostic the
#     runner keeps, so print it.
#
# Sourced, not executed (bash on the Windows runners):
#     . .github/scripts/msi_smoke.sh
#
# Usage:
#     msi_install   <msi> <log> [PROPERTY=value ...]
#     msi_uninstall <msi> <log>
#
# <msi> is a unix-style path; conversion happens here.
msi_run() {
  _msi_verb=$1
  _msi_file="$(cygpath -w "$2")"
  _msi_log=$3
  shift 3
  _msi_rc=0
  MSYS2_ARG_CONV_EXCL='*' msiexec "$_msi_verb" "$_msi_file" "$@" \
    /qn /norestart /l\*v "$_msi_log" || _msi_rc=$?
  if [ "$_msi_rc" -ne 0 ] && [ "$_msi_rc" -ne 3010 ]; then
    echo "msiexec $_msi_verb exited $_msi_rc; last 100 log lines:" >&2
    tail -n 100 "$_msi_log" >&2
    return "$_msi_rc"
  fi
  return 0
}

msi_install() {
  _msi_target=$1
  _msi_logfile=$2
  shift 2
  msi_run /i "$_msi_target" "$_msi_logfile" "$@"
}

msi_uninstall() {
  msi_run /x "$1" "$2"
}
