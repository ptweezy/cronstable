#!/bin/sh
# Runs before removal on both .deb and .rpm.
#
# Stop and disable the service, but only on a real removal. RPM passes "1" here
# during an upgrade (one package left after this transaction) and "0" on the
# final removal; dpkg passes the word "upgrade". Stopping on an upgrade would
# take the scheduler down for every package refresh, which postinstall's
# try-restart handles properly instead.
set -e

case "${1:-}" in
    1|upgrade) exit 0 ;;
esac

if [ -d /run/systemd/system ]; then
    systemctl --no-reload disable --now cronstable.service >/dev/null 2>&1 || true
fi

exit 0
