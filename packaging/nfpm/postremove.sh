#!/bin/sh
# Runs after removal on both .deb and .rpm.
#
# Reloads systemd so the removed unit stops being listed. The cronstable user,
# /etc/cronstable.d and /var/lib/cronstable are deliberately left behind: they
# hold configuration and run history, and an upgrade that removes and reinstalls
# must not take a machine's schedule with it. Remove them by hand when you mean
# to.
set -e

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi

exit 0
