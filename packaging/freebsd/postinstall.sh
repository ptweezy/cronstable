#!/bin/sh
# Runs after install and after upgrade on the FreeBSD package.
#
# Creates the service account the rc.d script runs the daemon as. It never
# enables or starts anything: the shipped configuration schedules no jobs, so
# `sysrc cronstable_enable=YES && service cronstable start` is the
# administrator's call.
#
# pw(8) is the FreeBSD account tool; useradd and adduser --system do not exist
# here, which is why this is its own script rather than the Debian/RPM one. No
# UID is pinned: a vendor package is not in the ports UIDs registry, so pw
# picks the next free system id.
set -e

if ! pw groupshow cronstable >/dev/null 2>&1; then
    pw groupadd cronstable
fi
if ! pw usershow cronstable >/dev/null 2>&1; then
    pw useradd cronstable \
        -g cronstable \
        -d /var/db/cronstable \
        -s /usr/sbin/nologin \
        -c "cronstable job scheduler" \
        -w no
fi

# On an upgrade, restart a service that was already running so it picks up the
# new binary. A stopped or never-enabled service (what a fresh install has) is
# left alone.
if [ -x /usr/local/etc/rc.d/cronstable ] && service cronstable status >/dev/null 2>&1; then
    service cronstable restart >/dev/null 2>&1 || true
fi

exit 0
