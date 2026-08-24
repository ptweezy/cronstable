#!/bin/sh
# Runs after install and after upgrade, on both .deb and .rpm.
#
# Creates the service account the unit runs as and tells systemd about the new
# unit. It never enables or starts anything: the shipped configuration
# schedules no jobs, so a package that started the daemon would leave a running
# service doing nothing on every machine that pulled it in as a dependency.
# `systemctl enable --now cronstable` is the administrator's call.
set -e

# useradd/groupadd rather than adduser --system: the former is in shadow-utils
# and passwd, present on every RPM and Debian base image, while adduser is a
# Debian convenience wrapper that slim images drop.
if ! getent group cronstable >/dev/null 2>&1; then
    groupadd --system cronstable
fi
if ! getent passwd cronstable >/dev/null 2>&1; then
    useradd --system --gid cronstable --home-dir /var/lib/cronstable \
        --no-create-home --shell /usr/sbin/nologin \
        --comment "cronstable job scheduler" cronstable
fi

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    # On an upgrade, restart a service that was already running so it picks up
    # the new binary. try-restart is a no-op on a stopped or never-enabled
    # unit, which is what a fresh install has.
    systemctl try-restart cronstable.service >/dev/null 2>&1 || true
fi

exit 0
