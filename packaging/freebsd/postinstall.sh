#!/bin/sh
# Runs after install and after upgrade on the FreeBSD package.
#
# Creates the service account the rc.d script runs the daemon as. It never
# enables the service: the shipped configuration schedules no jobs, so
# `sysrc cronstable_enable=YES && service cronstable start` is the
# administrator's call.
#
# pkg(8) kills every process a package script leaves running, so this script
# never starts the service. It restarts a running one through daemon(8).
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

# On SIGTERM, cronstable finishes its running jobs and exits, and daemon(8)
# starts the new binary in its place. The script signals only a child of the
# supervisor, so it skips an empty or stale pidfile. pkg prints PKG_MSGFD
# lines at the end of its run.
if [ -x /usr/local/etc/rc.d/cronstable ]; then
    if service cronstable status >/dev/null 2>&1; then
        supervisor=$(cat /var/run/cronstable.pid 2>/dev/null || true)
        child=$(cat /var/run/cronstable-child.pid 2>/dev/null || true)
        parent=
        if [ -n "$child" ]; then
            parent=$(ps -o ppid= -p "$child" 2>/dev/null | tr -d ' ')
        fi
        if [ -n "$supervisor" ] && [ "$parent" = "$supervisor" ]; then
            kill -TERM "$child" 2>/dev/null || true
        fi
    elif service cronstable enabled >/dev/null 2>&1; then
        echo "cronstable is enabled but not running." \
            "To start it, run: service cronstable start" \
            >&"${PKG_MSGFD:-1}" || true
    fi
fi

exit 0
