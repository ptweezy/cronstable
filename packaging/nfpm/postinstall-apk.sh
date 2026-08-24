#!/bin/sh
# Runs after install on the Alpine package.
#
# The service account is created in the pre-install script. This one only tells
# the administrator how to start the service, and restarts it on an upgrade if
# it was already running.
#
# It deliberately does NOT run `rc-update add`: no package in aports enables its
# own service, and the shipped configuration schedules no jobs anyway.

if [ -x /sbin/rc-service ] && /sbin/rc-service cronstable status >/dev/null 2>&1; then
    # Already running, so this is an upgrade: pick up the new binary.
    /sbin/rc-service cronstable restart >/dev/null 2>&1 || true
else
    echo 'To enable cronstable: rc-update add cronstable default && rc-service cronstable start' 1>&2
fi

# Never exit nonzero: apk marks the package broken_script and `apk add` returns
# nonzero, which fails a docker build layer, even though the install succeeded.
exit 0
