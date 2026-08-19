#!/bin/sh
# Runs before removal on the Alpine package (apk's .pre-deinstall).
#
# Stops the service so the daemon does not keep running against a deleted
# binary, and drops it from the default runlevel it may have been added to.
# apk runs its upgrade hooks rather than the deinstall ones on an upgrade, so
# this fires on a real removal only.
#
# The cronstable user, /etc/cronstable.d and /var/lib/cronstable are
# deliberately left behind: they hold configuration and run history.

if [ -x /sbin/rc-service ]; then
    /sbin/rc-service cronstable stop >/dev/null 2>&1 || true
fi
if [ -x /sbin/rc-update ]; then
    /sbin/rc-update del cronstable default >/dev/null 2>&1 || true
fi

# Never exit nonzero; see the post-install script.
exit 0
