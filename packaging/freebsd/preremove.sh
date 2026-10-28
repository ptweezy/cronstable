#!/bin/sh
# Runs before removal on the FreeBSD package.
#
# Stops the service so the daemon does not keep running against a deleted
# binary. pkg(8) also runs this from the old package during an in-place
# upgrade, with PKG_UPGRADE=true; the daemon keeps running, and the new
# package's post-install restarts it on the new binary. pkg keeps PKG_UPGRADE
# set for the rest of its run, so a removal after any upgrade script in the
# same run also skips the stop.
#
# The cronstable user, /usr/local/etc/cronstable.d and /var/db/cronstable are
# deliberately left behind: they hold configuration and run history.
set -e

if [ "${PKG_UPGRADE:-}" = true ]; then
    exit 0
fi

if [ -x /usr/local/etc/rc.d/cronstable ]; then
    service cronstable stop >/dev/null 2>&1 || true
fi

exit 0
