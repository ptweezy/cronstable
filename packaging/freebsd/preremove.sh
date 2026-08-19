#!/bin/sh
# Runs before removal on the FreeBSD package.
#
# Stops the service so the daemon does not keep running against a deleted
# binary. pkg(8) runs this on a real deinstall; an upgrade goes through the
# install scripts of the new package, which restart what was running.
#
# The cronstable user, /usr/local/etc/cronstable.d and /var/db/cronstable are
# deliberately left behind: they hold configuration and run history.
set -e

if [ -x /usr/local/etc/rc.d/cronstable ]; then
    service cronstable stop >/dev/null 2>&1 || true
fi

exit 0
