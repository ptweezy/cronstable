#!/bin/sh
# Runs before install on the Alpine package.
#
# Creates the service account the OpenRC service runs as. Before rather than
# after, which is the aports convention, and busybox tools rather than the
# groupadd/useradd the Debian and RPM scripts use: neither exists on Alpine, and
# that mismatch is the usual reason a Debian packaging script fails here.
#
# apk hands install scripts an otherwise empty environment with
# PATH=/usr/sbin:/usr/bin:/sbin:/bin, run as root through this shebang, so
# /bin/sh here is busybox ash.
#
# addgroup must come first: `adduser -G` resolves the group through xgroup2gid,
# which exits on failure, so a missing group kills the script mid-way. Both
# commands fail nonzero when the account already exists, and a nonzero install
# script makes `apk add` return nonzero (failing a docker build layer) even
# though the package installed fine, hence the tests and the final exit 0.
#
# /etc/group and /etc/passwd are read directly rather than through getent: that
# comes from musl-utils, not busybox, so it is not guaranteed on a minimal
# install.
grep -q '^cronstable:' /etc/group || addgroup -S cronstable 2>/dev/null
grep -q '^cronstable:' /etc/passwd || adduser -S -D -H \
    -h /var/lib/cronstable \
    -s /sbin/nologin \
    -G cronstable \
    -g "cronstable job scheduler" \
    cronstable 2>/dev/null

exit 0
