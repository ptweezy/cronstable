#!/bin/sh
# Runs after removal on the Alpine package.
#
# The cronstable user, /etc/cronstable.d and /var/lib/cronstable are
# deliberately left behind: they hold configuration and run history, and a
# remove-then-reinstall must not take a machine's schedule with it. Remove them
# by hand when you mean to.
set -e
exit 0
