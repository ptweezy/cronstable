#!/bin/sh
# Let the driver observe readiness before the 30-second retry budget starts.
count=0
while [ ! -f retry.release ]; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then exit 24; fi
    sleep 1
done
echo attempt >> attempts.log
if [ -f failed-once ]; then
    echo recovered
    exit 0
fi
echo failed > failed-once
echo deliberate failure >&2
exit 23
