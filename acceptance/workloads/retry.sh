#!/bin/sh
echo attempt >> attempts.log
if [ -f failed-once ]; then
    echo recovered
    exit 0
fi
echo failed > failed-once
echo deliberate failure >&2
exit 23
