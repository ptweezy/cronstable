#!/bin/sh
if [ -f read-only ]; then
    exec cronstable state get beats >> reads.log
fi
cronstable state get beats >> reads.log
result=$?
if [ "$result" -ne 0 ] && [ "$result" -ne 4 ]; then exit "$result"; fi
exec cronstable state set beats "$ACCEPTANCE_VALUE"
