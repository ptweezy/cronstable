#!/bin/sh
echo started > started.log
count=0
while [ ! -f drain.release ]; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
        echo expired > expired.log
        exit 24
    fi
    sleep 1
done
echo finished > finished.log
