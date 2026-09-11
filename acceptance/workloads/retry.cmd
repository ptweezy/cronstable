@echo off
echo attempt>> attempts.log
if exist failed-once (
    echo recovered
    exit /b 0
)
echo failed> failed-once
echo deliberate failure >&2
exit /b 23
