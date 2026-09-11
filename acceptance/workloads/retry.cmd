@echo off
set /a count=0 >nul
:wait
if exist retry.release goto attempt
set /a count+=1 >nul
if %count% GEQ 60 exit /b 24
ping -n 2 127.0.0.1 >nul
goto wait
:attempt
echo attempt>> attempts.log
if exist failed-once (
    echo recovered
    exit /b 0
)
echo failed> failed-once
echo deliberate failure >&2
exit /b 23
