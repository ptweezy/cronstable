@echo off
echo started> started.log
set /a count=0 >nul
:wait
if exist drain.release goto finish
set /a count+=1 >nul
if %count% GEQ 60 (
    echo expired> expired.log
    exit /b 24
)
ping -n 2 127.0.0.1 >nul
goto wait
:finish
echo finished> finished.log
