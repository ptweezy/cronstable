@echo off
if exist read-only (
    cronstable state get beats >> reads.log
    exit /b
)
cronstable state get beats >> reads.log
if errorlevel 5 exit /b
if errorlevel 4 goto write
if errorlevel 1 exit /b
:write
cronstable state set beats %ACCEPTANCE_VALUE%
exit /b
