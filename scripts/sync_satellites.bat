@echo off
REM %0 is filepath of this batch file. %~dp0 is drive and path of batch file.
REM Alternatively, make a .lnk, but that requires an absolute path.

REM How to activate alternative environment:
REM 1) Use 'call' when you are in a batch script:
REM call activate oldpil
REM 2) Alternatively, call the environment-specific pythonexecutable in the environment:
REM "C:\Program Files (x86)\Anaconda\envs\oldpil\python.exe" %~dp0\..\gelutils\gelannotator_gui.py %1

python %~dp0\labsync.py -v sync


REM IF ERRORLEVEL 1 pause
pause
