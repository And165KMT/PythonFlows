@echo off
REM Easy local run without poetry/venv hassles
setlocal

REM Use venv if present; else fall back to python on PATH
set PY=python
if exist .venv\Scripts\python.exe set PY=.venv\Scripts\python.exe

REM Default host/port
set HOST=127.0.0.1
set PORT=8000
if not "%1"=="" set PORT=%1

REM Reduce VS Code debugger warning
set PYDEVD_DISABLE_FILE_VALIDATION=1

%PY% -Xfrozen_modules=off -m uvicorn backend.main:app --reload --host %HOST% --port %PORT%
