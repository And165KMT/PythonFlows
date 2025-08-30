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

%PY% -m uvicorn backend.main:app --reload --host %HOST% --port %PORT%
