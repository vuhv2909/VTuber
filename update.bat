@echo off
setlocal
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0update.ps1"
if errorlevel 1 (
  echo Update failed.
  echo Make sure TV Media is closed before updating.
  exit /b 1
)
echo Update completed.
echo Reopen TV Media. If the browser tab was already open, press Ctrl+F5.
exit /b 0
