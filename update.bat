@echo off
setlocal
cd /d "%~dp0"
git pull --ff-only
if errorlevel 1 (
  echo Update failed.
  exit /b 1
)
echo Update completed.
exit /b 0
