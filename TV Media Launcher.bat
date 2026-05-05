@echo off
setlocal
pushd "%~dp0"
echo Launching TV Media from: %CD%
py -3 -m yt_reup_tool web
popd
endlocal
