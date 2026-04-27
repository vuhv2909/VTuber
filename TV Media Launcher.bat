@echo off
setlocal
pushd "%~dp0"
py -3.12 -m yt_reup_tool web
popd
endlocal
