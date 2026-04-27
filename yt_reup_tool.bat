@echo off
setlocal
pushd "%~dp0"

if "%~1"=="" (
    py -3.12 -m yt_reup_tool web
) else (
    py -3.12 -m yt_reup_tool %*
)

popd
endlocal
