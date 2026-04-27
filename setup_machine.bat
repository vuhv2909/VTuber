@echo off
setlocal
pushd "%~dp0"

echo Checking Python 3.12...
py -3.12 -c "import sys; print(sys.version)" >nul 2>nul
if errorlevel 1 (
    echo Python 3.12 was not found.
    exit /b 1
)

echo Checking ffmpeg...
ffmpeg -version >nul 2>nul
if errorlevel 1 (
    echo ffmpeg was not found in PATH.
    exit /b 1
)

echo Checking ffprobe...
ffprobe -version >nul 2>nul
if errorlevel 1 (
    echo ffprobe was not found in PATH.
    exit /b 1
)

echo Checking C:\YAMasterTub...
if not exist "C:\YAMasterTub\AAS_check_delete_video.py" (
    echo Missing C:\YAMasterTub or required AAS_check_delete_video.py.
    exit /b 1
)
if not exist "C:\YAMasterTub\storage\audio-subtitles-videos-channels.json" (
    echo Missing C:\YAMasterTub\storage\audio-subtitles-videos-channels.json.
    exit /b 1
)
if not exist "C:\YAMasterTub\storage\language-codes.txt" (
    echo Missing C:\YAMasterTub\storage\language-codes.txt.
    exit /b 1
)

echo Checking tool startup...
py -3.12 -m yt_reup_tool status
if errorlevel 1 (
    echo Tool status check failed.
    exit /b 1
)

echo.
echo Setup check passed.
echo Web UI address: http://127.0.0.1:8765
popd
endlocal
