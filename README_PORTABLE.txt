YT Reup Tool Bundle

Mode:
- Localhost Web UI
- Default address: http://127.0.0.1:8765

Assumption:
- YAMasterTub is installed at C:\YAMasterTub

Files:
- TV Media Launcher.bat: start the localhost Web UI
- update.bat: download and apply the latest version from GitHub
- setup_machine.bat: verify Python, ffmpeg, ffprobe, and C:\YAMasterTub
- yt_reup_tool\runtime\config.json: shared bundle config
- yt_reup_tool\runtime\state.<machine>.json: per-machine local state
- reup_outputs\: rendered and processed outputs

Use on another machine:
1. Copy this whole folder.
2. Make sure Python 3.12 is installed.
3. Make sure ffmpeg and ffprobe are in PATH.
4. Make sure YAMasterTub exists at C:\YAMasterTub and has your channels/cookies.
5. Run setup_machine.bat.
6. Run TV Media Launcher.bat.
7. The browser should open automatically. If not, open:
   http://127.0.0.1:8765

Update later:
1. Open this folder.
2. Run update.bat.
3. No git knowledge is required.

Notes:
- Each machine keeps its own selected channel and local runtime state.
- If a machine has a different channel list, the tool will fall back to the first loaded channel on that machine.
- update.bat keeps local runtime state, logs, and reup_outputs on that machine.
