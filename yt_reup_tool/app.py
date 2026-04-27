from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import tkinter as tk
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable

from .pairing import PairRow, build_pair_rows

APP_TITLE = "YT Reup Tool"
STATE_VERSION = 2
RENDERED_STATUS = "Rendered"
PROCESSED_STATUS = "Processed"
UPLOADED_STATUS = "Uploaded"
VIDEO_CREATED_STATUS = "VideoCreated"
PREMIUM_DONE_STATUS = "PremiumDone"
PENDING_RETRY_STATUS = "PendingRetry"
DEFAULT_LANGUAGE_CODES = [
    "en",
    "en-AU",
    "en-CA",
    "en-IN",
    "en-IE",
    "en-GB",
    "en-US",
    "es",
    "es-419",
    "es-MX",
    "es-ES",
    "es-US",
]

MUSIC_FILETYPES = [
    ("Audio files", "*.mp3 *.wav *.m4a *.flac *.aac *.ogg"),
    ("All files", "*.*"),
]
VIDEO_FILETYPES = [
    ("Video files", "*.mp4 *.mov *.mkv *.avi *.webm"),
    ("All files", "*.*"),
]
MUSIC_EXTENSIONS = {".mp3", ".wav", ".m4a", ".flac", ".aac", ".ogg"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}

UI_BG = "#eef3f8"
SURFACE_BG = "#ffffff"
SURFACE_ALT_BG = "#f7f9fc"
SURFACE_BORDER = "#d7dee8"
TEXT_PRIMARY = "#101828"
TEXT_MUTED = "#667085"
ACCENT_BLUE = "#0071e3"
ACCENT_BLUE_SOFT = "#e8f2ff"
SUCCESS_GREEN = "#127b42"
SUCCESS_SOFT = "#eaf8ef"
WARNING_AMBER = "#9a6700"
WARNING_SOFT = "#fff4db"
ERROR_RED = "#c0362c"
ERROR_SOFT = "#fdecea"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def normalize_path_string(value: str | os.PathLike[str] | None) -> str:
    if value is None:
        return ""
    return str(Path(value).resolve(strict=False)).casefold()


def machine_name_slug(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        raw = "unknown-machine"
    slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    return slug or "unknown-machine"


@contextmanager
def pushd(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def parse_json_maybe(value: Any, fallback: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return value if value is not None else fallback


def phase_error_label(phase: str) -> str:
    return f"Error: {phase.capitalize()}"


def probe_duration_seconds(path: Path, ffprobe_bin: str) -> float:
    result = subprocess.run(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    value = (result.stdout or "").strip()
    if not value:
        raise RuntimeError(f"ffprobe did not return duration for {path}")
    return float(value)


def probe_primary_stream_codec(path: Path, ffprobe_bin: str, stream_selector: str) -> str:
    result = subprocess.run(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-select_streams",
            stream_selector,
            "-show_entries",
            "stream=codec_name",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return (result.stdout or "").strip().lower()


class QueueLogHandler(logging.Handler):
    def __init__(self, sink: queue.Queue[str]):
        super().__init__()
        self.sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.sink.put_nowait(self.format(record))
        except Exception:
            return


class YaMasterTubBackend:
    def __init__(self, root: Path, logs_dir: Path, logger: logging.Logger):
        self.root = root.resolve()
        self.logs_dir = logs_dir
        self.logger = logger
        self._api = None
        self._checker_proc: subprocess.Popen[str] | None = None
        self._checker_log_handle = None

    def _ensure_api(self):
        if self._api is not None:
            return self._api

        if str(self.root) not in sys.path:
            sys.path.insert(0, str(self.root))

        with pushd(self.root):
            import app.api_handlers as api_handlers  # type: ignore

        self._api = api_handlers
        return self._api

    def _call(self, name: str, *args):
        api = self._ensure_api()
        with pushd(self.root):
            return getattr(api, name)(*args)

    def get_channels(self) -> list[str]:
        return list(self._call("getChannels") or [])

    def get_language_codes(self) -> str:
        return str(self._call("getLanguageCodes") or "")

    def save_language_codes(self, language_codes: str) -> Any:
        return self._call("saveLanguageCodes", language_codes)

    def get_aas_config(self) -> dict[str, Any]:
        return parse_json_maybe(self._call("getAudioSubtitlesVideosChannels"), {})

    def save_aas_config(self, config: dict[str, Any]) -> Any:
        return self._call("saveAudioSubtitlesVideosChannels", config)

    def upload_video_file(self, channel_folder_name: str, index: int) -> Any:
        return self._call("AAS_uploadVideoFile", channel_folder_name, index)

    def create_video(self, channel_folder_name: str, index: int) -> Any:
        return self._call("AAS_createVideo", channel_folder_name, index)

    def add_subtitles_premium(self) -> Any:
        return self._call("addSubtitlesPremium")

    def is_aas_checker_running(self) -> bool:
        try:
            return bool(self._call("AAS_isCheckDeleteVideoRunning"))
        except Exception as exc:
            self.logger.warning("Could not query AAS checker status: %s", exc)
            return False

    def ensure_aas_checker_running(self) -> bool:
        if self.is_aas_checker_running():
            if self._checker_proc is not None and self._checker_proc.poll() is not None:
                self.logger.warning("Local AAS checker process exited; external checker is still running.")
            return False

        log_path = self.logs_dir / "aas_check_delete_video.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._checker_log_handle = log_path.open("a", encoding="utf-8")
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        self._checker_proc = subprocess.Popen(
            [sys.executable, "AAS_check_delete_video.py"],
            cwd=str(self.root),
            stdout=self._checker_log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=creationflags,
        )
        self.logger.info("Started AAS_check_delete_video.py (pid=%s)", self._checker_proc.pid)
        time.sleep(2.0)
        return True

    def shutdown(self) -> None:
        proc = self._checker_proc
        if proc is not None and proc.poll() is None:
            self.logger.info("Stopping AAS_check_delete_video.py (pid=%s)", proc.pid)
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        self._checker_proc = None
        if self._checker_log_handle is not None:
            self._checker_log_handle.close()
            self._checker_log_handle = None


class ReupPipelineService:
    def __init__(
        self,
        config_path: Path | None = None,
        state_path: Path | None = None,
        backend: Any | None = None,
    ):
        package_dir = Path(__file__).resolve().parent
        self.bundle_root = package_dir.parent
        self.runtime_dir = package_dir / "runtime"
        self.logs_dir = self.runtime_dir / "logs"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.machine_name = os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "unknown-machine"
        self.machine_key = machine_name_slug(self.machine_name)
        self.legacy_state_path = self.runtime_dir / "state.json"

        self.config_path = config_path or self.runtime_dir / "config.json"
        self.state_path = state_path or self.runtime_dir / f"state.{self.machine_key}.json"
        self.logger = self._build_logger()
        self._lock = threading.RLock()

        self.config = self._load_or_create_config()
        self.state = self._load_or_create_state()
        self.language_codes = list(self.config.get("language_codes") or DEFAULT_LANGUAGE_CODES)
        self.yamastertub_root = self._resolve_configured_path(self.config["yamastertub_root"])
        self.output_dir = self._resolve_configured_path(self.config["output_dir"])
        self.ffmpeg_bin = self._resolve_binary(self.config.get("ffmpeg_path") or "ffmpeg")
        self.ffprobe_bin = self._resolve_binary(self.config.get("ffprobe_path") or "ffprobe")
        self.backend = backend or YaMasterTubBackend(self.yamastertub_root, self.logs_dir, self.logger)
        self._ffmpeg_encoder_text: str | None = None

        self._validate_runtime()
        try:
            self._sync_current_inputs()
        except RuntimeError as exc:
            self.logger.warning("Clearing invalid folder inputs from state: %s", exc)
            self.state["music_folder"] = ""
            self.state["video_folder"] = ""
            self.sync_pairings_from_lists([], [])

    def _build_logger(self) -> logging.Logger:
        logger = logging.getLogger(f"yt_reup_tool.{id(self)}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler = logging.FileHandler(self.logs_dir / "app.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        return logger

    def _default_yamastertub_root(self) -> Path:
        for candidate in ("YAMasterTub", "YaMasterTub"):
            path = self.bundle_root / candidate
            if path.exists():
                return path
        return self.bundle_root / "YAMasterTub"

    def _default_channel_name(self) -> str:
        watcher_config = self.bundle_root / "yt_premium_watcher" / "runtime" / "config.json"
        if watcher_config.exists():
            try:
                data = json.loads(watcher_config.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return ""
            return str(data.get("channel_folder_name") or "").strip()
        return ""

    def _load_or_create_config(self) -> dict[str, Any]:
        if self.config_path.exists():
            config = json.loads(self.config_path.read_text(encoding="utf-8-sig"))
            changed = False
            if "auto_start_aas_delete_checker" not in config:
                config["auto_start_aas_delete_checker"] = False
                changed = True
            if changed:
                self.config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
            return config

        config = {
            "yamastertub_root": os.path.relpath(self._default_yamastertub_root(), start=self.config_path.parent),
            "channel_folder_name": self._default_channel_name(),
            "language_codes": DEFAULT_LANGUAGE_CODES,
            "output_dir": os.path.relpath(self.bundle_root / "reup_outputs", start=self.config_path.parent),
            "ffmpeg_path": "ffmpeg",
            "ffprobe_path": "ffprobe",
            "auto_start_aas_delete_checker": False,
        }
        self.config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        return config

    def _blank_state(self) -> dict[str, Any]:
        return {
            "version": STATE_VERSION,
            "machine_name": self.machine_name,
            "machine_key": self.machine_key,
            "bundle_root": str(self.bundle_root),
            "selected_channel": "",
            "music_folder": "",
            "video_folder": "",
            "music_files": [],
            "video_files": [],
            "jobs": {},
        }

    def _load_state_file(self, path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8-sig"))

    def _normalize_loaded_state(self, data: dict[str, Any] | None) -> dict[str, Any]:
        source = data or {}
        state = self._blank_state()
        state["version"] = int(source.get("version") or STATE_VERSION)
        state["selected_channel"] = str(source.get("selected_channel") or "").strip()
        state["music_folder"] = str(source.get("music_folder") or "").strip()
        state["video_folder"] = str(source.get("video_folder") or "").strip()
        state["music_files"] = list(source.get("music_files") or [])
        state["video_files"] = list(source.get("video_files") or [])
        state["jobs"] = dict(source.get("jobs") or {})

        source_machine_key = str(source.get("machine_key") or "").strip()
        source_machine_name = str(source.get("machine_name") or "").strip()
        if source_machine_key and source_machine_key != self.machine_key:
            self.logger.warning(
                "State file %s belongs to machine %s (%s); starting with a clean local state.",
                self.state_path,
                source_machine_name or "unknown",
                source_machine_key,
            )
            return self._blank_state()

        return state

    def _load_or_create_state(self) -> dict[str, Any]:
        data: dict[str, Any] | None = None
        if self.state_path.exists():
            data = self._load_state_file(self.state_path)
        elif self.state_path == self.legacy_state_path and self.legacy_state_path.exists():
            data = self._load_state_file(self.legacy_state_path)
        elif self.state_path != self.legacy_state_path and self.legacy_state_path.exists():
            self.logger.info(
                "Ignoring legacy shared state file %s to avoid cross-machine conflicts; using %s instead.",
                self.legacy_state_path,
                self.state_path,
            )

        state = self._normalize_loaded_state(data)
        self._save_state(state)
        return state

    def _save_state(self, state: dict[str, Any] | None = None) -> None:
        if state is not None:
            self.state = state
        self.state["version"] = int(self.state.get("version") or STATE_VERSION)
        self.state["machine_name"] = self.machine_name
        self.state["machine_key"] = self.machine_key
        self.state["bundle_root"] = str(self.bundle_root)
        self.state["selected_channel"] = str(self.state.get("selected_channel") or "").strip()
        self.state["music_folder"] = str(self.state.get("music_folder") or "").strip()
        self.state["video_folder"] = str(self.state.get("video_folder") or "").strip()
        self.state_path.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    def _save_config(self) -> None:
        self.config_path.write_text(json.dumps(self.config, indent=2), encoding="utf-8")

    def _resolve_configured_path(self, raw_path: str | os.PathLike[str]) -> Path:
        path = Path(raw_path)
        if path.is_absolute():
            return path.resolve(strict=False)
        return (self.config_path.parent / path).resolve(strict=False)

    def _resolve_binary(self, raw_value: str) -> str:
        if not raw_value:
            raise RuntimeError("Empty binary path in config.")
        candidate = Path(raw_value)
        if candidate.is_absolute() or candidate.parent != Path("."):
            return str(self._resolve_configured_path(raw_value))
        found = shutil.which(raw_value)
        return found or raw_value

    def _validate_runtime(self) -> None:
        if sys.version_info[:2] != (3, 12):
            raise RuntimeError("yt_reup_tool must run on Python 3.12.")

        if shutil.which(self.ffmpeg_bin) is None and not Path(self.ffmpeg_bin).exists():
            raise RuntimeError(f"ffmpeg not found: {self.ffmpeg_bin}")
        if shutil.which(self.ffprobe_bin) is None and not Path(self.ffprobe_bin).exists():
            raise RuntimeError(f"ffprobe not found: {self.ffprobe_bin}")

        if not self.yamastertub_root.exists():
            raise RuntimeError(f"YaMasterTub root not found: {self.yamastertub_root}")

        storage_dir = self.yamastertub_root / "storage"
        required_paths = [
            storage_dir / "audio-subtitles-videos-channels.json",
            storage_dir / "language-codes.txt",
            self.yamastertub_root / "AAS_check_delete_video.py",
        ]
        missing = [str(path) for path in required_paths if not path.exists()]
        if missing:
            raise RuntimeError(f"Missing required YaMasterTub files: {', '.join(missing)}")

        channels = list(self.backend.get_channels() or [])
        channel_name = self.channel_folder_name
        if not channel_name:
            if not channels:
                raise RuntimeError("No YaMasterTub channels found.")
            channel_name = channels[0]
            self.state["selected_channel"] = channel_name
            self._save_state()
            self.logger.info("Selected channel was empty for this machine; defaulted to %s", channel_name)
        elif channel_name not in channels:
            if not channels:
                raise RuntimeError(f"Configured channel not found in YaMasterTub: {channel_name}")
            fallback_channel = channels[0]
            self.logger.warning(
                "Selected channel %s was not found in YaMasterTub on this machine; falling back to %s",
                channel_name,
                fallback_channel,
            )
            self.state["selected_channel"] = fallback_channel
            self._save_state()

    @property
    def channel_folder_name(self) -> str:
        selected = str(self.state.get("selected_channel") or "").strip()
        if selected:
            return selected
        return str(self.config.get("channel_folder_name") or "").strip()

    def get_available_channels(self) -> list[str]:
        return list(self.backend.get_channels() or [])

    @property
    def auto_start_aas_delete_checker(self) -> bool:
        return bool(self.config.get("auto_start_aas_delete_checker"))

    def set_channel_folder_name(self, channel_name: str) -> None:
        target = str(channel_name or "").strip()
        channels = self.get_available_channels()
        if not target:
            raise RuntimeError("Channel name cannot be empty.")
        if target not in channels:
            raise RuntimeError(f"Channel not found in YaMasterTub: {target}")
        if target == self.channel_folder_name:
            return
        self.state["selected_channel"] = target
        self._save_state()
        self.logger.info("Switched upload channel on machine %s to %s", self.machine_name, target)

    @property
    def music_files(self) -> list[str]:
        return list(self.state.get("music_files") or [])

    @property
    def video_files(self) -> list[str]:
        return list(self.state.get("video_files") or [])

    @property
    def music_folder(self) -> str:
        return str(self.state.get("music_folder") or "").strip()

    @property
    def video_folder(self) -> str:
        return str(self.state.get("video_folder") or "").strip()

    @property
    def language_codes_string(self) -> str:
        return " ".join(self.language_codes)

    def _normalize_input_folder(self, raw_path: str | os.PathLike[str] | None) -> str:
        value = str(raw_path or "").strip()
        if not value:
            return ""
        return str(Path(value).expanduser().resolve(strict=False))

    def _scan_media_folder(self, folder_path: str, extensions: set[str]) -> list[str]:
        normalized = self._normalize_input_folder(folder_path)
        if not normalized:
            return []
        folder = Path(normalized)
        if not folder.exists():
            raise RuntimeError(f"Folder not found: {folder}")
        if not folder.is_dir():
            raise RuntimeError(f"Path is not a folder: {folder}")
        files = [
            path.resolve(strict=False)
            for path in folder.iterdir()
            if path.is_file() and path.suffix.casefold() in extensions
        ]
        files.sort(key=lambda path: (path.name.casefold(), str(path).casefold()))
        return [str(path) for path in files]

    def _build_folder_pairings(
        self,
        music_folder: str,
        video_folder: str,
    ) -> tuple[list[str], list[str], list[str], list[str]]:
        source_music_files = self._scan_media_folder(music_folder, MUSIC_EXTENSIONS)
        source_video_files = self._scan_media_folder(video_folder, VIDEO_EXTENSIONS)
        if not source_video_files:
            return source_music_files, source_video_files, [], []
        if not source_music_files:
            return source_music_files, source_video_files, [""] * len(source_video_files), list(source_video_files)

        paired_music_files = [
            source_music_files[index % len(source_music_files)]
            for index in range(len(source_video_files))
        ]
        paired_video_files = list(source_video_files)
        return source_music_files, source_video_files, paired_music_files, paired_video_files

    def _current_pairing_lists(self) -> tuple[list[str], list[str]]:
        if self.music_folder or self.video_folder:
            _source_music, _source_video, paired_music, paired_video = self._build_folder_pairings(
                self.music_folder,
                self.video_folder,
            )
            return paired_music, paired_video
        return self.music_files, self.video_files

    def _sync_current_inputs(self) -> list[PairRow]:
        music_files, video_files = self._current_pairing_lists()
        return self.sync_pairings_from_lists(music_files, video_files)

    def get_input_overview(self) -> dict[str, Any]:
        with self._lock:
            source_music_files: list[str] = list(self.music_files)
            source_video_files: list[str] = list(self.video_files)
            paired_music_files, paired_video_files = self.music_files, self.video_files
            if self.music_folder or self.video_folder:
                source_music_files, source_video_files, paired_music_files, paired_video_files = self._build_folder_pairings(
                    self.music_folder,
                    self.video_folder,
                )
            rows = build_pair_rows(paired_music_files, paired_video_files)
            return {
                "music_folder": self.music_folder,
                "video_folder": self.video_folder,
                "source_music_count": len(source_music_files),
                "source_video_count": len(source_video_files),
                "paired_count": len(rows),
                "rows": rows,
            }

    def add_log_handler(self, handler: logging.Handler) -> None:
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(handler)

    def remove_log_handler(self, handler: logging.Handler) -> None:
        if handler in self.logger.handlers:
            self.logger.removeHandler(handler)

    def _job_artifacts(self, output_base: str) -> dict[str, str]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        return {
            "output_mp4": str((self.output_dir / f"{output_base}.mp4").resolve(strict=False)),
            "output_m4a": str((self.output_dir / f"{output_base}.m4a").resolve(strict=False)),
            "processed_mp4": str((self.output_dir / f"{output_base}_processed.mp4").resolve(strict=False)),
        }

    def _new_job(self, row: PairRow) -> dict[str, Any]:
        artifacts = self._job_artifacts(row.output_base)
        return {
            "output_base": row.output_base,
            "index": row.index,
            "music_path": row.music_path,
            "video_path": row.video_path,
            "row_status": row.status,
            "render_status": "",
            "process_status": "",
            "upload_status": "",
            "premium_status": "",
            "overall_status": row.status,
            "video_id": "",
            "last_error": "",
            "stale": False,
            "attempts": {"upload": 0, "create": 0, "premium": 0},
            "created_at": now_iso(),
            "updated_at": now_iso(),
            **artifacts,
        }

    def _job_sources_match(self, job: dict[str, Any], row: PairRow) -> bool:
        return (
            normalize_path_string(job.get("music_path")) == normalize_path_string(row.music_path)
            and normalize_path_string(job.get("video_path")) == normalize_path_string(row.video_path)
        )

    def _reset_from_phase(self, job: dict[str, Any], phase: str) -> None:
        if phase == "render":
            job["render_status"] = ""
            job["process_status"] = ""
            job["upload_status"] = ""
            job["premium_status"] = ""
            job["video_id"] = ""
        elif phase == "process":
            job["process_status"] = ""
            job["upload_status"] = ""
            job["premium_status"] = ""
            job["video_id"] = ""
        elif phase == "upload":
            job["upload_status"] = ""
            job["premium_status"] = ""
            job["video_id"] = ""
        elif phase == "premium":
            job["premium_status"] = ""
        job["last_error"] = ""

    def _mark_job_error(self, job: dict[str, Any], phase: str, message: str) -> None:
        key = f"{phase}_status"
        job[key] = "Error"
        if phase == "render":
            job["process_status"] = ""
            job["upload_status"] = ""
            job["premium_status"] = ""
            job["video_id"] = ""
        elif phase == "process":
            job["upload_status"] = ""
            job["premium_status"] = ""
            job["video_id"] = ""
        elif phase == "upload":
            job["premium_status"] = ""
            job["video_id"] = ""
        job["overall_status"] = phase_error_label(phase)
        job["last_error"] = message
        job["updated_at"] = now_iso()
        self._save_state()

    def _update_overall_status(self, job: dict[str, Any]) -> None:
        row_status = str(job.get("row_status") or "")
        if row_status and row_status != "Ready":
            job["overall_status"] = row_status
            return
        if job.get("render_status") == "Error":
            job["overall_status"] = phase_error_label("render")
            return
        if job.get("process_status") == "Error":
            job["overall_status"] = phase_error_label("process")
            return
        if job.get("upload_status") == "Error":
            job["overall_status"] = phase_error_label("upload")
            return
        if job.get("premium_status") in {"Error", PENDING_RETRY_STATUS}:
            job["overall_status"] = phase_error_label("premium")
            return
        if job.get("premium_status") == PREMIUM_DONE_STATUS:
            job["overall_status"] = PREMIUM_DONE_STATUS
            return
        if job.get("upload_status") == VIDEO_CREATED_STATUS:
            job["overall_status"] = VIDEO_CREATED_STATUS
            return
        if job.get("process_status") == PROCESSED_STATUS:
            job["overall_status"] = PROCESSED_STATUS
            return
        if job.get("render_status") == RENDERED_STATUS:
            job["overall_status"] = RENDERED_STATUS
            return
        job["overall_status"] = "Ready"

    def _refresh_job_artifact_state(self, job: dict[str, Any]) -> None:
        output_mp4 = Path(str(job.get("output_mp4") or ""))
        output_m4a = Path(str(job.get("output_m4a") or ""))
        processed_mp4 = Path(str(job.get("processed_mp4") or ""))

        if job.get("render_status") == RENDERED_STATUS and (not output_mp4.exists() or not output_m4a.exists()):
            self._reset_from_phase(job, "render")
        if job.get("process_status") == PROCESSED_STATUS and not processed_mp4.exists():
            self._reset_from_phase(job, "process")
        self._update_overall_status(job)

    def sync_pairings_from_lists(self, music_files: list[str], video_files: list[str]) -> list[PairRow]:
        with self._lock:
            self.state["music_files"] = list(music_files)
            self.state["video_files"] = list(video_files)
            jobs = self.state.setdefault("jobs", {})
            rows = build_pair_rows(music_files, video_files)
            active_bases = set()

            for row in rows:
                active_bases.add(row.output_base)
                existing = jobs.get(row.output_base)
                if existing is None:
                    job = self._new_job(row)
                    jobs[row.output_base] = job
                elif row.status == "Ready" and not self._job_sources_match(existing, row):
                    previous_video_id = str(existing.get("video_id") or "").strip()
                    job = self._new_job(row)
                    job["created_at"] = str(existing.get("created_at") or now_iso())
                    jobs[row.output_base] = job
                    if previous_video_id:
                        self.logger.info("Reset job %s because source files changed", row.output_base)
                else:
                    job = existing
                    job["index"] = row.index
                    job["music_path"] = row.music_path
                    job["video_path"] = row.video_path
                    job.update(self._job_artifacts(row.output_base))

                job["output_base"] = row.output_base
                job["row_status"] = row.status
                job["stale"] = False
                job["updated_at"] = now_iso()
                self._refresh_job_artifact_state(job)

            for base_name, job in jobs.items():
                if base_name not in active_bases:
                    job["stale"] = True
                    job["updated_at"] = now_iso()

            self._save_state()
            return rows

    def get_display_rows(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._sync_current_inputs()
            jobs = self.state.setdefault("jobs", {})
            aas_config = parse_json_maybe(self.backend.get_aas_config(), {})
            channel_cfg = aas_config.get(self.channel_folder_name) or {}
            entries = channel_cfg.get("audioSubtitlesVideos") or []
            display_rows: list[dict[str, Any]] = []
            for row in rows:
                job = jobs[row.output_base]
                self._refresh_job_artifact_state(job)
                _entry_index, entry = self._find_matching_entry(entries, job)
                display_rows.append(
                    {
                        "index": row.index,
                        "output_base": row.output_base,
                        "music_path": row.music_path,
                        "video_path": row.video_path,
                        "render_status": job.get("render_status") or "",
                        "process_status": job.get("process_status") or "",
                        "upload_status": job.get("upload_status") or "",
                        "video_id": job.get("video_id") or "",
                        "premium_status": job.get("premium_status") or "",
                        "addsub_done": self._entry_addsub_done_label(entry),
                        "overall_status": job.get("overall_status") or row.status,
                        "last_error": job.get("last_error") or "",
                    }
                )
            self._save_state()
            return display_rows

    def get_job(self, output_base: str) -> dict[str, Any]:
        self._sync_current_inputs()
        job = self.state.setdefault("jobs", {}).get(output_base)
        if job is None:
            raise KeyError(output_base)
        self._refresh_job_artifact_state(job)
        self._save_state()
        return job

    def status_lines(self) -> list[str]:
        rows = self.get_display_rows()
        lines = [
            f"State file: {self.state_path}",
            f"Machine: {self.machine_name} ({self.machine_key})",
        ]
        if not rows:
            lines.append("No jobs recorded.")
            return lines
        lines.extend(
            f"{row['output_base']}: overall={row['overall_status']} render={row['render_status'] or '-'} "
            f"process={row['process_status'] or '-'} upload={row['upload_status'] or '-'} "
            f"video_id={row['video_id'] or '-'} addsub={row['addsub_done']} premium={row['premium_status'] or '-'}"
            for row in rows
        )
        return lines

    def save_pairings(self, music_files: list[str], video_files: list[str]) -> None:
        self.state["music_folder"] = ""
        self.state["video_folder"] = ""
        self.sync_pairings_from_lists(music_files, video_files)
        self.logger.info("Saved pairing state with %s music files and %s video files", len(music_files), len(video_files))

    def save_input_folders(self, music_folder: str, video_folder: str) -> None:
        normalized_music_folder = self._normalize_input_folder(music_folder)
        normalized_video_folder = self._normalize_input_folder(video_folder)
        source_music_files, source_video_files, paired_music, paired_video = self._build_folder_pairings(
            normalized_music_folder,
            normalized_video_folder,
        )
        self.state["music_folder"] = normalized_music_folder
        self.state["video_folder"] = normalized_video_folder
        rows = self.sync_pairings_from_lists(paired_music, paired_video)
        self.logger.info(
            "Saved folder inputs with %s music files, %s video files, %s generated rows",
            len(source_music_files),
            len(source_video_files),
            len(rows),
        )

    def _run_command(self, command: list[str]) -> None:
        self.logger.info("Running command: %s", " ".join(command))
        try:
            subprocess.run(
                command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or exc.stdout or "").strip()
            stderr = stderr[-1200:] if stderr else ""
            raise RuntimeError(stderr or f"Command failed with exit code {exc.returncode}") from exc

    def _ffmpeg_encoder_available(self, encoder_name: str) -> bool:
        if self._ffmpeg_encoder_text is None:
            try:
                result = subprocess.run(
                    [self.ffmpeg_bin, "-hide_banner", "-encoders"],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
            except subprocess.CalledProcessError as exc:
                self.logger.warning("Could not query ffmpeg encoders: %s", exc)
                self._ffmpeg_encoder_text = ""
            else:
                self._ffmpeg_encoder_text = "\n".join([result.stdout or "", result.stderr or ""])
        return encoder_name in (self._ffmpeg_encoder_text or "")

    def _get_render_video_options(self, video_path: Path) -> tuple[str, list[str]]:
        codec = ""
        try:
            codec = probe_primary_stream_codec(video_path, self.ffprobe_bin, "v:0")
        except Exception as exc:
            self.logger.warning("Could not probe source video codec for %s: %s", video_path, exc)

        if codec in {"h264", "hevc", "mpeg4"}:
            return "Direct Video Copy", ["-c:v", "copy"]

        if self._ffmpeg_encoder_available("h264_nvenc"):
            return "NVIDIA NVENC", ["-c:v", "h264_nvenc", "-preset", "p1", "-cq", "24"]

        return "CPU libx264", ["-c:v", "libx264", "-preset", "veryfast"]

    def _get_render_audio_options(self, music_path: Path) -> tuple[str, list[str]]:
        ext = music_path.suffix.lower()
        if ext in {".mp3", ".m4a", ".aac"}:
            return "Direct Audio Copy", ["-c:a", "copy"]
        return "AAC Audio Encode", ["-c:a", "aac", "-b:a", "192k"]

    def _render_commands(self, video_path: Path, music_path: Path, output_mp4: Path, duration: float) -> list[tuple[str, list[str]]]:
        base_prefix = [
            self.ffmpeg_bin,
            "-y",
            "-threads",
            "0",
            "-fflags",
            "+genpts",
            "-stream_loop",
            "-1",
            "-i",
            str(video_path),
            "-i",
            str(music_path),
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-t",
            f"{duration:.3f}",
        ]
        video_mode, video_options = self._get_render_video_options(video_path)
        audio_mode, audio_options = self._get_render_audio_options(music_path)

        common_suffix = ["-movflags", "+faststart", str(output_mp4)]
        if "copy" not in video_options:
            common_suffix = video_options + ["-pix_fmt", "yuv420p"] + audio_options + common_suffix
        else:
            common_suffix = video_options + audio_options + common_suffix

        primary_mode = f"{video_mode} + {audio_mode}"
        commands: list[tuple[str, list[str]]] = [(primary_mode, base_prefix + common_suffix)]

        if video_mode == "NVIDIA NVENC":
            fallback = (
                "CPU libx264 + " + audio_mode,
                base_prefix
                + ["-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p"]
                + audio_options
                + ["-movflags", "+faststart", str(output_mp4)],
            )
            commands.append(fallback)

        return commands

    def _extract_audio_to_m4a(self, input_path: Path, output_path: Path, *, source_has_video: bool) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        copy_command = [self.ffmpeg_bin, "-y", "-i", str(input_path), "-vn", "-c:a", "copy", str(output_path)]
        try:
            self._run_command(copy_command)
            return
        except RuntimeError:
            pass

        fallback_command = [self.ffmpeg_bin, "-y", "-i", str(input_path), "-vn", "-c:a", "aac", str(output_path)]
        if not source_has_video:
            fallback_command.insert(-1, "-b:a")
            fallback_command.insert(-1, "192k")
        self._run_command(fallback_command)

    def render_job(self, job: dict[str, Any]) -> None:
        music_path = Path(job["music_path"])
        video_path = Path(job["video_path"])
        output_mp4 = Path(job["output_mp4"])
        output_m4a = Path(job["output_m4a"])
        duration = probe_duration_seconds(music_path, self.ffprobe_bin)
        output_mp4.parent.mkdir(parents=True, exist_ok=True)

        last_error: RuntimeError | None = None
        for render_mode, command in self._render_commands(video_path, music_path, output_mp4, duration):
            try:
                self.logger.info("Render mode for %s: %s", job["output_base"], render_mode)
                self._run_command(command)
                last_error = None
                break
            except RuntimeError as exc:
                last_error = exc
                if output_mp4.exists():
                    output_mp4.unlink(missing_ok=True)
                self.logger.warning("Render mode failed for %s: %s -> %s", job["output_base"], render_mode, exc)
        if last_error is not None:
            raise last_error

        self._extract_audio_to_m4a(music_path, output_m4a, source_has_video=False)
        job["render_status"] = RENDERED_STATUS
        job["updated_at"] = now_iso()
        self._update_overall_status(job)
        self._save_state()
        self.logger.info("Rendered %s", job["output_base"])

    def process_job(self, job: dict[str, Any]) -> None:
        output_mp4 = Path(job["output_mp4"])
        output_m4a = Path(job["output_m4a"])
        processed_mp4 = Path(job["processed_mp4"])
        duration = probe_duration_seconds(output_mp4, self.ffprobe_bin)
        duration_int = int(duration)
        last_second = max(duration_int - 1, 0)

        self._extract_audio_to_m4a(output_mp4, output_m4a, source_has_video=True)
        filter_expr = f"volume=enable='gte(mod(t,10),3)*lt(mod(t,10),10)*lt(t,{last_second})':volume=0"
        command = [
            self.ffmpeg_bin,
            "-y",
            "-i",
            str(output_mp4),
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-af",
            filter_expr,
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            str(processed_mp4),
        ]
        self._run_command(command)
        job["process_status"] = PROCESSED_STATUS
        job["updated_at"] = now_iso()
        self._update_overall_status(job)
        self._save_state()
        self.logger.info("Processed %s", job["output_base"])

    def ensure_language_codes(self) -> None:
        current = " ".join(str(self.backend.get_language_codes()).replace(",", " ").split())
        wanted = " ".join(self.language_codes_string.replace(",", " ").split())
        if current == wanted:
            return
        response = self.backend.save_language_codes(self.language_codes_string)
        if response != "OK":
            raise RuntimeError(f"saveLanguageCodes failed: {response}")
        self.logger.info("Updated YaMasterTub language codes to %s", self.language_codes_string)

    def _same_path(self, left: str | None, right: str | None) -> bool:
        return normalize_path_string(left) == normalize_path_string(right)

    def _find_matching_entry(
        self,
        entries: list[dict[str, Any]],
        job: dict[str, Any],
    ) -> tuple[int | None, dict[str, Any] | None]:
        processed_path = str(job.get("processed_mp4") or "")
        audio_path = str(job.get("output_m4a") or "")
        for index, entry in enumerate(entries):
            if self._same_path(entry.get("videoSubtitleFilePath"), processed_path) and self._same_path(
                entry.get("audioSubtitleFilePath"), audio_path
            ):
                return index, entry

        for index, entry in enumerate(entries):
            if self._same_path(entry.get("videoSubtitleFilePath"), processed_path):
                return index, entry

        for index, entry in enumerate(entries):
            if self._same_path(entry.get("audioSubtitleFilePath"), audio_path):
                return index, entry

        return None, None

    def _refresh_channel_video_ids(self, channel_cfg: dict[str, Any]) -> None:
        video_ids = []
        for entry in channel_cfg.get("audioSubtitlesVideos", []):
            video_id = str(entry.get("videoId") or "").strip()
            if video_id:
                video_ids.append(video_id)
        channel_cfg["videoIds"] = "\n".join(video_ids)
        channel_cfg["current_visibility"] = None

    def sync_job_to_aas(self, job: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        config = parse_json_maybe(self.backend.get_aas_config(), {})
        channel_cfg = config.setdefault(
            self.channel_folder_name,
            {"audioSubtitlesVideos": [], "videoIds": "", "current_visibility": None},
        )
        entries = channel_cfg.setdefault("audioSubtitlesVideos", [])
        index, entry = self._find_matching_entry(entries, job)
        if entry is None or index is None:
            entry = {
                "videoId": str(job.get("video_id") or ""),
                "videoSubtitleFilePath": str(Path(job["processed_mp4"]).resolve(strict=False)),
                "audioSubtitleFilePath": str(Path(job["output_m4a"]).resolve(strict=False)),
                "numberOfAddedLanguageCodes": 0,
                "addedLanguageCodes": [],
            }
            entries.append(entry)
            index = len(entries) - 1
        else:
            entry["videoSubtitleFilePath"] = str(Path(job["processed_mp4"]).resolve(strict=False))
            entry["audioSubtitleFilePath"] = str(Path(job["output_m4a"]).resolve(strict=False))
            if job.get("video_id") and not str(entry.get("videoId") or "").strip():
                entry["videoId"] = str(job.get("video_id") or "")

        self._refresh_channel_video_ids(channel_cfg)
        response = self.backend.save_aas_config(config)
        if response != "OK":
            raise RuntimeError(f"saveAudioSubtitlesVideosChannels failed: {response}")
        return index, entry

    def get_job_entry(self, job: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        config = parse_json_maybe(self.backend.get_aas_config(), {})
        channel_cfg = config.get(self.channel_folder_name) or {}
        entries = channel_cfg.get("audioSubtitlesVideos") or []
        index, entry = self._find_matching_entry(entries, job)
        if entry is None or index is None:
            raise RuntimeError(f"Could not locate job entry for {job['output_base']} in YaMasterTub AAS config.")
        return index, entry

    def _entry_has_upload_artifacts(self, entry: dict[str, Any]) -> bool:
        return bool(str(entry.get("scottyResourceId") or "").strip()) and bool(str(entry.get("frontEndUID") or "").strip())

    def _valid_video_id(self, value: str | None) -> str:
        video_id = str(value or "").strip()
        return video_id if len(video_id) == 11 else ""

    def _entry_is_premium_complete(self, entry: dict[str, Any]) -> bool:
        return self._entry_added_language_count(entry) >= len(self.language_codes)

    def _entry_added_language_count(self, entry: dict[str, Any] | None) -> int:
        if not entry:
            return 0
        added_codes = entry.get("addedLanguageCodes") or []
        if isinstance(added_codes, str):
            added_codes = [part for part in added_codes.replace(",", " ").split() if part]
        added_set = {str(code).strip() for code in added_codes if str(code).strip()}
        if added_set:
            allowed = set(self.language_codes)
            return min(len(added_set.intersection(allowed)), len(self.language_codes))
        try:
            count = int(entry.get("numberOfAddedLanguageCodes") or 0)
        except (TypeError, ValueError):
            count = 0
        return max(0, min(count, len(self.language_codes)))

    def _entry_addsub_done_label(self, entry: dict[str, Any] | None) -> str:
        return f"{self._entry_added_language_count(entry)}/{len(self.language_codes)}"

    def upload_job(self, job: dict[str, Any]) -> None:
        self.ensure_language_codes()
        index, _entry = self.sync_job_to_aas(job)
        index, entry = self.get_job_entry(job)
        video_id = self._valid_video_id(entry.get("videoId"))

        if not video_id and not self._entry_has_upload_artifacts(entry):
            job["attempts"]["upload"] = int(job.get("attempts", {}).get("upload") or 0) + 1
            response = self.backend.upload_video_file(self.channel_folder_name, index)
            self.logger.info("AAS_uploadVideoFile(%s, %s) -> %s", self.channel_folder_name, index, response)
            index, entry = self.get_job_entry(job)
            if not self._entry_has_upload_artifacts(entry):
                raise RuntimeError(
                    str(entry.get("uploadVideoFileError") or entry.get("message") or response or "Upload step did not create scottyResourceId/frontEndUID.")
                )
            job["upload_status"] = UPLOADED_STATUS

        video_id = self._valid_video_id(entry.get("videoId"))
        if not video_id:
            if self.auto_start_aas_delete_checker:
                self.backend.ensure_aas_checker_running()
            else:
                try:
                    if self.backend.is_aas_checker_running():
                        self.logger.warning(
                            "AAS_check_delete_video.py is running. It can mark newly created AAS videos as DELETED."
                        )
                except Exception:
                    pass
            job["attempts"]["create"] = int(job.get("attempts", {}).get("create") or 0) + 1
            response = self.backend.create_video(self.channel_folder_name, index)
            self.logger.info("AAS_createVideo(%s, %s) -> %s", self.channel_folder_name, index, response)
            index, entry = self.get_job_entry(job)
            video_id = self._valid_video_id(entry.get("videoId"))
            if not video_id:
                raise RuntimeError(
                    str(entry.get("createVideoError") or entry.get("message") or response or "Create step did not produce a valid 11-character videoId.")
                )
        job["video_id"] = video_id
        job["upload_status"] = VIDEO_CREATED_STATUS
        job["updated_at"] = now_iso()
        self._update_overall_status(job)
        self._save_state()
        self.logger.info("Created video for %s -> %s", job["output_base"], video_id)

    def premium_job(self, job: dict[str, Any]) -> None:
        self.sync_job_to_aas(job)
        _index, entry = self.get_job_entry(job)
        if self._entry_is_premium_complete(entry):
            job["premium_status"] = PREMIUM_DONE_STATUS
            job["updated_at"] = now_iso()
            self._update_overall_status(job)
            self._save_state()
            return

        job["attempts"]["premium"] = int(job.get("attempts", {}).get("premium") or 0) + 1
        response = self.backend.add_subtitles_premium()
        self.logger.info("addSubtitlesPremium() -> %s", response)
        _index, entry = self.get_job_entry(job)
        if self._entry_is_premium_complete(entry):
            job["premium_status"] = PREMIUM_DONE_STATUS
            job["last_error"] = ""
            self.logger.info("Premium subtitles complete for %s", job["output_base"])
        else:
            message = str(entry.get("message") or response or "Premium subtitles are not complete yet.")
            job["premium_status"] = PENDING_RETRY_STATUS
            job["last_error"] = message
            self.logger.warning("Premium subtitles pending retry for %s: %s", job["output_base"], message)
        job["updated_at"] = now_iso()
        self._update_overall_status(job)
        self._save_state()

    def _runnable_rows(self) -> list[PairRow]:
        music_files, video_files = self._current_pairing_lists()
        return build_pair_rows(music_files, video_files)

    def render_ready(self) -> int:
        with self._lock:
            self._sync_current_inputs()
            rows = list(self._runnable_rows())
        count = 0
        for row in rows:
            if row.status != "Ready":
                continue
            job = self.get_job(row.output_base)
            if job.get("render_status") == RENDERED_STATUS:
                continue
            try:
                self.render_job(job)
                count += 1
            except Exception as exc:
                self._mark_job_error(job, "render", str(exc))
                self.logger.exception("Render failed for %s", row.output_base)
        return count

    def process_ready(self) -> int:
        with self._lock:
            self._sync_current_inputs()
            rows = list(self._runnable_rows())
        count = 0
        for row in rows:
            if row.status != "Ready":
                continue
            job = self.get_job(row.output_base)
            if job.get("render_status") != RENDERED_STATUS or job.get("process_status") == PROCESSED_STATUS:
                continue
            try:
                self.process_job(job)
                count += 1
            except Exception as exc:
                self._mark_job_error(job, "process", str(exc))
                self.logger.exception("Process failed for %s", row.output_base)
        return count

    def upload_ready(self) -> int:
        with self._lock:
            self._sync_current_inputs()
            rows = list(self._runnable_rows())
        count = 0
        for row in rows:
            if row.status != "Ready":
                continue
            job = self.get_job(row.output_base)
            if job.get("process_status") != PROCESSED_STATUS or str(job.get("video_id") or "").strip():
                continue
            try:
                self.upload_job(job)
                count += 1
            except Exception as exc:
                self._mark_job_error(job, "upload", str(exc))
                self.logger.exception("Upload failed for %s", row.output_base)
        return count

    def add_premium_ready(self) -> int:
        with self._lock:
            self._sync_current_inputs()
            rows = list(self._runnable_rows())
        count = 0
        for row in rows:
            if row.status != "Ready":
                continue
            job = self.get_job(row.output_base)
            if not str(job.get("video_id") or "").strip() or job.get("premium_status") == PREMIUM_DONE_STATUS:
                continue
            try:
                self.premium_job(job)
                count += 1
            except Exception as exc:
                self._mark_job_error(job, "premium", str(exc))
                self.logger.exception("Premium failed for %s", row.output_base)
        return count

    def run_next_phase(self) -> tuple[str, int]:
        if any(row.status == "Ready" and self.get_job(row.output_base).get("render_status") != RENDERED_STATUS for row in self._runnable_rows()):
            return "render", self.render_ready()
        if any(
            row.status == "Ready"
            and self.get_job(row.output_base).get("render_status") == RENDERED_STATUS
            and self.get_job(row.output_base).get("process_status") != PROCESSED_STATUS
            for row in self._runnable_rows()
        ):
            return "process", self.process_ready()
        if any(
            row.status == "Ready"
            and self.get_job(row.output_base).get("process_status") == PROCESSED_STATUS
            and not str(self.get_job(row.output_base).get("video_id") or "").strip()
            for row in self._runnable_rows()
        ):
            return "upload", self.upload_ready()
        if any(
            row.status == "Ready"
            and str(self.get_job(row.output_base).get("video_id") or "").strip()
            and self.get_job(row.output_base).get("premium_status") != PREMIUM_DONE_STATUS
            for row in self._runnable_rows()
        ):
            return "premium", self.add_premium_ready()
        self.logger.info("No runnable phase found.")
        return "idle", 0

    def retry_job(self, output_base: str) -> None:
        with self._lock:
            job = self.get_job(output_base)
            if job.get("premium_status") in {"Error", PENDING_RETRY_STATUS}:
                self._reset_from_phase(job, "premium")
            elif job.get("upload_status") == "Error":
                self._reset_from_phase(job, "upload")
            elif job.get("process_status") == "Error":
                self._reset_from_phase(job, "process")
            else:
                self._reset_from_phase(job, "render")
            job["updated_at"] = now_iso()
            self._update_overall_status(job)
            self._save_state()
            self.logger.info("Reset %s for retry", output_base)

    def shutdown(self) -> None:
        if hasattr(self.backend, "shutdown"):
            self.backend.shutdown()


class ReupTableApp:
    def __init__(self, root: tk.Tk, service: ReupPipelineService):
        self.root = root
        self.service = service
        self.root.title(APP_TITLE)
        self.root.geometry("1500x900")

        self.music_files = list(service.music_files)
        self.video_files = list(service.video_files)
        self._input_rows: list[dict[str, tk.StringVar]] = []
        self.summary_var = tk.StringVar()
        self.activity_var = tk.StringVar(value="Ready")
        self.channel_var = tk.StringVar(value=service.channel_folder_name)
        self._busy = False
        self._buttons: list[ttk.Button] = []
        self._disable_widgets: list[tk.Widget] = []
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._log_handler = QueueLogHandler(self._log_queue)
        self.service.add_log_handler(self._log_handler)

        self._build_ui()
        self._refresh_all()
        self.root.after(150, self._poll_logs)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _button(
        self,
        parent: ttk.Frame,
        text: str,
        command: Callable[[], None],
        *,
        row: int,
        column: int,
        padx: tuple[int, int] = (0, 6),
        track: bool = True,
    ) -> ttk.Button:
        button = ttk.Button(parent, text=text, command=command)
        button.grid(row=row, column=column, padx=padx)
        if track:
            self._buttons.append(button)
        return button

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(2, weight=1)
        self.root.rowconfigure(3, weight=1)

        toolbar = ttk.Frame(self.root, padding=12)
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(1, weight=1)

        left_actions = ttk.Frame(toolbar)
        left_actions.grid(row=0, column=0, sticky="w")
        self._button(left_actions, "Add Row", self.add_empty_row, row=0, column=0)
        self._button(left_actions, "Add Music Files", self.add_music_files, row=0, column=1)
        self._button(left_actions, "Add Video Files", self.add_video_files, row=0, column=2)
        self._button(left_actions, "Clear All", self.clear_all, row=0, column=3)
        self._button(left_actions, "Save Pairing", self.save_pairing, row=0, column=4)
        self._button(left_actions, "Open Output Folder", self.open_output_folder, row=0, column=5)

        channel_frame = ttk.Frame(toolbar)
        channel_frame.grid(row=0, column=1, sticky="w", padx=(16, 0))
        ttk.Label(channel_frame, text="Channel").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.channel_combo = ttk.Combobox(channel_frame, textvariable=self.channel_var, state="readonly", width=28)
        self.channel_combo.grid(row=0, column=1, sticky="w", padx=(0, 8))
        self.channel_combo.bind("<<ComboboxSelected>>", self._on_channel_selected)
        self._disable_widgets.append(self.channel_combo)
        self._button(channel_frame, "Refresh Channels", self.refresh_channels, row=0, column=2)
        self.refresh_channels(show_message=False)

        ttk.Label(toolbar, textvariable=self.summary_var).grid(row=0, column=2, sticky="e")

        phases = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        phases.grid(row=1, column=0, sticky="ew")
        self._button(phases, "Render Ready", lambda: self._run_action("Render", self.service.render_ready), row=0, column=0)
        self._button(phases, "Process Ready", lambda: self._run_action("Process", self.service.process_ready), row=0, column=1)
        self._button(phases, "Upload Ready", lambda: self._run_action("Upload", self.service.upload_ready), row=0, column=2)
        self._button(phases, "Add Premium Ready", lambda: self._run_action("Premium", self.service.add_premium_ready), row=0, column=3)
        self._button(phases, "Run Next Phase", self.run_next_phase, row=0, column=4)
        self._button(phases, "Retry Selected", self.retry_selected, row=0, column=5)
        ttk.Label(phases, textvariable=self.activity_var).grid(row=0, column=6, sticky="w", padx=(12, 0))

        input_frame = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        input_frame.grid(row=2, column=0, sticky="nsew")
        input_frame.columnconfigure(0, weight=1)
        input_frame.rowconfigure(1, weight=1)

        ttk.Label(
            input_frame,
            text="Input Rows: paste đường dẫn trực tiếp vào cột Music Path và Video Path, mỗi row tương ứng 1 job.",
        ).grid(row=0, column=0, sticky="w", pady=(0, 8))

        canvas_frame = ttk.Frame(input_frame)
        canvas_frame.grid(row=1, column=0, sticky="nsew")
        canvas_frame.columnconfigure(0, weight=1)
        canvas_frame.rowconfigure(0, weight=1)

        self.input_canvas = tk.Canvas(canvas_frame, highlightthickness=0, height=210)
        self.input_canvas.grid(row=0, column=0, sticky="nsew")
        input_scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.input_canvas.yview)
        input_scrollbar.grid(row=0, column=1, sticky="ns")
        self.input_canvas.configure(yscrollcommand=input_scrollbar.set)

        self.input_table = ttk.Frame(self.input_canvas)
        self.input_canvas_window = self.input_canvas.create_window((0, 0), window=self.input_table, anchor="nw")
        self.input_table.bind("<Configure>", self._on_input_table_configure)
        self.input_canvas.bind("<Configure>", self._on_input_canvas_configure)
        self._bind_mousewheel_tree(self.input_canvas, self._scroll_input_canvas)
        self._rebuild_input_rows()

        table_frame = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        table_frame.grid(row=3, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(1, weight=3)
        table_frame.rowconfigure(3, weight=2)

        ttk.Label(table_frame, text="Pipeline Table: music 1 + video 1, music 2 + video 2, ...").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 8),
        )

        columns = ("index", "output", "music", "video", "render", "process", "upload", "video_id", "premium", "addsub_done", "overall")
        self.pair_tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=14)
        self.pair_tree.grid(row=1, column=0, sticky="nsew")
        headings = {
            "index": "#",
            "output": "Output",
            "music": "Music File",
            "video": "Video File",
            "render": "Render",
            "process": "Process",
            "upload": "Upload",
            "video_id": "Video ID",
            "premium": "Premium",
            "addsub_done": "AddSub Done",
            "overall": "Overall",
        }
        widths = {
            "index": 60,
            "output": 100,
            "music": 240,
            "video": 240,
            "render": 100,
            "process": 100,
            "upload": 120,
            "video_id": 120,
            "premium": 120,
            "addsub_done": 110,
            "overall": 140,
        }
        for column, label in headings.items():
            self.pair_tree.heading(column, text=label)
            self.pair_tree.column(column, width=widths[column], anchor="w")

        scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.pair_tree.yview)
        scroll.grid(row=1, column=1, sticky="ns")
        self.pair_tree.configure(yscrollcommand=scroll.set)

        ttk.Label(table_frame, text="Log").grid(row=2, column=0, sticky="w", pady=(12, 6))
        self.log_text = tk.Text(table_frame, height=10, wrap="word", state="disabled")
        self.log_text.grid(row=3, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=3, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

    def _row_snapshots(self) -> list[dict[str, str]]:
        rows = []
        if self._input_rows:
            for row in self._input_rows:
                rows.append(
                    {
                        "music": row["music_var"].get().strip(),
                        "video": row["video_var"].get().strip(),
                    }
                )
            return rows

        max_len = max(len(self.music_files), len(self.video_files))
        for index in range(max_len):
            rows.append(
                {
                    "music": self.music_files[index] if index < len(self.music_files) else "",
                    "video": self.video_files[index] if index < len(self.video_files) else "",
                }
            )
        return rows

    def _rebuild_input_rows(self, snapshots: list[dict[str, str]] | None = None) -> None:
        if snapshots is None:
            snapshots = self._row_snapshots()
        for child in self.input_table.winfo_children():
            child.destroy()
        self._input_rows = []

        headers = ["#", "Music Path", "", "Video Path", "", "Actions"]
        for column, text in enumerate(headers):
            ttk.Label(self.input_table, text=text).grid(row=0, column=column, sticky="w", padx=(0, 8), pady=(0, 6))

        self.input_table.columnconfigure(1, weight=1)
        self.input_table.columnconfigure(3, weight=1)

        for index, snapshot in enumerate(snapshots):
            music_var = tk.StringVar(value=snapshot["music"])
            video_var = tk.StringVar(value=snapshot["video"])
            self._input_rows.append({"music_var": music_var, "video_var": video_var})

            ttk.Label(self.input_table, text=str(index + 1), width=4).grid(row=index + 1, column=0, sticky="w", padx=(0, 8), pady=4)

            music_entry = ttk.Entry(self.input_table, textvariable=music_var)
            music_entry.grid(row=index + 1, column=1, sticky="ew", padx=(0, 8), pady=4)
            music_entry.bind("<FocusOut>", lambda _event: self._refresh_all())

            self._button(
                self.input_table,
                "...",
                lambda row_index=index: self._browse_for_row(row_index, is_music=True),
                row=index + 1,
                column=2,
                padx=(0, 8),
                track=False,
            )

            video_entry = ttk.Entry(self.input_table, textvariable=video_var)
            video_entry.grid(row=index + 1, column=3, sticky="ew", padx=(0, 8), pady=4)
            video_entry.bind("<FocusOut>", lambda _event: self._refresh_all())

            self._button(
                self.input_table,
                "...",
                lambda row_index=index: self._browse_for_row(row_index, is_music=False),
                row=index + 1,
                column=4,
                padx=(0, 8),
                track=False,
            )

            actions = ttk.Frame(self.input_table)
            actions.grid(row=index + 1, column=5, sticky="w", pady=4)
            self._button(actions, "Up", lambda row_index=index: self.move_row(row_index, -1), row=0, column=0, padx=(0, 4), track=False)
            self._button(actions, "Down", lambda row_index=index: self.move_row(row_index, 1), row=0, column=1, padx=(0, 4), track=False)
            self._button(actions, "Remove", lambda row_index=index: self.remove_row(row_index), row=0, column=2, padx=(0, 0), track=False)

        if not snapshots:
            ttk.Label(self.input_table, text="No input rows. Click `Add Row` or use `Add Music Files` / `Add Video Files`.").grid(
                row=1,
                column=0,
                columnspan=6,
                sticky="w",
                pady=8,
            )

        self._bind_mousewheel_tree(self.input_table, self._scroll_input_canvas)
        self.input_canvas.configure(scrollregion=self.input_canvas.bbox("all"))

    def _on_input_table_configure(self, _event: tk.Event) -> None:
        self.input_canvas.configure(scrollregion=self.input_canvas.bbox("all"))

    def _on_input_canvas_configure(self, event: tk.Event) -> None:
        self.input_canvas.itemconfigure(self.input_canvas_window, width=event.width)

    def _bind_mousewheel_tree(self, widget: tk.Misc, handler: Callable[[tk.Event], str | None]) -> None:
        widget.bind("<MouseWheel>", handler, add="+")
        widget.bind("<Button-4>", handler, add="+")
        widget.bind("<Button-5>", handler, add="+")
        for child in widget.winfo_children():
            self._bind_mousewheel_tree(child, handler)

    def _scroll_input_canvas(self, event: tk.Event) -> str | None:
        region = self.input_canvas.bbox("all")
        if not region:
            return None
        _, top, _, bottom = region
        if bottom - top <= int(self.input_canvas.winfo_height()):
            return None

        if getattr(event, "num", None) == 4:
            delta_units = -1
        elif getattr(event, "num", None) == 5:
            delta_units = 1
        else:
            raw_delta = int(getattr(event, "delta", 0) or 0)
            if raw_delta == 0:
                return None
            delta_units = -1 if raw_delta > 0 else 1

        self.input_canvas.yview_scroll(delta_units, "units")
        return "break"

    def _sync_inputs_to_lists(self, *, persist: bool) -> None:
        snapshots = self._row_snapshots()
        last_index = -1
        for index, snapshot in enumerate(snapshots):
            if snapshot["music"] or snapshot["video"]:
                last_index = index
        if last_index < 0:
            self.music_files = []
            self.video_files = []
        else:
            kept = snapshots[: last_index + 1]
            self.music_files = [row["music"] for row in kept]
            self.video_files = [row["video"] for row in kept]

        if persist:
            self.service.save_pairings(self.music_files, self.video_files)
        else:
            self.service.sync_pairings_from_lists(self.music_files, self.video_files)

    def add_empty_row(self) -> None:
        snapshots = self._row_snapshots()
        snapshots.append({"music": "", "video": ""})
        self._input_rows = []
        self.music_files = [row["music"] for row in snapshots]
        self.video_files = [row["video"] for row in snapshots]
        self._rebuild_input_rows(snapshots)
        self._refresh_all()

    def move_row(self, row_index: int, direction: int) -> None:
        snapshots = self._row_snapshots()
        new_index = row_index + direction
        if new_index < 0 or new_index >= len(snapshots):
            return
        snapshots[row_index], snapshots[new_index] = snapshots[new_index], snapshots[row_index]
        self.music_files = [row["music"] for row in snapshots]
        self.video_files = [row["video"] for row in snapshots]
        self._rebuild_input_rows(snapshots)
        self._refresh_all()

    def remove_row(self, row_index: int) -> None:
        snapshots = self._row_snapshots()
        if row_index < 0 or row_index >= len(snapshots):
            return
        del snapshots[row_index]
        self.music_files = [row["music"] for row in snapshots]
        self.video_files = [row["video"] for row in snapshots]
        self._rebuild_input_rows(snapshots)
        self._refresh_all()

    def _browse_for_row(self, row_index: int, *, is_music: bool) -> None:
        if row_index < 0 or row_index >= len(self._input_rows):
            return
        if is_music:
            path = filedialog.askopenfilename(title="Choose music file", filetypes=MUSIC_FILETYPES)
            if path:
                self._input_rows[row_index]["music_var"].set(str(Path(path).resolve()))
        else:
            path = filedialog.askopenfilename(title="Choose short video file", filetypes=VIDEO_FILETYPES)
            if path:
                self._input_rows[row_index]["video_var"].set(str(Path(path).resolve()))
        self._refresh_all()

    def _poll_logs(self) -> None:
        processed = False
        while True:
            try:
                message = self._log_queue.get_nowait()
            except queue.Empty:
                break
            processed = True
            self.log_text.configure(state="normal")
            self.log_text.insert(tk.END, message + "\n")
            self.log_text.see(tk.END)
            self.log_text.configure(state="disabled")
        if processed:
            self.log_text.update_idletasks()
        self.root.after(150, self._poll_logs)

    def _set_busy(self, busy: bool, message: str) -> None:
        self._busy = busy
        self.activity_var.set(message)
        state = tk.DISABLED if busy else tk.NORMAL
        for button in self._buttons:
            button.configure(state=state)
        for widget in self._disable_widgets:
            widget.configure(state=tk.DISABLED if busy else "readonly")

    def refresh_channels(self, *, show_message: bool = True) -> None:
        channels = self.service.get_available_channels()
        self.channel_combo.configure(values=channels)

        current = self.channel_var.get().strip()
        if not current or current not in channels:
            current = self.service.channel_folder_name
        if current not in channels and channels:
            current = channels[0]

        if current:
            self.channel_var.set(current)
        if current and current != self.service.channel_folder_name:
            self.service.set_channel_folder_name(current)

        if show_message:
            messagebox.showinfo(APP_TITLE, f"Loaded {len(channels)} channel(s) from YaMasterTub.")

    def _on_channel_selected(self, _event: tk.Event) -> None:
        selected = self.channel_var.get().strip()
        try:
            self.service.set_channel_folder_name(selected)
        except Exception as exc:
            self.channel_var.set(self.service.channel_folder_name)
            messagebox.showerror(APP_TITLE, f"Could not switch channel:\n{exc}")
            return
        self._refresh_all()

    def _run_action(self, label: str, callback: Callable[[], int]) -> None:
        if self._busy:
            return
        self._sync_inputs_to_lists(persist=True)

        def worker() -> None:
            try:
                count = callback()
            except Exception as exc:
                self.root.after(0, lambda: self._finish_action(label, 0, exc))
                return
            self.root.after(0, lambda: self._finish_action(label, count, None))

        self._set_busy(True, f"{label} is running...")
        threading.Thread(target=worker, daemon=True).start()

    def _finish_action(self, label: str, count: int, error: Exception | None) -> None:
        self._set_busy(False, "Ready")
        self._refresh_all()
        if error is not None:
            messagebox.showerror(APP_TITLE, f"{label} failed:\n{error}")
            return
        messagebox.showinfo(APP_TITLE, f"{label} completed. Jobs handled: {count}")

    def run_next_phase(self) -> None:
        if self._busy:
            return
        self._sync_inputs_to_lists(persist=True)

        def worker() -> None:
            try:
                phase, count = self.service.run_next_phase()
            except Exception as exc:
                self.root.after(0, lambda: self._finish_next_phase("idle", 0, exc))
                return
            self.root.after(0, lambda: self._finish_next_phase(phase, count, None))

        self._set_busy(True, "Running next phase...")
        threading.Thread(target=worker, daemon=True).start()

    def _finish_next_phase(self, phase: str, count: int, error: Exception | None) -> None:
        self._set_busy(False, "Ready")
        self._refresh_all()
        if error is not None:
            messagebox.showerror(APP_TITLE, f"Run Next Phase failed:\n{error}")
            return
        if phase == "idle":
            messagebox.showinfo(APP_TITLE, "No runnable phase found.")
            return
        messagebox.showinfo(APP_TITLE, f"Ran phase `{phase}`. Jobs handled: {count}")

    def add_music_files(self) -> None:
        files = filedialog.askopenfilenames(title="Choose music files", filetypes=MUSIC_FILETYPES)
        if not files:
            return
        snapshots = self._row_snapshots()
        resolved = [str(Path(path).resolve()) for path in files]
        for path in resolved:
            target = next((row for row in snapshots if not row["music"]), None)
            if target is None:
                snapshots.append({"music": path, "video": ""})
            else:
                target["music"] = path
        self.music_files = [row["music"] for row in snapshots]
        self.video_files = [row["video"] for row in snapshots]
        self._rebuild_input_rows(snapshots)
        self._refresh_all()

    def add_video_files(self) -> None:
        files = filedialog.askopenfilenames(title="Choose short video files", filetypes=VIDEO_FILETYPES)
        if not files:
            return
        snapshots = self._row_snapshots()
        resolved = [str(Path(path).resolve()) for path in files]
        for path in resolved:
            target = next((row for row in snapshots if not row["video"]), None)
            if target is None:
                snapshots.append({"music": "", "video": path})
            else:
                target["video"] = path
        self.music_files = [row["music"] for row in snapshots]
        self.video_files = [row["video"] for row in snapshots]
        self._rebuild_input_rows(snapshots)
        self._refresh_all()

    def clear_all(self) -> None:
        snapshots = self._row_snapshots()
        if not any(row["music"] or row["video"] for row in snapshots):
            return
        if not messagebox.askyesno(APP_TITLE, "Clear all input rows from the table?"):
            return
        self.music_files = []
        self.video_files = []
        self._input_rows = []
        self._rebuild_input_rows([])
        self._refresh_all()
        self.save_pairing(show_message=False)

    def _refresh_all(self) -> None:
        self._sync_inputs_to_lists(persist=False)
        self._refresh_pair_table()

    def _refresh_pair_table(self) -> None:
        for item_id in self.pair_tree.get_children():
            self.pair_tree.delete(item_id)

        rows = self.service.get_display_rows()
        ready_count = 0
        for row in rows:
            if row["overall_status"] in {"Ready", RENDERED_STATUS, PROCESSED_STATUS, VIDEO_CREATED_STATUS, PREMIUM_DONE_STATUS}:
                ready_count += 1
            self.pair_tree.insert(
                "",
                tk.END,
                iid=row["output_base"],
                values=(
                    row["index"],
                    row["output_base"],
                    Path(row["music_path"]).name if row["music_path"] else "",
                    Path(row["video_path"]).name if row["video_path"] else "",
                    row["render_status"],
                    row["process_status"],
                    row["upload_status"],
                    row["video_id"],
                    row["premium_status"],
                    row["addsub_done"],
                    row["overall_status"],
                ),
            )
        self.summary_var.set(
            f"Music: {len(self.music_files)} | Videos: {len(self.video_files)} | Table rows: {len(rows)} | Active rows: {ready_count}"
        )

    def save_pairing(self, *, show_message: bool = True) -> None:
        self._sync_inputs_to_lists(persist=True)
        self._refresh_all()
        if show_message:
            messagebox.showinfo(APP_TITLE, "Pairing saved.")

    def retry_selected(self) -> None:
        selected = self.pair_tree.selection()
        if not selected:
            messagebox.showwarning(APP_TITLE, "Select one row to retry.")
            return
        output_base = selected[0]
        self.service.retry_job(output_base)
        self._refresh_all()
        messagebox.showinfo(APP_TITLE, f"Retry queued for {output_base}")

    def open_output_folder(self) -> None:
        self.service.output_dir.mkdir(parents=True, exist_ok=True)
        os.startfile(self.service.output_dir)  # type: ignore[attr-defined]

    def _on_close(self) -> None:
        self.service.remove_log_handler(self._log_handler)
        self.root.destroy()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="YT Reup Tool")
    parser.add_argument("--config", default=None, help="Optional config file path.")
    subparsers = parser.add_subparsers(dest="command")
    web_parser = subparsers.add_parser("web", help="Run localhost Web UI")
    web_parser.add_argument("--host", default="127.0.0.1", help="Bind host for the local web UI.")
    web_parser.add_argument("--port", type=int, default=8765, help="Bind port for the local web UI.")
    web_parser.add_argument("--no-browser", action="store_true", help="Do not auto-open the browser.")
    subparsers.add_parser("ui", help="Open desktop UI")
    subparsers.add_parser("status", help="Print current job status")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "web"
    service = ReupPipelineService(config_path=Path(args.config) if args.config else None)
    try:
        if command == "status":
            for line in service.status_lines():
                print(line)
            return 0
        if command == "web":
            from .webui import run_web_ui

            return run_web_ui(
                service,
                host=args.host,
                port=args.port,
                open_browser=not args.no_browser,
            )
        if command == "ui":
            root = tk.Tk()
            ReupTableApp(root, service)
            root.mainloop()
            return 0
        parser.error(f"Unsupported command: {command}")
        return 2
    finally:
        service.shutdown()
