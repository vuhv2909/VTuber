from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any


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


class YaMasterTubBackend:
    backend_name = "yamastertub"

    def __init__(self, root: Path, logs_dir: Path, logger: logging.Logger):
        self.root = root.resolve()
        self.logs_dir = logs_dir
        self.logger = logger
        self._api = None
        self._checker_proc: subprocess.Popen[str] | None = None
        self._checker_log_handle = None

    def required_local_paths(self) -> list[Path]:
        storage_dir = self.root / "storage"
        return [
            storage_dir / "audio-subtitles-videos-channels.json",
            storage_dir / "language-codes.txt",
            self.root / "AAS_check_delete_video.py",
        ]

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


def create_backend(root: Path, logs_dir: Path, logger: logging.Logger, backend_type: str):
    normalized = str(backend_type or "").strip().lower() or "yamastertub"
    if normalized == "yamastertub":
        return YaMasterTubBackend(root, logs_dir, logger)
    raise RuntimeError(f"Unsupported backend_type: {backend_type}")
