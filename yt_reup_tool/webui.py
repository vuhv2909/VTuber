from __future__ import annotations

import json
import threading
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Any

from .app import APP_TITLE, PREMIUM_DONE_STATUS, PROCESSED_STATUS, RENDERED_STATUS, VIDEO_CREATED_STATUS, ReupPipelineService


def _read_asset_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _tail_lines(path: Path, limit: int = 200) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-limit:]


class WebUIController:
    def __init__(self, service: ReupPipelineService):
        self.service = service
        self._lock = threading.RLock()
        self._busy = False
        self._activity = "Ready"
        self._last_error = ""
        self._last_message = ""

    def _set_busy(self, value: bool, message: str) -> None:
        with self._lock:
            self._busy = value
            self._activity = message

    def _summary(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        input_overview = self.service.get_input_overview()
        return {
            "music_count": int(input_overview["source_music_count"]),
            "video_count": int(input_overview["source_video_count"]),
            "row_count": len(rows),
            "ready_count": sum(
                1
                for row in rows
                if row["overall_status"] in {"Ready", RENDERED_STATUS, PROCESSED_STATUS, VIDEO_CREATED_STATUS, PREMIUM_DONE_STATUS}
            ),
        }

    def snapshot(self) -> dict[str, Any]:
        input_overview = self.service.get_input_overview()
        rows = self.service.get_display_rows()
        return {
            "title": APP_TITLE,
            "busy": self._busy,
            "activity": self._activity,
            "last_error": self._last_error,
            "last_message": self._last_message,
            "machine_name": self.service.machine_name,
            "machine_key": self.service.machine_key,
            "state_file": str(self.service.state_path),
            "output_dir": str(self.service.output_dir),
            "selected_channel": self.service.channel_folder_name,
            "channels": self.service.get_available_channels(),
            "rows": rows,
            "music_folder": input_overview["music_folder"],
            "video_folder": input_overview["video_folder"],
            "input_rows": [
                {
                    "index": row.index,
                    "music": row.music_path,
                    "video": row.video_path,
                    "status": row.status,
                }
                for row in input_overview["rows"]
            ],
            "summary": self._summary(rows),
        }

    def save_folders(self, music_folder: str, video_folder: str) -> dict[str, Any]:
        self.service.save_input_folders(music_folder, video_folder)
        self._last_message = "Folders saved."
        return self.snapshot()

    def clear_rows(self) -> dict[str, Any]:
        self.service.save_input_folders("", "")
        self._last_message = "Folders cleared."
        return self.snapshot()

    def set_channel(self, channel: str) -> dict[str, Any]:
        self.service.set_channel_folder_name(channel)
        self._last_message = f"Channel switched to {channel}."
        return self.snapshot()

    def retry(self, output_base: str) -> dict[str, Any]:
        self.service.retry_job(output_base)
        self._last_message = f"Retry queued for {output_base}."
        return self.snapshot()

    def logs(self, limit: int = 200) -> dict[str, Any]:
        return {
            "lines": _tail_lines(self.service.logs_dir / "app.log", limit=limit),
            "busy": self._busy,
            "activity": self._activity,
        }

    def run_action_async(self, action: str) -> dict[str, Any]:
        actions = {
            "render": ("Render Ready", self.service.render_ready),
            "process": ("Process Ready", self.service.process_ready),
            "upload": ("Upload Ready", self.service.upload_ready),
            "premium": ("Add Premium Ready", self.service.add_premium_ready),
            "next": ("Run Next Phase", self.service.run_next_phase),
        }
        if action not in actions:
            raise RuntimeError(f"Unsupported action: {action}")

        with self._lock:
            if self._busy:
                raise RuntimeError("Another action is still running.")
            label, callback = actions[action]
            self._busy = True
            self._activity = f"{label} is running..."
            self._last_error = ""
            self._last_message = ""

        def worker() -> None:
            try:
                result = callback()
                if action == "next":
                    phase, count = result
                    if phase == "idle":
                        message = "No runnable phase found."
                    else:
                        message = f"Ran phase `{phase}`. Jobs handled: {count}"
                else:
                    message = f"{label} completed."
                with self._lock:
                    self._busy = False
                    self._activity = "Ready"
                    self._last_message = message
            except Exception as exc:
                with self._lock:
                    self._busy = False
                    self._activity = "Ready"
                    self._last_error = str(exc)

        threading.Thread(target=worker, daemon=True).start()
        return self.snapshot()


class WebRequestHandler(BaseHTTPRequestHandler):
    controller: WebUIController
    assets_dir: Path

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, body: str, content_type: str = "text/plain; charset=utf-8", status: int = 200) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode("utf-8") or "{}")

    def _serve_asset(self, relative_path: str, content_type: str) -> None:
        target = self.assets_dir / relative_path
        if not target.exists():
            self._send_text("Not found", status=404)
            return
        self._send_text(_read_asset_text(target), content_type=content_type)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._serve_asset("index.html", "text/html; charset=utf-8")
            return
        if parsed.path == "/styles.css":
            self._serve_asset("styles.css", "text/css; charset=utf-8")
            return
        if parsed.path == "/app.js":
            self._serve_asset("app.js", "application/javascript; charset=utf-8")
            return
        if parsed.path == "/api/state":
            self._send_json(self.controller.snapshot())
            return
        if parsed.path == "/api/logs":
            params = parse_qs(parsed.query)
            limit = int((params.get("limit") or ["200"])[0])
            self._send_json(self.controller.logs(limit=limit))
            return
        self._send_text("Not found", status=404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            payload = self._read_json_body()
            if parsed.path == "/api/folders":
                self._send_json(
                    self.controller.save_folders(
                        str(payload.get("music_folder") or ""),
                        str(payload.get("video_folder") or ""),
                    )
                )
                return
            if parsed.path == "/api/rows":
                if "music_folder" in payload or "video_folder" in payload:
                    self._send_json(
                        self.controller.save_folders(
                            str(payload.get("music_folder") or ""),
                            str(payload.get("video_folder") or ""),
                        )
                    )
                    return
                self._send_json(self.controller.save_folders("", ""))
                return
            if parsed.path == "/api/clear":
                self._send_json(self.controller.clear_rows())
                return
            if parsed.path == "/api/channel":
                self._send_json(self.controller.set_channel(str(payload.get("channel") or "")))
                return
            if parsed.path == "/api/retry":
                self._send_json(self.controller.retry(str(payload.get("output_base") or "")))
                return
            if parsed.path == "/api/action":
                self._send_json(self.controller.run_action_async(str(payload.get("action") or "")))
                return
            self._send_text("Not found", status=404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)


def run_web_ui(
    service: ReupPipelineService,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = True,
) -> int:
    controller = WebUIController(service)
    assets_dir = Path(__file__).resolve().parent / "web"
    WebRequestHandler.controller = controller
    WebRequestHandler.assets_dir = assets_dir
    server = ThreadingHTTPServer((host, port), WebRequestHandler)
    url = f"http://{host}:{port}"
    print(f"{APP_TITLE} Web UI running at {url}")
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0
