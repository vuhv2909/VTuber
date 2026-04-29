from __future__ import annotations

import json
import threading
import webbrowser
import zlib
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Any

from .app import APP_TITLE, PREMIUM_DONE_STATUS, PROCESSED_STATUS, RENDERED_STATUS, VIDEO_CREATED_STATUS, ReupPipelineService


def _create_server_with_available_port(
    host: str,
    preferred_port: int,
    handler: type[BaseHTTPRequestHandler],
    attempts: int = 100,
) -> tuple[ThreadingHTTPServer, int]:
    last_error: OSError | None = None
    for port in range(preferred_port, preferred_port + attempts):
        try:
            return ThreadingHTTPServer((host, port), handler), port
        except OSError as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise RuntimeError(
            f"Could not bind a free localhost port in range {preferred_port}-{preferred_port + attempts - 1}: {last_error}"
        ) from last_error
    raise RuntimeError(
        f"Could not bind a free localhost port in range {preferred_port}-{preferred_port + attempts - 1}."
    )


def _preferred_port_for_install(service: ReupPipelineService, base_port: int = 8765, spread: int = 50) -> int:
    install_root = str(service.runtime_dir.resolve().parent)
    offset = zlib.crc32(install_root.encode("utf-8")) % spread
    return base_port + offset


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
        self._last_message_tone = "idle"

    def _set_busy(self, value: bool, message: str) -> None:
        with self._lock:
            self._busy = value
            self._activity = message

    def _summary(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        input_overview = self.service.get_input_overview()
        workspace_counts = self.service.get_workspace_counts()
        return {
            "music_count": int(input_overview["source_music_count"]),
            "video_count": int(input_overview["source_video_count"]),
            "row_count": len(rows),
            "ready_count": sum(
                1
                for row in rows
                if row["overall_status"] in {"Ready", RENDERED_STATUS, PROCESSED_STATUS, VIDEO_CREATED_STATUS, PREMIUM_DONE_STATUS}
            ),
            **workspace_counts,
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
            "last_message_tone": self._last_message_tone,
            "machine_name": self.service.machine_name,
            "machine_key": self.service.machine_key,
            "state_file": str(self.service.state_path),
            "output_dir": str(self.service.output_dir),
            "selected_channel": self.service.channel_folder_name,
            "channels": self.service.get_available_channels(),
            "workflow_mode": input_overview["workflow_mode"],
            "rows": rows,
            "music_folder": input_overview["music_folder"],
            "video_folder": input_overview["video_folder"],
            "source_folder": input_overview["source_folder"],
            "warnings": self.service.get_workspace_warnings(),
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

    def save_workspace(self, workflow_mode: str, music_folder: str, video_folder: str, source_folder: str) -> dict[str, Any]:
        self.service.save_workspace_inputs(
            workflow_mode,
            music_folder=music_folder,
            video_folder=video_folder,
            source_folder=source_folder,
        )
        self._last_message = "Workspace saved."
        self._last_message_tone = "success"
        return self.snapshot()

    def clear_rows(self) -> dict[str, Any]:
        self.service.clear_workspace()
        self._last_message = "Workspace cleared."
        self._last_message_tone = "success"
        return self.snapshot()

    def reset_job_state(self) -> dict[str, Any]:
        self.service.reset_job_state()
        self._last_message = "Job state reset for the current workspace."
        self._last_message_tone = "success"
        return self.snapshot()

    def set_channel(self, channel: str) -> dict[str, Any]:
        self.service.set_channel_folder_name(channel)
        self._last_message = f"Channel switched to {channel}."
        self._last_message_tone = "success"
        return self.snapshot()

    def retry(self, output_base: str) -> dict[str, Any]:
        self.service.retry_job(output_base)
        self._last_message = f"Retry queued for {output_base}."
        self._last_message_tone = "success"
        return self.snapshot()

    def retry_all_failed(self) -> dict[str, Any]:
        count = self.service.retry_all_failed()
        self._last_message = f"Retry queued for {count} failed/pending job(s)."
        self._last_message_tone = "success"
        return self.snapshot()

    def logs(self, limit: int = 200) -> dict[str, Any]:
        return {
            "lines": _tail_lines(self.service.logs_dir / "app.log", limit=limit),
            "busy": self._busy,
            "activity": self._activity,
        }

    def run_action_async(self, action: str) -> dict[str, Any]:
        actions = {
            "render": "Render",
            "process": "Process",
            "upload": "Upload",
            "premium": "Add Premium",
            "next": "Run Next Phase",
        }
        if action not in actions:
            raise RuntimeError(f"Unsupported action: {action}")

        with self._lock:
            if self._busy:
                raise RuntimeError("Another action is still running.")
            label = actions[action]
            self._busy = True
            self._activity = f"{label} is running..."
            self._last_error = ""
            self._last_message = ""
            self._last_message_tone = "running"

        def worker() -> None:
            try:
                summary = self.service.run_phase_action(action)
                if summary.phase == "idle":
                    message = "No runnable phase found."
                    tone = "idle"
                else:
                    message = (
                        f"{label} finished: done {summary.done}, failed {summary.failed}, "
                        f"skipped {summary.skipped}, pending {summary.pending}"
                    )
                    tone = "warning" if (summary.failed or summary.pending) else "success"
                    if action == "next":
                        message = (
                            f"Run Next Phase -> {summary.phase}: done {summary.done}, failed {summary.failed}, "
                            f"skipped {summary.skipped}, pending {summary.pending}"
                        )
                with self._lock:
                    self._busy = False
                    self._activity = "Ready"
                    self._last_message = message
                    self._last_message_tone = tone
            except Exception as exc:
                with self._lock:
                    self._busy = False
                    self._activity = "Ready"
                    self._last_error = str(exc)
                    self._last_message_tone = "error"
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
                    self.controller.save_workspace(
                        str(payload.get("workflow_mode") or ""),
                        str(payload.get("music_folder") or ""),
                        str(payload.get("video_folder") or ""),
                        str(payload.get("source_folder") or ""),
                    )
                )
                return
            if parsed.path == "/api/rows":
                if "music_folder" in payload or "video_folder" in payload:
                    self._send_json(
                        self.controller.save_workspace(
                            str(payload.get("workflow_mode") or ""),
                            str(payload.get("music_folder") or ""),
                            str(payload.get("video_folder") or ""),
                            str(payload.get("source_folder") or ""),
                        )
                    )
                    return
                self._send_json(self.controller.save_workspace("", "", "", ""))
                return
            if parsed.path == "/api/clear":
                self._send_json(self.controller.clear_rows())
                return
            if parsed.path == "/api/reset-job-state":
                self._send_json(self.controller.reset_job_state())
                return
            if parsed.path == "/api/channel":
                self._send_json(self.controller.set_channel(str(payload.get("channel") or "")))
                return
            if parsed.path == "/api/retry":
                self._send_json(self.controller.retry(str(payload.get("output_base") or "")))
                return
            if parsed.path == "/api/retry-all-failed":
                self._send_json(self.controller.retry_all_failed())
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
    port: int | None = None,
    open_browser: bool = True,
) -> int:
    controller = WebUIController(service)
    assets_dir = Path(__file__).resolve().parent / "web"
    WebRequestHandler.controller = controller
    WebRequestHandler.assets_dir = assets_dir
    preferred_port = port if port is not None else _preferred_port_for_install(service)
    server, selected_port = _create_server_with_available_port(host, preferred_port, WebRequestHandler)
    url = f"http://{host}:{selected_port}"
    last_url_path = service.runtime_dir / "last_webui_url.txt"
    last_url_path.write_text(url, encoding="utf-8")
    print(f"{APP_TITLE} Web UI running at {url}")
    if port is None:
        print(f"Preferred start port for this folder: {preferred_port}")
    if selected_port != preferred_port:
        print(f"Preferred port {preferred_port} was busy. Switched to {selected_port}.")
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
