from __future__ import annotations

import csv
import io
import json
import mimetypes
import sqlite3
from datetime import datetime
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import services
import runtime_context
from pages import RUNTIME_APP_JS, render_index_html
from schemas import json_optional_string, json_required_string, query_value

ROOT = runtime_context.ROOT
WEB_DIR = runtime_context.WEB_DIR
TEXT_TYPES = runtime_context.TEXT_TYPES
DB_PATH = runtime_context.DB_PATH
ATTACHMENT_DIR = runtime_context.ATTACHMENT_DIR

GET_ROUTES = {
    "/api/dashboard": "handle_dashboard",
    "/api/profit": "handle_profit",
    "/api/operations": "handle_operations",
    "/api/inventory": "handle_inventory",
    "/api/receivables": "handle_receivables",
    "/api/exceptions": "handle_exceptions",
    "/api/month-close": "handle_month_close",
    "/api/uploads": "handle_uploads",
    "/api/order-lookup": "handle_order_lookup",
    "/api/download-preview": "handle_download_preview",
    "/api/export": "handle_export",
}

POST_ROUTES = {
    "/api/upload": "handle_upload",
    "/api/run-monthly": "handle_run_monthly",
    "/api/manual/save": "handle_manual_save",
    "/api/exception/save": "handle_exception_save",
    "/api/inventory/save": "handle_inventory_save",
    "/api/month-close/action": "handle_month_close_action",
    "/api/removal-controls/save": "handle_removal_controls_save",
}

class DashboardHandler(SimpleHTTPRequestHandler):
    extensions_map = {
        **SimpleHTTPRequestHandler.extensions_map,
        **TEXT_TYPES,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"", "/", "/index.html"}:
            self.send_text(render_index_html(WEB_DIR), "text/html; charset=utf-8")
            return
        if parsed.path == "/runtime-app.js":
            self.send_text(RUNTIME_APP_JS, "application/javascript; charset=utf-8")
            return
        if parsed.path == "/api/health":
            self.send_json({"ok": True})
            return
        handler_name = GET_ROUTES.get(parsed.path)
        if handler_name is not None:
            getattr(self, handler_name)(parsed.query)
            return
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        handler_name = POST_ROUTES.get(parsed.path)
        if handler_name is not None:
            getattr(self, handler_name)(parsed.query)
            return
        self.send_json({"error": "Unsupported endpoint."}, status=HTTPStatus.NOT_FOUND)

    def guess_type(self, path: str) -> str:
        suffix = Path(path).suffix
        if suffix in TEXT_TYPES:
            return TEXT_TYPES[suffix]
        guessed = super().guess_type(path)
        if guessed == "application/octet-stream":
            return mimetypes.guess_type(path)[0] or guessed
        return guessed

    def log_message(self, fmt: str, *args) -> None:
        print(f"[{self.log_date_time_string()}] {fmt % args}")

    def read_body(self) -> bytes:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        return self.rfile.read(content_length) if content_length > 0 else b""

    def read_json(self) -> dict:
        body = self.read_body()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))

    def handle_dashboard(self, query: str) -> None:
        month = query_value(query, "month")
        try:
            payload = services.get_dashboard_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_operations(self, query: str) -> None:
        month = query_value(query, "month")
        try:
            payload = services.get_operations_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_receivables(self, query: str) -> None:
        month = query_value(query, "month")
        try:
            payload = services.get_receivables_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_profit(self, query: str) -> None:
        month = query_value(query, "month")
        try:
            payload = services.get_profit_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_inventory(self, query: str) -> None:
        month = query_value(query, "month")
        try:
            payload = services.get_inventory_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_exceptions(self, query: str) -> None:
        month = query_value(query, "month")
        try:
            payload = services.get_exceptions_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_month_close(self, query: str) -> None:
        month = query_value(query, "month")
        try:
            payload = services.get_month_close_payload(month)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_uploads(self, query: str) -> None:
        try:
            payload = services.get_uploads_payload()
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_order_lookup(self, query: str) -> None:
        order_id = query_value(query, "order_id", "", strip=True)
        if not order_id:
            self.send_json({"error": "order_id is required."}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            payload = services.get_order_lookup_payload(order_id)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_download_preview(self, query: str) -> None:
        month = query_value(query, "month")
        dataset = query_value(query, "dataset", "order_type_rollup")
        sku_filter = query_value(query, "sku")
        order_id = query_value(query, "order_id")
        group_by = query_value(query, "group_by")
        keyword = query_value(query, "keyword")
        order_type_filter = query_value(query, "order_type")
        basis = query_value(query, "basis")
        try:
            payload = services.get_download_preview(
                month,
                dataset,
                sku_filter=sku_filter,
                order_id=order_id,
                group_by=group_by,
                keyword=keyword,
                order_type_filter=order_type_filter,
                basis=basis,
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json(payload)

    def handle_export(self, query: str) -> None:
        month = query_value(query, "month")
        dataset = query_value(query, "dataset", "sku_details")
        sku_filter = query_value(query, "sku")
        order_id = query_value(query, "order_id")
        group_by = query_value(query, "group_by")
        keyword = query_value(query, "keyword")
        order_type_filter = query_value(query, "order_type")
        basis = query_value(query, "basis")
        try:
            filename, rows = services.export_dataset(
                month,
                dataset,
                sku_filter=sku_filter,
                order_id=order_id,
                group_by=group_by,
                keyword=keyword,
                order_type_filter=order_type_filter,
                basis=basis,
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_csv(filename, rows)

    def handle_upload(self, query: str) -> None:
        target = query_value(query, "target", "source")
        filename_param = query_value(query, "filename", "")
        body = self.read_body()
        try:
            result = services.upload_file(target, filename_param, body)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.send_json(result)

    def handle_run_monthly(self, query: str) -> None:
        try:
            payload = self.read_json()
            target_month = json_required_string(payload, "target_month")
            skip_init = bool(payload.get("skip_init", True))
            if len(target_month) != 7 or target_month[4] != "-":
                raise ValueError("target_month must use YYYY-MM format.")
            job = services.run_monthly_job(target_month, skip_init)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except RuntimeError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        self.send_json({"ok": True, "job": job})

    def handle_manual_save(self, query: str) -> None:
        try:
            payload = self.read_json()
            file_key = json_required_string(payload, "file_key")
            rows = payload.get("rows", [])
            if not isinstance(rows, list):
                raise ValueError("rows must be a list.")
            result = services.save_manual_file(file_key, rows)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def handle_exception_save(self, query: str) -> None:
        try:
            payload = self.read_json()
            result = services.save_exception_case(payload)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def handle_inventory_save(self, query: str) -> None:
        try:
            payload = self.read_json()
            result = services.save_inventory_movement(payload)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def handle_month_close_action(self, query: str) -> None:
        try:
            payload = self.read_json()
            month = json_required_string(payload, "month")
            action_code = json_required_string(payload, "action_code")
            note = json_optional_string(payload, "note")
            if not month:
                raise ValueError('month is required.')
            result = services.perform_month_close_action(month, action_code, note)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def handle_removal_controls_save(self, query: str) -> None:
        try:
            payload = self.read_json()
            rows = payload.get("rows", [])
            if not isinstance(rows, list) or not rows:
                raise ValueError("rows must be a non-empty list.")
            result = services.save_removal_control_rows(rows)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self.send_json({"ok": True, "result": result})

    def send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, body: str, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def send_csv(self, filename: str, rows: list[dict]) -> None:
        buffer = io.StringIO()
        if rows:
            writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        body = buffer.getvalue().encode("utf-8-sig")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


