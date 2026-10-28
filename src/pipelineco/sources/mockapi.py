"""A small paginated REST API over the generated payments file.

Real HTTP on localhost, stdlib only. `fail_first` makes the first N requests return 503 so
the retry path is exercised by real network responses, not by a mock.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

API_KEY = "demo-key"


class ApiServer:
    def __init__(self, data_path: Path, port: int = 0, fail_first: int = 0):
        self.data_path = Path(data_path)
        self.fail_first = fail_first
        self.requests = 0
        self.failures = 0
        self._lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # silence
                pass

            def _send(self, code: int, body: dict):
                payload = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):
                url = urlparse(self.path)
                with outer._lock:
                    outer.requests += 1
                    if outer.failures < outer.fail_first:
                        outer.failures += 1
                        return self._send(503, {"error": "upstream unavailable"})
                if self.headers.get("X-Api-Key") != API_KEY:
                    return self._send(401, {"error": "bad api key"})
                if url.path != "/payments":
                    return self._send(404, {"error": "not found"})
                q = parse_qs(url.query)
                since = q.get("updated_since", [""])[0]
                page = int(q.get("page", ["1"])[0])
                size = int(q.get("page_size", ["100"])[0])
                rows = [r for r in json.loads(outer.data_path.read_text()) if not since or r["updated_at"] >= since]
                rows.sort(key=lambda r: (r["updated_at"], r["payment_id"]))
                chunk = rows[(page - 1) * size : page * size]
                more = page * size < len(rows)
                self._send(200, {"data": chunk, "next_page": page + 1 if more else None, "total": len(rows)})

        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def start(self) -> "ApiServer":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
