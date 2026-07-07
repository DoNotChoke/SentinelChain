"""Minimal synchronous health HTTP server (PLAN §23).

Serves ``/health/live`` and ``/health/ready`` from a stdlib threaded HTTP server. Readiness is
driven by a caller-supplied probe (Redis reachable + last poll succeeded). Prometheus
``/metrics`` is served separately by ``prometheus_client`` on ``METRICS_PORT``.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ReadinessProbe = Callable[[], bool]


def _make_handler(is_ready: ReadinessProbe) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/health/live":
                self._respond(200, {"status": "alive"})
            elif self.path == "/health/ready":
                ok = is_ready()
                self._respond(200 if ok else 503, {"status": "ready" if ok else "not_ready"})
            else:
                self._respond(404, {"status": "not_found"})

        def _respond(self, code: int, body: dict[str, str]) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args: object) -> None:
            return

    return _Handler


def start_health_server(port: int, is_ready: ReadinessProbe) -> ThreadingHTTPServer:
    """Start the health server on a daemon thread and return it (call ``shutdown()`` to stop)."""
    server = ThreadingHTTPServer(("0.0.0.0", port), _make_handler(is_ready))
    thread = threading.Thread(target=server.serve_forever, name="health-server", daemon=True)
    thread.start()
    return server
