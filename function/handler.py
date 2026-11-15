"""HTTP entry for the Beam Q8 window join."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from nexmark_join.runtime import ObjectBackend
from nexmark_join.service import execute
from nexmark_join.timing import Phases, close_handler

BACKEND = ObjectBackend()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        self._send(200, {"ok": True, "backend": BACKEND.mode})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            self._send(400, {"ok": False, "error": "invalid json"})
            return
        headers = {
            "state-location": self.headers.get("state-location"),
            "dependent-state-location": self.headers.get("dependent-state-location"),
            "vdagid": self.headers.get("vDAGID") or self.headers.get("vdagid"),
        }
        result = execute(body, headers, BACKEND)
        raw_times = result.pop("timings_raw", None)
        if raw_times:
            phases = Phases()
            phases.add_managed(raw_times["load_s"] + raw_times["publish_s"])
            phases.add_direct_validate(raw_times["direct_validate_s"])
            closed = close_handler(raw_times["handler_s"], phases)
            result["timings"] = closed
            result["timing_schema"] = "stateful-e2e-v1"
            result["metrics"] = {
                "total_time": closed["handler_s"],
                "pre_state_processing": 0.0,
                "post_state_processing": 0.0,
                "state_handling": {
                    "level": "L1" if result.get("access") == "direct" else "L2",
                    "process_time": closed["compute_s"],
                    "data_transfer": closed["state_s"],
                    "idle_time": 0.0,
                },
            }
        self._send(200 if result.get("ok") else 409, result)

    def _send(self, status: int, body: dict) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        if body.get("metrics"):
            self.send_header("X-Function-Metrics", json.dumps(body["metrics"]))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args) -> None:
        return


def main() -> None:
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
