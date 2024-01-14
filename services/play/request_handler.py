import json
from http.server import BaseHTTPRequestHandler
from typing import TYPE_CHECKING, Any

import nebula

if TYPE_CHECKING:
    from .play import PlayoutHTTPServer


class PlayoutRequestHandler(BaseHTTPRequestHandler):
    server: "PlayoutHTTPServer"

    def log_request(self, code="-", size="-"):
        _ = code, size
        pass

    def result(self, data: dict[str, Any], response: int = 200):
        self.send_response(response)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Content-type", "application/json")
        self.end_headers()
        payload = json.dumps(data)
        self.wfile.write(bytes(payload, "utf-8"))

    def do_GET(self):
        pass

    def do_POST(self):
        ctype = self.headers.get("content-type")
        if ctype != "application/json":
            self.result(
                {"response": 400, "message": "Play service received a bad request."},
                response=400,
            )
            return

        length = int(self.headers.get("content-length", -1))
        # read1 is not an error!
        postvars = json.loads(self.rfile.read1(length))  # type: ignore

        method = self.path.lstrip("/").split("/")[0]

        if method not in self.server.methods:
            self.result(
                {
                    "response": 501,
                    "message": f"Playout service does not support {method}",
                },
                response=501,
            )
            return

        result: dict[str, Any] | None = None
        try:
            result = self.server.methods[method](**postvars)
        except Exception as e:
            nebula.log.traceback()
            result = {"response": 500, "message": str(e)}

        if result is None:
            result = {}

        assert isinstance(result, dict), "Result is not a dict."
        if "response" not in result:
            result["response"] = 200

        self.result(result, result["response"])
