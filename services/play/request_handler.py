import json
from http.server import BaseHTTPRequestHandler
from typing import TYPE_CHECKING

import nebula
from nebula.response import NebulaResponse

if TYPE_CHECKING:
    from .play import PlayoutHTTPServer


class PlayoutRequestHandler(BaseHTTPRequestHandler):
    server: "PlayoutHTTPServer"

    def log_request(self, code="-", size="-"):
        _ = code, size
        pass

    def _do_headers(
        self,
        mime="application/json",
        response=200,
        headers: list[tuple[str, str]] | None = None,
    ):
        if headers is None:
            headers = []
        self.send_response(response)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        for h in headers:
            self.send_header(h[0], h[1])
        self.send_header("Content-type", mime)
        self.end_headers()

    def _echo(self, istring):
        self.wfile.write(bytes(istring, "utf-8"))

    def result(self, data):
        self._do_headers()
        self._echo(json.dumps(data))

    def error(self, response, message=""):
        self._do_headers()  # return 200 anyway
        self._echo(json.dumps({"response": response, "message": message}))

    def do_GET(self):
        pass

    def do_POST(self):
        ctype = self.headers.get("content-type")
        if ctype != "application/json":
            self.error(400, "Play service received a bad request.")
            return

        length = int(self.headers.get("content-length", -1))
        # read1 is no an error!
        postvars = json.loads(self.rfile.read1(length))  # type: ignore

        method = self.path.lstrip("/").split("/")[0]

        if method not in self.server.methods:
            self.error(501, f"Method {method} is not implemented.")
            return

        try:
            result = self.server.methods[method](**postvars)
            if result.is_error:
                nebula.log.error(result.message)
            elif result["message"]:
                nebula.log.info(result.message)
            self.result(result.dict)
        except AssertionError as e:
            nebula.log.error(e)
            self.result(NebulaResponse(500, str(e)).dict)
        except Exception:
            msg = nebula.log.traceback()
            self.result(NebulaResponse(500, msg).dict)
