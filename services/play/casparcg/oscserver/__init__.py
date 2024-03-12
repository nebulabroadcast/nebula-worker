import socketserver
from socket import socket
from typing import Any, Callable, Tuple

from .bundle import _BUNDLE_PREFIX
from .osc_types import OSCParseError
from .packet import OSCPacket


class OSCHandler(socketserver.BaseRequestHandler):
    server: Any

    def handle(self) -> None:
        try:
            packet = OSCPacket(self.request[0])
            for timed_msg in packet.messages:
                message = timed_msg.message
                if hasattr(self.server, "handle"):
                    self.server.handle(message.address, *message.params)
        except OSCParseError:
            pass


class OSCServer(socketserver.UDPServer):
    def __init__(self, host: str, port: int, handler: Callable) -> None:
        self.handle = handler
        super().__init__((host, port), OSCHandler)

    def verify_request(
        self,
        request: socket | tuple[bytes, socket],
        client_address: Tuple[str, int] | str,
    ) -> bool:
        _ = client_address
        if isinstance(request, socket):
            return True
        data = request[0]
        return data.startswith(_BUNDLE_PREFIX) or data.startswith(b"/")
