import socket
import telnetlib

from nebula.log import log


DELIM = b"\r\n"


class CasparResponse:
    """Caspar query response object"""

    def __init__(self, code: int, data: str):
        self.code = code
        self.data = data

    @property
    def response(self) -> int:
        """AMCP response code"""
        return self.code

    @property
    def is_error(self) -> bool:
        """Returns True if query failed"""
        return self.code >= 400

    @property
    def is_success(self) -> bool:
        """Returns True if query succeeded"""
        return self.code < 400

    def __repr__(self) -> str:
        if self.is_success:
            return "<Caspar response: OK>"
        return f"<CasparResponse: Error {self.code}>"

    def __bool__(self) -> bool:
        return self.is_success


class CasparCG:
    """CasparCG client object"""

    def __init__(self, host: str = "localhost", port: int = 5250, timeout: float = 2):
        assert isinstance(port, int) and port <= 65535, "Invalid port number"
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connection = False

    def __str__(self) -> str:
        return f"amcp://{self.host}:{self.port}"

    def connect(self, **kwargs) -> bool:
        """Create a connection to CasparCG Server"""
        try:
            self.connection = telnetlib.Telnet(
                self.host, self.port, timeout=self.timeout
            )
        except ConnectionRefusedError:
            log.error(
                f"Unable to connect {self}. Connection refused"
            )
            return False
        except socket.timeout:
            log.error(
                f"Unable to connect {self}. Timeout."
            )
            return False
        except Exception:
            log.traceback()
            return False
        return True

    def query(self, query: str, **kwargs) -> CasparResponse:
        """Send an AMCP command"""
        if not self.connection:
            if not self.connect(**kwargs):
                return CasparResponse(500, "Unable to connect CasparCG server")

        query = query.strip()
        if kwargs.get("verbose", True):
            if not query.startswith("INFO"):
                log.debug(f"Executing AMCP: {query}")

        query = bytes(query.encode("utf-8")) + DELIM

        try:
            self.connection.write(query)
            result = self.connection.read_until(DELIM).strip()
        except ConnectionResetError:
            self.connection = None
            return CasparResponse(500, "Connection reset by peer")
        except Exception:
            log.traceback()
            return CasparResponse(500, "Query failed")

        result = result.decode("utf-8")

        if not result:
            return CasparResponse(500, "No result")

        try:
            if result[:3] == "202":
                return CasparResponse(202, "No result")

            elif result[:3] in ["201", "200"]:
                stat = int(result[0:3])
                result = self.connection.read_until(DELIM).decode("utf-8").strip()
                return CasparResponse(stat, result)

            elif result[0] in ["3", "4", "5"]:
                stat = int(result[0:3])
                return CasparResponse(stat, result)

        except Exception:
            return CasparResponse(500, f"Malformed result: {result}")
        return CasparResponse(500, f"Unexpected result: {result}")
