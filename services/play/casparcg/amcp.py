import socket
import threading

from nebula.log import log

DELIM = "\r\n"
DELIM_BYTES = DELIM.encode("utf-8")


class CasparException(Exception):
    pass


class CasparConnectionException(CasparException):
    pass


class CasparBadRequestException(CasparException):
    pass


class CasparNotFoundException(CasparException):
    pass


class CasparCG:
    """CasparCG client object"""

    def __init__(self, host: str = "localhost", port: int = 5250, timeout: float = 2):
        assert isinstance(port, int) and port <= 65535, "Invalid port number"
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connection: socket.socket | None = None
        self._buffer = b""
        self.lock = threading.Lock()

    def __str__(self) -> str:
        return f"amcp://{self.host}:{self.port}"

    def connect(self, **kwargs) -> None:
        """Create a connection to CasparCG Server"""
        _ = kwargs
        try:
            self.close()
            self.connection = socket.create_connection(
                (self.host, self.port), timeout=self.timeout
            )
            self.connection.settimeout(self.timeout)
        except ConnectionRefusedError as e:
            m = f"Unable to connect {self}. Connection refused"
            log.error(m)
            raise CasparConnectionException(m) from e
        except TimeoutError as e:
            m = f"Unable to connect {self}. Connection timeout"
            log.error(m)
            raise CasparConnectionException(m) from e
        except Exception as e:
            log.traceback()
            raise CasparConnectionException("Unable to connect CasparCG") from e

    def close(self) -> None:
        if self.connection is not None:
            try:
                self.connection.close()
            finally:
                self.connection = None
                self._buffer = b""

    def _read_until_delim(self) -> bytes:
        assert self.connection is not None

        while DELIM_BYTES not in self._buffer:
            chunk = self.connection.recv(4096)
            if not chunk:
                raise ConnectionResetError("CasparCG connection closed by peer")
            self._buffer += chunk

        result, self._buffer = self._buffer.split(DELIM_BYTES, 1)
        return result

    def query(self, query: str, **kwargs) -> str | None:
        """Send an AMCP command"""
        if self.lock.locked():
            log.trace(f"Waiting for CasparCG connection unlock: {query}")
        with self.lock:
            if not self.connection:
                self.connect(**kwargs)

            assert self.connection is not None

            query = query.strip()
            if kwargs.get("verbose", True):
                if not query.startswith("INFO"):
                    log.debug(f"Executing AMCP: {query}")

            query_bytes = f"{query}{DELIM}".encode()

            try:
                self.connection.sendall(query_bytes)
                result_bytes = self._read_until_delim()
            except ConnectionResetError as e:
                self.close()
                raise CasparConnectionException(
                    "CasparCG connection reset by peer"
                ) from e
            except BrokenPipeError as e:
                self.close()
                raise CasparConnectionException("CasparCG connection broken") from e
            except TimeoutError as e:
                self.close()
                raise CasparConnectionException("CasparCG query timed out") from e
            except OSError as e:
                self.close()
                raise CasparConnectionException("CasparCG query failed") from e
            except Exception as e:
                log.traceback()
                raise CasparConnectionException("CasparCG query failed") from e

            result = result_bytes.decode("utf-8").strip()

            if not result:
                raise CasparException("No result from CasparCG")

            try:
                if result[:3] == "202":
                    return None

                elif result[:3] in ["201", "200"]:
                    # stat = int(result[0:3])
                    result_bytes = self._read_until_delim()
                    result = result_bytes.decode("utf-8").strip()
                    return result

                elif result[0] in ["3", "4", "5"]:
                    # stat = int(result[0:3])

                    if result.startswith("400"):
                        # 400 error is followed by one more line with
                        # the original query
                        _ = self._read_until_delim()

                    raise CasparException(f"{result} error in CasparCG query '{query}'")
            except CasparException as e:
                raise e
            except Exception as e:
                raise CasparException(f"Malformed CasparCG response: {result}") from e
            raise CasparException(f"Unexpected CasparCG response: {result}")
