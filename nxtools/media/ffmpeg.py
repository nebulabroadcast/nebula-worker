__all__ = ["FFMPEG", "ffmpeg"]

import re
import signal
import subprocess
from collections.abc import Callable
from typing import IO

from nxtools.logging import logging
from nxtools.text import indent

re_position = re.compile(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})\d*", re.U | re.I)


def time2sec(search: re.Match[str]) -> float:
    hh, mm, ss, cs = search.group(1), search.group(2), search.group(3), search.group(4)
    return int(hh) * 3600 + int(mm) * 60 + int(ss) + int(cs) / 100.0


class FFMPEG:
    def __init__(self, *args) -> None:
        self.proc = None
        self.cmd = ["ffmpeg", "-hide_banner"]
        self.cmd.extend(str(arg) for arg in args)

    def reset_stderr(self) -> None:
        self.buff = b""
        self.error_log = ""

    @property
    def is_running(self) -> bool:
        if not self.proc:
            return False
        if self.proc.poll() is not None:
            self.proc = None
            return False
        return True

    @property
    def stdin(self) -> IO[bytes]:
        if self.proc is None or self.proc.stdin is None:
            raise RuntimeError("FFMPEG process is not running")
        return self.proc.stdin

    @property
    def stdout(self) -> IO[bytes]:
        if self.proc is None or self.proc.stdout is None:
            raise RuntimeError("FFMPEG process is not running")
        return self.proc.stdout

    @property
    def stderr(self) -> IO[bytes]:
        if self.proc is None or self.proc.stderr is None:
            raise RuntimeError("FFMPEG process is not running")
        return self.proc.stderr

    @property
    def return_code(self) -> int | None:
        if self.proc is None:
            return None
        return self.proc.returncode

    def start(
        self,
        stdin: IO[bytes] | int | None = None,
        stdout: IO[bytes] | int | None = None,
        stderr: IO[bytes] | int | None = subprocess.PIPE,
    ):
        self.reset_stderr()
        logging.debug("Executing", " ".join(self.cmd))
        self.proc = subprocess.Popen(  # type: ignore[assignment]
            self.cmd,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )

    def stop(self) -> bool:
        if not self.proc:
            return False
        self.proc.send_signal(signal.SIGINT)
        return True

    def wait(self, progress_handler=None):
        interrupted = False
        try:
            while self.process(progress_handler=progress_handler):
                pass
        except KeyboardInterrupt:
            self.stop()
            interrupted = True
        if not self.proc:
            return
        self.proc.wait()
        self.error_log += self.stderr.read().decode("utf-8")
        if interrupted:
            raise KeyboardInterrupt

    def process(self, progress_handler=None):
        ch = self.stderr.read(1)
        if not ch:
            return False
        if ch in [b"\n", b"\r"]:
            line = self.buff.decode("utf-8").strip()

            position_match = re_position.search(line)
            if position_match:
                position = time2sec(position_match)
                if progress_handler:
                    progress_handler(position)
                self.error_log = ""

            elif line == "Press [q] to stop, [?] for help":
                self.error_log = ""

            else:
                self.error_log += line + "\n"

            self.buff = b""
        else:
            self.buff += ch
        return True


def ffmpeg(
    *args,
    progress_handler: Callable[[float], None] | None = None,
    stdin: IO[bytes] | int | None = subprocess.PIPE,
    stdout: IO[bytes] | int | None = None,
    stderr: IO[bytes] | int | None = subprocess.PIPE,
):
    """
    FFMpeg wrapper with progress and error handling

    Args:
        *args (list[any]):
            List of ffmpeg command line arguments.
            Each argument is converted to a string.

        progress_handler (function):
            Function to be called with the current position (seconds) as argument.

        stdin (file):
            File object to be used as stdin.
            Default is subprocess.PIPE

        stdout (file):
            File object to be used as stdout.
            Default is None

        stderr (file):
            File object to be used as stderr.
            Default is subprocess.PIPE (used to compute progress).

        debug (bool):
            Enable debug mode (write ffmpeg output to stderr).

    Returns:
        boolean: indicate if the process was successful
    """

    ff = FFMPEG(*args)
    ff.start(
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
    )

    ff.wait(progress_handler=progress_handler)

    if ff.return_code:
        err = indent(ff.error_log)
        logging.error(f"Problem occured during transcoding\n\n{err}\n\n")
        return False
    return True
