"""Running ffmpeg: progress from -progress, errors from stderr."""

import signal
import subprocess
import tempfile
from collections.abc import Callable
from typing import IO, Any

from nebula.log import log

__all__ = ["FFmpeg", "FFmpegError", "run_ffmpeg"]


class FFmpegError(Exception):
    pass


class FFmpeg:
    """One ffmpeg process.

    start(), then wait() with a handler that gets the seconds of output
    written so far. stop() asks ffmpeg to finish (the output is closed
    properly), kill() ends it at once.
    """

    def __init__(self, *args: Any) -> None:
        self.args = [str(arg) for arg in args]
        self.proc: subprocess.Popen[str] | None = None
        self._stderr: IO[bytes] | None = None
        self._error_lines: list[str] | None = None

    @property
    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    @property
    def returncode(self) -> int | None:
        return self.proc.returncode if self.proc else None

    def start(self) -> None:
        cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-nostats", "-loglevel", "error"]
        cmd.extend(["-progress", "pipe:1", *self.args])
        log.debug("Executing", " ".join(cmd))
        # stderr goes to a file: a full pipe would block ffmpeg while stdout
        # is read
        self._stderr = tempfile.TemporaryFile()
        self._error_lines = None
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=self._stderr, text=True
        )

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def wait(self, progress_handler: Callable[[float], None] | None = None) -> None:
        """Wait for ffmpeg to exit. If the handler raises, ffmpeg keeps
        running: the caller decides (kill())."""
        assert self.proc and self.proc.stdout
        for line in self.proc.stdout:
            key, _, value = line.strip().partition("=")
            if progress_handler and key == "out_time_us" and value.isdigit():
                progress_handler(int(value) / 1_000_000)
        self.proc.wait()

    @property
    def error_lines(self) -> list[str]:
        """What ffmpeg wrote to stderr (errors only). Read after it exited."""
        if self._error_lines is None:
            if self._stderr is None:
                return []
            self._stderr.seek(0)
            text = self._stderr.read().decode("utf-8", errors="replace")
            self._stderr.close()
            self._stderr = None
            self._error_lines = [line for line in text.splitlines() if line.strip()]
        return self._error_lines


def run_ffmpeg(
    args: list[Any], progress_handler: Callable[[float], None] | None = None
) -> None:
    """Run ffmpeg, reporting progress as seconds of output written.

    Raises FFmpegError with ffmpeg's error output on failure.
    """
    ffmpeg = FFmpeg(*args)
    ffmpeg.start()
    try:
        ffmpeg.wait(progress_handler)
    except BaseException:
        # Don't leave ffmpeg running if reporting progress fails
        ffmpeg.kill()
        raise
    if ffmpeg.returncode:
        # The cause comes first; a broken source can repeat errors a lot
        error = "\n".join(ffmpeg.error_lines[:50]).strip()
        raise FFmpegError(error or f"ffmpeg exited with {ffmpeg.returncode}")
