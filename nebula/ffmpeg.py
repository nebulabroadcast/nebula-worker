"""Running ffmpeg: progress, abort and errors.

The synchronous counterpart of the ffmpeg wrapper used in async code:
ffmpeg(cmd, progress_handler, check_abort) returns when ffmpeg finished,
raises FFmpegAbortedError when check_abort said so and FFmpegError with
ffmpeg's error output when it failed.
"""

import signal
import subprocess
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from nebula.log import log

__all__ = [
    "AbortWatcher",
    "FFmpegAbortedError",
    "FFmpegError",
    "FFmpegProgress",
    "ffmpeg",
]

BASE_FFMPEG_CMD = ["ffmpeg", "-y", "-hide_banner", "-nostdin", "-nostats"]
BASE_FFMPEG_CMD += ["-loglevel", "error", "-progress", "pipe:1"]

# How long an aborted process gets to finish by itself before it is killed
ABORT_TIMEOUT = 10


class FFmpegError(Exception):
    pass


class FFmpegAbortedError(Exception):
    pass


@dataclass
class FFmpegProgress:
    speed: float = 0.0
    position: float = 0.0  # seconds of output written


class AbortWatcher:
    """Stop a process when check_abort() says so.

    Asked once a second from a thread of its own, so an abort doesn't wait
    for the process to print progress. Not ffmpeg specific (melt uses it too).

        with AbortWatcher(proc, check_abort) as watcher:
            ...  # read the output, proc.wait()
        if watcher.aborted:
            ...
    """

    interval = 1.0

    def __init__(
        self,
        proc: "subprocess.Popen[Any]",
        check_abort: Callable[[], bool] | None,
    ) -> None:
        self.proc = proc
        self.check_abort = check_abort
        self.aborted = False
        self._done = threading.Event()
        self._thread: threading.Thread | None = None

    def _check(self) -> bool:
        assert self.check_abort
        try:
            return bool(self.check_abort())
        except Exception:
            # e.g. the database is away: keep encoding, ask again
            log.traceback("Unable to check for abort")
            return False

    def _watch(self) -> None:
        while not self._done.wait(self.interval):
            if not self._check():
                continue
            self.aborted = True
            if self.proc.poll() is None:
                log.warning("Aborting the process")
                # SIGINT: ffmpeg and melt close their output and exit
                self.proc.send_signal(signal.SIGINT)
                try:
                    self.proc.wait(ABORT_TIMEOUT)
                except subprocess.TimeoutExpired:
                    log.warning("The process ignores the abort, killing it")
                    self.proc.kill()
            return

    def __enter__(self) -> "AbortWatcher":
        if self.check_abort:
            self._thread = threading.Thread(target=self._watch, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._done.set()
        if self._thread:
            self._thread.join()
        # Asked for just as the process exited, between two checks
        if self.check_abort and not self.aborted and exc[0] is None:
            self.aborted = self._check()


def update_progress_object(progress: FFmpegProgress, line: str) -> bool:
    """Returns True when the position changed."""
    key, _, value = line.strip().partition("=")
    if key == "out_time_us" and value.isdigit():
        progress.position = int(value) / 1_000_000
        return True
    if key == "speed":
        try:
            progress.speed = float(value.strip().rstrip("x"))
        except ValueError:
            pass  # N/A
    return False


def ffmpeg(
    cmd: list[Any],
    progress_handler: Callable[[FFmpegProgress], None] | None = None,
    check_abort: Callable[[], bool] | None = None,
    niceness: int | None = None,
) -> None:
    """Run ffmpeg with the given arguments (-y is added).

    progress_handler gets an FFmpegProgress whenever the position changes.
    check_abort is asked once a second; when it returns True, ffmpeg is
    stopped and FFmpegAbortedError raised. If progress_handler raises,
    ffmpeg is killed and the exception passed on.
    """
    progress = FFmpegProgress()

    pre_cmd = ["nice", f"-n{niceness}"] if niceness is not None else []
    full_cmd = pre_cmd + BASE_FFMPEG_CMD + [str(arg) for arg in cmd]
    log.debug(" ".join(full_cmd))

    # stderr goes to a file: a full pipe would block ffmpeg while stdout is read
    with tempfile.TemporaryFile() as stderr:
        proc = subprocess.Popen(
            full_cmd, stdout=subprocess.PIPE, stderr=stderr, text=True
        )
        assert proc.stdout is not None
        try:
            with AbortWatcher(proc, check_abort) as watcher:
                for line in proc.stdout:
                    if update_progress_object(progress, line) and progress_handler:
                        progress_handler(progress)
                proc.wait()
        except BaseException:
            # Don't leave ffmpeg running if reporting progress fails
            proc.kill()
            proc.wait()
            raise

        if watcher.aborted:
            raise FFmpegAbortedError("FFmpeg process was aborted")

        if proc.returncode:
            stderr.seek(0)
            lines = stderr.read().decode("utf-8", errors="replace").splitlines()
            # The cause comes first; a broken source can repeat errors a lot
            error = "\n".join(lines[:50]).strip()
            raise FFmpegError(error or f"ffmpeg exited with {proc.returncode}")
