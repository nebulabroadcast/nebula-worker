__all__ = ["FFMPEG", "ffmpeg", "enable_ffmpeg_debug"]

import re
import signal
import subprocess
import sys

from nxtools.common import PLATFORM
from nxtools.logging import logging
from nxtools.text import indent

FFMPEG_DEBUG = False

re_position = re.compile(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})\d*", re.U | re.I)


def enable_ffmpeg_debug():
    global FFMPEG_DEBUG
    FFMPEG_DEBUG = True


def time2sec(search):
    hh, mm, ss, cs = search.group(1), search.group(2), search.group(3), search.group(4)
    return int(hh) * 3600 + int(mm) * 60 + int(ss) + int(cs) / 100.0


class FFMPEG:
    def __init__(self, *args, **kwargs):
        if kwargs.get("debug", False):
            enable_ffmpeg_debug()
        if FFMPEG_DEBUG:
            logging.warning("FFMPEG debug mode is enabled")

        self.proc = None
        self.cmd = ["ffmpeg", "-hide_banner"]
        self.cmd.extend(str(arg) for arg in args)

    def reset_stderr(self):
        self.buff = b""
        self.error_log = ""

    @property
    def is_running(self):
        return bool(self.proc) and self.proc.poll() is None

    @property
    def stdin(self):
        return self.proc.stdin

    @property
    def stdout(self):
        return self.proc.stdout

    @property
    def stderr(self):
        return self.proc.stderr

    @property
    def return_code(self):
        return self.proc.returncode

    def start(self, stdin=None, stdout=None, stderr=subprocess.PIPE):
        self.reset_stderr()
        logging.debug("Executing", " ".join(self.cmd))
        self.proc = subprocess.Popen(
            self.cmd,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )

    def stop(self):
        if not self.proc:
            return False
        if PLATFORM == "windows":
            self.proc.send_signal(signal.CTRL_C_EVENT)
        else:
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
        self.proc.wait()
        self.error_log += self.stderr.read().decode("utf-8")
        if interrupted:
            raise KeyboardInterrupt

    def process(self, progress_handler=None):
        ch = self.proc.stderr.read(1)
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

            if FFMPEG_DEBUG:
                sys.stderr.write(line + "\n")

            self.buff = b""
        else:
            self.buff += ch
        return True


def ffmpeg(
    *args,
    progress_handler=None,
    stdin=subprocess.PIPE,
    stdout=None,
    stderr=subprocess.PIPE,
    debug=False,
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

    ff = FFMPEG(*args, debug=debug)
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
