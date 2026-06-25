__all__ = ["Logging", "logging", "log_traceback", "critical_error", "log_to_file"]

import functools
import os
import sys
import time
import traceback

from .common import PLATFORM
from .text import indent
from .timeutils import format_time

try:
    from colorama import Fore, Style, init

    init()
    has_colorama = True
except ModuleNotFoundError:
    has_colorama = False


DEBUG, INFO, WARNING, ERROR, GOOD_NEWS = range(5)

# Log handlers


def null_handler(**kwargs):
    return True


def log_to_file(log_path):
    def log_to_file_handler(path, **kwargs):
        placeholders = {"date": format_time(time.time(), "%Y-%m-%d")}
        path = path.format(**placeholders)
        dirname, _ = os.path.split(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        with open(path, "a") as f:
            tstamp = format_time(time.time())
            ltype = {
                0: "DEBUG    ",
                1: "INFO     ",
                2: "WARNING  ",
                3: "ERROR    ",
                4: "GOOD NEWS",
            }[kwargs["message_type"]]
            f.write(f"{tstamp}  {ltype}  {kwargs['message']}\n")

    return functools.partial(log_to_file_handler, log_path)


# Logging


class Logging:
    """nxtools universal logger"""

    def __init__(self, user=""):
        self.show_time = True
        self.show_colors = True
        self.user = user
        self.handlers = []
        self.file = sys.stderr

        self.formats_ansi = {
            INFO: "\033[1;30m{timestamp}\033[0m{type} {user} {message}",
            DEBUG: "\033[1;30m{timestamp}\033[0m\033[34m{type} {user} {message}\033[0m",
            WARNING: "\033[1;30m{timestamp}\033[0m\033[33m{type}\033[0m {user} {message}",
            ERROR: "\033[1;30m{timestamp}\033[0m\033[31m{type}\033[0m {user} {message}",
            GOOD_NEWS: "\033[1;30m{timestamp}\033[0m\033[32m{type}\033[0m {user} {message}",
        }

        self.formats_nocolor = {
            DEBUG: "{timestamp}{type} {user} {message}",
            INFO: "{timestamp}{type} {user} {message}",
            WARNING: "{timestamp}{type} {user} {message}",
            ERROR: "{timestamp}{type} {user} {message}",
            GOOD_NEWS: "{timestamp}{type} {user} {message}",
        }

        if has_colorama:
            self.formats_colorama = {
                DEBUG: Style.BRIGHT
                + Fore.BLACK
                + "{timestamp}"
                + Style.RESET_ALL
                + Fore.BLUE
                + "{type}"
                + "{user} {message}"
                + Style.RESET_ALL,
                INFO: Style.BRIGHT
                + Fore.BLACK
                + "{timestamp}"
                + Style.NORMAL
                + Fore.WHITE
                + "{type}"
                + Fore.RESET
                + "{user} {message}"
                + Style.RESET_ALL,
                WARNING: Style.BRIGHT
                + Fore.BLACK
                + "{timestamp}"
                + Style.NORMAL
                + Fore.YELLOW
                + "{type}"
                + Fore.RESET
                + "{user} {message}"
                + Style.RESET_ALL,
                ERROR: Style.BRIGHT
                + Fore.BLACK
                + "{timestamp}"
                + Style.NORMAL
                + Fore.RED
                + "{type}"
                + Fore.RESET
                + "{user} {message}"
                + Style.RESET_ALL,
                GOOD_NEWS: Style.BRIGHT
                + Fore.BLACK
                + "{timestamp}"
                + Style.NORMAL
                + Fore.GREEN
                + "{type}"
                + Fore.RESET
                + "{user} {message}"
                + Style.RESET_ALL,
            }
        else:
            self.formats_colorama = self.formats_nocolor

    def add_handler(self, handler):
        if handler not in self.handlers:
            self.handlers.append(handler)

    def _send(self, msgtype, *args, **kwargs):
        message = " ".join([str(arg) for arg in args])
        user = kwargs.get("user", self.user)

        if kwargs.get("handlers", True):
            for handler in self.handlers:
                handler(user=self.user, message_type=msgtype, message=message)

        ldata = {
            "user": f" {user:<15}" if user else "",
            "message": message,
            "timestamp": format_time(time.time()) + " " if self.show_time else "",
            "type": {
                DEBUG: "DEBUG     ",
                INFO: "INFO      ",
                WARNING: "WARNING   ",
                ERROR: "ERROR     ",
                GOOD_NEWS: "GOOD NEWS ",
            }[msgtype],
        }

        if self.show_colors and has_colorama:
            fstring = self.formats_colorama[msgtype]
        elif self.show_colors and PLATFORM == "unix":
            fstring = self.formats_ansi[msgtype]
        else:
            fstring = self.formats_nocolor[msgtype]

        try:
            print(fstring.format(**ldata), file=self.file)
        except Exception:
            pass

    def debug(self, *args, **kwargs):
        """Log a debug message"""
        self._send(DEBUG, *args, **kwargs)

    def info(self, *args, **kwargs):
        """Log an info message"""
        self._send(INFO, *args, **kwargs)

    def warning(self, *args, **kwargs):
        """Log a warning message"""
        self._send(WARNING, *args, **kwargs)

    def error(self, *args, **kwargs):
        """Log an error message"""
        self._send(ERROR, *args, **kwargs)

    def goodnews(self, *args, **kwargs):
        """Log good news"""
        self._send(GOOD_NEWS, *args, **kwargs)


logging = Logging()


def log_traceback(message="Exception!", **kwargs):
    """Log the current exception traceback"""
    tb = traceback.format_exc()
    msg = f"{message}\n\n{indent(tb)}"
    logging.error(msg, **kwargs)
    return msg


def critical_error(message, return_code=1, **kwargs):
    """Log an error message and exit program."""
    logging.error(message, **kwargs)
    logging.debug("Critical error. Terminating program.")
    sys.exit(return_code)
