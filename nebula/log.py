import enum
import sys
import traceback
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from nebula.messaging import Messaging


def indent(text: str, level: int = 4) -> str:
    return text.replace("\n", f"\n{' '*level}")


class LogLevel(enum.IntEnum):
    """Log level."""

    TRACE = 0
    DEBUG = 1
    INFO = 2
    SUCCESS = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5


class Logger:
    user: str = "nebula"
    level = LogLevel.DEBUG
    messaging: Optional["Messaging"] = None
    user_max_length: int = 16

    def __call__(
        self,
        level: LogLevel,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if level < self.level:
            return

        lvl = level.name.upper()
        usr = kwargs.get("user") or self.user
        usr = usr[: self.user_max_length].ljust(self.user_max_length)
        msg = " ".join([str(arg) for arg in args])

        print(
            f"{lvl:<8} {usr} {msg}",
            file=sys.stderr,
            flush=True,
        )

        if self.messaging:
            self.messaging(
                "log",
                level=level,
                user=usr,
                message=msg,
            )

    def trace(self, *args: Any, **kwargs: Any) -> None:
        self(LogLevel.TRACE, *args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self(LogLevel.DEBUG, *args, **kwargs)

    def info(self, *args: Any, **kwargs: Any) -> None:
        self(LogLevel.INFO, *args, **kwargs)

    def success(self, *args: Any, **kwargs: Any) -> None:
        self(LogLevel.SUCCESS, *args, **kwargs)

    def warn(self, *args: Any, **kwargs: Any) -> None:
        self(LogLevel.WARNING, *args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self(LogLevel.WARNING, *args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        self(LogLevel.ERROR, *args, **kwargs)

    def traceback(self, *args: Any, **kwargs: Any) -> str:
        msg = " ".join([str(arg) for arg in args])
        tb = traceback.format_exc()
        msg = f"{msg}\n\n{indent(tb)}"
        self(LogLevel.ERROR, msg, **kwargs)
        return msg

    def critical(self, *args, **kwargs):
        self(LogLevel.CRITICAL, *args, **kwargs)
        sys.exit(-1)


log = Logger()
