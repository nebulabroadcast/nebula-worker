import os
import subprocess
from collections.abc import Callable
from typing import Any
from xml.etree import ElementTree as etree

import nebula
from nebula.storages import storages
from nxtools import get_temp


def temp_file(id_storage: int, ext: str) -> str | None:
    temp_dir = os.path.join(storages[id_storage].local_path, ".nx", "creating")
    try:
        os.makedirs(temp_dir, exist_ok=True)
    except Exception:
        nebula.log.traceback()
        return None
    return get_temp(ext, temp_dir)


class ConversionError(Exception):
    pass


class BaseEncoder:
    def __init__(
        self,
        asset: nebula.Asset,
        task: etree.Element,
        params: dict[str, Any],
    ) -> None:
        self.asset = asset
        self.task = task
        self.params = params
        self.proc: subprocess.Popen | None = None  # type: ignore[type-arg]
        self.progress = 0
        self.message = "Started"
        self.aborted = False

    def configure(self) -> None: ...

    @property
    def is_running(self) -> bool:
        return False

    def start(self) -> None: ...

    def stop(self) -> None: ...

    def wait(self, progress_handler: Callable[[float], None]) -> None: ...

    def finalize(self) -> None: ...
