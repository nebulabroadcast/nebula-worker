import os
import subprocess
from typing import Any, Callable

from nxtools import get_temp

import nebula
from nebula.storages import storages


def temp_file(id_storage, ext):
    temp_dir = os.path.join(storages[id_storage].local_path, ".nx", "creating")
    if not os.path.isdir(temp_dir):
        try:
            os.makedirs(temp_dir)
        except Exception:
            nebula.log.traceback()
            return False
    return get_temp(ext, temp_dir)


class ConversionError(Exception):
    pass


class BaseEncoder:
    def __init__(self, asset: nebula.Asset, task, params: dict[str, Any]):
        self.asset = asset
        self.task = task
        self.params = params
        self.proc: subprocess.Popen | None = None
        self.progress = 0
        self.message = "Started"
        self.aborted = False

    def configure(self):
        raise NotImplementedError

    @property
    def is_running(self) -> bool:
        raise NotImplementedError

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError

    def wait(self, progress_handler: Callable) -> None:
        raise NotImplementedError

    def finalize(self) -> None:
        raise NotImplementedError
