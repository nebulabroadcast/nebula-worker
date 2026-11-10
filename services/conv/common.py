import os
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


class ConversionAborted(Exception):
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
        # temp path -> target path, moved into place by finalize()
        self.files: dict[str, str] = {}

    def configure(self) -> None: ...

    def run(
        self,
        progress_handler: Callable[[float], None],
        check_abort: Callable[[], bool] | None = None,
    ) -> None:
        """Encode. progress_handler gets percents. Raises ConversionAborted
        when check_abort said so, ConversionError when encoding failed."""

    def finalize(self) -> None:
        """Move the finished files into place."""
        for temp_path, target_path in self.files.items():
            try:
                nebula.log.debug(f"Moving {temp_path} to {target_path}")
                os.rename(temp_path, target_path)
            except OSError as e:
                raise ConversionError("Unable to move output file") from e

    def remove_temp_files(self) -> None:
        for temp_path in self.files:
            try:
                os.remove(temp_path)
            except FileNotFoundError:
                pass
            except OSError:
                nebula.log.warning(f"Unable to remove temp file {temp_path}")
