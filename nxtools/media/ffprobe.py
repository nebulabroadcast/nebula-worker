__all__ = ["ffprobe"]

import json
import os
import subprocess
from typing import Any

from nxtools.files import FileObject
from nxtools.logging import logging
from nxtools.text import indent


def ffprobe(input_file: str | FileObject, verbose: bool = False) -> dict[str, Any]:
    """
    Extract metadata from a media file using ffprobe
    and returns a dictionary object with the result

    Args:
        input_file (str):
            Path to the media file

        verbose (bool):
            Log the ffprobe command. Default is False

    Returns:
        dict: metadata
    """
    if isinstance(input_file, FileObject):
        exists = input_file.exists
        path = input_file.path
    elif isinstance(input_file, str):
        exists = os.path.exists(input_file)
        path = input_file
    else:
        raise TypeError("input_path must be of string or FileObject type")
    if not exists:
        logging.error(f"ffprobe: file '{input_file}' does not exist")
        return {}
    cmd = ["ffprobe", "-show_format", "-show_streams", "-print_format", "json", path]
    open(os.devnull, "w")
    if verbose:
        logging.debug(f"Executing {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert proc.stdout is not None
    res = proc.stdout.read().decode("utf-8")
    proc.wait()
    if proc.returncode:
        if verbose:
            assert proc.stderr is not None
            error_msg = indent(proc.stderr.read())
            logging.error(f"Unable to read media file {input_file}\n\n{error_msg}\n\n")
        else:
            logging.warning(f"Unable to read media file {input_file}")
        return {}
    return json.loads(res)
