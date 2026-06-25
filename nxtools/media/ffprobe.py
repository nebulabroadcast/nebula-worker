__all__ = ["ffprobe"]

import json
import os
import subprocess

from nxtools.files import FileObject
from nxtools.logging import *
from nxtools.text import indent


def ffprobe(input_file: str, verbose: bool = False) -> dict:
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
    elif type(input_file) == str:
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
    res = proc.stdout.read().decode("utf-8")
    proc.wait()
    if proc.returncode:
        if verbose:
            error_msg = indent(proc.stderr.read())
            logging.error(f"Unable to read media file {input_file}\n\n{error_msg}\n\n")
        else:
            logging.warning(f"Unable to read media file {input_file}")
        return False
    return json.loads(res)
