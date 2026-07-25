__all__ = [
    "decode_if_py3",
    "encode_if_py3",
    "PLATFORM",
    "find_binary",
    "get_guid",
    "xml",
    "string_type",
    "string_types",
]

import os
import sys
import uuid
from xml.etree import ElementTree


def decode_if_py3(x, enc="utf-8"):
    return x.decode(enc)


def encode_if_py3(x, enc="utf-8"):
    return bytes(x, enc) if isinstance(x, str) else x


string_type = str
string_types = [str]

PLATFORM = "windows" if sys.platform == "win32" else "unix"


def xml(data):
    """Parse an XML string using ElementTree

    Args:
        data (str): The XML document to parse

    Returns:
        ElementTree.Element: The root element of the parsed XML string
    """
    return ElementTree.XML(data)


def get_guid() -> str:
    """Return a GUID

    Returns:
        str: GUID
    """
    return str(uuid.uuid1())


def find_binary(file_name: str) -> str:
    """Attempt to find a given executable and return its path

    Args:
        file_name (str): The name of the executable to find

    Returns:
        str: The path to the executable
    """
    if os.path.exists(file_name) and file_name == os.path.basename(file_name):
        return "./" + file_name
    return file_name
