__all__ = [
    "FileObject",
    "join_path",
    "get_files",
    "get_path_pairs",
    "get_temp",
    "get_base_name",
    "file_to_title",
    "get_file_siblings",
]

import os
import stat
import tempfile
from collections.abc import Iterable

from .common import get_guid
from .logging import log_traceback
from .text import slugify


class FileObject:
    """An object representing a file on the filesystem.

    The class provides a number of utility methods and properties
    for easy access to file metadata.
    """

    def __init__(self, *args, **kwargs):
        """The constructor accepts a path or a list of path components."""
        self.path = os.path.join(*args)
        self.attrs = kwargs

    def __str__(self):
        return self.path

    def __repr__(self):
        return str(self)

    def __getitem__(self, key):
        return self.attrs[key]

    def __setitem__(self, key, value):
        self.attrs[key] = value

    def _load_stat(self):
        stat_result = os.stat(self.path)
        self.attrs.update(
            {
                "mode": stat_result[stat.ST_MODE],
                "ino": stat_result[stat.ST_INO],
                "uid": stat_result[stat.ST_UID],
                "gid": stat_result[stat.ST_GID],
                "size": stat_result[stat.ST_SIZE],
                "atime": stat_result[stat.ST_ATIME],
                "mtime": stat_result[stat.ST_MTIME],
                "ctime": stat_result[stat.ST_CTIME],
            }
        )

    @property
    def atime(self) -> int:
        """Return the last accessed time as Unix timestamp"""
        if "atime" not in self.attrs:
            self._load_stat()
        return self["atime"]

    @property
    def ctime(self) -> int:
        """Return the creation time as Unix timestamp"""
        if "ctime" not in self.attrs:
            self._load_stat()
        return self["ctime"]

    @property
    def mtime(self) -> int:
        """Return the last modified time as Unix timestamp"""
        if "mtime" not in self.attrs:
            self._load_stat()
        return self["mtime"]

    @property
    def size(self):
        """Return the size of the file in bytes"""
        if "size" not in self.attrs:
            self._load_stat()
        return self["size"]

    @property
    def is_dir(self):
        """Return `True` if the path is a directory"""
        if "mode" not in self.attrs:
            self._load_stat()
        return stat.S_ISDIR(self["mode"])

    @property
    def is_reg(self):
        """Return `True` if the path is a regular file"""
        if "mode" not in self.attrs:
            self._load_stat()
        return stat.S_ISREG(self["mode"])

    @property
    def is_file(self):
        """Return `True` if the path is a regular file"""
        return os.path.isfile(self.path)

    @property
    def is_link(self):
        """Return `True` if the path is a symbolic link"""
        if "mode" not in self.attrs:
            self._load_stat()
        return stat.S_ISLNK(self["mode"])

    @property
    def is_fifo(self):
        """Return `True` if the path is a named pipe"""
        if "mode" not in self.attrs:
            self._load_stat()
        return stat.S_ISFIFO(self["mode"])

    @property
    def exists(self):
        """Return `True` if the path exists"""
        return os.path.exists(self.path)

    @property
    def ext(self):
        """Return the file extension"""
        return os.path.splitext(self.path)[1].lstrip(".")

    @property
    def dir_name(self):
        """Return the directory component of the path"""
        return os.path.split(self.path)[0]

    @property
    def base_name(self):
        """Return the base name (without a directory and an extension) of the path"""
        return os.path.splitext(os.path.basename(self.path))[0]

    def open(self, mode="r", **kwargs):
        """Return a file-like object opened with the specified mode"""
        return open(self.path, mode, **kwargs)


def join_path(*args):
    elms = []
    for arg in args:
        if isinstance(arg, FileObject):
            elms.append(arg.path)
        else:
            elms.append(arg)
    return os.path.join(*elms)


def get_files(
    base_path: str,
    *,
    recursive: bool = False,
    hidden: bool = False,
    exts: Iterable[str] | None = None,
    case_sensitive_exts: bool = False,
    relative_path: bool = False,
    strip_path: str | None = None,
):
    """Crawl a given directory

    For each file found in `base_path` yield a FileObject object.

    Args:
        base_path (str):
            Path to the directory to be crawled

        recursive (bool):
            Crawl recursively (default: False)

        hidden (bool):
            Yield hidden (dot)files too (default: False)

        exts (list):
            If specified, yields only files matching given extensions

        case_sensitive_exts (bool):
            Do not ignore cases when `exts` list is used (default: False)
    """

    if exts is None:
        exts = []
    if not case_sensitive_exts:
        exts = [ext.lower() for ext in exts]

    if strip_path is None:
        strip_path = base_path

    if os.path.exists(base_path):
        for dir_entry in os.scandir(base_path):
            file_name = dir_entry.name
            file_path = dir_entry.path
            stat_result = dir_entry.stat()

            file_object = FileObject(
                file_path,
                mode=stat_result[stat.ST_MODE],
                ino=stat_result[stat.ST_INO],
                uid=stat_result[stat.ST_UID],
                gid=stat_result[stat.ST_GID],
                size=stat_result[stat.ST_SIZE],
                atime=stat_result[stat.ST_ATIME],
                mtime=stat_result[stat.ST_MTIME],
                ctime=stat_result[stat.ST_CTIME],
            )

            if not hidden and file_name.startswith("."):
                continue

            if file_object.is_reg:
                ext = os.path.splitext(file_name)[1].lstrip(".")
                if not case_sensitive_exts:
                    ext = ext.lower()
                if exts and ext not in exts:
                    continue
                if relative_path:
                    file_object.path = file_object.path.replace(
                        strip_path, "", 1
                    ).lstrip(os.path.sep)
                    yield file_object
                else:
                    yield file_object
            elif file_object.is_dir and recursive:
                yield from get_files(
                    file_object.path,
                    recursive=recursive,
                    hidden=hidden,
                    case_sensitive_exts=case_sensitive_exts,
                    exts=exts,
                    relative_path=relative_path,
                    strip_path=strip_path,
                )


def get_path_pairs(
    input_dir,
    output_dir,
    *,
    target_ext: str | None = None,
    target_slugify: bool = False,
    recursive: bool = True,
    hidden: bool = False,
    exts: Iterable[str] | None = None,
    case_sensitive_exts: bool = False,
):
    """For each file found in `input_dir` and yield a tuple of (input_path, output_path)

    This function is useful for batch conversion, when you need to process files
    from `input_dir` and output the result to `output_dir`.

    Most arguments are the same as for `get_files`.
    You can also specify a target extension,
    and use a slugifier for the output path.

    Args:
        target_ext (str):
        target_slugify (bool): (default: False)
    """

    if exts is None:
        exts = []
    for input_file in get_files(
        input_dir,
        relative_path=True,
        recursive=recursive,
        hidden=hidden,
        exts=exts,
        case_sensitive_exts=case_sensitive_exts,
    ):
        input_path = input_file.path.replace("\\", "/")
        if target_slugify:
            output_path = join_path(
                output_dir,
                *[slugify(f) for f in input_file.dir_name.split("/")]
                + [slugify(input_file.base_name)],
            )
            if input_file.ext:
                output_path += "." + input_file.ext
        else:
            output_path = join_path(output_dir, input_file.path)

        input_file = FileObject(input_dir, input_path)
        output_file = FileObject(output_path)

        if target_ext:
            output_file.path = os.path.splitext(output_file.path)[0] + "." + target_ext
        yield input_file, output_file


def get_temp(extension: str | None = None, root: str | None = None) -> str:
    """Return a path to a temporary file

    Args:
        extension (str)
        root (str)
    """
    if not root:
        root = tempfile.gettempdir()
    filename = join_path(root, get_guid())
    if extension:
        filename = filename + "." + extension
    return filename


def get_base_name(file_name: str) -> str:
    """Strip a directory and extension from a given path.

    `/etc/foo/bar.baz` becomes `bar`

    Args:
        file_name (str): path-like object, string or FileObject

    Returns:
        str
    """
    return os.path.splitext(os.path.basename(str(file_name)))[0]


def file_to_title(file_name: str) -> str:
    """Attempt to un-slugify a file name"""
    base = get_base_name(file_name)
    base = base.replace("_", " ").replace("-", " - ").strip()
    elms = []
    capd = False
    for elm in base.split(" "):
        if not elm:
            continue
        if not capd and not (elm.isdigit() or elm.upper() == elm):
            elm = elm.capitalize()
            capd = True
        elms.append(elm)
    return " ".join(elms)


def get_file_siblings(path, exts=None):
    # TODO: Rewrite this
    if exts is None:
        exts = []
    root = os.path.splitext(path)[0]
    for f in exts:
        tstf = root + "." + f
        if os.path.exists(tstf):
            yield tstf


def get_file_size(path: str) -> int:
    """Return a file size in bytes

    Instead of using `os.path.getsize` this function opens a file and
    uses the `seek` and `tell` methods to get the size, which is slower,
    but more accurate in some cases.

    Args:
        path (str): path-like object, string or FileObject

    Returns:
        int: file size in bytes
    """
    try:
        f = open(str(path), "rb")
    except Exception:
        log_traceback(f"Exception! File {path} is not accessible")
        return 0
    f.seek(0, 2)
    return f.tell()
