import os
import pprint
import time
from typing import Any

from nebula.enum import ContentType, MediaType
from nebula.log import log
from nebula.metadata.format import format_meta
from nebula.metadata.normalize import normalize_meta
from nebula.settings import settings
from nebula.storages import storages


class BaseObject:
    """Base object properties."""

    required: list[str] = []
    defaults: dict[str, Any] = {}

    def __init__(self, id: int | None = None, **kwargs):
        """Object constructor."""
        self.text_changed = self.meta_changed = False
        self.is_new = True
        self.meta = {}
        meta = kwargs.get("meta", {})
        if id:
            assert type(id) == int, f"{self.object_type} ID must be integer"
        assert (
            meta is not None
        ), f"Unable to load {self.object_type}. Meta must not be 'None'"
        assert hasattr(meta, "keys"), "Incorrect meta!"
        for key in meta:
            self.meta[key] = meta[key]
        if "id" in self.meta:
            self.is_new = False
        elif not self.meta:
            if id:
                self.load(id)
                self.is_new = False
            else:
                self.new()
                self.is_new = True
                self["ctime"] = self["mtime"] = time.time()
        for key in self.defaults:
            if key not in self.meta:
                self.meta[key] = self.defaults[key]

    @property
    def id(self) -> int | None:
        """Return object ID."""
        return self.meta.get("id", False)

    @property
    def id_folder(self) -> int | None:
        """Return folder ID."""
        return self.meta.get("id_folder")

    @property
    def object_type(self) -> str:
        return self.__class__.__name__.lower()

    def keys(self) -> list[str]:
        """Return list of metadata keys."""
        return list(self.meta.keys())

    def get(self, key: str, default: Any = None) -> Any:
        """Return a metadata value."""
        if key in self.meta:
            return self[key]
        return default

    def __getitem__(self, key: str) -> Any:
        value = self.meta.get(key)
        if value is None and key in settings.metatypes:
            default = settings.metatypes[key].default
            return default
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        try:
            value = normalize_meta(key, value)
        except ValueError as e:
            raise ValueError(f"Invalid value for {key}: {value}") from e
        if value is None:
            self.meta.pop(key, None)
        else:
            self.meta[key] = value

    def update(self, data: dict[str, Any]) -> None:
        for key in data.keys():
            self[key] = data[key]

    def new(self):
        pass

    def load(self, id):
        pass

    def save(self, **kwargs):
        if not kwargs.get("silent", False):
            log.debug(f"Saving {self}")
        self["ctime"] = self["ctime"] or time.time()
        if kwargs.get("set_mtime", True):
            self["mtime"] = time.time()
        for key in self.required:
            if (key not in self.meta) and (key in self.defaults):
                self[key] = self.defaults[key]
            assert key in self.meta, f"Unable to save {self}. {key} is required"

    def delete(self, **kwargs):
        assert self.id, "Unable to delete unsaved object"

    def __delitem__(self, key):
        key = key.lower().strip()
        if key not in self.meta:
            return
        del self.meta[key]

    def __repr__(self):
        if self.id:
            result = f"{self.object_type} ID:{self.id}"
        else:
            result = f"new {self.object_type}"
        if self.object_type == "item" and not hasattr(self, "_asset"):
            title = ""
        else:
            title = self["title"]
        if title:
            result += f" ({title})"
        return result

    def __len__(self):
        return not self.is_new

    def show(self, key, **kwargs):
        return format_meta(self, key, **kwargs)

    def show_meta(self):
        return pprint.pformat(self.meta)


#
# Mixins
#


class AssetMixIn:
    object_type_id = 0
    required = ["media_type", "content_type", "id_folder"]
    defaults = {"media_type": MediaType.VIRTUAL, "content_type": ContentType.TEXT}

    def mark_in(self, new_val=False):
        if new_val:
            self["mark_in"] = new_val
        return max(float(self["mark_in"] or 0), 0)

    def mark_out(self, new_val=False):
        if new_val:
            self["mark_out"] = new_val
        return max(float(self["mark_out"] or 0), 0)

    @property
    def file_path(self):
        if self["media_type"] != MediaType.FILE:
            return ""
        try:
            return os.path.join(
                storages[int(self["id_storage"])].local_path, self["path"]
            )
        except (KeyError, IndexError, ValueError):
            # Yes. empty string. keep it this way!!!
            # (because of os.path.exists and so on)
            # Also: it evals as false
            return ""

    @property
    def duration(self):
        dur = float(self.meta.get("duration", 0))
        mark_in = float(self.meta.get("mark_in", 0))
        mark_out = float(self.meta.get("mark_out", 0))
        if not dur:
            return 0
        if mark_out > 0:
            dur = mark_out + (1 / self.fps)
        if mark_in > 0:
            dur -= mark_in
        return dur

    @property
    def fps(self):
        n, d = [int(k) for k in self.meta.get("fps", "25/1").split("/")]
        return n / d


class ItemMixIn:
    object_type_id = 1
    required = ["id_bin", "id_asset", "position"]

    def __getitem__(self, key):
        key = key.lower().strip()
        if key not in self.meta:
            if key == "id_asset":
                return None
            elif self.asset and key not in ["mark_in", "mark_out"]:
                return self.asset[key]
            elif key in settings.metatypes:
                return settings.metatypes[key].default
            else:
                return None
        return self.meta[key]

    @property
    def id_folder(self):
        if self.asset:
            return self.asset.id_folder
        return self.meta.get("id_folder")

    def mark_in(self, new_val=False):
        if new_val:
            self["mark_in"] = new_val
        return max(float(self["mark_in"] or 0), 0)

    def mark_out(self, new_val=False):
        if new_val:
            self["mark_out"] = new_val
        return max(float(self["mark_out"] or 0), 0)

    @property
    def asset(self):
        pass

    @property
    def bin(self):
        pass

    @property
    def event(self):
        pass

    @property
    def duration(self) -> float:
        """Final duration of the item"""

        if not self["id_asset"]:
            return self["duration"]

        if self.meta.get("mark_out"):
            return (self.meta.get("mark_out") or 0) - (self.meta.get("mark_in") or 0)

        # Item does not have explicit duration so we use raw asset duration instead
        # do not use marked (asset.duration) duration here.
        # marks from items must be used

        return self.asset["duration"] or 0

        #
        # if self["id_asset"]:
        #     dur = self.asset["duration"] or 0
        # elif self["duration"]:
        #     dur = self["duration"] or 0
        # else:
        #     return self.mark_out() - self.mark_in()
        # if not dur:
        #     return 0
        # mark_in = self.mark_in()
        # mark_out = self.mark_out()
        # if mark_out > 0:
        #     dur = mark_out + (1 / self.fps)
        # if mark_in > 0:
        #     dur -= mark_in
        # return dur

    @property
    def fps(self):
        return self.asset.fps

    @property
    def file_path(self):
        if not self["id_asset"]:
            return ""
        return self.asset.file_path


class BinMixIn:
    object_type_id = 2
    required = ["bin_type"]
    defaults = {"bin_type": 0}

    @property
    def duration(self):
        if "duration" not in self.meta:
            duration = 0
            for item in self.items:
                duration += item.duration
            self["duration"] = duration
        return self["duration"]


class EventMixIn:
    object_type_id = 3
    required = ["start", "id_channel"]


class UserMixIn:
    object_type_id = 4
    required = ["login", "password"]

    def __getitem__(self, key):
        if key == "title":
            return self.meta.get("login", "Anonymous")
        return super(UserMixIn, self).__getitem__(key)

    # def set_password(self, password):
    #     self["password"] = get_hash(password)

    def has_right(self, key, val=True, anyval=False):
        if self["is_admin"]:
            return True
        key = f"can/{key}"
        if not self[key]:
            return False
        if anyval:
            return True
        return self[key] is True or (type(self[key]) == list and val in self[key])
