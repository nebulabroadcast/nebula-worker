import json
import pprint
import time
from typing import TYPE_CHECKING, Any, Type

from nxtools import slugify

from nebula.db import DB
from nebula.log import log
from nebula.messaging import messaging
from nebula.metadata.format import format_meta
from nebula.metadata.normalize import normalize_meta
from nebula.settings import settings

if TYPE_CHECKING:
    from nebula.objects.asset import Asset
    from nebula.objects.bin import Bin
    from nebula.objects.event import Event
    from nebula.objects.item import Item
    from nebula.objects.user import User


def create_ft_index(meta):
    ft = {}
    if "subclips" in meta:
        weight = 8
        for sc in [k.get("title", "") for k in meta["subclips"]]:
            try:
                for word in slugify(sc, make_set=True, min_length=3):
                    if word not in ft:
                        ft[word] = weight
                    else:
                        ft[word] = max(ft[word], weight)
            except Exception:
                log.error("Unable to slugify subclips data")
    for key in meta:
        # TODO
        # if key not in meta_types:
        #     continue
        # weight = meta_types[key].get("fulltext")
        weight = 0
        if isinstance(meta[key], str):
            weight = 1

        if not weight:
            continue
        try:
            for word in slugify(meta[key], make_set=True, min_length=3):
                if word not in ft:
                    ft[word] = weight
                else:
                    ft[word] = max(ft[word], weight)
        except Exception:
            log.error(f"Unable to slugify key {key} with value {meta[key]}")
    return ft


class BaseObject:
    """Base object properties."""

    object_type_id: int
    table_name: str
    db_cols: list[str] = []
    required: list[str] = []
    defaults: dict[str, Any] = {}

    def __init__(self, id: int | None = None, **kwargs):
        """Object constructor."""

        if "db" in kwargs:
            self._db = kwargs["db"]

        self.text_changed = self.meta_changed = False
        self.is_new = True
        self.meta = {}
        meta = kwargs.get("meta", {})
        if id:
            assert isinstance(id, int), f"{self.object_type} ID must be integer"
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

    #
    # Database access
    #

    @property
    def db(self):
        if not hasattr(self, "_db"):
            log.debug(f"{self} is opening DB connection")
            self._db = DB()
        return self._db

    #
    # Object metadata
    #

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

    def __delitem__(self, key: str):
        key = key.lower().strip()
        if key not in self.meta:
            return
        del self.meta[key]

    def __repr__(self) -> str:
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

    def __bool__(self) -> bool:
        return not self.is_new

    def show(self, key, **kwargs):
        return format_meta(self, key, **kwargs)

    def show_meta(self):
        return pprint.pformat(self.meta)

    def update(self, data: dict[str, Any]) -> None:
        for key in data:
            self[key] = data[key]

    #
    # Crud
    #

    def new(self):
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

        is_new = self.is_new
        if is_new:
            self._insert(**kwargs)
        else:
            self._update(**kwargs)
            self.invalidate()
        if self.text_changed or is_new:
            self.update_ft_index(is_new)
        if kwargs.get("commit", True):
            self.db.commit()
        self.text_changed = self.meta_changed = False
        self.is_new = False
        if kwargs.get("notify", True):
            messaging.send(
                "objects_changed", objects=[self.id], object_type=self.object_type
            )

    def delete(self, **kwargs):
        _ = kwargs
        if not self.id:
            return
        log.info(f"Deleting {self}")
        self.delete_children()
        self.db.query(f"DELETE FROM {self.table_name} WHERE id=%s", [self.id])
        self.db.query(
            "DELETE FROM ft WHERE object_type=%s AND id=%s",
            [self.object_type_id, self.id],
        )
        self.db.commit()

    def load(self, id):
        self.db.query(f"SELECT meta FROM {self.table_name} WHERE id = {id}")
        try:
            self.meta = self.db.fetchall()[0][0]
        except IndexError:
            log.error(
                f"Unable to load {self.__class__.__name__}"
                f"ID:{id}. Object does not exist"
            )
            return False

    def _insert(self, **kwargs):
        _ = kwargs
        cols: list[str] = []
        vals: list[Any] = []
        if self.id:
            cols.append("id")
            vals.append(self.id)
        for col in self.db_cols:
            cols.append(col)
            vals.append(self[col])
        if self.id:
            cols.append("meta")
            vals.append(json.dumps(self.meta))

        if cols:
            query = "INSERT INTO {} ({}) VALUES ({}) RETURNING id".format(
                self.table_name, ", ".join(cols), ", ".join(["%s"] * len(cols))
            )
        else:
            query = f"""
                INSERT INTO {self.table_name}
                DEFAULT VALUES RETURNING id
            """
        self.db.query(query, vals)

        if not self.id:
            new_id = self.db.fetchall()[0][0]
            assert new_id, "Unable to insert new object, database returned no ID"
            self["id"] = new_id
            self.db.query(
                f"UPDATE {self.table_name} SET meta=%s WHERE id=%s",
                [json.dumps(self.meta), new_id],
            )

    def _update(self, **kwargs):
        _ = kwargs
        assert self.id, "Unable to update object, no ID"
        cols: list[str] = ["meta"]
        vals: list[Any] = [json.dumps(self.meta)]

        for col in self.db_cols:
            cols.append(col)
            vals.append(self[col])

        query = "UPDATE {} SET {} WHERE id=%s".format(
            self.table_name, ", ".join([key + "=%s" for key in cols])
        )
        self.db.query(query, vals + [self.id])

    def update_ft_index(self, is_new=False):
        if not is_new:
            self.db.query(
                "DELETE FROM ft WHERE object_type=%s AND id=%s",
                [self.object_type_id, self.id],
            )
        ft = create_ft_index(self.meta)
        if not ft:
            return
        args = [(self.id, self.object_type_id, ft[word], word) for word in ft]
        tpls = ",".join(["%s"] * len(args))
        self.db.query(
            f"""
            INSERT INTO ft (id, object_type, weight, value)
            VALUES {tpls}""",
            args,
        )

    #
    # Methods overriden by subclasses
    #

    def delete_children(self):
        pass

    def invalidate(self):
        """Invalidate all cache objects which references this one"""
        pass


class ObjectHelper:
    """
    This class is used to register all object classes,
    so they can be accessed from anywhere in the nebula.object
    without circular imports.

    It shouldn't be used anywhere else.
    """

    def __init__(self):
        self.classes = {}

    def __setitem__(self, key: str, value):
        self.classes[key] = value

    @property
    def Asset(self) -> Type["Asset"]:
        assert "asset" in self.classes, "Asset class is not registered"
        return self.classes["asset"]

    @property
    def Item(self) -> Type["Item"]:
        assert "item" in self.classes, "Item class is not registered"
        return self.classes["item"]

    @property
    def Bin(self) -> Type["Bin"]:
        assert "bin" in self.classes, "Bin class is not registered"
        return self.classes["bin"]

    @property
    def Event(self) -> Type["Event"]:
        assert "event" in self.classes, "Event class is not registered"
        return self.classes["event"]

    @property
    def User(self) -> Type["User"]:
        assert "user" in self.classes, "User class is not registered"
        return self.classes["user"]

    # unused?
    # def invalidate(self, object_type, meta):
    #     obj = self.classes[object_type](meta=meta)
    #     obj.invalidate()


object_helper = ObjectHelper()
