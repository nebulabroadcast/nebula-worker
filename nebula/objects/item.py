from typing import TYPE_CHECKING, Any, Optional

from nebula.objects.base import BaseObject, object_helper
from nebula.settings import settings

if TYPE_CHECKING:
    from nebula.objects.asset import Asset
    from nebula.objects.bin import Bin
    from nebula.objects.event import Event


class Item(BaseObject):
    table_name = "items"
    db_cols = ["id_asset", "id_bin", "position"]
    defaults = {"id_asset": 0, "position": 0}
    object_type_id = 1
    required = ["id_bin", "id_asset", "position"]

    def __getitem__(self, key: str) -> Any:
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
    def id_folder(self) -> int:
        if self.asset:
            return self.asset["id_folder"]
        raise KeyError(f"Folder id is not set for {self}")

    def mark_in(self, new_val: float | None = None) -> float:
        """Get or set mark_in value"""

        if new_val is not None:
            new_val = max(new_val, 0)
            self["mark_in"] = new_val
        return self.get("mark_in", 0)

    def mark_out(self, new_val: float | None = None) -> float:
        """Get or set mark_out value

        Note that 0 is considered "without mark out"
        """
        if new_val is not None:
            new_val = max(new_val, 0)
            self["mark_out"] = new_val
        return self.get("mark_out", 0)

    @property
    def duration(self) -> float:
        """Final duration of the item"""

        if not self["id_asset"]:
            return self["duration"] or 0

        if self.meta.get("mark_out"):
            return (self.meta.get("mark_out") or 0) - (self.meta.get("mark_in") or 0)

        # Item does not have explicit duration so we use raw asset duration instead
        # do not use marked (asset.duration) duration here.
        # marks from items must be used

        if asset := self.asset:
            return asset["duration"] or 0
        return 0

    @property
    def fps(self) -> float:
        if asset := self.asset:
            return asset.fps
        return 25

    @property
    def file_path(self) -> str:
        if asset := self.asset:
            return asset.file_path
        return ""

    @property
    def asset(self) -> Optional["Asset"]:
        if not hasattr(self, "_asset"):
            if not self.meta.get("id_asset", False):
                self._asset = None  # Virtual items
            else:
                self._asset = object_helper.Asset(self["id_asset"], db=self.db) or None
        return self._asset

    @property
    def bin(self) -> "Bin":
        if not hasattr(self, "_bin"):
            self._bin = object_helper.Bin(self["id_bin"], db=self.db)
        return self._bin

    @property
    def event(self) -> Optional["Event"]:
        _bin = self.bin
        if not _bin:
            return None
        return _bin.event


object_helper["item"] = Item
