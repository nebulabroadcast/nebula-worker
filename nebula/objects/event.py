from typing import TYPE_CHECKING, Optional

from nebula.objects.base import BaseObject, object_helper

if TYPE_CHECKING:
    from nebula.objects.asset import Asset
    from nebula.objects.bin import Bin


class Event(BaseObject):
    table_name = "events"
    db_cols = ["id_channel", "start", "stop", "id_magic"]
    object_type_id = 3
    required = ["start", "id_channel"]

    @property
    def bin(self) -> Optional["Bin"]:
        if not hasattr(self, "_bin"):
            if not self["id_magic"]:  # non-playout events
                self._bin = None
            else:
                self._bin = object_helper.Bin(self["id_magic"], db=self.db)
        return self._bin

    @property
    def asset(self) -> Optional["Asset"]:
        if not hasattr(self, "_asset"):
            # TODO: non-playout events (by channel_type)
            if not self["id_asset"]:
                self._asset = None
            else:
                self._asset = object_helper.Asset(self["id_asset"], db=self.db)
        return self._asset


object_helper["event"] = Event
