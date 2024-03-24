from typing import TYPE_CHECKING, Optional

from nxtools import s2tc

from nebula.log import log
from nebula.objects.base import BaseObject, object_helper

if TYPE_CHECKING:
    from nebula.objects.event import Event
    from nebula.objects.item import Item


class Bin(BaseObject):
    object_type_id = 2
    table_name = "bins"
    db_cols = ["bin_type"]
    required = ["bin_type"]
    defaults = {"bin_type": 0}

    @property
    def duration(self) -> float:
        if "duration" not in self.meta:
            duration: float = 0
            for item in self.items:
                duration += item.duration
            self["duration"] = duration
        return self["duration"]

    @property
    def items(self) -> list["Item"]:
        if not hasattr(self, "_items"):
            if not self.id:
                self._items = []
            else:
                self.db.query(
                    """
                    SELECT meta FROM items
                    WHERE id_bin=%s ORDER BY position ASC, id ASC
                    """,
                    [self.id],
                )
                self._items = [
                    object_helper.Item(meta=meta, db=self.db)
                    for (meta,) in self.db.fetchall()
                ]
        return self._items

    @items.setter
    def items(self, value: list["Item"]) -> None:
        assert isinstance(value, list)
        self._items = value

    def append(self, item: "Item") -> None:
        assert isinstance(item, object_helper.Item)
        self._items.append(item)

    @property
    def event(self) -> Optional["Event"]:
        if not hasattr(self, "_event"):
            self._event: Optional["Event"]
            self.db.query(
                """
                SELECT meta FROM events
                WHERE id_magic=%s
                """,
                [self.id],
            )  # TODO: playout only
            try:
                self._event = object_helper.Event(meta=self.db.fetchall()[0][0])
            except IndexError:
                log.error(f"Unable to get {self} event")
                self._event = None
            except Exception:
                log.traceback()
                self._event = None
        return self._event

    def delete_children(self) -> None:
        """Delete all items in bin

        This method is used to delete all items in bin.
        It is called from BaseObject.delete() method.
        """
        for item in self.items:
            item.delete()
        self._items = []

    def save(self, **kwargs) -> None:
        """Save bin and recalculate duration

        This is an override of BaseObject.save() method
        it is needed to recalculate duration of the bin
        based on its items.
        """
        duration: float = 0
        for item in self.items:
            duration += item.duration
        if duration != self.duration:
            log.debug(f"New duration of {self} is {s2tc(duration)}")
            self["duration"] = duration
        super().save(**kwargs)


object_helper["bin"] = Bin
