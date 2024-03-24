import os

from nebula.config import config
from nebula.enum import ContentType, MediaType
from nebula.objects.base import BaseObject, object_helper
from nebula.settings import settings
from nebula.storages import storages


class Asset(BaseObject):
    table_name = "assets"
    db_cols = [
        "id_folder",
        "content_type",
        "media_type",
        "status",
        "version_of",
        "ctime",
        "mtime",
    ]

    object_type_id = 0
    required = ["media_type", "content_type", "id_folder"]
    defaults = {"media_type": MediaType.VIRTUAL, "content_type": ContentType.TEXT}

    #
    # Crud methods
    #

    def delete_children(self) -> None:
        """Delete all database objects related to this asset

        This method is called by the BaseObject.delete method
        """

        if self.id:
            self.db.query("DELETE FROM jobs WHERE id_asset = %s", [self.id])
            # db.commit is called by the delete method

    #
    # Props
    #

    @property
    def file_path(self) -> str:
        """Get file path for this asset

        This is the full path to the file on the filesystem
        If the asset is not a file, it returns an empty string,
        so it can be evaluated as False, but won't raise an exception
        in os.path.exists and so on.
        """

        if self["media_type"] != MediaType.FILE:
            return ""
        try:
            storage_path = storages[int(self["id_storage"])].local_path
            return os.path.join(storage_path, self["path"])
        except (KeyError, IndexError, ValueError):
            return ""

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
    def fps(self) -> float:
        n, d = (int(k) for k in self.meta.get("fps", "25/1").split("/"))
        return n / d

    #
    # Proxy helpers
    #

    @property
    def has_proxy(self):
        return os.path.exists(self.proxy_full_path)

    @property
    def proxy_full_path(self):
        if not self.id:
            return ""
        if not hasattr(self, "_proxy_full_path"):
            self._proxy_full_path = os.path.join(
                storages[self.proxy_storage].local_path, self.proxy_path
            )
        return self._proxy_full_path

    @property
    def proxy_storage(self):
        return settings.system.proxy_storage

    @property
    def proxy_path(self):
        if not self.id:
            return ""
        if not hasattr(self, "_proxy_path"):
            tpl = settings.system.proxy_path
            id1000 = int(self.id / 1000)
            self._proxy_path = tpl.format(id1000=id1000, **self.meta)
        return self._proxy_path

    #
    # Playout helpers
    #

    def get_playout_name(self, id_channel: int) -> str:
        _ = id_channel
        return f"{config.site_name}-{self.id}"

    def get_playout_storage(self, id_channel: int) -> int | None:
        playout_config = settings.get_playout_channel(id_channel)
        if playout_config is None:
            return None
        return playout_config.playout_storage

    def get_playout_path(self, id_channel) -> str | None:
        playout_config = settings.get_playout_channel(id_channel)
        if playout_config is None:
            return None
        if playout_config.playout_dir is None:
            return None
        playout_name = self.get_playout_name(id_channel)
        container = playout_config.playout_container
        return os.path.join(playout_config.playout_dir, f"{playout_name}.{container}")

    def get_playout_full_path(self, id_channel) -> str | None:
        id_storage = self.get_playout_storage(id_channel)
        playout_path = self.get_playout_path(id_channel)
        if not (id_storage and playout_path):
            return None
        return os.path.join(storages[id_storage].local_path, playout_path)

    #
    # Unused methods?
    #

    def load_sidecar_metadata(self):
        pass


object_helper["asset"] = Asset
