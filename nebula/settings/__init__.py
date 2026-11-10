import threading
from typing import Any, cast

from nebula.config import config
from nebula.db import DB
from nebula.settings.metatypes import MetaType
from nebula.settings.models import (
    CSItemModel,
    CSModel,
    FolderSettings,
    PlayoutChannelSettings,
    ServerSettings,
    StorageSettings,
    ViewSettings,
)

_settings = ServerSettings()
_loaded = False
_load_lock = threading.Lock()


class _LazySettings:
    """Loads settings from the database on first use.

    `import nebula` doesn't touch the database; everything else uses
    `settings` as before (it forwards to the loaded ServerSettings).
    """

    def __getattr__(self, name: str) -> Any:
        if not _loaded:
            load_settings()
        return getattr(_settings, name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(_settings, name, value)


settings = cast(ServerSettings, _LazySettings())


#
# Load settings from database
#


def get_server_settings() -> ServerSettings:
    result: dict[str, Any] = {}
    db = DB()

    # System settings

    db.query("SELECT key, value FROM settings")
    result["system"] = dict(db.fetchall())
    result["system"]["site_name"] = config.site_name

    # Storages

    _storages: list[StorageSettings] = []
    db.query("SELECT id, settings FROM storages ORDER BY id ASC")
    for id, settings in db.fetchall():
        _storages.append(StorageSettings(id=id, **settings))
    result["storages"] = _storages

    # Playout channels

    _playout_channels: list[PlayoutChannelSettings] = []
    db.query("SELECT id, settings FROM channels WHERE channel_type = 0 ORDER BY id ASC")
    for id, settings in db.fetchall():
        _playout_channels.append(PlayoutChannelSettings(id=id, **settings))
    result["playout_channels"] = _playout_channels

    # Folders

    _folders: list[FolderSettings] = []
    db.query("SELECT id, settings FROM folders ORDER BY id ASC")
    for id, settings in db.fetchall():
        _folders.append(FolderSettings(id=id, **settings))
    result["folders"] = _folders

    # Views

    _views: list[ViewSettings] = []
    db.query("SELECT id, settings FROM views ORDER BY id ASC")
    for id, settings in db.fetchall():
        _views.append(ViewSettings(id=id, **settings))
    result["views"] = _views

    # Metatypes

    _metatypes = {}
    db.query("SELECT key, settings FROM meta_types")
    for key, settings in db.fetchall():
        _metatypes[key] = MetaType.from_settings(settings)
    result["metatypes"] = _metatypes

    # Classification schemes

    _cs: dict[str, CSModel] = {}
    db.query("SELECT cs, value, settings FROM cs ORDER BY value")
    for scheme, value, settings in db.fetchall():
        item = CSItemModel.from_settings(value, settings)
        if scheme not in _cs:
            _cs[scheme] = {}
        _cs[scheme][value] = item
    result["cs"] = _cs

    # Return loaded settings
    return ServerSettings(**result)


def load_settings() -> None:
    """Load (or reload) settings from the database."""
    global _loaded
    with _load_lock:
        new_settings = get_server_settings()
        for key in type(new_settings).model_fields:
            setattr(_settings, key, getattr(new_settings, key))
        _loaded = True
