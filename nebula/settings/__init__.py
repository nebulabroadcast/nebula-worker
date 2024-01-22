from typing import Any

from nebula.config import config
from nebula.db import DB
from nebula.settings.metatypes import MetaType
from nebula.settings.models import (
    CSItemModel,
    CSModel,
    FolderSettings,
    PlayoutChannelSettings,
    ServerSettings,
    ViewSettings,
)

settings = ServerSettings()


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

    # TODO

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


def load_settings():
    new_settings = get_server_settings()
    for key in new_settings.dict().keys():
        if key in settings.dict().keys():
            setattr(settings, key, getattr(new_settings, key))
