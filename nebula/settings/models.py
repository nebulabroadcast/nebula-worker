from typing import Annotated, Any, Literal

from pydantic import Field

from nebula.enum import ContentType, MediaType, ServiceState
from nebula.settings.common import SettingsModel
from nebula.settings.metatypes import MetaType

CSItemRole = Literal["hidden", "header", "label", "option"]


class CSAlias(SettingsModel):
    title: str
    description: str | None = None


class CSItemModel(SettingsModel):
    role: CSItemRole | None = Field(None)
    aliases: dict[str, CSAlias] = Field(default_factory=dict)

    @classmethod
    def from_settings(cls, value: str, settings: dict[str, Any]) -> "CSItemModel":
        aliases = {}
        adef: dict[str, str] = settings.get("aliases", {})
        ddef: dict[str, str] = settings.get("description", {})

        for lang in adef:
            aliases[lang] = CSAlias(
                title=adef[lang],
                description=ddef.get(lang),
            )

        # If no alias is defined, use the value as an alias
        if not aliases:
            aliases["en"] = CSAlias(title=value, description="")

        return cls(role=settings.get("role"), aliases=aliases)


CSModel = dict[str, CSItemModel]


#
# System settings.
#


class BaseSystemSettings(SettingsModel):
    """Base system settings.

    Contains settings that are common for server and client.
    Not all settings are used by the client.
    """

    site_name: str = Field(
        "nebula",
        pattern=r"^[a-zA-Z0-9_]+$",
        title="Site name",
        description="A name used as the site (instance) identification",
    )

    ui_asset_create: bool = Field(
        True,
        title="Create assets in UI",
        description="Allow creating assets in the UI"
        "(when set to false, assets can only be created via API and watch folders)",
    )

    ui_asset_preview: bool = Field(
        True,
        title="Preview assets in UI",
        description="Allow previewing low-res proxies of assets in the UI",
    )

    ui_asset_upload: bool = Field(
        False,
        title="Upload assets in UI",
        description="Allow uploading asset media files in the UI "
        "(when set to false, assets can only be uploaded via API and watch folders)",
    )


class SystemSettings(BaseSystemSettings):
    """System settings.

    Expanded version of the base system settings.
    Contains settings that are used only by the server.
    """

    proxy_storage: int = Field(1, title="Proxy storage")
    proxy_path: str = Field(".nx/proxy/{id1000:04d}/{id}.mp4")
    worker_plugin_storage: int = Field(1)
    worker_plugin_path: str = Field(".nx/plugins")
    upload_storage: int | None = Field(None)
    upload_dir: str | None = Field(None)

    smtp_host: str | None = Field(None, title="SMTP host")
    smtp_port: int | None = Field(None, title="SMTP port")
    smtp_user: str | None = Field(None, title="SMTP user")
    smtp_password: str | None = Field(None, title="SMTP password")
    mail_from: str | None = Field(
        None,
        title="Mail from",
        description="Email address used as the sender",
        examples=["Nebula <noreply@example.com>"],
    )


class BaseListItemModel(SettingsModel):
    id: int = Field(..., title="ID", examples=[1])
    name: str = Field(..., title="Name", examples=["Name"])


#
# Action settings
#


class BaseActionSettings(SettingsModel):
    id: int = Field(...)
    name: str = Field(...)
    type: str = Field(...)


class ActionSettings(BaseActionSettings):
    settings: str = Field("<action/>")


#
# Service settings
#


class BaseServiceSettings(SettingsModel):
    id: int = Field(..., title="Service ID", examples=[1])
    name: str = Field(..., title="Service name", examples=["conv01"])
    type: str = Field(..., title="Service type", examples=["conv"])
    host: str = Field(..., title="Host", examples=["node01"])
    autostart: bool = Field(True, title="Autostart", examples=[True])
    loop_delay: int = Field(
        5, title="Loop delay", description="Seconds of sleep between runs"
    )
    state: ServiceState = Field(ServiceState.STOPPED)
    last_seen: int = Field(0, title="Last seen", examples=[1949155890])


class ServiceSettings(BaseServiceSettings):
    settings: str = Field("<service/>")
    pid: int = Field(0)


#
# Storage settings.
#


class BaseStorageSettings(BaseListItemModel):
    protocol: Annotated[
        Literal["samba", "local"],
        Field(
            title="Connection protocol",
            examples=["samba", "local"],
        ),
    ]

    path: Annotated[
        str,
        Field(
            title="Path",
            examples=["//server/share"],
        ),
    ]


class ExtendedStorageSettings(BaseStorageSettings):
    options: Annotated[
        dict[str, Any],
        Field(
            default_factory=dict,
            title="Connection options",
        ),
    ]


class StorageOverrideSettings(SettingsModel):
    hostname: Annotated[
        str,
        Field(
            title="Hostname",
            description=(
                "Hostname of the host for which the override applies"
                " (use __server__ for the server hosts)"
            ),
            examples=[
                "worker01",
                "__server__",
            ],
        ),
    ]

    enabled: Annotated[
        bool,
        Field(
            title="Enabled",
            description="Set to false to disale the storage access on the host",
        ),
    ] = True

    protocol: Annotated[
        Literal["samba", "local"] | None,
        Field(
            title="Connection protocol",
            examples=["samba", "local"],
        ),
    ] = None

    path: Annotated[
        str | None,
        Field(
            title="Path",
            examples=["//server/share"],
        ),
    ] = None

    options: Annotated[
        dict[str, Any],
        Field(
            default_factory=dict,
            title="Connection options",
        ),
    ]


class StorageSettings(ExtendedStorageSettings):
    overrides: Annotated[
        list[StorageOverrideSettings],
        Field(
            default_factory=list,
            title="Overrides",
            description="List of storage overrides for specific hosts",
        ),
    ]


#
# Folder settings
#


class FolderField(SettingsModel):
    name: str
    mode: str | None = None
    format: str | None = None
    order: str | None = None
    filter: str | None = None
    links: list[Any] = Field(default_factory=list)


class FolderLink(SettingsModel):
    name: str
    view: int
    source_key: str
    target_key: str


class FolderSettings(SettingsModel):
    id: int = Field(...)
    name: str = Field(...)
    color: str = Field(...)
    fields: list[FolderField] = Field(default_factory=list)
    links: list[FolderLink] = Field(default_factory=list)


class ViewSettings(SettingsModel):
    id: int = Field(...)
    name: str = Field(...)
    position: int = Field(...)
    folders: list[int] | None = Field(None)
    states: list[int] | None = Field(None)
    columns: list[str] | None = Field(None)
    conditions: list[str] | None = Field(None)
    separator: bool = Field(False)


DayStart = tuple[int, int]


class AcceptModel(SettingsModel):
    folders: list[int] | None = Field(
        None,
        title="Folders",
        description="List of folder IDs",
    )
    content_types: list[ContentType] | None = Field(
        title="Content types",
        description="List of content types that are accepted. "
        "None means all types are accepted.",
        default_factory=lambda: [ContentType.VIDEO],
    )
    media_types: list[MediaType] | None = Field(
        title="Media types",
        description="List of media types that are accepted. "
        "None means all types are accepted.",
        default_factory=lambda: [MediaType.FILE],
    )


class BasePlayoutChannelSettings(SettingsModel):
    id: int = Field(...)
    name: str = Field(...)
    fps: float = Field(25.0)
    plugins: list[str] = Field(default_factory=list)
    solvers: list[str] = Field(default_factory=list)
    day_start: DayStart = Field((7, 0))
    rundown_columns: list[str] = Field(default_factory=list)
    fields: list[FolderField] = Field(
        title="Fields",
        description="Metadata fields available for the channel events",
        default_factory=lambda: [
            FolderField(name="title"),
            FolderField(name="subtitle"),
            FolderField(name="description"),
            FolderField(name="color"),  # to distinguish events in the scheduler view
        ],
    )
    send_action: int | None = None
    scheduler_accepts: AcceptModel = Field(default_factory=AcceptModel)
    rundown_accepts: AcceptModel = Field(default_factory=AcceptModel)


class PlayoutChannelSettings(BasePlayoutChannelSettings):
    engine: str = Field(..., title="Playout engine")
    config: dict[str, Any] = Field(
        default_factory=dict,
        title="Engine configuration",
        description="Engine specific configuration",
    )
    playout_storage: int | None = None
    playout_dir: str | None = None
    playout_container: str | None = None
    allow_remote: bool = Field(False)
    controller_host: str
    controller_port: int


#
# Server settings
#


class ServerSettings(SettingsModel):
    installed: bool = True
    system: SystemSettings = Field(default_factory=SystemSettings)
    storages: list[StorageSettings] = Field(default_factory=list)
    folders: list[FolderSettings] = Field(default_factory=list)
    views: list[ViewSettings] = Field(default_factory=list)
    metatypes: dict[str, MetaType] = Field(default_factory=dict)
    cs: dict[str, CSModel] = Field(
        default_factory=dict,
        description="Key is a URN, value is CSModel `{value: CSItemModel}` dict",
    )
    playout_channels: list[PlayoutChannelSettings] = Field(default_factory=list)

    def get_folder(self, id_folder: int) -> FolderSettings | None:
        for item in self.folders:
            if item.id == id_folder:
                return item
        return None

    def get_view(self, id_view: int) -> ViewSettings | None:
        for item in self.views:
            if item.id == id_view:
                return item
        return None

    def get_storage(self, id_storage: int) -> StorageSettings | None:
        for item in self.storages:
            if item.id == id_storage:
                return item
        return None

    def get_playout_channel(self, id_channel: int) -> PlayoutChannelSettings | None:
        for item in self.playout_channels:
            if item.id == id_channel:
                return item
        return None
