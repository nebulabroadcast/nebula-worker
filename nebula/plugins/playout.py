import os
import threading
from typing import TYPE_CHECKING, Any, Callable, Literal

from pydantic import BaseModel, Field

from nebula.log import log
from nebula.storages import storages

if TYPE_CHECKING:
    from nebula.settings.models import PlayoutChannelSettings
    from services.play import Service as PlayService


class PlayoutPluginSlotOption(BaseModel):
    value: str
    title: str | None = None


class PlayoutPluginSlot(BaseModel):
    type: Literal["action", "text", "number", "select"] = Field(...)
    name: str = Field(...)
    options: list[PlayoutPluginSlotOption] = Field(default_factory=list)
    value: Any = None

    @property
    def title(self):
        return self.name.capitalize()


class PlayoutPluginManifest(BaseModel):
    name: str
    title: str
    slots: list[PlayoutPluginSlot] | None = None


class PlayoutPlugin:
    name: str
    title: str | None = None
    id_layer: int = 0
    slots: list[PlayoutPluginSlot] = []
    tasks: list[Callable] = []
    busy: bool = False

    def __init__(self, service: "PlayService"):
        self.service: "PlayService" = service
        self.busy: bool = True
        if self.channel.playout_storage and self.channel.playout_dir:
            self.playout_dir = os.path.join(
                storages[self.channel.playout_storage].local_path,
                self.channel.playout_dir,
            )

        self.on_init()
        self.busy: bool = False

    def __str__(self):
        return f"playout plugin '{self.title}'"

    @property
    def manifest(self) -> PlayoutPluginManifest:
        return PlayoutPluginManifest(
            name=self.name,
            title=self.title or self.name.capitalize(),
            slots=self.slots,
        )

    @property
    def id_channel(self) -> int:
        return self.service.channel.id

    @property
    def channel(self) -> "PlayoutChannelSettings":
        return self.service.channel

    @property
    def current_asset(self):
        return self.service.current_asset

    @property
    def current_item(self):
        return self.service.current_item

    @property
    def position(self):
        return self.service.controller.position

    @property
    def duration(self):
        return self.service.controller.duration

    def layer(self, id_layer: int | None = None) -> str:
        if id_layer is None:
            id_layer = self.id_layer
        if not hasattr(self.service.controller, "caspar_channel"):
            return ""
        return f"{self.service.controller.caspar_channel}-{id_layer}"

    def query(self, query, **kwargs):
        try:
            return self.service.controller.query(query, **kwargs)
        except Exception as e:
            log.error(f"Plugin '{self.name}': {e}")

    def main(self):
        if not self.busy:
            self.busy = True
            thread = threading.Thread(target=self.main_thread, args=())
            thread.start()
        else:
            return False

    def main_thread(self):
        try:
            self.on_main()
        except Exception:
            log.traceback()
        self.busy = False

    def on_main(self):
        if not self.tasks:
            return
        if self.tasks[0]():
            del self.tasks[0]
            return

    def on_init(self):
        pass

    def on_change(self):
        pass

    def on_command(self, action: str, data: Any) -> bool:
        return True
