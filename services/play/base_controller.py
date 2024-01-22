from typing import TYPE_CHECKING, Any

import nebula

if TYPE_CHECKING:
    from .play import Service as PlayService


class BaseController:
    parent: "PlayService"
    time_unit: str = "s"
    current_item: nebula.Item | None = None

    def __init__(self, parent: "PlayService"):
        self.parent = parent

    def shutdown(self) -> None:
        pass

    def cue(
        self,
        fname: str,
        item: nebula.Item,
        layer: int | None = None,
        play: bool = False,
        auto: bool = True,
        loop: bool = False,
        **kwargs,
    ) -> None:
        _ = fname, item, layer, play, auto, loop, kwargs
        nebula.log.warning("cue() not implemented for {self.__class__.__name__}")

    def clear(self, layer: int | None = None) -> None:
        _ = layer
        nebula.log.warning("clear() not implemented for {self.__class__.__name__}")

    def take(self, layer: int | None = None) -> None:
        _ = layer
        nebula.log.warning("take() not implemented for {self.__class__.__name__}")

    def retake(self, layer: int | None = None) -> None:
        _ = layer
        nebula.log.warning("retake() not implemented for {self.__class__.__name__}")

    def freeze(self, layer: int | None = None) -> None:
        _ = layer
        nebula.log.warning("freeze() not implemented for {self.__class__.__name__}")

    def abort(self, layer: int | None = None) -> None:
        _ = layer
        nebula.log.warning("abort() not implemented for {self.__class__.__name__}")

    def set(self, key: str, value: Any) -> None:
        _ = key, value
        nebula.log.warning("set() not implemented for {self.__class__.__name__}")
