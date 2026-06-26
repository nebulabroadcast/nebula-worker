import nebula
from conti import Conti, ContiSource

from ..base_controller import BaseController


class NebulaContiSource(ContiSource):
    def __init__(self, parent: "Conti", path: str, **kwargs):
        super().__init__(parent, path, **kwargs)
        self.item = kwargs["item"]


class NebulaConti(Conti):
    parent: "ContiController"

    def append_next_item(self) -> None:
        self.parent.parent.cue_next()

    def progress_handler(self) -> None:
        self.parent._position = self.current.position if self.current else 0
        self.parent._duration = self.current.duration if self.current else 0
        self.parent.parent.on_progress()


class ContiController(BaseController):
    time_unit = "s"

    def __init__(self, parent):
        self.parent = parent
        self.cueing = None
        self.cued = None
        self._position = self._duration = 0
        settings = {
            "playlist_length": 2,
            "blocking": False,
            "outputs": self.parent.channel.config.get("conti_outputs", []),
        }
        settings.update(self.parent.channel.config.get("conti_settings", {}))
        self.conti = NebulaConti(None, logger=nebula.log, **settings)
        self.conti.parent = self

    @property
    def current_item(self):
        return self.conti.current.item if self.conti.current else None

    @current_item.setter
    def current_item(self, value: nebula.Item | None) -> None:
        _ = value
        nebula.log.warning("current_item is read-only")

    @property
    def current_fname(self):
        return self.conti.current.path if self.conti.current else None

    @current_fname.setter
    def current_fname(self, value: str | None) -> None:
        _ = value
        nebula.log.warning("current_fname is read-only")

    @property
    def cued_item(self):
        return self.cued.item if self.cued else None

    @cued_item.setter
    def cued_item(self, value: nebula.Item | None) -> None:
        _ = value
        nebula.log.warning("cued_item is read-only")

    @property
    def cued_fname(self):
        return self.cued.path if self.cued else None

    @cued_fname.setter
    def cued_fname(self, value: str | None) -> None:
        _ = value
        nebula.log.warning("cued_fname is read-only")

    @property
    def id_channel(self):
        return self.parent.channel.id

    @property
    def fps(self):
        return self.parent.fps

    @property
    def paused(self):
        return self.conti.paused

    @property
    def loop(self):
        # TODO: Not implemented in conti
        return False

    def set(self, prop, value):
        _ = prop, value
        return True

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

        kwargs["item"] = item
        kwargs["meta"] = item.asset.meta if item.asset else {}

        if kwargs.get("mark_in") is None:
            kwargs["mark_in"] = 0
        if kwargs.get("mark_out") is None:
            kwargs["mark_out"] = 0

        self.cued = NebulaContiSource(self.conti, fname, **kwargs)
        # TODO: add per-source filters here
        self.cued.open()
        self.cueing = None

        assert self.cued, "Failed to cue item"

        if len(self.conti.playlist) > 1:
            del self.conti.playlist[1:]
        self.conti.playlist.append(self.cued)

        if not self.conti.started:
            nebula.log.info("Starting Conti")
            self.conti.start()

        if kwargs.get("play", False):
            return self.take()
        nebula.log.info(f"Cued item {self.cued_item} ({fname})")

    def take(self, layer: int | None = None) -> None:
        _ = layer
        self.conti.take()

    def freeze(self, layer: int | None = None) -> None:
        _ = layer
        self.conti.freeze()

    def retake(self, layer: int | None = None) -> None:
        _ = layer
        pass

    def abort(self, layer: int | None = None) -> None:
        _ = layer
        self.conti.abort()

    def shutdown(self):
        self.conti.stop()

    @property
    def position(self) -> float:
        return self._position

    @property
    def duration(self) -> float | None:
        return self._duration
