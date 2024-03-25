import time

from conti import Conti, ContiSource

import nebula

from ..base_controller import BaseController


class NebulaContiSource(ContiSource):
    def __init__(self, parent, path, **kwargs):
        super().__init__(parent, path, **kwargs)
        self.item = kwargs["item"]


class NebulaConti(Conti):
    def append_next_item(self):
        self.parent.parent.cue_next()

    def progress_handler(self):
        self.parent.position = self.current.position
        self.parent.duration = self.current.duration
        self.parent.request_time = time.time()
        self.parent.parent.on_progress()


class ContiController(BaseController):
    time_unit = "s"

    def __init__(self, parent):
        self.parent = parent
        self.cueing = None
        self.cued = None
        self.request_time = time.time()
        self.position = self.duration = 0
        settings = {
            "playlist_length": 2,
            "blocking": False,
            "outputs": self.parent.channel.config.get("conti_outputs", []),
        }
        settings.update(self.parent.channel.config.get("conti_settings", {}))
        self.conti = NebulaConti(None, **settings)
        self.conti.parent = self

    @property
    def current_item(self):
        return self.conti.current.item if self.conti.current else None

    @property
    def current_fname(self):
        return self.conti.current.path if self.conti.current else None

    @property
    def cued_item(self):
        return self.cued.item if self.cued else None

    @property
    def cued_fname(self):
        return self.cued.path if self.cued else None

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

    def cue(self, item, full_path, **kwargs):
        kwargs["item"] = item
        kwargs["meta"] = item.asset.meta

        if kwargs.get("mark_in") is None:
            kwargs["mark_in"] = 0
        if kwargs.get("mark_out") is None:
            kwargs["mark_out"] = 0

        self.cued = NebulaContiSource(self.conti, full_path, **kwargs)
        # TODO: add per-source filters here
        self.cued.open()
        self.cueing = False

        assert self.cued, "Failed to cue item"

        if len(self.conti.playlist) > 1:
            del self.conti.playlist[1:]
        self.conti.playlist.append(self.cued)

        if not self.conti.started:
            nebula.log.info("Starting Conti")
            self.conti.start()

        if kwargs.get("play", False):
            return self.take()
        nebula.log.info(f"Cued item {self.cued_item} ({full_path})")

    def take(self, **kwargs):
        _ = kwargs
        self.conti.take()

    def freeze(self, **kwargs):
        _ = kwargs
        self.conti.freeze()

    def retake(self, **kwargs):
        _ = kwargs

    def abort(self, **kwargs):
        _ = kwargs
        self.conti.abort()

    def shutdown(self):
        self.conti.stop()
