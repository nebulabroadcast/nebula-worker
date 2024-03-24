import os
import threading
import time
from typing import TYPE_CHECKING, Any

import nebula
from nebula.helpers import bin_refresh

from ..base_controller import BaseController
from .amcp import CasparCG, CasparException
from .caspar_data import CasparOSCServer

if TYPE_CHECKING:
    from services.play import Service as PlayService


class CasparController(BaseController):
    time_unit = "s"
    service: "PlayService"

    def __init__(self, parent: "PlayService"):
        self.parent = parent

        self.caspar_host: str = parent.channel.config.get("caspar_host", "localhost")
        self.caspar_port: int = int(parent.channel.config.get("caspar_port", 5250))
        self.caspar_osc_port: int = int(
            parent.channel.config.get("caspar_osc_port", 5253)
        )
        self.caspar_channel: int = int(parent.channel.config.get("caspar_channel", 1))
        self.caspar_feed_layer: int = int(
            parent.channel.config.get("caspar_feed_layer", 10)
        )

        self.should_run = True
        self.current_item: nebula.Item | None = None
        self.current_fname: str | None = None
        self.cued_item: nebula.Item | None = None
        self.cued_fname: str | None = None
        self.cueing: str | bool = False
        self.cueing_time: float = 0
        self.cueing_item: nebula.Item | None = None
        self.stalled = False

        # To be updated based on CCG data
        self.channel_fps: float = self.fps
        self.paused: bool = False
        self.loop: bool = False
        self.pos: float = 0
        self.dur: float = 0

        try:
            self.connect()
        except Exception:
            nebula.log.error("Unable to connect CasparCG Server. Shutting down.")
            self.parent.shutdown()
            return

        self.caspar_data = CasparOSCServer(self.caspar_osc_port)
        self.lock = threading.Lock()
        self.work_thread = threading.Thread(target=self.work, args=())
        self.work_thread.start()

    def shutdown(self):
        nebula.log.info("Controller shutdown requested")
        self.should_run = False
        self.caspar_data.shutdown()

    def on_main(self):
        if time.time() - self.caspar_data.last_osc > 5:
            nebula.log.warning("Waiting for OSC")

    @property
    def id_channel(self) -> int:
        return self.parent.channel.id

    @property
    def request_time(self) -> float:
        return time.time()

    @property
    def fps(self) -> float:
        return self.parent.fps

    @property
    def position(self) -> float:
        """Time position (seconds) of the clip currently playing"""
        if self.current_item:
            return self.pos - self.current_item.mark_in()
        return self.pos

    @property
    def duration(self) -> float:
        """Duration (seconds) of the clip currently playing"""
        if self.parent.current_live:
            return 0
        return self.dur

    def connect(self) -> None:
        """Connect to a running CasparCG instance using AMCP protocol"""
        self.cmdc = CasparCG(self.caspar_host, self.caspar_port)
        self.cmdc.connect()

    def query(self, *args, **kwargs) -> str | None:
        """Send an AMCP query to the CasparCG server"""
        return self.cmdc.query(*args, **kwargs)

    def work(self):
        while self.should_run:
            try:
                self.main()
            except Exception:
                nebula.log.traceback()
            time.sleep(1 / self.fps)
        nebula.log.info("Controller work thread shutdown")

    def main(self):
        channel = self.caspar_data[self.caspar_channel]
        if not channel:
            return

        layer = channel[self.caspar_feed_layer]
        if not layer:
            return

        foreground = layer["foreground"]
        background = layer["background"]

        current_fname = os.path.splitext(foreground.name)[0]
        cued_fname = os.path.splitext(background.name)[0]
        pos = foreground.position
        dur = foreground.duration

        self.channel_fps = channel.fps
        self.paused = foreground.paused
        self.loop = foreground.loop

        # casparcg duration is of the complete clip when osc is used
        # stupid.
        if self.current_item is not None:
            if self.current_item.mark_out():
                dur = min(dur, self.current_item.mark_out())
            if self.current_item.mark_in():
                dur -= self.current_item.mark_in()

        self.pos = pos
        self.dur = dur

        #
        # Playlist advancing
        #

        advanced = False
        if self.parent.cued_live:
            if (
                (background.producer == "empty")
                and (foreground.producer != "empty")
                and not self.cueing
            ):
                self.current_item = self.cued_item
                self.current_fname = "LIVE"
                advanced = True
                self.cued_item = None
                self.parent.on_live_enter()

        else:
            if (not cued_fname) and (current_fname):
                if current_fname == self.cued_fname:
                    self.current_item = self.cued_item
                    self.current_fname = self.cued_fname
                    advanced = True
                self.cued_item = None

        if advanced and not self.cueing:
            nebula.log.debug(
                f"OnChange: current {self.current_item} cued {self.cued_item}"
            )
            try:
                self.parent.on_change()
            except Exception:
                nebula.log.traceback("Playout on_change failed")

        if self.current_item and (self.cued_item is None) and not self.cueing:
            self.cueing = True
            if not self.parent.cue_next():
                self.cueing = False

        if self.cueing:
            if cued_fname == self.cueing:
                self.cued_item = self.cueing_item
                nebula.log.success(
                    f"Cued {self.cued_item}. Current item {self.current_item}"
                )
                self.cueing_item = None
                self.cueing = False
            elif self.parent.cued_live:
                if background.producer != "empty":
                    nebula.log.success(f"Cued {self.cueing}")
                    self.cued_item = self.cueing_item
                    self.cueing_item = None
                    self.cueing = False

            else:
                # nebula.log.debug(f"Waiting for cue {self.cueing} (is {cued_fname})")
                if time.time() - self.cueing_time > 5 and self.current_item:
                    nebula.log.warning("Cueing again")
                    self.cueing = False
                    self.parent.cue_next()

        elif (
            not self.cueing
            and self.cued_item
            and cued_fname
            and cued_fname != self.cued_fname
            and not self.parent.cued_live
        ):
            nebula.log.error(
                f"Cue mismatch: IS: {cued_fname} SHOULDBE: {self.cued_fname}"
            )
            self.cued_item = None

        self.current_fname = current_fname
        self.cued_fname = cued_fname

        try:
            self.parent.on_progress()
        except Exception:
            nebula.log.traceback("Playout on_progress failed")

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
        _ = kwargs
        if layer is None:
            layer = self.caspar_feed_layer

        query_list = ["PLAY" if play else "LOADBG"]
        query_list.append(f"{self.caspar_channel}-{layer}")
        query_list.append(fname)

        if auto:
            query_list.append("AUTO")
        if loop:
            query_list.append("LOOP")
        if item.mark_in():
            query_list.append(f"SEEK {int(item.mark_in() * self.channel_fps)}")
        if item.mark_out():
            query_list.append(f"LENGTH {int(item.duration * self.channel_fps)}")

        query = " ".join(query_list)

        self.cueing = fname
        self.cueing_item = item
        self.cueing_time = time.time()

        try:
            self.query(query)
        except CasparException:
            self.cued_item = None
            self.cued_fname = None
            self.cueing = False
            self.cueing_item = None
            self.cueing_time = 0
            raise

        if play:
            self.cueing = False
            self.cueing_item = None
            self.cueing_time = 0
            self.current_item = item
            self.current_fname = fname

    def clear(self, layer: int | None = None) -> None:
        if layer is None:
            layer = self.caspar_feed_layer
        self.query(f"CLEAR {self.caspar_channel}-{layer}")

    def take(self, layer: int | None = None) -> None:
        if layer is None:
            layer = self.caspar_feed_layer
        try:
            self.query(f"PLAY {self.caspar_channel}-{layer}")
            if self.parent.current_live:
                self.parent.on_live_leave()
            self.stalled = False
            return
        except CasparException as e:
            raise CasparException(f"Take failed: {e}") from e

    def retake(self, layer: int | None = None) -> None:
        if layer is None:
            layer = self.caspar_feed_layer
        assert not self.parent.current_live, "Unable to retake live item"
        assert self.current_item, "Unable to retake. No current item"
        seekparams = "SEEK " + str(int(self.current_item.mark_in() * self.channel_fps))
        if self.current_item.mark_out():
            seekparams += " LENGTH " + str(
                int(
                    (self.current_item.mark_out() - self.current_item.mark_in())
                    * self.channel_fps
                )
            )
        try:
            query = (
                f"PLAY {self.caspar_channel}-{layer} {self.current_fname} {seekparams}"
            )
            self.query(query)
            self.stalled = False
            self.parent.cue_next()
        except CasparException as e:
            message = f"Take command failed: {e}"
            raise CasparException(message) from e

    def freeze(self, layer: int | None = None) -> None:
        if layer is None:
            layer = self.caspar_feed_layer
        assert not self.parent.current_live, "Unable to freeze live item"
        if self.paused:
            query = f"RESUME {self.caspar_channel}-{layer}"
        else:
            query = f"PAUSE {self.caspar_channel}-{layer}"
        try:
            self.query(query)
        except CasparException as e:
            raise CasparException(f"Freeze failed: {e}") from e

    def abort(self, layer: int | None = None) -> None:
        if layer is None:
            layer = self.caspar_feed_layer
        assert self.cued_item, "Unable to abort. No item is cued."
        query = f"LOAD {self.caspar_channel}-{layer} {self.cued_fname}"
        if self.cued_item.mark_in():
            seek = int(self.cued_item.mark_in() * self.channel_fps)
            query += f" SEEK {seek}"
        if self.cued_item.mark_out():
            length = int(
                (self.cued_item.mark_out() - self.cued_item.mark_in())
                * self.channel_fps
            )
            query += f" LENGTH {length}"
        self.query(query)

    def set(self, key: str, value: Any) -> None:
        if key == "loop":
            _loop = int(str(value) in ["1", "True", "true"])
            try:
                q = f"CALL {self.caspar_channel}-{self.caspar_feed_layer} LOOP {_loop}"
                self.query(q)
            except CasparException as e:
                raise CasparException(f"Unable to set loop: {e}") from e

            if self.current_item and bool(self.current_item["loop"] != bool(_loop)):
                self.current_item["loop"] = bool(_loop)
                self.current_item.save(notify=False)
                bin_refresh([self.current_item["id_bin"]], db=self.current_item.db)
        else:
            raise ValueError(f"Unknown set key: {key}")
