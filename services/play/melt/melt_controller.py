import os
import threading
import time

from typing import Any

import nebula
from nebula.response import NebulaResponse

try:
    import mlt

    has_mlt = True
except (ImportError, ModuleNotFoundError):
    nebula.log.error("MLT is not available")
    has_mlt = False


class MeltSource:
    def __init__(self, item: nebula.Item, path: str):
        self.item = item
        self.path = path


class MeltController:
    time_unit = "s"

    def __init__(self, parent):
        if not has_mlt:
            raise Exception("MLT is not available")

        mlt.mlt_log_set_level(20)
        mlt.Factory().init()

        config = {
            "profile": "atsc_1080p_25",
            "output_path": "/tmp/nebula",
        }

        self.profile = mlt.Profile(config["profile"])
        self.consumer = mlt.Consumer(self.profile, f"avformat:{config['output_path']}")
        self.tractor = mlt.Tractor(self.profile)

        self.playlist = mlt.Playlist(self.profile)
        self.tractor.set_track(self.playlist, 0)
        self.consumer.connect(self.tractor)

        self.parent = parent
        self.cueing = False
        self.cued: MeltSource | None = None
        self.request_time = time.time()
        self.position = self.duration = 0

    @property
    def current_item(self) -> nebula.Item | None:
        return None  # TODO

    @property
    def current_fname(self) -> str | None:
        return None

    @property
    def cued_item(self) -> nebula.Item | None:
        return self.cued.item if self.cued else None

    @property
    def cued_fname(self) -> str | None:
        return self.cued.path if self.cued else None

    @property
    def id_channel(self):
        return self.parent.channel.id

    @property
    def fps(self):
        return self.parent.fps

    @property
    def paused(self) -> bool:
        return False  # TODO

    @property
    def loop(self):
        # TODO: Not implemented in conti
        return False

    def set(self, prop: str, value: Any):
        _ = prop, value
        return True

    def cue(self, item: nebula.Item, full_path: str, **kwargs):
        kwargs["item"] = item
        kwargs["meta"] = item.asset.meta

        if kwargs.get("mark_in") is None:
            kwargs["mark_in"] = 0
        if kwargs.get("mark_out") is None:
            kwargs["mark_out"] = 0

        self.cued = MeltSource(item, full_path, **kwargs)
        self.cueing = False

        if not self.cued:
            return NebulaResponse(500)

        if len(self.conti.playlist) > 1:
            del self.conti.playlist[1:]
        self.conti.playlist.append(self.cued)

        if not self.conti.started:
            nebula.log.info("Starting Conti")
            self.conti.start()

        if kwargs.get("play", False):
            return self.take()
        message = f"Cued item {self.cued_item} ({full_path})"
        return NebulaResponse(200, message)

    def take(self, **kwargs):
        return NebulaResponse(200)

    def freeze(self, **kwargs):
        return NebulaResponse(200)

    def retake(self, **kwargs):
        return NebulaResponse(200)

    def abort(self, **kwargs):
        pass
        return NebulaResponse(200)

    def shutdown(self):
        pass

    # From meltdown

    def setup_consumer(self):
        for key, value in config.format.items():
            self.consumer.set(key, value)

    def start(self):
        """Start the producer"""
        self.setup_consumer()
        self.should_run = True
        self.thread = threading.Thread(target=self.main_loop)
        self.thread.start()

    def stop(self):
        """Stop the producer (shutdown the playback)"""
        self.consumer.stop()
        self.should_run = False

    def main(self):
        # If we advanced in the playlist, remove aired clip

        # while self.playlist.current_clip() > 1:
        #    logging.info("Removing tail")
        #    self.playlist.remove(0)

        # If there is no next clip in the playlist, add one
        if self.playlist.get_clip(self.playlist.current_clip() + 1) is None:
            nebula.log.info(f"Current clip: {self.playlist.current_clip()}")
            path = self.playlist_getter()
            nebula.log.info(f"Adding new clip to playlist: {os.path.basename(path)}")
            self.playlist.append(mlt.Producer(self.profile, path))

        if self.consumer.is_stopped():
            nebula.log.info("Starting consumer")
            self.consumer.start()
