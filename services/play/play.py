import threading
import time
from http.server import HTTPServer
from typing import Any

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import ObjectStatus, RunMode
from nebula.helpers import get_item_event, get_next_item
from services.play.plugins import PlayoutPlugins
from services.play.request_handler import PlayoutRequestHandler

DEFAULT_STATUS = {
    "status": ObjectStatus.OFFLINE,
}


def create_controller(parent):
    engine = parent.channel.engine
    if engine == "vlc":
        from .vlc.vlc_controller import VlcController

        return VlcController(parent)
    elif engine == "conti":
        from .conti.conti_controller import ContiController

        return ContiController(parent)
    elif engine == "casparcg":
        from .casparcg.caspar_controller import CasparController

        return CasparController(parent)


class PlayoutHTTPServer(HTTPServer):
    service: "Service"
    methods: dict[str, Any]


class Service(BaseService):
    current_item: nebula.Item | None = None
    current_asset: nebula.Asset | None = None
    current_event: nebula.Event | None = None

    def on_init(self):
        channel_tag = self.settings.find("id_channel")
        assert channel_tag.text, "No channel specified"  # type: ignore
        id_channel = int(channel_tag.text)  # type: ignore

        channel = nebula.settings.get_playout_channel(id_channel)

        if channel is None:
            nebula.log.error("No playout channel configured")
            self.shutdown(no_restart=True)
            return

        self.channel = channel
        assert self.channel.controller_port, "No controller port configured"

        self.fps = float(self.channel.fps)

        self.last_run: int | None = None
        self.last_info: float = 0
        self.current_live: bool = False
        self.cued_live: bool = False
        self.auto_event: int | None = None

        self.status_key = f"playout_status/{self.channel.id}"

        self.plugins = PlayoutPlugins(self)
        self.controller = create_controller(self)
        if not self.controller:
            nebula.log.error("Invalid controller specified")
            self.shutdown(no_restart=True)
            return

        port = int(self.channel.controller_port)
        nebula.log.info(f"Using port {port} for the HTTP interface.")

        self.server = PlayoutHTTPServer(("", port), PlayoutRequestHandler)
        self.server.service = self
        self.server.methods = {
            "take": self.take,
            "cue": self.cue,
            "cue_forward": self.cue_forward,
            "cue_backward": self.cue_backward,
            "freeze": self.freeze,
            "set": self.set,
            "retake": self.retake,
            "abort": self.abort,
            "stat": self.stat,
            "plugin_list": self.plugin_list,
            "plugin_exec": self.plugin_exec,
            "recover": self.channel_recover,
        }
        self.server_thread = threading.Thread(
            target=self.server.serve_forever, args=(), daemon=True
        )
        self.server_thread.start()
        self.plugins.load()
        self.on_progress()
        # self.channel_recover()

    def on_shutdown(self):
        if not hasattr(self, "controller"):
            return
        if self.controller and hasattr(self.controller, "shutdown"):
            self.controller.shutdown()

    #
    # API Commands
    #

    def cue(self, **kwargs) -> None:
        db = kwargs.get("db", DB())
        assert self.channel, f"Unable to cue. Channel {self.channel.id} not found"
        assert self.controller, "Unable to cue. Controller not found"

        if "item" in kwargs and isinstance(kwargs["item"], nebula.Item):
            item = kwargs["item"]
            del kwargs["item"]
        elif "id_item" in kwargs:
            item = nebula.Item(int(kwargs["id_item"]), db=db)
            _ = item.asset
            del kwargs["id_item"]
        else:
            raise AssertionError("Unable to cue. No item specified")

        assert item, f"Unable to cue. Item {item} not found"

        if item["item_role"] == "live":
            fname = self.channel.config.get("live_source")
            nebula.log.info("Next is item is live")
            assert fname is not None, "Live source is not configured"
            try:
                response = self.controller.cue(fname, item, **kwargs)
            except Exception as e:
                nebula.log.error(f"Unable to cue live source: {e}")
                raise e
            self.cued_live = True
            return response

        assert item["id_asset"], f"Unable to cue virtual {item}"

        asset = item.asset
        assert asset, f"Unable to cue. Asset {item['id_asset']} not found"
        playout_status = asset.get(self.status_key, DEFAULT_STATUS)["status"]

        kwargs["fname"] = kwargs["full_path"] = None
        if playout_status in [
            ObjectStatus.ONLINE,
            ObjectStatus.CREATING,
            ObjectStatus.UNKNOWN,
        ]:
            kwargs["fname"] = asset.get_playout_name(self.channel.id)
            kwargs["full_path"] = asset.get_playout_full_path(self.channel.id)

        if (
            not kwargs["full_path"]
            and self.channel.allow_remote
            and asset["status"] in (ObjectStatus.ONLINE, ObjectStatus.CREATING)
        ):
            kwargs["fname"] = kwargs["full_path"] = asset.file_path
            kwargs["remote"] = True

        if not kwargs["full_path"]:
            state = ObjectStatus(playout_status).name
            raise AssertionError(f"Unable to cue {state} playout file")

        kwargs["mark_in"] = item["mark_in"]
        kwargs["mark_out"] = item["mark_out"]

        if item["run_mode"] == 1:
            kwargs["auto"] = False
        else:
            kwargs["auto"] = True

        kwargs["loop"] = bool(item["loop"])

        self.cued_live = False
        return self.controller.cue(item=item, **kwargs)

    def cue_forward(self, **kwargs) -> None:
        _ = kwargs
        assert self.controller, "Unable to cue. Controller not found"
        cc = self.controller.cued_item
        assert cc, "Unable to cue cue_forward. No cued item"
        db = DB()
        nc = get_next_item(cc, db=db, force="next")
        assert nc, "Unable to cue. No next item"
        return self.cue(item=nc, db=db)

    def cue_backward(self, **kwargs) -> None:
        _ = kwargs
        assert self.controller, "Unable to cue. Controller not found"
        cc = self.controller.cued_item
        assert cc, "Unable to cue cue_backward. No cued item"
        db = DB()
        nc = get_next_item(cc, db=db, force="prev")
        assert nc, "Unable to cue. No previous item"
        return self.cue(item=nc, db=db, level=5)

    def cue_next(
        self,
        item: nebula.Item | None = None,
        db: DB | None = None,
        level: int = 0,
        play: bool = False,
    ) -> nebula.Item | None:
        nebula.log.trace("Cueing the next item")
        assert self.controller, "Unable to cue. Controller not found"

        # TODO: deprecate. controller should handle this
        self.controller.cueing = True

        if item is None:
            item = self.controller.current_item

        if db is None:
            db = DB()

        if not item:
            nebula.log.warning("Unable to cue next item. No current clip")
            return None

        item_next = get_next_item(
            item,
            db=db,
            force_next_event=bool(self.auto_event),
        )

        if not item_next:
            nebula.log.warning("Unable to cue next item. No next clip")
            return None

        if item_next["run_mode"] == 1:
            auto = False
        else:
            auto = True

        nebula.log.info(f"Auto-cueing {item_next}")
        try:
            self.cue(item=item_next, play=play, auto=auto)
        except Exception as e:
            if level > 5:
                nebula.log.error("Cue it yourself....")
                return None
            nebula.log.warning(f"Unable to cue {item_next}: {e}. Trying next.")
            item_next = self.cue_next(item=item_next, db=db, level=level + 1, play=play)
        return item_next

    def take(self, **kwargs) -> None:
        assert self.controller, "Unable to take. Controller not found"
        self.controller.take(**kwargs)

    def freeze(self, **kwargs) -> None:
        assert self.controller, "Unable to freeze. Controller not found"
        self.controller.freeze(**kwargs)

    def retake(self, **kwargs) -> None:
        assert self.controller, "Unable to retake. Controller not found"
        self.controller.retake(**kwargs)

    def abort(self, **kwargs) -> None:
        assert self.controller, "Unable to abort. Controller not found"
        self.controller.abort(**kwargs)

    def set(self, **kwargs):
        """Set a controller property.
        This is controller specific.
        Args:
            key (str): Name of the property
            value: Value to be set
        """
        assert self.controller, "Unable to set. Controller not found"
        key = kwargs.get("key", None)
        value = kwargs.get("value", None)
        assert key, "Unable to set. Key not specified"
        assert value, "Unable to set. Value not specified"
        assert hasattr(self.controller, "set"), "Unable to set. Method not found"
        return self.controller.set(key, value)

    def stat(self, **kwargs) -> dict[str, Any]:
        """Returns current status of the playback"""
        _ = kwargs
        return {"data": self.playout_status}

    def plugin_list(self, **kwargs) -> dict[str, Any]:
        _ = kwargs
        result = []
        for plugin in self.plugins:
            if not plugin.manifest.slots:
                continue
            result.append(plugin.manifest.dict())
        return {"plugins": result}

    def plugin_exec(self, **kwargs):
        plugin_name = kwargs.get("name", None)
        action = kwargs.get("action", None)
        data = kwargs.get("data", None)

        assert plugin_name, "Plugin name not specified"
        assert action, "Plugin action not specified"

        nebula.log.debug(f"Executing {plugin_name}.{action}")
        plugin = self.plugins[plugin_name]
        assert plugin.on_command(action, data), "Plugin call failed"

    #
    # Props
    #

    @property
    def playout_status(self):
        assert self.channel
        assert self.controller

        ctrl = self.controller
        stat = {
            "id_channel": self.channel.id,
            "fps": float(self.fps),
            "current_fname": ctrl.current_fname,
            "cued_fname": ctrl.cued_fname,
            "request_time": ctrl.request_time,
            "paused": ctrl.paused,
            "position": ctrl.position,
            "duration": ctrl.duration,
            "current_item": ctrl.current_item and ctrl.current_item.id,
            "cued_item": ctrl.cued_item and ctrl.cued_item.id,
            "current_title": ctrl.current_item and ctrl.current_item["title"],
            "cued_title": ctrl.cued_item and ctrl.cued_item["title"],
            "loop": ctrl.loop,
            "cueing": ctrl.cueing,
            "id_event": self.current_event.id if self.current_event else None,
        }
        return stat

    #
    # Events
    #

    def on_progress(self):
        if not self.controller:
            # fix the race condition, when on_progress is created,
            # but not yet added to the service
            return
        if time.time() - self.last_info > 0.3:
            nebula.msg("playout_status", **self.playout_status)
            self.last_info = time.time()

        for plugin in self.plugins:
            plugin.main()

    def on_change(self):
        db = DB()

        self.current_item = self.controller.current_item
        if self.current_item is None:
            self.current_asset = None
            self.current_event = None
            return

        self.current_asset = self.current_item.asset or None
        self.current_event = self.current_item.event or None

        nebula.log.info(f"Advanced to {self.current_item}")

        if self.last_run is not None:
            db.query(
                """
                UPDATE asrun SET stop = %s
                WHERE id = %s""",
                [int(time.time()), self.last_run],
            )
            db.commit()

        if self.current_item:
            db.query(
                """
                INSERT INTO asrun (id_channel, id_item, start)
                VALUES (%s, %s, %s)
                """,
                [self.channel.id, self.current_item.id, time.time()],
            )
            self.last_run = db.lastid()
            db.commit()
        else:
            self.last_run = None

        for plugin in self.plugins:
            try:
                plugin.on_change()
            except Exception:
                nebula.log.error("Plugin on-change: {e}")

    def on_live_enter(self):
        nebula.log.success("Entering a live event")
        self.current_live = True
        self.cued_live = False

    def on_live_leave(self):
        nebula.log.success("Leaving a live event")
        self.current_live = False

    def on_main(self):
        """
        This method checks if the following event
        should start automatically at given time.
        It does not handle AUTO playlist advancing
        """

        if not hasattr(self, "controller"):
            return

        if hasattr(self.controller, "on_main"):
            self.controller.on_main()

        current_item = self.controller.current_item  # YES. CURRENT
        if not current_item:
            return

        db = DB()

        current_event = get_item_event(current_item.id, db=db)

        if not current_event:
            nebula.log.warning("Unable to fetch the current event")
            return

        db.query(
            """
            SELECT DISTINCT(e.id), e.meta, e.start FROM events AS e, items AS i
                WHERE e.id_channel = %s
                AND e.start > %s
                AND e.start <= %s
                AND i.id_bin = e.id_magic
            ORDER BY e.start ASC LIMIT 1
            """,
            [self.channel.id, current_event["start"], time.time()],
        )

        try:
            next_event = nebula.Event(meta=db.fetchall()[0][1], db=db)
        except IndexError:
            self.auto_event = None
            return

        if self.auto_event == next_event.id:
            return

        run_mode = int(next_event["run_mode"]) or RunMode.RUN_AUTO

        if not run_mode:
            return

        assert next_event.bin, "Next event has no bin loaded"
        assert current_event.bin, "Current event has no bin loaded"

        if not next_event.bin.items:
            return

        elif run_mode == RunMode.RUN_MANUAL:
            pass  # ?????

        elif run_mode == RunMode.RUN_SOFT:
            nebula.log.info("Soft cue", next_event)
            # if current item is live, take next block/lead out automatically
            play = self.current_live
            for i, r in enumerate(current_event.bin.items):
                if r["item_role"] == "lead_out":
                    try:
                        self.cue(
                            id_channel=self.channel.id,
                            id_item=current_event.bin.items[i + 1].id,
                            db=db,
                            play=play,
                        )
                        self.auto_event = next_event.id
                        break
                    except IndexError:
                        pass
            else:
                try:
                    id_item = next_event.bin.items[0].id
                except KeyError:
                    id_item = 0
                if not self.controller.cued_item:
                    return
                if id_item != self.controller.cued_item.id:
                    self.cue(id_channel=self.channel.id, id_item=id_item, db=db)
                    self.auto_event = next_event.id
                return

        elif run_mode == RunMode.RUN_HARD:
            nebula.log.info("Hard cue", next_event)
            id_item = next_event.bin.items[0].id
            self.cue(id_channel=self.channel.id, id_item=id_item, play=True, db=db)
            self.auto_event = next_event.id
            return

    def channel_recover(self):
        nebula.log.warning("Performing recovery")

        assert self.channel
        assert self.controller

        db = DB()
        db.query(
            """
            SELECT id_item, start FROM asrun
            WHERE id_channel = %s ORDER BY id DESC LIMIT 1
            """,
            [self.channel.id],
        )
        try:
            last_id_item, last_start = db.fetchall()[0]
        except IndexError:
            nebula.log.error("Unable to perform recovery.")
            return

        last_item = nebula.Item(last_id_item, db=db)
        _ = last_item.asset

        self.current_item = last_item

        if last_start + last_item.duration <= time.time():
            nebula.log.info(f"Last {last_item} has been broadcasted.")
            new_item = self.cue_next(item=last_item, db=db, play=True)
        else:
            nebula.log.info(f"Last {last_item} has not been fully broadcasted.")
            new_item = self.cue_next(item=last_item, db=db)

        if not new_item:
            nebula.log.error("Recovery failed. Unable to cue")
            return

        self.on_change()
