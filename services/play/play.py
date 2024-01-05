import threading
import time
from http.server import HTTPServer

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import ObjectStatus, RunMode
from nebula.helpers import get_item_event, get_next_item
from nebula.response import NebulaResponse
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

    elif engine == "melt":
        from .melt.melt_controller import MeltController

        return MeltController(parent)

    raise ValueError(f"Unknown engine {engine}")


class Service(BaseService):
    def on_init(self):
        try:
            id_channel = int(self.settings.find("id_channel").text or 0)
            if not id_channel:
                raise ValueError
        except ValueError:
            nebula.log.error("No playout channel configured")
            self.shutdown(no_restart=True)
            return

        if channel := nebula.settings.get_playout_channel(id_channel):
            self.channel = channel
        else:
            nebula.log.error(f"Cant find playout channel {id_channel}")
            self.shutdown(no_restart=True)
            return

        self.fps = float(self.channel.fps)

        self.current_item: nebula.Item | None = None
        self.current_asset: nebula.Asset | None = None
        self.current_event: nebula.Event | None = None

        self.last_run = False
        self.last_info = 0
        self.current_live = False
        self.cued_live = False
        self.auto_event = 0

        self.status_key = f"playout_status/{self.channel.id}"

        self.plugins = PlayoutPlugins(self)
        try:
            self.controller = create_controller(self)
        except Exception as e:
            nebula.log.error(f"Unable to create controller: {e}")
            self.shutdown(no_restart=True)
            return

        port = self.channel.controller_port
        nebula.log.info(f"Using port {port} for the HTTP interface.")

        self.server = HTTPServer(("", port), PlayoutRequestHandler)
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

    def cue(self, **kwargs):
        db = kwargs.get("db", DB())

        if "item" in kwargs and isinstance(kwargs["item"], nebula.Item):
            item = kwargs["item"]
            del kwargs["item"]
        elif "id_item" in kwargs:
            item = nebula.Item(int(kwargs["id_item"]), db=db)
            item.asset
            del kwargs["id_item"]
        else:
            return NebulaResponse(400, "Unable to cue. No item specified")

        if not item:
            return NebulaResponse(404, f"Unable to cue. {item} does not exist")

        if item["item_role"] == "live":
            fname = self.channel.config.get("live_source")
            if fname is None:
                return NebulaResponse(400, "Live source is not configured")
            nebula.log.info("Next is item is live")
            response = self.controller.cue(fname, item, **kwargs)
            if response.is_success:
                self.cued_live = True
            return response

        if not item["id_asset"]:
            return NebulaResponse(400, f"Unable to cue virtual {item}")

        asset = item.asset
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
            return NebulaResponse(404, f"Unable to cue {state} playout file")

        kwargs["mark_in"] = item["mark_in"]
        kwargs["mark_out"] = item["mark_out"]

        if item["run_mode"] == 1:
            kwargs["auto"] = False
        else:
            kwargs["auto"] = True

        kwargs["loop"] = bool(item["loop"])

        self.cued_live = False
        return self.controller.cue(item=item, **kwargs)

    def cue_forward(self, **kwargs):
        cc = self.controller.cued_item
        if not cc:
            return NebulaResponse(204)
        db = DB()
        nc = get_next_item(cc.id, db=db, force="next")
        return self.cue(item=nc, db=db)

    def cue_backward(self, **kwargs):
        cc = self.controller.cued_item
        if not cc:
            return NebulaResponse(204)
        db = DB()
        nc = get_next_item(cc.id, db=db, force="prev")
        return self.cue(item=nc, db=db, level=5)

    def cue_next(self, **kwargs):
        nebula.log.info("Cueing the next item")
        # TODO: deprecate. controller should handle this
        self.controller.cueing = True
        item = kwargs.get("item", self.controller.current_item)
        level = kwargs.get("level", 0)
        db = kwargs.get("db", DB())
        play = kwargs.get("play", False)

        if not item:
            nebula.log.warning("Unable to cue next item. No current clip")
            return

        item_next = get_next_item(
            item.id, db=db, force_next_event=bool(self.auto_event)
        )

        if item_next["run_mode"] == 1:
            auto = False
        else:
            auto = True

        nebula.log.info(f"Auto-cueing {item_next}")
        result = self.cue(item=item_next, play=play, auto=auto)

        if result.is_error:
            if level > 5:
                nebula.log.error("Cue it yourself....")
                return False
            nebula.log.warning(
                f"Unable to cue {item_next} ({result.message}). Trying next."
            )
            item_next = self.cue_next(item=item_next, db=db, level=level + 1, play=play)
        return item_next

    def take(self, **kwargs):
        return self.controller.take(**kwargs)

    def freeze(self, **kwargs):
        return self.controller.freeze(**kwargs)

    def retake(self, **kwargs):
        return self.controller.retake(**kwargs)

    def abort(self, **kwargs):
        return self.controller.abort(**kwargs)

    def set(self, **kwargs):
        """Set a controller property.
        This is controller specific.
        Args:
            key (str): Name of the property
            value: Value to be set
        """
        key = kwargs.get("key", None)
        value = kwargs.get("value", None)
        if (key is None) or (value is None):
            return NebulaResponse(400)
        if hasattr(self.controller, "set"):
            return self.controller.set(key, value)
        return NebulaResponse(501)

    def stat(self, **kwargs):
        """Returns current status of the playback"""
        return NebulaResponse(200, data=self.playout_status)

    def plugin_list(self, **kwargs):
        result = []
        for plugin in self.plugins:
            if not plugin.manifest.slots:
                continue
            result.append(plugin.manifest.dict())
        return NebulaResponse(200, plugins=result)

    def plugin_exec(self, **kwargs):
        plugin_name = kwargs.get("name", None)
        action = kwargs.get("action", None)
        data = kwargs.get("data", None)

        if not (plugin_name and action):
            return NebulaResponse(400, "plugin or action not specified")

        nebula.log.debug(f"Executing {plugin_name}.{action}")
        try:
            plugin = self.plugins[plugin_name]
        except KeyError:
            nebula.log.traceback()
            return NebulaResponse(400, f"Plugin {plugin_name} not active")

        if plugin.on_command(action, data):
            return NebulaResponse(200)
        else:
            return NebulaResponse(500, "Playout plugin failed")

    #
    # Props
    #

    @property
    def playout_status(self):
        ctrl = self.controller
        if not ctrl:
            return {}
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

        if self.last_run:
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
            self.last_run = False

        for plugin in self.plugins:
            try:
                plugin.on_change()
            except Exception:
                nebula.log.traceback("Plugin on-change failed")

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
            self.auto_event = False
            return

        if self.auto_event == next_event.id:
            return

        run_mode = int(next_event["run_mode"]) or RunMode.RUN_AUTO

        if not run_mode:
            return

        elif not next_event.bin.items:
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
        last_item = nebula.Item(last_id_item, db=db)
        last_item.asset

        self.controller.current_item = last_item
        self.controller.cued_item = None
        self.controller.cued_fname = None

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
