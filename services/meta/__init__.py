import os
import time
from typing import Literal

from nxtools import FileObject

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import ContentType, MediaType, ObjectStatus
from nebula.objects import Asset
from nebula.settings import settings
from nebula.storages import storages

from .ffprobe import ffprobe_asset


class Service(BaseService):
    restart_on_update: Literal["all"] | list[int] | None

    def on_init(self):
        self.conds = []
        rou = self.settings.attrib.get("restart_on_update", "all")
        if rou.lower() == "all":
            self.restart_on_update = "all"
        elif all(k.strip().isdigit() for k in rou.split(",")):
            self.restart_on_update = [int(k.strip()) for k in rou.split(",")]
        else:
            self.restart_on_update = None
        nebula.log.debug(
            "Following actions will be restarted on source update:",
            self.restart_on_update,
        )

        for cond in self.settings.findall("cond"):
            if cond is None:
                continue
            if not cond.text:
                continue
            x = eval(f"lambda asset: {cond.text}")
            self.conds.append(x)

    def on_main(self):
        self.mounted_storages = []
        for id_storage in storages:
            storage_path = storages[id_storage].local_path
            if os.path.exists(storage_path) and len(os.listdir(storage_path)) != 0:
                self.mounted_storages.append(id_storage)

        db = DB()
        # do not scan trashed and archived files
        db.query(
            """
            SELECT meta FROM assets
            WHERE media_type=%s
            AND status NOT IN (3, 4)
            """,
            [MediaType.FILE],
        )
        i = 0
        for (meta,) in db.fetchall():
            asset = Asset(meta=meta, db=db)
            self.process(asset)
            i += 1

    def process(self, asset):
        for cond in self.conds:
            if not cond(asset):
                return

        asset_file = FileObject(asset.file_path)
        id_storage = asset["id_storage"]
        if not id_storage:
            return
        if (
            storages[id_storage].protocol != "local"
            and id_storage not in self.mounted_storages
        ):
            nebula.log.warning(
                f"Skipping unmounted storage {asset['id_storage']} of {asset}"
            )
            return

        try:
            file_exists = asset_file.is_reg
        except OSError:
            file_exists = False

        if not file_exists:
            if asset["status"] in [
                ObjectStatus.ONLINE,
                ObjectStatus.RESET,
                ObjectStatus.CREATING,
            ]:
                nebula.log.warning(f"{asset}: Turning offline")
                asset["status"] = ObjectStatus.OFFLINE
                asset.save()
            return

        fmtime = int(asset_file.mtime)
        fsize = int(asset_file.size)

        if fsize == 0:
            if asset["status"] not in [ObjectStatus.OFFLINE, ObjectStatus.RETRIEVING]:
                nebula.log.warning(f"{asset}: Turning offline (empty file)")
                asset["status"] = ObjectStatus.OFFLINE
                asset.save()
            return

        if fmtime != asset["file/mtime"] or asset["status"] in [
            ObjectStatus.RESET,
            ObjectStatus.RETRIEVING,
        ]:
            try:
                f = asset_file.open("rb")
            except Exception:
                nebula.log.debug(f"{asset} is not readable (transfer in progress?)")
                return
            else:
                f.seek(0, 2)
                fsize = f.tell()
                f.close()

            if asset["status"] == ObjectStatus.RESET:
                asset.load_sidecar_metadata()

            # Filesize must be changed to update metadata automatically.

            if fsize == asset["file/size"] and asset["status"] not in [
                ObjectStatus.RESET,
                ObjectStatus.RETRIEVING,
            ]:
                nebula.log.debug(
                    f"{asset}: File mtime has been changed. Updating metadata."
                )
                asset["file/mtime"] = fmtime
                asset.save(set_mtime=False, notify=False)
            elif fsize != asset["file/size"] or asset["status"] in [
                ObjectStatus.RESET,
                ObjectStatus.RETRIEVING,
            ]:
                if asset["status"] in [ObjectStatus.RESET, ObjectStatus.RETRIEVING]:
                    nebula.log.info(f"{asset}: Reset requested. Updating metadata.")
                else:
                    nebula.log.info(
                        f"{asset}: File has been changed. Updating metadata."
                    )

                keys = list(asset.meta.keys())
                for key in keys:
                    if key in settings.metatypes:
                        if settings.metatypes[key].ns in ("f", "q"):
                            del asset.meta[key]

                asset["file/size"] = fsize
                asset["file/mtime"] = fmtime
                asset["file/ctime"] = int(asset_file.ctime)
                asset.save()

                if asset["content_type"] in (
                    ContentType.VIDEO,
                    ContentType.AUDIO,
                    ContentType.IMAGE,
                ):
                    nebula.log.debug(f"{asset}: probing asset")
                    result = ffprobe_asset(asset)
                else:
                    result = None

                if result:
                    asset = result
                elif asset["status"] != ObjectStatus.CREATING:
                    asset["status"] = ObjectStatus.CREATING
                    return
                else:
                    return

                if asset["status"] == ObjectStatus.RESET:
                    asset["status"] = ObjectStatus.ONLINE
                    nebula.log.info(f"{asset}: Metadata reset completed")
                else:
                    asset["status"] = ObjectStatus.CREATING
                asset.save()

        if (
            asset["status"] == ObjectStatus.CREATING
            and asset["mtime"] + 15 > time.time()
        ):
            nebula.log.debug(f"{asset}: Waiting for completion assurance")
            asset.save(set_mtime=False, notify=False)

        elif asset["status"] in (ObjectStatus.CREATING, ObjectStatus.OFFLINE):
            nebula.log.success(f"{asset}: Turning online")

            # Do not restart actions if file just reappeared
            restart_actions = asset["status"] == ObjectStatus.CREATING

            asset["status"] = ObjectStatus.ONLINE
            asset["qc/state"] = 0
            asset.save()

            if self.restart_on_update and restart_actions:
                if isinstance(self.restart_on_update, list):
                    rou = [str(x) for x in self.restart_on_update]
                    actions_to_restart = ",".join(rou)
                    action_cond = f"AND id_action in ({actions_to_restart})"
                else:
                    action_cond = ""
                db = DB()
                db.query(
                    f"""
                    UPDATE jobs SET
                        status=5,
                        retries=0,
                        progress=0,
                        creation_time=%s,
                        start_time=NULL,
                        end_time=NULL,
                        id_service=NULL,
                        message='Restarting after source update'
                    WHERE
                        id_asset=%s
                        AND status IN (1,2,3,4,6)
                        AND id_action NOT IN
                            (SELECT id FROM actions WHERE service_type='import')
                        {action_cond}
                    RETURNING id
                    """,
                    [time.time(), asset.id],
                )

                res = db.fetchall()
                if res:
                    joblist = ", ".join([str(jid[0]) for jid in res])
                    nebula.log.info(
                        f"{asset} has been changed.", f"Restarting jobs {joblist}"
                    )
                db.commit()
