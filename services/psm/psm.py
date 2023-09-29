import os
import stat
import time
from typing import Any

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import ObjectStatus
from nebula.jobs import send_to
from nebula.mediaprobe import mediaprobe
from nebula.objects import Asset
from nebula.storages import storages

SCHEDULE_INTERVAL = 60
UNSCHEDULE_INTERVAL = 86400
DEFAULT_STATUS: dict[str, Any] = {
    "status": ObjectStatus.OFFLINE,
    "size": 0,
    "mtime": 0,
    "duration": 0,
}
STORAGE_STATUS: dict[int, bool] = {}


def get_scheduled_assets(id_channel: int, **kwargs):
    db = kwargs.get("db", DB())
    db.query(
        """
            SELECT
                a.meta, dist
            FROM (
                SELECT
                    i.id_asset,
                    MIN(ABS(e.start - extract(epoch from now()))) AS dist
                FROM
                    events as e, items as i
                WHERE
                    e.start > extract(epoch from now()) - 86400*7
                    AND e.id_channel = %s
                    AND i.id_bin = e.id_magic
                    AND i.id_asset > 0
                GROUP BY i.id_asset) i
                LEFT JOIN assets a ON a.id = i.id_asset
            ORDER BY
                dist ASC
        """,
        [id_channel],
    )
    for meta, dist in db.fetchall():
        yield Asset(meta=meta, db=db), dist < 86400


def check_file_validity(asset, id_channel):
    path = asset.get_playout_full_path(id_channel)
    try:
        res = mediaprobe(path)
    except Exception:
        nebula.log.error("Unable to read", path)
        return ObjectStatus.CORRUPTED, 0
    if not res:
        return ObjectStatus.CORRUPTED, 0
    if res["duration"]:
        return ObjectStatus.CREATING, res["duration"]
    return ObjectStatus.UNKNOWN, 0


class PlayoutStorageTool:
    def __init__(self, playout_config):
        self.db = DB()
        self.id_channel = playout_config.id
        self.playout_config = playout_config
        self.status_key = f"playout_status/{self.id_channel}"
        self.send_action = self.playout_config.send_action
        self.scheduled_ids = []

    def __len__(self):
        return self.playout_config.playout_storage and self.playout_config.playout_dir

    def main(self):
        if self.playout_config.playout_storage is None:
            return
        db = self.db
        storage = storages[self.playout_config.playout_storage]
        if not storage:
            if STORAGE_STATUS.get(storage.id, True):
                nebula.log.error(f"{storage} is not available")
                STORAGE_STATUS[storage.id] = False
            return
        STORAGE_STATUS[storage.id] = True

        for asset, scheduled in get_scheduled_assets(self.id_channel, db=db):
            old_status = asset.get(self.status_key, DEFAULT_STATUS)

            # read playout file props
            try:
                fs = os.stat(asset.get_playout_full_path(self.id_channel))
                file_exists = stat.S_ISREG(fs[stat.ST_MODE])
            except FileNotFoundError:
                file_exists = False

            if file_exists:
                file_size = fs[stat.ST_SIZE]
                file_mtime = fs[stat.ST_MTIME]
            else:
                file_size = file_mtime = 0

            if file_exists:
                if file_size:
                    file_status = ObjectStatus.ONLINE
                else:
                    file_status = ObjectStatus.CORRUPTED
            else:
                file_status = ObjectStatus.OFFLINE

            ostatus = old_status.get("status", ObjectStatus.OFFLINE)
            omtime = old_status.get("mtime", 0)
            osize = old_status.get("size", 0)
            duration = old_status.get("duration", 0)

            now = time.time()

            # if file changed, check using ffprobe
            if file_status == ObjectStatus.ONLINE:
                if omtime != file_mtime or osize != file_size:
                    file_status, duration = check_file_validity(asset, self.id_channel)
                else:
                    if ostatus == ObjectStatus.CREATING:
                        if now - file_mtime > 10 and omtime == file_mtime:
                            file_status = ObjectStatus.ONLINE
                        else:
                            file_status = ObjectStatus.CREATING
                    elif ostatus == ObjectStatus.UNKNOWN:
                        if now - file_mtime > 10:
                            file_status = ObjectStatus.CORRUPTED

            if ostatus != file_status or omtime != file_mtime or osize != file_size:
                fs = ObjectStatus(file_status).name
                nebula.log.info(f"Set {asset} playout status to {fs}")
                asset[self.status_key] = {
                    "status": file_status,
                    "size": file_size,
                    "mtime": file_mtime,
                    "duration": duration,
                }
                asset.save()

            if (
                file_status
                not in [
                    ObjectStatus.ONLINE,
                    ObjectStatus.CREATING,
                    ObjectStatus.CORRUPTED,
                ]
                and self.send_action
                and asset["status"] == ObjectStatus.ONLINE
                and scheduled
            ):
                result = send_to(
                    asset.id,
                    self.send_action,
                    restart_existing=True,
                    restart_running=False,
                    priority=4,
                    db=db,
                )
                chtitle = self.playout_config.name
                if result.response == 201:
                    nebula.log.info(f"Sending {asset} to {chtitle}: {result.message}")


class Service(BaseService):
    def on_init(self):
        pass

    def on_main(self):
        for pconfg in nebula.settings.playout_channels:
            pst = PlayoutStorageTool(pconfg)
            pst.main()
