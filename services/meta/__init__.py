import os
import time
from stat import S_ISREG
from typing import Literal

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import ContentType, MediaType, ObjectStatus
from nebula.objects import Asset
from nebula.settings import settings
from nebula.storages import storages
from nxtools import FileObject

from .ffprobe import ffprobe_asset, validate_package_asset

# Assets that need attention on every pass, not only during a sweep
HOT_STATUSES = [ObjectStatus.CREATING, ObjectStatus.RESET, ObjectStatus.RETRIEVING]
# Margin for clock skew between hosts and the integer mtime column
MTIME_MARGIN = 5


def needs_processing(
    path: str, status: int, file_mtime: float | None, file_size: int | None
) -> bool:
    """False when process() would neither save nor probe the asset."""
    try:
        st = os.stat(path)
    except OSError:
        st = None
    if st is None or not S_ISREG(st.st_mode):
        return status in (
            ObjectStatus.ONLINE,
            ObjectStatus.RESET,
            ObjectStatus.CREATING,
        )
    if st.st_size == 0:
        return status not in (ObjectStatus.OFFLINE, ObjectStatus.RETRIEVING)
    if int(st.st_mtime) != file_mtime or st.st_size != file_size:
        return True
    return status in (*HOT_STATUSES, ObjectStatus.OFFLINE)


class Service(BaseService):
    restart_on_update: Literal["all"] | list[int] | None

    def on_init(self):
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

        self.sweep_interval = int(self.settings.attrib.get("sweep_interval", 30))
        self.last_sweep = 0.0
        self.last_pass = 0.0
        self.unmounted: set[int] = set()

        if self.settings.findall("cond"):
            nebula.log.warning(
                "Conditions (<cond>) in meta service settings are ignored. "
                "Run a single meta service and remove them."
            )

    def on_main(self):
        start = time.time()
        sweep = start - self.last_sweep >= self.sweep_interval

        roots: dict[int, str] = {}
        unmounted: set[int] = set()
        for storage in storages:
            path = storage.local_path
            mounted = os.path.isdir(path) and len(os.listdir(path)) != 0
            if storage.protocol != "local" and not mounted:
                unmounted.add(storage.id)
            roots[storage.id] = path
        for id_storage in unmounted - self.unmounted:
            nebula.log.warning(f"Skipping assets on unmounted storage {id_storage}")
        for id_storage in self.unmounted - unmounted:
            nebula.log.info(f"Storage {id_storage} is mounted again")
        self.unmounted = unmounted

        # do not scan trashed and archived files
        query = """
            SELECT id, status, (meta->>'id_storage')::integer, meta->>'path',
                (meta->>'file/mtime')::numeric, (meta->>'file/size')::bigint
            FROM assets
            WHERE media_type = %s AND status NOT IN (3, 4)
            AND meta->>'path' IS NOT NULL AND meta->>'id_storage' IS NOT NULL
            AND (meta->>'id_storage')::integer <> ALL(%s::integer[])
        """
        args: list[object] = [MediaType.FILE, list(unmounted)]
        if not sweep:
            query += " AND (status = ANY(%s) OR mtime >= %s)"
            args += [HOT_STATUSES, int(self.last_pass) - MTIME_MARGIN]

        db = DB()
        db.query(query, args)
        rows = db.fetchall()
        processed = 0
        for id_asset, status, id_storage, path, file_mtime, file_size in rows:
            if id_storage not in roots:
                roots[id_storage] = storages[id_storage].local_path
            if not needs_processing(
                os.path.join(roots[id_storage], path), status, file_mtime, file_size
            ):
                continue
            self.process(Asset(id_asset, db=db))
            processed += 1

        self.last_pass = start
        if sweep:
            self.last_sweep = start
            nebula.log.debug(
                f"Swept {len(rows)} assets in {time.time() - start:.2f} s, "
                f"processed {processed}"
            )

    def process(self, asset):
        asset_file = FileObject(asset.file_path)
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
                # The storage may have been unmounted since the pass started
                root = storages[asset["id_storage"]].local_path
                if not os.path.exists(os.path.join(root, ".nebula_root")):
                    nebula.log.warning(
                        f"{asset}: File not found, but storage {asset['id_storage']} "
                        "has no .nebula_root (unmounted?). Not turning offline"
                    )
                    return
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

        # Size too: some copy tools keep the source mtime while writing
        if (
            fmtime != asset["file/mtime"]
            or fsize != asset["file/size"]
            or asset["status"] in [ObjectStatus.RESET, ObjectStatus.RETRIEVING]
        ):
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
                elif asset["content_type"] == ContentType.PACKAGE:
                    nebula.log.debug(f"{asset}: validating package asset")
                    result = validate_package_asset(asset)
                else:
                    result = None

                if result:
                    asset = result
                elif asset["status"] != ObjectStatus.CREATING:
                    asset["status"] = ObjectStatus.CREATING
                    asset.save()
                    return

                if asset["status"] == ObjectStatus.RESET:
                    if result:
                        asset["status"] = ObjectStatus.ONLINE
                    else:
                        asset["status"] = ObjectStatus.CORRUPTED
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
            if asset["content_type"] == ContentType.PACKAGE:
                result = validate_package_asset(asset)
            else:
                result = ffprobe_asset(asset)
            if not result:
                nebula.log.warning(f"{asset}: Asset is corrupted")
                asset["status"] = ObjectStatus.CORRUPTED
                asset["qc/state"] = 0
                asset.save()
                return

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
