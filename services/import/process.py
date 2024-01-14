import os
import time

from nxtools import FileObject

import nebula
from nebula.db import DB
from nebula.enum import ObjectStatus
from nebula.jobs import Job, send_to
from nebula.objects import Asset

from .common import ImportDefinition, create_error
from .transcoder import ImportTranscoder


def get_import_job(
    import_file: FileObject,
    asset: Asset,
    action,
    service,
    db: DB | None = None,
) -> Job | None:
    if not db:
        db = DB()

    try:
        assert asset.id, f"Asset {asset} has no id"
        id_job = send_to(
            id_asset=asset.id,
            id_action=action.action_id,
            id_service=service,
            restart_existing=True,
            db=db,
        )
    except Exception as e:
        create_error(import_file, f"Unable to create job for {asset}: {e}")
        return None

    job = Job(id_job, db=db)
    job.set_progress(0, "Importing")
    return job


def ensure_target(file_path: str) -> bool:
    """Ensure that the target file is writable."""
    # ensure target dir exists
    target_dir = os.path.split(file_path)[0]
    if not os.path.isdir(target_dir):
        try:
            os.makedirs(target_dir)
        except Exception:
            nebula.log.traceback()
            return False

    # ensure target file is writable
    if os.path.exists(file_path):
        try:
            _ = open(file_path, "a")
        except Exception:
            return False
    return True


def import_asset(
    service,
    action: ImportDefinition,
    asset: nebula.Asset,
    import_file: FileObject,
):
    db = nebula.DB()

    nebula.log.info(f"Importing {import_file} to {asset}")

    job = get_import_job(import_file, asset, action, service.id_service, db)
    if job is None:
        return False
    db.query(
        """
        UPDATE jobs SET
            start_time = %s,
            id_service = %s
        WHERE id = %s
        """,
        [time.time(), service.id, job.id],
    )
    db.commit()

    def progress_handler(progress: float) -> None:
        if not job:
            return
        job.set_progress(progress, f"Importing {progress:.02f}%")

    if not ensure_target(asset.file_path):
        job.fail("Unable to create target file")
        return False

    # Get temp file path
    temp_file = os.path.join(
        nebula.storages[asset["id_storage"]].local_path,
        ".nx",
        "creating",
        f"{time.time()}.{os.path.split(asset.file_path)[1]}",
    )
    # ensure temp file dir exists
    if not os.path.isdir(os.path.dirname(temp_file)):
        try:
            os.makedirs(os.path.dirname(temp_file))
        except Exception:
            nebula.log.traceback()
            job.fail("Unable to create temp file")
            return False

    # Transcode

    transcoder = ImportTranscoder(
        import_file.path,
        temp_file,
        action.profile,
    )

    result = transcoder.start(progress_handler)

    # Move temp file to asset file

    if result:
        try:
            os.rename(temp_file, asset.file_path)
        except Exception:
            nebula.log.traceback()
            job.fail("Unable to move temp file to asset file")
            result = False

    #
    # Backup the import file
    #

    backup_dir = os.path.join(
        import_file.dir_name,
        "backup",
    )
    if not os.path.isdir(backup_dir):
        try:
            os.makedirs(backup_dir)
        except Exception:
            nebula.log.traceback()
            result = False

    backup_file = os.path.join(
        backup_dir,
        f"{import_file.base_name}.{import_file.ext}",
    )
    nebula.log.info(f"Moving {import_file} to {backup_file}")
    try:
        os.rename(import_file.path, backup_file)
    except Exception:
        nebula.log.traceback()
        result = False

    #
    # Update asset metadata
    #

    if result:
        # Reload asset
        asset = nebula.Asset(asset.id, db=db)
        # Remove technical metadata
        allkeys = list(asset.meta)
        for key in allkeys:
            metatype = nebula.settings.metatypes.get(key)
            if metatype and (metatype.ns in ["q", "f"]):
                del asset.meta[key]
        asset["status"] = ObjectStatus.OFFLINE
        asset.save()

        job.done("Import finished")
        nebula.log.info(f"Import {asset} finished")
    else:
        job.fail("Import failed")
        nebula.log.error(f"Import {asset} failed")
