import os
import time

import nebula
from nebula.db import DB
from nebula.enum import ObjectStatus
from nebula.jobs import Job, send_to
from nebula.objects import Asset
from nxtools import FileObject

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

    # Transcode. The asset file is touched only after a successful transcode,
    # so a failed import leaves the asset as it was.

    error: str | None = None
    try:
        transcoder = ImportTranscoder(
            import_file.path,
            temp_file,
            service.profiles[action.profile],
        )
        transcoder.start(progress_handler)
    except Exception as e:
        error = str(e) or e.__class__.__name__

    if error is None:
        if os.path.exists(asset.file_path):
            service.version_backup(asset)
        try:
            os.rename(temp_file, asset.file_path)
        except Exception:
            nebula.log.traceback()
            error = "Unable to move temp file to asset file"

    if error is not None and os.path.exists(temp_file):
        try:
            os.remove(temp_file)
        except OSError:
            nebula.log.warning(f"Unable to remove temp file {temp_file}")

    #
    # Move the import file away: backup/ on success, failed/ on failure.
    # A failed file can be retried by moving it back (or uploading again).
    #

    target_dir = os.path.join(
        import_file.dir_name,
        "failed" if error else "backup",
    )
    target_file = os.path.join(
        target_dir,
        f"{import_file.base_name}.{import_file.ext}",
    )
    nebula.log.info(f"Moving {import_file} to {target_file}")
    try:
        os.makedirs(target_dir, exist_ok=True)
        os.rename(import_file.path, target_file)
    except Exception:
        nebula.log.traceback()
        if error is None:
            error = f"Unable to move {import_file.base_name} to {target_dir}"

    if error is not None:
        if os.path.exists(target_file):
            create_error(FileObject(target_file), error)
        else:
            nebula.log.error(error)
        job.fail(f"Import failed: {error.splitlines()[0][:200]}")
        nebula.log.error(f"Import {asset} failed")
        return False

    #
    # Update asset metadata
    #

    # Reload asset
    asset = nebula.Asset(asset.id, db=db)
    # Remove technical metadata
    allkeys = list(asset.meta)
    for key in allkeys:
        metatype = nebula.settings.metatypes.get(key)
        if metatype and (metatype.ns in ["q", "f"]):
            del asset.meta[key]
    asset["status"] = ObjectStatus.CREATING
    # The file was renamed into place complete: meta needn't wait for it
    asset["__file/written"] = os.path.getsize(asset.file_path)
    asset.save()

    job.done("Import finished")
    nebula.log.info(f"Import {asset} finished")
    return True
