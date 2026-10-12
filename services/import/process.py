import time
from pathlib import Path

import nebula
from nebula.db import DB
from nebula.enum import ObjectStatus
from nebula.jobs import Job, send_to
from nebula.objects import Asset

from .common import ImportDefinition, create_error
from .transcoder import ImportTranscoder


def get_import_job(
    import_file: Path,
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


def ensure_target(file_path: Path) -> bool:
    """Ensure that the target file is writable."""
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        nebula.log.traceback()
        return False

    if file_path.exists():
        try:
            with file_path.open("a"):
                pass
        except OSError:
            return False
    return True


def import_asset(
    service,
    action: ImportDefinition,
    asset: nebula.Asset,
    import_file: Path,
):
    db = nebula.DB()
    asset_file = Path(asset.file_path)

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

    if not ensure_target(asset_file):
        job.fail("Unable to create target file")
        return False

    temp_dir = Path(nebula.storages[asset["id_storage"]].local_path, ".nx", "creating")
    temp_file = temp_dir / f"{time.time()}.{asset_file.name}"
    try:
        temp_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        nebula.log.traceback()
        job.fail("Unable to create temp file")
        return False

    # Transcode. The asset file is touched only after a successful transcode,
    # so a failed import leaves the asset as it was.

    error: str | None = None
    try:
        transcoder = ImportTranscoder(
            import_file,
            temp_file,
            service.profiles[action.profile],
        )
        transcoder.start(progress_handler)
    except Exception as e:
        error = str(e) or e.__class__.__name__

    if error is None:
        if asset_file.exists():
            service.version_backup(asset)
        try:
            temp_file.rename(asset_file)
        except OSError:
            nebula.log.traceback()
            error = "Unable to move temp file to asset file"

    if error is not None:
        try:
            temp_file.unlink(missing_ok=True)
        except OSError:
            nebula.log.warning(f"Unable to remove temp file {temp_file}")

    #
    # Move the import file away: backup/ on success, failed/ on failure.
    # A failed file can be retried by moving it back (or uploading again).
    #

    target_dir = import_file.parent / ("failed" if error else "backup")
    target_file = target_dir / import_file.name
    nebula.log.info(f"Moving {import_file} to {target_file}")
    try:
        target_dir.mkdir(exist_ok=True)
        import_file.rename(target_file)
    except OSError:
        nebula.log.traceback()
        if error is None:
            error = f"Unable to move {import_file.name} to {target_dir}"

    if error is not None:
        if target_file.exists():
            create_error(target_file, error)
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
    asset["__file/written"] = asset_file.stat().st_size
    asset.save()

    job.done("Import finished")
    nebula.log.info(f"Import {asset} finished")
    return True
