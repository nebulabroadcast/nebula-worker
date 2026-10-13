import time
from pathlib import Path

import nebula
from nebula.db import DB
from nebula.enum import ObjectStatus
from nebula.jobs import Job, send_to
from nebula.objects import Asset

from .common import ImportDefinition, create_error
from .transcoder import ImportTranscoder


def get_import_job(asset: Asset, action: ImportDefinition, service, db: DB) -> Job:
    assert asset.id, f"Asset {asset} has no id"
    id_job = send_to(
        id_asset=asset.id,
        id_action=action.action_id,
        id_service=service.id_service,
        restart_existing=True,
        db=db,
    )
    db.query(
        "UPDATE jobs SET start_time = %s, id_service = %s WHERE id = %s",
        [time.time(), service.id, id_job],
    )
    db.commit()
    job = Job(id_job, db=db)
    job.set_progress(0, "Importing")
    return job


def ensure_target(file_path: Path) -> None:
    """Ensure that the target file is writable."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if file_path.exists():
        with file_path.open("a"):
            pass


def transcode_to_asset(
    service,
    action: ImportDefinition,
    asset: nebula.Asset,
    import_file: Path,
    job: Job,
) -> None:
    """Transcode the import file and put it in place of the asset file.

    Raises on failure. The asset file is touched only after a successful
    transcode, so a failed import leaves the asset as it was.
    """
    asset_file = Path(asset.file_path)
    try:
        ensure_target(asset_file)
    except OSError as e:
        raise Exception(f"Unable to write the asset file {asset_file}: {e}") from e

    temp_dir = Path(nebula.storages[asset["id_storage"]].local_path, ".nx", "creating")
    temp_file = temp_dir / f"{time.time()}.{asset_file.name}"

    def progress_handler(progress: float) -> None:
        job.set_progress(progress, f"Importing {progress:.02f}%")

    try:
        temp_dir.mkdir(parents=True, exist_ok=True)
        transcoder = ImportTranscoder(
            import_file,
            temp_file,
            service.profiles[action.profile],
        )
        transcoder.start(progress_handler)
    except Exception:
        remove_temp_file(temp_file)
        raise

    backup = service.version_backup(asset) if asset_file.exists() else None
    try:
        temp_file.rename(asset_file)
    except OSError as e:
        remove_temp_file(temp_file)
        if backup:
            # Put the previous version back, the asset must not lose its file
            backup.rename(asset_file)
        raise Exception(f"Unable to move the new file to {asset_file}: {e}") from e


def remove_temp_file(temp_file: Path) -> None:
    try:
        temp_file.unlink(missing_ok=True)
    except OSError:
        nebula.log.warning(f"Unable to remove temp file {temp_file}")


def move_import_file(import_file: Path, error: str | None) -> bool:
    """Move the import file to backup/ on success or failed/ on failure.

    A failed file gets its error in a .txt beside it; it can be retried by
    moving it back (or uploading again). Returns False if the file couldn't
    be moved; the error then goes beside it in the import directory.
    """
    target_dir = import_file.parent / ("failed" if error else "backup")
    target_file = target_dir / import_file.name
    nebula.log.info(f"Moving {import_file} to {target_file}")
    try:
        target_dir.mkdir(exist_ok=True)
        import_file.rename(target_file)
    except OSError as e:
        nebula.log.traceback()
        message = f"Unable to move {import_file.name} to {target_dir.name}/: {e}"
        create_error(import_file, f"{error}\n\n{message}" if error else message)
        return False
    if error:
        create_error(target_file, error)
    return True


def import_asset(
    service,
    action: ImportDefinition,
    asset: nebula.Asset,
    import_file: Path,
) -> bool:
    """Import the file to the asset and move it out of the import directory.

    Returns False if the import file had to be left where it is: the caller
    must not import it again until it changes.
    """
    db = nebula.DB()
    nebula.log.info(f"Importing {import_file} to {asset}")

    job: Job | None = None
    error: str | None = None
    try:
        job = get_import_job(asset, action, service, db)
        transcode_to_asset(service, action, asset, import_file, job)
    except Exception as e:
        error = str(e).strip() or e.__class__.__name__

    moved = move_import_file(import_file, error)

    if error is not None:
        reason = error.splitlines()[0][:200]
        if job:
            job.fail(f"Import failed: {reason}")
        nebula.log.error(f"Import {asset} failed: {reason}")
        return moved

    # The asset file is in place, even if the import file couldn't be moved

    asset = nebula.Asset(asset.id, db=db)
    # Remove technical metadata
    for key in list(asset.meta):
        metatype = nebula.settings.metatypes.get(key)
        if metatype and (metatype.ns in ["q", "f"]):
            del asset.meta[key]
    asset["status"] = ObjectStatus.CREATING
    # The file was renamed into place complete: meta needn't wait for it
    asset["__file/written"] = Path(asset.file_path).stat().st_size
    asset.save()

    assert job is not None
    if moved:
        job.done("Import finished")
    else:
        job.done("Import finished, but the source couldn't be moved to backup/")
    nebula.log.info(f"Import {asset} finished")
    return moved
