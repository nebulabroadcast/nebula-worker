import os
import time
import nebula
from nebula.jobs import send_to, Job
from nebula.enum import ObjectStatus
from nxtools import FileObject

from .common import create_error, ImportDefinition
from .transcoder import ImportTranscoder


def import_asset(
    action: ImportDefinition,
    asset: nebula.Asset,
    import_file: FileObject,
):
    db = nebula.DB()

    # for condition in parent.conditions:
    #     value = parent.conditions[condition]
    #     if value != probe.get(condition, None):
    #         match = False
    #         break
    #
    # if match:
    #     logging.info(f"Fast importing {import_file} to {asset}")
    #     try:
    #         os.rename(import_file.path, asset.file_path)
    #     except Exception:
    #         log_traceback()
    #         mk_error(import_file, "Unable to fast import. See logs.")
    #         return False

    nebula.log.info(f"Importing {import_file} to {asset}")

    res = send_to(
        id_asset=asset.id,
        id_action=action.action_id,
        restart_existing=True,
        db=db,
    )

    if not res:
        create_error(import_file, f"Unable to create job for {asset}")
        return

    id_job = res.get("id")
    if not id_job:
        create_error(import_file, f"Unable to get job ID for {asset}")
        return

    job = Job(id_job, db=db)
    job.set_progress(0, "Importing")

    def progress_handler(progress):
        job.set_progress(progress, "Importing")

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
        import_file,
        temp_file,
        "xdcamhd422-1080i50",
    )

    result = transcoder.start(progress_handler)
    print("RESULT", result)

    # Move temp file to asset file

    if result:
        try:
            os.rename(temp_file, asset.file_path)
        except Exception:
            nebula.log.traceback()
            job.fail("Unable to move temp file to asset file")
            return False

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
            return False

    backup_file = os.path.join(
        backup_dir,
        f"{import_file.base_name}.{import_file.ext}",
    )
    nebula.log.info(f"Moving {import_file} to {backup_file}")
    os.rename(import_file.path, backup_file)

    #
    # Update asset metadata
    #

    if result:
        allkeys = list(asset.meta)
        for key in allkeys:
            metatype = nebula.settings.metatypes.get(key)
            if metatype is None or (metatype.ns in ["q", "f"]):
                del asset.meta[key]
        asset["status"] = ObjectStatus.CREATING
        asset.save()

        job.done("Import finished")
        nebula.log.info(f"Import {asset} finished")
    else:
        job.fail("Import failed")
        nebula.log.error(f"Import {asset} failed")
