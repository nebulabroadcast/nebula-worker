import os

from nxtools import FileObject, get_files, xml

import nebula
from nebula.base_service import BaseService
from nebula.enum import ContentType, JobState, MediaType
from nebula.filetypes import FileTypes

from .common import ImportDefinition, create_error
from .process import import_asset


class Service(BaseService):
    def on_init(self):
        self.actions: list[ImportDefinition] = []
        self.exts: list[str] = FileTypes.exts_by_type(ContentType.VIDEO)
        self.filesizes: dict[str, int] = {}

        db = nebula.DB()
        db.query(
            "UPDATE jobs SET status = 5 WHERE STATUS = 1 AND id_service = %s",
            [self.id_service],
        )
        db.commit()
        db.query(
            "SELECT id, title, settings FROM actions WHERE service_type = 'import'"
        )
        for id, title, settings in db.fetchall():
            action_settings = xml(settings)

            try:
                import_storage = int(action_settings.find("id_storage").text)  # type: ignore
            except (AttributeError, ValueError):
                import_storage = nebula.settings.system.upload_storage  # type: ignore

            try:
                import_dir = action_settings.find("import_dir").text  # type: ignore
            except AttributeError:
                import_dir = nebula.settings.system.upload_dir

            try:
                identifier = action_settings.find("identifier").text  # type: ignore
                assert identifier
            except (AttributeError, AssertionError):
                identifier = "id"

            try:
                profile = action_settings.find("profile").text  # type: ignore
                assert profile
            except (AttributeError, AssertionError):
                profile = "xdcamhd422-1080i50"

            if not (import_storage and import_dir):
                nebula.log.error(
                    f"Import action {title} has no storage or import directory defined."
                )
                continue

            path = os.path.join(nebula.storages[import_storage].local_path, import_dir)
            if not os.path.isdir(path):
                nebula.log.error(
                    f"Import directory {path} does not exist. "
                    f"Skipping import action {title}."
                )
                continue

            action = ImportDefinition(
                action_id=id,
                import_dir=path,
                backup_dir=None,
                identifier=identifier,
                profile=profile,
            )
            self.actions.append(action)
            nebula.log.debug(f"Import action {title} added.")

    def on_main(self):
        for action in self.actions:
            if not os.path.isdir(action.import_dir):
                continue

            for import_file in get_files(
                action.import_dir,
                exts=self.exts,
                recursive=False,
            ):
                try:
                    with import_file.open("rb") as f:
                        f.seek(0, 2)
                        fsize = f.tell()
                except OSError:
                    nebula.log.debug(f"Import file {import_file.base_name} is busy.")
                    continue

                if not (
                    import_file.path in self.filesizes
                    and self.filesizes[import_file.path] == fsize
                ):
                    self.filesizes[import_file.path] = fsize
                    nebula.log.debug(f"New file '{import_file.base_name}' detected")
                    continue

                self.import_file(action, import_file)

    def import_file(self, action: ImportDefinition, path: FileObject):
        # check whether the file matches the ident in the DB

        db = nebula.DB()
        db.query(
            f"""
            SELECT meta FROM assets WHERE
            meta->>'{action.identifier}' = '{path.base_name}'
            """
        )

        try:
            asset = nebula.Asset(meta=db.fetchall()[0][0])
        except IndexError:
            create_error(path, f"Unexpected file {path.base_name}")
            return

        if not (asset["id_storage"] and asset["path"]):
            create_error(path, f"{asset} has no target path.")
            return

        if asset["media_type"] != MediaType.FILE:
            create_error(path, f"{asset} is not a file.")
            return

        # Check whether there is unfinished job in the db
        statuses = [JobState.PENDING, JobState.IN_PROGRESS]
        db.query(
            """
            SELECT id FROM jobs WHERE
            id_asset = %s AND
            id_action = %s AND
            status = ANY(%s)
            """,
            [asset.id, action.action_id, statuses],
        )

        if db.fetchall():
            nebula.log.trace(f"{asset} is already being processed.")
            return

        nebula.log.info(f"Importing {asset}")

        if os.path.exists(asset.file_path):
            self.version_backup(asset)

        import_asset(self, action, asset, path)

        # Clean up error files
        for fname in os.listdir(action.import_dir):
            if not fname.endswith(".txt"):
                continue
            idec = os.path.splitext(fname)[0]
            if idec not in [
                os.path.splitext(f)[0] for f in os.listdir(action.import_dir)
            ]:
                os.remove(os.path.join(action.import_dir, fname))

    def version_backup(self, asset: nebula.Asset):
        if asset.id is None:
            return
        target_dir = os.path.join(
            nebula.storages[asset["id_storage"]].local_path,
            ".nx",
            "versions",
            f"{int(asset.id/1000):04d}",
            f"{asset.id:d}",
        )

        ext = os.path.splitext(asset.file_path)[1]
        target_fname = f"{asset.id}{asset['mtime']}{ext}"

        if not os.path.isdir(target_dir):
            try:
                os.makedirs(target_dir)
            except OSError:
                pass
        try:
            os.rename(asset.file_path, os.path.join(target_dir, target_fname))
        except OSError:
            nebula.log.traceback()
            nebula.log.warning(f"Unable to create version backup of {asset}")
