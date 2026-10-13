from pathlib import Path
from xml.etree import ElementTree

import nebula
from nebula.base_service import BaseService
from nebula.enum import ContentType, JobState, MediaType
from nebula.filetypes import FileTypes

from .common import ImportDefinition, create_error
from .process import import_asset
from .transcoder import ImportProfile, load_profiles


def setting(root: ElementTree.Element, name: str) -> str | None:
    """Text of a settings element, None when missing or empty."""
    element = root.find(name)
    if element is None or not element.text:
        return None
    return element.text.strip() or None


class Service(BaseService):
    def on_init(self):
        self.actions: list[ImportDefinition] = []
        self.exts: list[str] = FileTypes.exts_by_type(ContentType.VIDEO)
        # Size of each file seen in the previous pass: a file is imported
        # once its size is the same in two passes
        self.filesizes: dict[Path, int] = {}
        # Files that couldn't be moved out of the import directory, with
        # their (size, mtime): not imported again until they change
        self.left_in_place: dict[Path, tuple[int, int]] = {}
        self.profiles: dict[str, ImportProfile] = load_profiles()
        nebula.log.debug(f"Import profiles: {', '.join(self.profiles) or 'none'}")

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
            action_settings = ElementTree.XML(settings)

            id_storage = setting(action_settings, "id_storage")
            import_storage = (
                int(id_storage)
                if id_storage and id_storage.isdigit()
                else nebula.settings.system.upload_storage
            )
            import_dir = (
                setting(action_settings, "import_dir")
                or nebula.settings.system.upload_dir
            )
            identifier = setting(action_settings, "identifier") or "id"
            profile = setting(action_settings, "profile") or "xdcamhd422-1080i50"

            if profile not in self.profiles:
                nebula.log.error(
                    f"Import action {title} uses unknown profile {profile}. "
                    f"Available: {', '.join(self.profiles) or 'none'}."
                )
                continue

            if not (import_storage and import_dir):
                nebula.log.error(
                    f"Import action {title} has no storage or import directory defined."
                )
                continue

            path = Path(nebula.storages[import_storage].local_path, import_dir)
            try:
                path.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                nebula.log.error(
                    f"Unable to create import directory {path}: {e}. "
                    f"Skipping import action {title}."
                )
                continue

            action = ImportDefinition(
                action_id=id,
                import_dir=path,
                identifier=identifier,
                profile=profile,
            )
            self.actions.append(action)
            nebula.log.debug(f"Import action {title} added.")

    def on_main(self):
        seen: set[Path] = set()
        for action in self.actions:
            if not action.import_dir.is_dir():
                continue

            for import_file in sorted(action.import_dir.iterdir()):
                if (
                    import_file.name.startswith(".")
                    or not import_file.is_file()
                    or import_file.suffix[1:].lower() not in self.exts
                ):
                    continue
                seen.add(import_file)
                try:
                    with import_file.open("rb") as f:
                        f.seek(0, 2)
                        fsize = f.tell()
                    mtime = import_file.stat().st_mtime_ns
                except OSError:
                    nebula.log.debug(f"Import file {import_file.name} is busy.")
                    continue

                if self.left_in_place.get(import_file) == (fsize, mtime):
                    continue
                self.left_in_place.pop(import_file, None)

                if self.filesizes.get(import_file) != fsize:
                    self.filesizes[import_file] = fsize
                    nebula.log.debug(f"New file '{import_file.name}' detected")
                    continue

                # A file of the same name arriving later starts over
                del self.filesizes[import_file]
                if self.import_file(action, import_file) is False:
                    self.left_in_place[import_file] = (fsize, mtime)

        # Forget files that are gone
        self.filesizes = {k: v for k, v in self.filesizes.items() if k in seen}
        self.left_in_place = {k: v for k, v in self.left_in_place.items() if k in seen}

    def import_file(self, action: ImportDefinition, path: Path) -> bool | None:
        """Import the file if it matches an asset.

        Returns False if it was imported (or failed) but couldn't be moved
        out of the import directory, None if it wasn't imported.
        """
        # check whether the file matches the ident in the DB

        db = nebula.DB()
        db.query(
            "SELECT meta FROM assets WHERE meta->>%s = %s",
            [action.identifier, path.stem],
        )

        try:
            asset = nebula.Asset(meta=db.fetchall()[0][0])
        except IndexError:
            create_error(path, f"Unexpected file {path.name}")
            return None

        if not (asset["id_storage"] and asset["path"]):
            create_error(path, f"{asset} has no target path.")
            return None

        if asset["media_type"] != MediaType.FILE:
            create_error(path, f"{asset} is not a file.")
            return None

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
            return None

        nebula.log.info(f"Importing {asset}")

        moved = import_asset(self, action, asset, path)

        # Clean up error files whose import file is gone
        stems = {f.stem for f in action.import_dir.iterdir() if f.suffix != ".txt"}
        for error_file in action.import_dir.glob("*.txt"):
            if error_file.stem not in stems:
                error_file.unlink(missing_ok=True)
        return None if moved else False

    def version_backup(self, asset: nebula.Asset) -> Path | None:
        """Move the asset file to .nx/versions/. Returns the backup path."""
        if asset.id is None:
            return None
        source = Path(asset.file_path)
        target_dir = Path(
            nebula.storages[asset["id_storage"]].local_path,
            ".nx",
            "versions",
            f"{asset.id // 1000:04d}",
            str(asset.id),
        )
        target = target_dir / f"{asset.id}-{int(asset['mtime'])}{source.suffix}"
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            source.rename(target)
        except OSError:
            nebula.log.traceback()
            nebula.log.warning(f"Unable to create version backup of {asset}")
            return None
        return target
