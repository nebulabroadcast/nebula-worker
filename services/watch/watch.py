import os
import time
from typing import TYPE_CHECKING, Generator

from nxtools import FileObject, get_base_name, get_files

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import MediaType, ObjectStatus
from nebula.filetypes import FileTypes
from nebula.helpers import asset_by_path
from nebula.objects import Asset

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element


class Watchfolder:
    id_storage: int
    rel_path: str
    quarantine_time: int
    id_folder: int
    recursive: bool
    hidden: bool
    case_sensitive_exts: bool

    def __init__(self, settings: "Element"):
        self.id_storage = int(settings.attrib["id_storage"])
        self.rel_path = settings.attrib["path"]
        self.quarantine_time = int(settings.attrib.get("quarantine_time", "10"))
        self.id_folder = int(settings.attrib.get("id_folder", 12))
        self.recursive = bool(settings.attrib.get("recursive", False))
        self.hidden = bool(settings.attrib.get("hidden", False))
        self.case_sensitive_exts = bool(
            settings.attrib.get("case_sensitive_exts", False)
        )

    @property
    def storage_path(self):
        return nebula.storages[self.id_storage].local_path

    @property
    def path(self):
        return os.path.join(self.storage_path, self.rel_path)

    def __bool__(self):
        return os.path.isdir(self.path)

    def get_files(self) -> Generator[FileObject, None, None]:
        for file_object in get_files(
            self.path,
            recursive=self.recursive,
            hidden=self.hidden,
            case_sensitive_exts=self.case_sensitive_exts,
        ):
            if not file_object.size:
                continue

            if (
                self.quarantine_time
                and time.time() - file_object.mtime < self.quarantine_time
            ):
                nebula.log.trace(f"{file_object.base_name} is too young. Skipping")
                continue

            yield file_object


class Service(BaseService):
    def on_init(self):
        pass

    def on_main(self):
        db = DB()
        self.existing = []
        db.query("SELECT meta FROM assets WHERE media_type=1 AND status=1")
        for (meta,) in db.fetchall():
            asset = Asset(meta=meta, db=db)
            file_path = asset.file_path
            self.existing.append(file_path)

        for wf_settings in self.settings.findall("folder"):
            watchfolder = Watchfolder(wf_settings)

            if not watchfolder:
                nebula.log.warning(f"Watchfolder {watchfolder.path} does not exist")

            i = 0
            for file_object in watchfolder.get_files():
                i += 1

                full_path = file_object.path
                if full_path in self.existing:
                    continue

                now = time.time()
                asset_path = full_path.replace(watchfolder.storage_path, "", 1).lstrip(
                    "/"
                )
                ext = os.path.splitext(asset_path)[1].lstrip(".").lower()
                if ext not in FileTypes.exts():
                    continue

                asset = asset_by_path(watchfolder.id_storage, asset_path, db=db)  # type: ignore
                if asset:
                    self.existing.append(full_path)
                    continue

                base_name = get_base_name(asset_path)

                asset = Asset(db=db)
                asset["content_type"] = FileTypes.by_ext(ext)
                asset["media_type"] = MediaType.FILE
                asset["id_storage"] = watchfolder.id_storage
                asset["path"] = asset_path
                asset["ctime"] = now
                asset["mtime"] = now
                asset["status"] = ObjectStatus.CREATING
                asset["id_folder"] = watchfolder.id_folder
                asset["title"] = base_name

                asset.load_sidecar_metadata()

                failed = False
                for post_script in wf_settings.findall("post"):
                    if post_script.text is None:
                        continue
                    try:
                        exec(post_script.text)
                    except Exception:
                        nebula.log.traceback(f"Error executing post-script on {asset}")
                        failed = True

                if not failed:
                    asset.save(set_mtime=False)
