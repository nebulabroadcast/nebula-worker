import functools
import json
import os
from typing import TYPE_CHECKING

from nxtools import ffmpeg

import nebula
from nebula.base_service import BaseService
from nebula.storages import storages

if TYPE_CHECKING:
    pass


class ThumbnailTool:
    def __init__(self, asset: nebula.Asset):
        self.asset = asset

    @functools.cached_property
    def thumb_dir(self) -> str | None:
        assert self.asset.id, "Asset ID not set. This should not happen."
        target_dir = os.path.join(
            storages[self.asset.proxy_storage].local_path,
            f".nx/thumbs/{int(self.asset.id / 1000)}/{self.asset.id}",
        )
        if not os.path.isdir(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
            except Exception:
                pass
        return target_dir

    @functools.cached_property
    def thumb_position(self) -> float | None:
        if self.asset["poster_frame"]:
            return self.asset["poster_frame"]

        if not self.asset.duration:
            return None

        duration = self.asset.duration
        pos = duration * 0.15
        pos += self.asset["mark_in"] or 0

        return pos

    @property
    def manifest_path(self) -> str:
        assert self.thumb_dir, "Thumb dir not set. This should not happen."
        return os.path.join(self.thumb_dir, "manifest.json")

    @property
    def thumb_exists(self):
        if not os.path.exists(self.manifest_path):
            return False

        with open(self.manifest_path) as f:
            manifest = json.load(f)

        if manifest.get("position") == self.thumb_position:
            return True

        return False

    def build(self):
        if not self.thumb_dir:
            return

        if not self.thumb_position:
            return

        if self.thumb_exists:
            return

        sizes = ["1920", "540", "160"]

        nebula.log.info(f"Building thumbs for {self.asset}")

        for size in sizes:
            thumb_path = os.path.join(self.thumb_dir, f"thumb-{size}.jpg")
            ffmpeg(
                "-y",
                "-ss",
                self.thumb_position,
                "-i",
                self.asset.file_path,
                "-vf",
                f"scale={size}:-1",
                "-vframes",
                "1",
                "-q:v",
                "2",
                thumb_path,
            )

        manifest = {"position": self.thumb_position, "sizes": sizes}

        with open(self.manifest_path, "w") as f:
            json.dump(manifest, f)


class Service(BaseService):
    def on_init(self):
        self.last_mtime = 0

    def on_main(self):
        db = nebula.DB()
        db.query(
            """
            SELECT meta FROM assets
            WHERE
                content_type = 2
            AND status = 1
            AND media_type = 1
            AND mtime > %s
            ORDER BY mtime ASC
            """, [self.last_mtime]
        )
        last_mtime = self.last_mtime
        for meta, in db.fetchall():
            asset = nebula.Asset(meta=meta)

            thumb_tool = ThumbnailTool(asset)
            thumb_tool.build()
            last_mtime = max(last_mtime, asset["mtime"])

        self.last_mtime = last_mtime



