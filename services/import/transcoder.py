from functools import cached_property
from typing import Any
from pydantic import BaseModel
from nxtools import ffprobe, ffmpeg

from .profiles import PROFILES


def guess_aspect(w: int, h: int) -> float:
    if 0 in [w, h]:
        return 0
    valid_aspects = [(16, 9), (4, 3), (2.35, 1), (5, 4), (1, 1)]
    ratio = float(w) / float(h)
    n, d = min(valid_aspects, key=lambda x: abs((float(x[0]) / x[1]) - ratio))
    return float(n) / d


class BaseTrack(BaseModel):
    faucet: str
    index: int


class VideoTrack(BaseTrack):
    width: int
    height: int
    fps: float
    aspect: float


class AudioTrack(BaseTrack):
    channels: int
    sample_rate: int


class ImportTranscoder:
    source_path: str
    target_path: str
    profile: str

    def __init__(self, source_path: str, target_path: str, profile: str):
        self.source_path = source_path
        self.target_path = target_path
        self.profile = profile

    @cached_property
    def meta(self) -> dict[str, Any]:
        return ffprobe(self.source_path)

    @cached_property
    def duration(self) -> float:
        try:
            return float(self.meta["format"]["duration"])
        except Exception:
            pass
        return 100

    @cached_property
    def video_track(self) -> VideoTrack:
        for stream in self.meta["streams"]:
            if stream["codec_type"] == "video":
                width = stream["width"]
                height = stream["height"]
                fps = stream["r_frame_rate"]

                try:
                    dar_n, dar_d = [
                        float(e) for e in stream["display_aspect_ratio"].split(":")
                    ]
                    if not (dar_n and dar_d):
                        raise Exception
                except Exception:
                    dar_n, dar_d = float(stream["width"]), float(stream["height"])
                aspect = dar_n / dar_d
                aspect = guess_aspect(dar_n, dar_d)

                return VideoTrack(
                    faucet=f"{0}:{stream['index']}",
                    index=len(result),
                    width=width,
                    height=height,
                    fps=fps,
                    aspect=aspect,
                )

    @cached_property
    def audio_tracks(self) -> list[AudioTrack]:
        result: list[AudioTrack] = []
        for stream in self.meta["streams"]:
            if stream["codec_type"] == "audio":
                channels = stream["channels"]
                sample_rate = stream["sample_rate"]
                return AudioTrack(
                    faucet=f"{0}:{stream['index']}",
                    index=len(result),
                    channels=channels,
                    sample_rate=sample_rate,
                )
        return result

    def create_filter_chain(self) -> str:
        filters = []
        return ";".join(filters)

    def start(self, progress_handler) -> bool:
        cmd = [
            "-i",
            self.source_path,
            # "-filter_complex",
            # self.create_filter_chain(),
        ]

        params = PROFILES[self.profile]["ffmpeg"]
        cmd.extend(params)
        cmd.append(self.target_path)

        return ffmpeg(*cmd, progress_handler=lambda x: progress_handler(x / self.duration * 100))
