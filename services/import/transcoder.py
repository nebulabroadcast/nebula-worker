import json
import os
import subprocess
from functools import cached_property

from nxtools import ffmpeg
from pydantic import BaseModel

import nebula

from .profiles import PROFILES


def guess_aspect(w: float, h: float) -> float:
    if 0 in [w, h]:
        return 0
    valid_aspects = [(16, 9), (4, 3), (2.35, 1), (5, 4), (1, 1)]
    ratio = w / h
    n, d = min(valid_aspects, key=lambda x: abs((x[0] / x[1]) - ratio))
    return n / d


class BaseTrack(BaseModel):
    faucet: str
    index: int


class VideoTrack(BaseTrack):
    commercial_name: str | None
    width: int
    height: int
    fps: float
    aspect: float


class AudioTrack(BaseTrack):
    channels: int
    sample_rate: int


class MediaInfo(BaseModel):
    duration: float
    video_track: VideoTrack
    audio_tracks: list[AudioTrack]


def mediainfo(path: str) -> MediaInfo:
    command = ["mediainfo", "--Output=JSON", path]
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    try:
        result = subprocess.check_output(command)
        data = json.loads(result)
    except Exception as e:
        raise Exception(f"Error while running {command}: {e}") from e

    video_track: VideoTrack | None = None
    audio_tracks = []
    duration = 0.0
    for track in data["media"]["track"]:
        index = track.get("StreamOrder", 0)
        if track["@type"] == "General":
            duration = float(track["Duration"])
        elif track["@type"] == "Video":
            video_track = VideoTrack(
                commercial_name=track.get("Format_Commercial_IfAny"),
                faucet=f"0:{index}",
                index=index,
                width=track["Width"],
                height=track["Height"],
                fps=track["FrameRate"],
                aspect=track["DisplayAspectRatio"],
            )
        elif track["@type"] == "Audio":
            audio_track = AudioTrack(
                faucet=f"0:{index}",
                index=index,
                channels=track["Channels"],
                sample_rate=track["SamplingRate"],
            )
            audio_tracks.append(audio_track)

    assert video_track is not None, "No video track found"

    return MediaInfo(
        duration=duration,
        video_track=video_track,
        audio_tracks=audio_tracks,
    )


class ImportTranscoder:
    source_path: str
    target_path: str

    def __init__(self, source_path: str, target_path: str, profile_name: str):
        self.source_path = source_path
        self.target_path = target_path
        self.profile_name = profile_name

    @property
    def profile(self) -> dict:
        return PROFILES[self.profile_name]

    @cached_property
    def meta(self) -> MediaInfo:
        return mediainfo(self.source_path)

    @property
    def duration(self) -> float:
        return self.meta.duration

    @property
    def video_track(self) -> VideoTrack:
        return self.meta.video_track

    @property
    def audio_tracks(self) -> list[AudioTrack]:
        return self.meta.audio_tracks

    def create_filter_chain(self) -> tuple[list[str], list[str], list[str]]:
        video_chain: list[str] = []
        audio_chain: list[str] = []
        mapping: list[str] = []

        # Video
        video_track = self.video_track
        if video_track:
            video_filters: list[str] = []

            # TODO: Video filtering

            if video_filters:
                video_chain.append(
                    f"[{video_track.faucet}]{','.join(video_filters)}[video]"
                )
                mapping.append("[video]")
            else:
                mapping.append(video_track.faucet)

        # Audio
        if len(self.audio_tracks) == 1:
            # Only one audio track
            # Keep it intact - not much else we can do
            mapping.append(self.audio_tracks[0].faucet)

        elif len(self.audio_tracks) > 1:
            if all(track.channels == 1 for track in self.audio_tracks):
                # All audio tracks are mono

                if self.profile.get("audio_layout") == "smca":
                    # merge all mono audio tracks into
                    # a single multi-channel audio track
                    link = ""
                    for track in self.audio_tracks:
                        link += f"[{track.faucet}]"
                    audio_chain.append(
                        f"{link}amerge=inputs={len(self.audio_tracks)}[audio]"
                    )
                    mapping.append("[audio]")

                else:
                    # Keep each track intact
                    mapping.extend([f"{track.faucet}" for track in self.audio_tracks])

            else:
                # Each track has different number of channels

                # TODO: option to merge all audio tracks into a single track
                # like 2xstereo -> 1x4ch etc

                # For now:
                # Keep as is
                for track in self.audio_tracks:
                    mapping.append(f"{track.faucet}")

        return video_chain, audio_chain, mapping

    def start(self, progress_handler) -> bool:
        video_chain, audio_chain, mapping = self.create_filter_chain()

        if not mapping:
            nebula.log.error("No tracks found")
            return False

        cmd = ["-i", self.source_path]

        filter_chain = ",".join(video_chain + audio_chain)
        if filter_chain:
            cmd.extend(["-filter_complex", filter_chain])

        for track in mapping:
            cmd.extend(["-map", track])

        # Check whether we can use fast import

        use_fast_import = False
        if self.profile.get("video_fast_import"):
            for fast_import_conds in self.profile["video_fast_import"]:
                for key, value in fast_import_conds.items():
                    if getattr(self.video_track, key) != value:
                        break
                else:
                    use_fast_import = True
                    break

        # Use fast import if possible

        if use_fast_import:
            cmd.extend(["-c:v", "copy"])
        else:
            cmd.extend(self.profile["video_encoding"])

        # Audio and output

        cmd.extend(self.profile["audio_encoding"])
        cmd.append(self.target_path)

        return ffmpeg(
            *cmd, progress_handler=lambda x: progress_handler(x / self.duration * 100)
        )
