import json
import subprocess
import tempfile
import tomllib
from collections.abc import Callable
from functools import cached_property
from pathlib import Path
from typing import Any

from pydantic import BaseModel, field_validator

import nebula

PROFILES_DIR = Path(__file__).parent / "profiles"


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


class ImportProfile(BaseModel):
    """Target format of an import, one profiles/<name>.toml per profile."""

    # "smca": merge several mono audio tracks into one multichannel track
    audio_layout: str | None = None
    # Sources matching all keys of any entry are copied, not re-encoded
    video_fast_import: list[dict[str, Any]] = []
    # Video options used instead of video_encoding for a fast import
    video_copy: list[str] = ["-c:v", "copy"]
    video_encoding: list[str]
    audio_encoding: list[str]

    @field_validator("video_fast_import")
    @classmethod
    def check_fast_import_keys(
        cls, value: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        for conds in value:
            if unknown := set(conds) - set(VideoTrack.model_fields):
                raise ValueError(f"unknown video track fields {sorted(unknown)}")
        return value


def load_profiles() -> dict[str, ImportProfile]:
    """Load profiles/*.toml. A broken file is logged and skipped."""
    profiles = {}
    for path in sorted(PROFILES_DIR.glob("*.toml")):
        try:
            with path.open("rb") as f:
                profiles[path.stem] = ImportProfile(**tomllib.load(f))
        except Exception as e:
            nebula.log.error(f"Invalid import profile {path.name}: {e}")
    return profiles


class AudioTrack(BaseTrack):
    channels: int
    sample_rate: int


class MediaInfo(BaseModel):
    duration: float
    video_track: VideoTrack
    audio_tracks: list[AudioTrack]


def mediainfo(path: Path) -> MediaInfo:
    command = ["mediainfo", "--Output=JSON", str(path)]
    if not path.exists():
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
            try:
                duration = float(track["Duration"])
            except (KeyError, ValueError):
                raise Exception(f"Invalid source duration of {path}") from None
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


def run_ffmpeg(args: list[str], progress_handler: Callable[[float], None]) -> None:
    """Run ffmpeg, reporting progress as seconds of output written.

    Raises an exception with ffmpeg's error output on failure.
    """
    cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-nostats", "-loglevel", "error"]
    cmd.extend(["-progress", "pipe:1", *args])
    nebula.log.debug("Executing", " ".join(cmd))

    # stderr goes to a file: a full pipe would block ffmpeg while stdout is read
    with tempfile.TemporaryFile() as stderr:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=stderr, text=True)
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                key, _, value = line.strip().partition("=")
                if key == "out_time_us" and value.isdigit():
                    progress_handler(int(value) / 1_000_000)
        except BaseException:
            # Don't leave ffmpeg running if reporting progress fails
            proc.kill()
            proc.wait()
            raise
        proc.wait()
        if proc.returncode:
            stderr.seek(0)
            lines = stderr.read().decode("utf-8", errors="replace").splitlines()
            # The cause comes first; a broken source can repeat errors a lot
            error = "\n".join(lines[:50]).strip()
            raise Exception(error or f"ffmpeg exited with {proc.returncode}")


class ImportTranscoder:
    source_path: Path
    target_path: Path

    def __init__(self, source_path: Path, target_path: Path, profile: ImportProfile):
        self.source_path = source_path
        self.target_path = target_path
        self.profile = profile

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

                if self.profile.audio_layout == "smca":
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

    def start(self, progress_handler) -> None:
        """Transcode the source. Raises an exception with the reason on failure."""
        video_chain, audio_chain, mapping = self.create_filter_chain()

        if not mapping:
            raise Exception("No tracks found")

        cmd = ["-i", str(self.source_path)]

        filter_chain = ",".join(video_chain + audio_chain)
        if filter_chain:
            cmd.extend(["-filter_complex", filter_chain])

        for track in mapping:
            cmd.extend(["-map", track])

        # Check whether we can use fast import

        use_fast_import = False
        if self.profile.video_fast_import:
            for fast_import_conds in self.profile.video_fast_import:
                for key, value in fast_import_conds.items():
                    if getattr(self.video_track, key) != value:
                        break
                else:
                    use_fast_import = True
                    break

        # Use fast import if possible

        if use_fast_import:
            cmd.extend(self.profile.video_copy)
        else:
            cmd.extend(self.profile.video_encoding)

        # Audio and output

        cmd.extend(self.profile.audio_encoding)
        cmd.append(str(self.target_path))

        run_ffmpeg(cmd, lambda x: progress_handler(x / self.duration * 100))
