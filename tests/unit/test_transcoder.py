import importlib
import shutil
import subprocess
from pathlib import Path

import pytest

# "import" is a keyword: services.import can't be imported with a statement
transcoder = importlib.import_module("services.import.transcoder")


def fit(width: int, height: int, aspect: float) -> list[str]:
    track = transcoder.VideoTrack(
        commercial_name=None,
        faucet="0:0",
        index=0,
        width=width,
        height=height,
        fps=25,
        aspect=aspect,
    )
    return transcoder.fit_frame(track, 1920, 1080)


def filters(w: int, h: int) -> list[str]:
    return [
        "fieldorder=tff",
        f"scale={w}:{h}:interl=-1",
        "setsar=1",
        "pad=1920:1080:(ow-iw)/2:(oh-ih)/2",
    ]


def test_matching_source_is_not_filtered() -> None:
    assert fit(1920, 1080, 16 / 9) == []


@pytest.mark.parametrize(
    ("source", "scaled"),
    [
        ((1440, 1080, 16 / 9), (1920, 1080)),  # anamorphic
        ((720, 576, 4 / 3), (1440, 1080)),  # pillarbox
        ((1920, 800, 2.35), (1920, 816)),  # letterbox
        ((1998, 1080, 1.85), (1920, 1036)),  # even pad offset (22)
        ((1080, 1920, 9 / 16), (608, 1080)),  # portrait
    ],
)
def test_fit_frame(source: tuple[int, int, float], scaled: tuple[int, int]) -> None:
    assert fit(*source) == filters(*scaled)


@pytest.mark.skipif(not shutil.which("mediainfo"), reason="needs mediainfo")
def test_rotated_source_size(tmp_path: Path) -> None:
    # Phone video: stored 1920x1080, flagged to be shown rotated
    plain, rotated = tmp_path / "plain.mp4", tmp_path / "rotated.mp4"
    ffmpeg = ["ffmpeg", "-loglevel", "error"]
    subprocess.run(
        [*ffmpeg, "-f", "lavfi", "-i", "testsrc=s=1920x1080:d=1", str(plain)],
        check=True,
    )
    subprocess.run(
        [
            *ffmpeg,
            "-display_rotation",
            "90",
            "-i",
            str(plain),
            "-c",
            "copy",
            str(rotated),
        ],
        check=True,
    )
    video = transcoder.mediainfo(rotated).video_track
    assert (video.width, video.height) == (1080, 1920)
    assert video.aspect == pytest.approx(9 / 16, abs=0.01)


def transcoder_for(width: int, height: int, aspect: float):
    profile = transcoder.load_profiles()["xdcamhd422-1080i50"]
    t = transcoder.ImportTranscoder(Path("in.mp4"), Path("out.mxf"), profile)
    track = transcoder.VideoTrack(
        commercial_name=None,
        faucet="0:0",
        index=0,
        width=width,
        height=height,
        fps=25,
        aspect=aspect,
    )
    t.__dict__["meta"] = transcoder.MediaInfo(  # no mediainfo run
        duration=1, video_track=track, audio_tracks=[]
    )
    return t


def test_profile_filters_on_matching_source() -> None:
    # Nothing to scale, but the frames must still be flagged top field first
    video_chain, _, mapping = transcoder_for(1920, 1080, 16 / 9).create_filter_chain(
        fast_import=False
    )
    assert video_chain == ["[0:0]setfield=tff[video]"]
    assert mapping == ["[video]"]


def test_profile_filters_follow_the_fit() -> None:
    video_chain, _, _ = transcoder_for(720, 576, 4 / 3).create_filter_chain(False)
    assert video_chain[0].endswith(",setfield=tff[video]")
    assert video_chain[0].startswith("[0:0]fieldorder=tff,scale=1440:1080")


def test_fast_import_is_not_filtered() -> None:
    video_chain, _, mapping = transcoder_for(1920, 1080, 16 / 9).create_filter_chain(
        fast_import=True
    )
    assert video_chain == []
    assert mapping == ["0:0"]
