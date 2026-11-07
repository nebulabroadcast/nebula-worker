# Plain asserts, no pytest. Needs the stack (importing nebula connects to the db):
#   docker compose exec worker python services/import/test_transcoder.py
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path[:0] = [str(Path(__file__).parent), str(Path(__file__).parents[2])]

from transcoder import VideoTrack, fit_frame, mediainfo  # noqa: E402


def track(width: int, height: int, aspect: float) -> VideoTrack:
    return VideoTrack(
        commercial_name=None,
        faucet="0:0",
        index=0,
        width=width,
        height=height,
        fps=25,
        aspect=aspect,
    )


def fit(width: int, height: int, aspect: float) -> list[str]:
    return fit_frame(track(width, height, aspect), 1920, 1080)


def filters(w: int, h: int) -> list[str]:
    return [
        "fieldorder=tff",
        f"scale={w}:{h}:interl=-1",
        "setsar=1",
        "pad=1920:1080:(ow-iw)/2:(oh-ih)/2",
    ]


assert fit(1920, 1080, 16 / 9) == []
assert fit(1440, 1080, 16 / 9) == filters(1920, 1080)
assert fit(720, 576, 4 / 3) == filters(1440, 1080)
assert fit(1920, 800, 2.35) == filters(1920, 816)
assert fit(1998, 1080, 1.85) == filters(1920, 1036)  # even pad offset (22)
assert fit(1080, 1920, 9 / 16) == filters(608, 1080)  # portrait

# Rotated phone video: mediainfo reports the stored 1920x1080
with tempfile.TemporaryDirectory() as tmp:
    plain, path = Path(tmp) / "plain.mp4", Path(tmp) / "rotated.mp4"
    ffmpeg = ["ffmpeg", "-loglevel", "error"]
    subprocess.run(
        [*ffmpeg, "-f", "lavfi", "-i", "testsrc=s=1920x1080:d=1", str(plain)],
        check=True,
    )
    # display_rotation is an input option: remux to set the rotation flag
    subprocess.run(
        [*ffmpeg, "-display_rotation", "90", "-i", str(plain), "-c", "copy", str(path)],
        check=True,
    )
    video = mediainfo(path).video_track
    assert (video.width, video.height) == (1080, 1920), video
    assert abs(video.aspect - 9 / 16) < 0.01, video

print("ok")
