import subprocess
import threading
from pathlib import Path

from nxtools.media import ffprobe


def test_missing_file(tmp_path: Path) -> None:
    assert ffprobe(str(tmp_path / "missing.mp4")) == {}


def test_lots_of_stderr_does_not_block(tmp_path: Path) -> None:
    # 3000 tags: ffprobe writes ~250 kB to stderr, more than a pipe holds.
    # Reading stdout first used to block both processes forever.
    meta = tmp_path / "meta.txt"
    meta.write_text(
        ";FFMETADATA1\n" + "".join(f"tag{i}={'x' * 60}\n" for i in range(3000))
    )
    clip = tmp_path / "tags.mkv"
    subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc=d=1"]
        + ["-i", str(meta), "-map_metadata", "1", str(clip)],
        check=True,
    )
    result: dict = {}
    probe = threading.Thread(
        target=lambda: result.update(ffprobe(str(clip))), daemon=True
    )
    probe.start()
    probe.join(timeout=30)
    assert not probe.is_alive(), "ffprobe blocked"
    assert result["streams"][0]["codec_type"] == "video"
    tags = {key.lower(): value for key, value in result["format"]["tags"].items()}
    assert tags["tag2999"] == "x" * 60  # mkv upper-cases tag names
