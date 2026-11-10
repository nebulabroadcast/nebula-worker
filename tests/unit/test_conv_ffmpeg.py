import os
import signal
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from xml.etree.ElementTree import fromstring

import pytest

from services.conv import common, ffmpeg
from services.conv.common import ConversionError
from services.conv.ffmpeg import NebulaFFMPEG

TASK = """
<task mode="ffmpeg">
    <param name="c:v">"libx264"</param>
    <param name="preset:v">"veryslow"</param>
    <output storage="1">"out.mp4"</output>
</task>
"""


class FakeAsset(dict):
    file_path = ""


@pytest.fixture
def encoder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> NebulaFFMPEG:
    storage = SimpleNamespace(is_writable=True, local_path=str(tmp_path))
    monkeypatch.setattr(ffmpeg, "storages", {1: storage})
    monkeypatch.setattr(common, "storages", {1: storage})
    source = tmp_path / "source.mp4"
    subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc=d=30"]
        + ["-c:v", "libx264", "-preset", "ultrafast", str(source)],
        check=True,
    )
    asset = FakeAsset(duration=30)
    asset.file_path = str(source)
    enc = NebulaFFMPEG(asset, fromstring(TASK), {})  # type: ignore[arg-type]
    enc.configure()
    return enc


def start(enc: NebulaFFMPEG) -> str:
    (temp,) = enc.files
    enc.start()
    for _ in range(200):
        if os.path.exists(temp) and os.path.getsize(temp) > 0:
            break
        time.sleep(0.05)
    return temp


def test_killed_ffmpeg_is_a_failure(encoder: NebulaFFMPEG, tmp_path: Path) -> None:
    temp = start(encoder)
    assert encoder.proc
    encoder.proc.send_signal(signal.SIGKILL)
    encoder.wait(lambda progress: None)
    assert encoder.proc.returncode == -signal.SIGKILL
    with pytest.raises(ConversionError):
        encoder.finalize()
    assert not (tmp_path / "out.mp4").exists()  # nothing moved into place
    encoder.remove_temp_files()
    assert not os.path.exists(temp)


def test_abort_leaves_no_temp_file(encoder: NebulaFFMPEG) -> None:
    temp = start(encoder)
    encoder.stop()
    encoder.wait(lambda progress: None)
    assert encoder.aborted
    encoder.remove_temp_files()
    assert not os.path.exists(temp)


def test_finished_encode_is_moved(encoder: NebulaFFMPEG, tmp_path: Path) -> None:
    # Short source: replace the 30 s one
    subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc=d=1"]
        + ["-c:v", "libx264", "-preset", "ultrafast", encoder.asset.file_path],
        check=True,
    )
    encoder.start()
    encoder.wait(lambda progress: None)
    encoder.finalize()
    assert (tmp_path / "out.mp4").stat().st_size > 0
    assert not encoder.files or not any(os.path.exists(t) for t in encoder.files)


def test_kill_stops_running_encoder(encoder: NebulaFFMPEG) -> None:
    # conv calls kill() when wait() fails, e.g. progress reporting raised
    start(encoder)
    with pytest.raises(RuntimeError):
        encoder.wait(lambda progress: (_ for _ in ()).throw(RuntimeError("db down")))
    assert encoder.proc and encoder.proc.poll() is None  # still encoding
    encoder.kill()
    assert encoder.proc.poll() is not None
    encoder.remove_temp_files()


def configured(task: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list:
    """ffmpeg arguments for a task; configure() doesn't read the source."""
    storage = SimpleNamespace(is_writable=True, local_path=str(tmp_path))
    monkeypatch.setattr(ffmpeg, "storages", {1: storage})
    monkeypatch.setattr(common, "storages", {1: storage})
    asset = FakeAsset(duration=10, title="x")
    asset.file_path = "in.mp4"
    enc = NebulaFFMPEG(asset, fromstring(task), {})  # type: ignore[arg-type]
    enc.configure()
    return enc.ffparams


def test_paramset_params_like_top_level(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Values on their own indented line used to raise IndentationError in a
    # paramset; ss goes before -i (input seeking) in both places
    args = configured(
        """
        <task mode="ffmpeg">
            <param name="r">
                25
            </param>
            <paramset condition="asset['title'] == 'x'">
                <param name="vf">
                    "scale=640:360"
                </param>
                <param name="ss">5</param>
                <param name="an"></param>
            </paramset>
            <output storage="1">"out.mp4"</output>
        </task>
        """,
        tmp_path,
        monkeypatch,
    )
    assert args[:5] == ["-y", "-ss", "5", "-i", "in.mp4"]
    assert args[5:-1] == ["-r", "25", "-vf", "scale=640:360", "-an"]


def test_paramset_condition_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = configured(
        """
        <task mode="ffmpeg">
            <paramset condition="asset['title'] == 'y'">
                <param name="vf">"scale=640:360"</param>
            </paramset>
            <output storage="1">"out.mp4"</output>
        </task>
        """,
        tmp_path,
        monkeypatch,
    )
    assert "-vf" not in args
