import os
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from xml.etree.ElementTree import fromstring

import pytest

from nebula.ffmpeg import AbortWatcher
from services.conv import common, ffmpeg
from services.conv.common import ConversionAborted, ConversionError
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


def temp_has_data(enc: NebulaFFMPEG) -> bool:
    (temp,) = enc.files
    return os.path.exists(temp) and os.path.getsize(temp) > 0


def test_abort_leaves_no_temp_file(encoder: NebulaFFMPEG, tmp_path: Path) -> None:
    (temp,) = encoder.files
    # Abort once ffmpeg is writing; asked by the watcher, not on progress
    with pytest.raises(ConversionAborted):
        encoder.run(lambda progress: None, lambda: temp_has_data(encoder))
    assert not (tmp_path / "out.mp4").exists()  # nothing moved into place
    encoder.remove_temp_files()
    assert not os.path.exists(temp)


def test_abort_just_as_ffmpeg_exits(encoder: NebulaFFMPEG) -> None:
    # Too late to stop anything, but the job must not end as done
    short_source(encoder)
    done: list[bool] = []

    def check_abort() -> bool:
        return bool(done)

    def run() -> None:
        encoder.run(lambda progress: done.append(True), check_abort)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(AbortWatcher, "interval", 3600)  # never asks while running
    try:
        with pytest.raises(ConversionAborted):
            run()
    finally:
        monkey.undo()


def test_failed_ffmpeg_is_a_failure(encoder: NebulaFFMPEG, tmp_path: Path) -> None:
    os.remove(encoder.asset.file_path)
    with pytest.raises(ConversionError, match="No such file"):
        encoder.run(lambda progress: None)
    assert not (tmp_path / "out.mp4").exists()


def short_source(encoder: NebulaFFMPEG) -> None:
    subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc=d=1"]
        + ["-c:v", "libx264", "-preset", "ultrafast", encoder.asset.file_path],
        check=True,
    )


def test_finished_encode_is_moved(encoder: NebulaFFMPEG, tmp_path: Path) -> None:
    short_source(encoder)
    progress: list[float] = []
    encoder.run(progress.append, lambda: False)
    assert progress and progress[-1] > 0
    encoder.finalize()
    assert (tmp_path / "out.mp4").stat().st_size > 0
    assert not any(os.path.exists(t) for t in encoder.files)


def test_failing_progress_handler_kills_ffmpeg(encoder: NebulaFFMPEG) -> None:
    # e.g. the database is away while reporting progress
    def handler(progress: float) -> None:
        raise RuntimeError("db down")

    started = time.time()
    with pytest.raises(RuntimeError):
        encoder.run(handler)
    assert time.time() - started < 20  # not the whole veryslow encode
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
    assert args[:4] == ["-ss", "5", "-i", "in.mp4"]
    assert args[4:-1] == ["-r", "25", "-vf", "scale=640:360", "-an"]


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
