"""The import service: a matching file replaces the asset's file, safely."""

import importlib
import subprocess
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

import nebula
from nebula.enum import JobState, MediaType, ObjectStatus
from nebula.storages import Storages

importer = importlib.import_module("services.import")  # "import" is a keyword
common = importlib.import_module("services.import.common")
transcoder = importlib.import_module("services.import.transcoder")

STORAGE = 97  # not configured on the stack: patched to a temp dir


@pytest.fixture
def storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    original = Storages.__getitem__

    def getitem(self: Storages, id_storage: int) -> object:
        if id_storage == STORAGE:
            return SimpleNamespace(
                id=STORAGE, local_path=str(tmp_path), protocol="local"
            )
        return original(self, id_storage)

    monkeypatch.setattr(Storages, "__getitem__", getitem)
    (tmp_path / "import").mkdir()
    (tmp_path / "media").mkdir()
    return tmp_path


@pytest.fixture
def service(make_action: Callable[..., int], storage: Path):
    # Without on_init: it reads every import action of the stack
    svc = object.__new__(importer.Service)
    svc.id_service = 9999
    svc.profiles = transcoder.load_profiles()
    svc.action = common.ImportDefinition(
        action_id=make_action("", service_type="import"),
        import_dir=storage / "import",
        profile="xdcamhd422-1080i50",
    )
    return svc


@pytest.fixture
def target(asset: nebula.Asset, storage: Path) -> nebula.Asset:
    (storage / "media" / "old.mxf").write_bytes(b"previous version")
    asset["media_type"] = MediaType.FILE
    asset["id_storage"] = STORAGE
    asset["path"] = "media/old.mxf"
    asset.save(notify=False)
    return asset


def clip(path: Path, size: str = "1280x720") -> Path:
    subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", f"testsrc=s={size}:d=1"]
        + ["-f", "lavfi", "-i", "sine=d=1:r=48000", "-shortest", str(path)],
        check=True,
    )
    return path


def job_of(asset: nebula.Asset, id_action: int) -> dict:
    row = nebula.db.fetchrow(
        "SELECT status, message FROM jobs WHERE id_asset = %s AND id_action = %s",
        asset.id,
        id_action,
    )
    assert row
    return row


def test_successful_import(service, target: nebula.Asset, storage: Path) -> None:
    source = clip(storage / "import" / f"{target.id}.mp4")
    assert service.import_file(service.action, source) is None

    fresh = nebula.Asset(target.id, db=nebula.DB())
    new_file = storage / "media" / "old.mxf"
    assert new_file.read_bytes() != b"previous version"
    assert fresh["status"] == ObjectStatus.CREATING
    assert fresh["__file/written"] == new_file.stat().st_size
    assert (storage / "import" / "backup" / source.name).exists()
    assert not source.exists()
    # The previous version is kept
    assert list((storage / ".nx" / "versions").rglob("*.mxf"))
    assert job_of(target, service.action.action_id)["status"] == JobState.COMPLETED

    video = transcoder.mediainfo(new_file).video_track
    assert (video.width, video.height) == (1920, 1080)  # profile frame size
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v"]
        + ["-show_entries", "stream=field_order", "-of", "csv=p=0", str(new_file)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert probe.stdout.strip().rstrip(",") == "tt"  # top field first


def test_failed_import_keeps_asset_file(
    service, target: nebula.Asset, storage: Path
) -> None:
    source = storage / "import" / f"{target.id}.mp4"
    source.write_bytes(b"not a video" * 1000)
    service.import_file(service.action, source)

    assert (storage / "media" / "old.mxf").read_bytes() == b"previous version"
    failed = storage / "import" / "failed"
    assert (failed / source.name).exists()
    assert (failed / f"{target.id}.txt").read_text()
    job = job_of(target, service.action.action_id)
    assert job["status"] == JobState.FAILED
    assert job["message"].startswith("Import failed")
    # No temp files left behind
    assert not list((storage / ".nx" / "creating").glob("*"))


def test_unexpected_file_is_left_with_error(service, storage: Path) -> None:
    source = clip(storage / "import" / "no-such-asset.mp4")
    assert service.import_file(service.action, source) is None
    assert source.exists()
    assert "Unexpected file" in (storage / "import" / "no-such-asset.txt").read_text()
