"""The meta service turning assets online/offline from their files."""

import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from xml.etree.ElementTree import fromstring

import pytest

import nebula
from nebula.enum import JobState, MediaType, ObjectStatus
from nebula.jobs import Job, get_job, send_to
from nebula.storages import Storages

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
    return tmp_path


@pytest.fixture
def meta() -> Iterator[object]:
    from services.meta import Service

    yield Service(9999, fromstring('<settings restart_on_update="all"/>'))


@pytest.fixture
def file_asset(db: nebula.DB, asset: nebula.Asset, storage: Path) -> nebula.Asset:
    subprocess.run(
        [
            "ffmpeg",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc=d=2",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            str(storage / "clip.mp4"),
        ],
        check=True,
    )
    asset["media_type"] = MediaType.FILE
    asset["content_type"] = 2  # video
    asset["id_storage"] = STORAGE
    asset["path"] = "clip.mp4"
    asset["status"] = ObjectStatus.CREATING
    asset.save(notify=False)
    return asset


def reload(asset: nebula.Asset) -> nebula.Asset:
    return nebula.Asset(asset.id, db=nebula.DB())


def test_new_file_turns_online(meta, file_asset: nebula.Asset, storage: Path) -> None:
    size = (storage / "clip.mp4").stat().st_size
    meta.process(reload(file_asset))
    fresh = reload(file_asset)
    # A fresh file waits for "completion assurance" first
    assert fresh["status"] == ObjectStatus.CREATING
    assert fresh["file/size"] == size
    assert fresh["video/codec"] == "h264"

    # A writer that vouches for the size skips the wait
    fresh["__file/written"] = size
    fresh.save(set_mtime=False, notify=False)
    meta.process(reload(file_asset))
    fresh = reload(file_asset)
    assert fresh["status"] == ObjectStatus.ONLINE
    assert "__file/written" not in fresh.meta


def online(meta, asset: nebula.Asset, storage: Path) -> None:
    meta.process(reload(asset))
    fresh = reload(asset)
    fresh["__file/written"] = (storage / "clip.mp4").stat().st_size
    fresh.save(set_mtime=False, notify=False)
    meta.process(reload(asset))
    assert reload(asset)["status"] == ObjectStatus.ONLINE


def test_missing_file_turns_offline(
    meta, file_asset: nebula.Asset, storage: Path
) -> None:
    online(meta, file_asset, storage)
    (storage / "clip.mp4").unlink()
    meta.process(reload(file_asset))
    assert reload(file_asset)["status"] == ObjectStatus.OFFLINE


def test_empty_file_turns_offline(
    meta, file_asset: nebula.Asset, storage: Path
) -> None:
    online(meta, file_asset, storage)
    (storage / "clip.mp4").write_bytes(b"")
    meta.process(reload(file_asset))
    assert reload(file_asset)["status"] == ObjectStatus.OFFLINE


def test_corrupted_file(meta, file_asset: nebula.Asset, storage: Path) -> None:
    (storage / "clip.mp4").write_bytes(b"not a video" * 100)
    fresh = reload(file_asset)
    fresh["__file/written"] = 1100
    fresh.save(set_mtime=False, notify=False)
    meta.process(reload(file_asset))
    meta.process(reload(file_asset))
    assert reload(file_asset)["status"] == ObjectStatus.CORRUPTED


def test_changed_file_restarts_jobs(
    db: nebula.DB,
    meta,
    file_asset: nebula.Asset,
    storage: Path,
    make_action: Callable[..., int],
) -> None:
    online(meta, file_asset, storage)
    id_action = make_action("")
    id_job = send_to(file_asset.id, id_action, db=db)
    job = get_job(901, [id_action])
    assert job
    job.done()

    # New content, different size
    subprocess.run(
        [
            "ffmpeg",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=d=4",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            str(storage / "clip.mp4"),
        ],
        check=True,
    )
    online(meta, file_asset, storage)
    assert Job(id_job).get_status() == JobState.RESTART


def test_restart_on_update_list(
    db: nebula.DB,
    file_asset: nebula.Asset,
    storage: Path,
    make_action: Callable[..., int],
) -> None:
    from services.meta import Service

    keep, restart = make_action(""), make_action("")
    meta = Service(9999, fromstring(f'<settings restart_on_update="{restart}"/>'))
    online(meta, file_asset, storage)
    jobs = {}
    for id_action in (keep, restart):
        jobs[id_action] = send_to(file_asset.id, id_action, db=db)
        job = get_job(901, [id_action])
        assert job
        job.done()

    (storage / "clip.mp4").write_bytes((storage / "clip.mp4").read_bytes() + b"\0" * 10)
    online(meta, file_asset, storage)
    assert Job(jobs[keep]).get_status() == JobState.COMPLETED
    assert Job(jobs[restart]).get_status() == JobState.RESTART
