"""The meta service turning assets online/offline from their files."""

from collections.abc import Callable
from pathlib import Path
from xml.etree.ElementTree import fromstring

import pytest

import nebula
from nebula.enum import JobState, MediaType, ObjectStatus
from nebula.jobs import Job, get_job, send_to
from services.meta import Service
from tests.integration.conftest import STORAGE, clip

H264 = ("-c:v", "libx264", "-preset", "ultrafast")


@pytest.fixture
def meta() -> Service:
    return Service(9999, fromstring('<settings restart_on_update="all"/>'))


@pytest.fixture
def file_asset(db: nebula.DB, asset: nebula.Asset, storage: Path) -> nebula.Asset:
    clip(storage / "clip.mp4", "testsrc=d=2", *H264)
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


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("clip", "Tagged title"),  # auto-generated from the file name
        ("My title", "My title"),  # entered by a user
    ],
)
def test_title_tag_replaces_only_generated_title(
    meta, file_asset: nebula.Asset, storage: Path, title: str, expected: str
) -> None:
    tags = ("-metadata", "title=Tagged title", "-metadata", "description=From tags")
    clip(storage / "clip.mp4", "testsrc=d=2", *H264, *tags)
    file_asset["title"] = title
    file_asset.save(notify=False)
    meta.process(reload(file_asset))
    fresh = reload(file_asset)
    assert fresh["title"] == expected
    assert fresh["description"] == "From tags"


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
    clip(storage / "clip.mp4", "testsrc=d=4", *H264)
    online(meta, file_asset, storage)
    assert Job(id_job).get_status() == JobState.RESTART


def test_restart_on_update_list(
    db: nebula.DB,
    file_asset: nebula.Asset,
    storage: Path,
    make_action: Callable[..., int],
) -> None:
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
