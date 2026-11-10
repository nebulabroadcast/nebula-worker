import os
from pathlib import Path

import pytest

from nebula.enum import ObjectStatus as S
from services.meta import needs_processing


@pytest.fixture
def clip(tmp_path: Path) -> tuple[str, int]:
    path = tmp_path / "clip.mxf"
    path.write_bytes(b"x" * 100)
    return str(path), int(os.stat(path).st_mtime)


@pytest.mark.parametrize(
    ("status", "expected"), [(S.ONLINE, True), (S.CREATING, True), (S.OFFLINE, False)]
)
def test_missing_file(tmp_path: Path, status: S, expected: bool) -> None:
    # Only assets that claim to have a file need turning offline
    assert needs_processing(str(tmp_path / "missing"), status, None, None) is expected


def test_directory_is_not_a_file(tmp_path: Path) -> None:
    assert needs_processing(str(tmp_path), S.ONLINE, None, None)


def test_empty_file(tmp_path: Path) -> None:
    empty = tmp_path / "empty.mxf"
    empty.touch()
    assert needs_processing(str(empty), S.ONLINE, 0, 0)
    assert not needs_processing(str(empty), S.OFFLINE, 0, 0)


def test_unchanged_file(clip: tuple[str, int]) -> None:
    path, mtime = clip
    assert not needs_processing(path, S.ONLINE, mtime, 100)
    # Hot statuses and offline assets are always looked at
    for status in (S.CREATING, S.RESET, S.OFFLINE):
        assert needs_processing(path, status, mtime, 100)


def test_changed_file(clip: tuple[str, int]) -> None:
    path, mtime = clip
    assert needs_processing(path, S.ONLINE, mtime, 99)
    assert needs_processing(path, S.ONLINE, mtime - 10, 100)
