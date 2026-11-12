"""Probed metadata applied to an asset, and the tags import takes from a source."""

import importlib
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from nebula.metadata import probe
from nebula.settings.metatypes import MetaType

process = importlib.import_module("services.import.process")  # "import" is a keyword


class FakeAsset(dict[str, Any]):
    file_path = "/media/clip.mxf"

    def __missing__(self, key: str) -> None:
        return None


@pytest.fixture(autouse=True)
def metatypes(monkeypatch: pytest.MonkeyPatch) -> None:
    types = {
        "title": MetaType(ns="m"),
        "description": MetaType(ns="m"),
        "video/codec": MetaType(ns="f"),
    }
    # Settings load from the database on first access: mark them loaded.
    # (`nebula.settings` is the settings object, it shadows the module)
    module = sys.modules["nebula.settings"]
    monkeypatch.setattr(module, "_loaded", True)
    monkeypatch.setattr(module._settings, "metatypes", types)


def apply(asset: FakeAsset, **meta: Any) -> FakeAsset:
    probe.apply_probed_meta(asset, meta)  # type: ignore[arg-type]
    return asset


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        (None, "Tagged"),  # no title yet
        ("clip", "Tagged"),  # auto-generated from the file name
        ("My title", "My title"),  # entered by a user
    ],
)
def test_title(title: str | None, expected: str) -> None:
    assert apply(FakeAsset(title=title), title="Tagged")["title"] == expected


def test_descriptive_values_only_fill_empty_fields() -> None:
    assert apply(FakeAsset(), description="Tag")["description"] == "Tag"
    asset = FakeAsset(description="Mine")
    assert apply(asset, description="Tag")["description"] == "Mine"


def test_technical_values_are_overwritten() -> None:
    asset = FakeAsset({"video/codec": "h264"})
    assert apply(asset, **{"video/codec": "mpeg2video"})["video/codec"] == "mpeg2video"


def test_unknown_keys_are_ignored() -> None:
    assert "audio_tracks" not in apply(FakeAsset(), audio_tracks=[])


def test_source_tags_are_descriptive_only(tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc=d=1"]
        + ["-metadata", "title=Tagged", "-metadata", "artist=Someone", str(source)],
        check=True,
    )
    assert process.probe_source_tags(source) == {
        "title": "Tagged",
        "role/performer": "Someone",
    }


def test_unreadable_source_has_no_tags(tmp_path: Path) -> None:
    broken = tmp_path / "broken.mp4"
    broken.write_bytes(b"not a video")
    assert process.probe_source_tags(broken) == {}
    assert process.probe_source_tags(tmp_path / "missing.mp4") == {}
