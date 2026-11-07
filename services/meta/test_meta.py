# Plain asserts, no pytest. Needs the stack (importing nebula connects to the db):
#   docker compose exec worker python services/meta/test_meta.py
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from nebula.enum import ObjectStatus  # noqa: E402
from services.meta import needs_processing  # noqa: E402

ONLINE, OFFLINE = ObjectStatus.ONLINE, ObjectStatus.OFFLINE
CREATING, RESET = ObjectStatus.CREATING, ObjectStatus.RESET

with tempfile.TemporaryDirectory() as tmp:
    missing = os.path.join(tmp, "missing.mxf")
    empty = os.path.join(tmp, "empty.mxf")
    clip = os.path.join(tmp, "clip.mxf")
    Path(empty).touch()
    Path(clip).write_bytes(b"x" * 100)
    mtime = int(os.stat(clip).st_mtime)

    # Missing file: only assets that claim to have one need turning offline
    assert needs_processing(missing, ONLINE, None, None)
    assert needs_processing(missing, CREATING, None, None)
    assert not needs_processing(missing, OFFLINE, None, None)
    # A directory is not a file
    assert needs_processing(tmp, ONLINE, None, None)

    # Empty file: offline unless it already is (or is being retrieved)
    assert needs_processing(empty, ONLINE, mtime, 0)
    assert not needs_processing(empty, OFFLINE, mtime, 0)

    # Unchanged file: nothing to do for a settled asset
    assert not needs_processing(clip, ONLINE, mtime, 100)
    # ... but hot statuses and offline assets are always looked at
    assert needs_processing(clip, CREATING, mtime, 100)
    assert needs_processing(clip, RESET, mtime, 100)
    assert needs_processing(clip, OFFLINE, mtime, 100)

    # Changed size or mtime
    assert needs_processing(clip, ONLINE, mtime, 99)
    assert needs_processing(clip, ONLINE, mtime - 10, 100)

print("ok")
