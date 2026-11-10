"""Integration tests write to the stack database.

Each test makes its own asset and actions (ids from SCRATCH_ACTION up) and
removes them afterwards, so they can run against a database in use.
"""

import subprocess
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from nebula.db import connection_kwargs

# Before anything queries: DB() waits forever for a database that isn't there
try:
    psycopg.connect(**connection_kwargs(), connect_timeout=3).close()
except psycopg.OperationalError as e:
    pytest.exit(f"Database not available: {e}", returncode=3)

import nebula  # noqa: E402
from nebula.enum import ObjectStatus  # noqa: E402
from nebula.jobs.actions import actions  # noqa: E402
from nebula.storages import Storages  # noqa: E402

SCRATCH_ACTION = 990
# Patched to a temp dir here. A meta service running in the stack must leave
# its assets alone, so the stack has to define it as a network storage that
# is never mounted (e.g. samba with an unreachable path): an undefined
# storage counts as local, and meta turns assets with no file there offline
# in the middle of a test.
STORAGE = 97


@pytest.fixture
def db() -> Iterator[nebula.DB]:
    db = nebula.DB()
    yield db
    db.rollback()
    db.close()


@pytest.fixture
def asset(db: nebula.DB) -> Iterator[nebula.Asset]:
    asset = nebula.Asset(db=db)
    asset["id_folder"] = 1
    asset["title"] = "integration test"
    asset["status"] = ObjectStatus.ONLINE
    asset.save(notify=False)
    yield asset
    db.rollback()
    asset.delete()


@pytest.fixture
def make_action(db: nebula.DB) -> Iterator[Callable[[str], int]]:
    """Create a conv action with the given settings XML body. Returns its id."""
    created: list[int] = []

    def make(body: str, service_type: str = "conv") -> int:
        id_action = SCRATCH_ACTION + len(created)
        # Left over by an interrupted run?
        db.query("DELETE FROM jobs WHERE id_action = %s", [id_action])
        db.query("DELETE FROM actions WHERE id = %s", [id_action])
        db.query(
            "INSERT INTO actions (id, title, service_type, settings)"
            " VALUES (%s, 'integration test', %s, %s)",
            [id_action, service_type, f"<settings>{body}</settings>"],
        )
        db.commit()
        actions.data.pop(id_action, None)  # drop a cached older version
        created.append(id_action)
        return id_action

    yield make
    db.rollback()
    for id_action in created:
        db.query("DELETE FROM jobs WHERE id_action = %s", [id_action])
        db.query("DELETE FROM actions WHERE id = %s", [id_action])
        actions.data.pop(id_action, None)
    db.commit()


@pytest.fixture
def storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Storage STORAGE, as a local storage in a temp dir."""
    configured = nebula.settings.get_storage(STORAGE)
    if configured is None or configured.protocol == "local":
        db = nebula.DB()
        # last_seen: a stopped worker leaves its services in state 1
        db.query(
            "SELECT id FROM services"
            " WHERE service_type = 'meta' AND state = 1 AND last_seen > %s",
            [time.time() - 30],
        )
        if db.fetchall():
            pytest.skip(
                f"A meta service is running and storage {STORAGE} isn't a network "
                "storage in this stack: it would turn the test assets offline"
            )
    original = Storages.__getitem__

    def getitem(self: Storages, id_storage: int) -> object:
        if id_storage == STORAGE:
            return SimpleNamespace(
                id=STORAGE, local_path=str(tmp_path), protocol="local"
            )
        return original(self, id_storage)

    monkeypatch.setattr(Storages, "__getitem__", getitem)
    return tmp_path


def clip(path: Path, source: str = "testsrc=d=1", *args: str) -> Path:
    """Write a test clip from a lavfi source; args go before the output."""
    subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-y", "-f", "lavfi", "-i", source]
        + [*args, str(path)],
        check=True,
    )
    return path
