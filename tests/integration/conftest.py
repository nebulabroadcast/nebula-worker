"""Integration tests write to the stack database.

Each test makes its own asset and actions (ids from SCRATCH_ACTION up) and
removes them afterwards, so they can run against a database in use.
"""

from collections.abc import Callable, Iterator

import pytest

import nebula
from nebula.enum import ObjectStatus
from nebula.jobs.actions import actions

SCRATCH_ACTION = 990


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
