"""The broker creating jobs from action create_if conditions."""

from collections.abc import Callable

import nebula
from nebula.jobs.actions import actions


def broker_for(id_action: int):
    from services.broker import Service

    broker = Service(9999, None)
    broker.actions = [actions[id_action]]
    broker.created_keys = [actions[id_action].created_key]
    return broker


def jobs_of(id_asset: int, id_action: int) -> int:
    db = nebula.DB()
    db.query(
        "SELECT count(*) FROM jobs WHERE id_asset = %s AND id_action = %s",
        [id_asset, id_action],
    )
    row = db.fetchone()
    assert row
    return row[0]


def test_creates_job_once(asset: nebula.Asset, make_action: Callable[..., int]) -> None:
    id_action = make_action(
        "<create_if>asset['title'] == 'integration test'</create_if>"
    )
    broker = broker_for(id_action)
    broker.proc(nebula.Asset(asset.id, db=nebula.DB()))
    assert jobs_of(asset.id, id_action) == 1
    fresh = nebula.Asset(asset.id, db=nebula.DB())
    assert fresh[actions[id_action].created_key] == 1

    # Marked as created: not again, even after the job is gone
    db = nebula.DB()
    db.query("DELETE FROM jobs WHERE id_action = %s", [id_action])
    db.commit()
    broker.proc(fresh)
    assert jobs_of(asset.id, id_action) == 0


def test_no_match_no_job(asset: nebula.Asset, make_action: Callable[..., int]) -> None:
    id_action = make_action("<create_if>asset['title'] == 'something else'</create_if>")
    broker_for(id_action).proc(nebula.Asset(asset.id, db=nebula.DB()))
    assert jobs_of(asset.id, id_action) == 0


def test_marking_does_not_overwrite_other_changes(
    asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    # The broker saves a copy loaded at the start of its scan
    id_action = make_action("<create_if>True</create_if>")
    stale = nebula.Asset(asset.id, db=nebula.DB())
    edited = nebula.Asset(asset.id, db=nebula.DB())
    edited["title"] = "edited meanwhile"
    edited.save(notify=False)
    broker_for(id_action).proc(stale)
    assert nebula.Asset(asset.id, db=nebula.DB())["title"] == "edited meanwhile"
