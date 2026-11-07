import pytest

import nebula
from nebula.helpers import bin_refresh, get_item_event


def test_get_item_event(db: nebula.DB) -> None:
    db.query(
        "SELECT i.id, e.id FROM items i JOIN events e ON e.id_magic = i.id_bin"
        " WHERE e.id_channel = ANY(%s) LIMIT 1",
        [[ch.id for ch in nebula.settings.playout_channels]],
    )
    rows = db.fetchall()
    if not rows:
        pytest.skip("no scheduled items (make seed)")
    id_item, id_event = rows[0]
    event = get_item_event(id_item, db=db)
    assert event and event.id == id_event


def test_get_item_event_unknown_item(db: nebula.DB) -> None:
    assert get_item_event(-1, db=db) is None


def test_bin_refresh_without_bins(db: nebula.DB) -> None:
    # Used to build "IN ()", a syntax error
    bin_refresh([0], db=db)
