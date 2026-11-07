import nebula
from nebula.enum import ObjectStatus


def test_concurrent_saves_keep_both_changes(db: nebula.DB, asset: nebula.Asset) -> None:
    # Two stale copies changing different keys don't overwrite each other
    a = nebula.Asset(asset.id, db=nebula.DB())
    b = nebula.Asset(asset.id, db=nebula.DB())
    a["title"] = "from a"
    a["status"] = ObjectStatus.OFFLINE
    a.save(notify=False)
    b["genre/original"] = "from b"
    b.save(notify=False)

    fresh = nebula.Asset(asset.id, db=db)
    assert fresh["title"] == "from a"
    assert fresh["genre/original"] == "from b"
    assert fresh["status"] == ObjectStatus.OFFLINE


def test_column_follows_meta(db: nebula.DB, asset: nebula.Asset) -> None:
    asset["status"] = ObjectStatus.OFFLINE
    asset.save(notify=False)
    db.query("SELECT status, meta->>'status' FROM assets WHERE id = %s", [asset.id])
    assert db.fetchall()[0] == (ObjectStatus.OFFLINE, str(int(ObjectStatus.OFFLINE)))


def test_deleted_key_is_removed(db: nebula.DB, asset: nebula.Asset) -> None:
    asset["genre/original"] = "x"
    asset.save(notify=False)
    other = nebula.Asset(asset.id, db=nebula.DB())
    other["title"] = "kept"
    other.save(notify=False)
    del asset.meta["genre/original"]  # a stale copy deletes one key
    asset.save(notify=False)

    fresh = nebula.Asset(asset.id, db=db)
    assert "genre/original" not in fresh.meta
    assert fresh["title"] == "kept"


def test_save_without_changes_writes_nothing(
    db: nebula.DB, asset: nebula.Asset
) -> None:
    db.query("SELECT meta FROM assets WHERE id = %s", [asset.id])
    before = db.fetchall()[0][0]
    nebula.Asset(asset.id, db=db).save(set_mtime=False, notify=False)
    db.query("SELECT meta FROM assets WHERE id = %s", [asset.id])
    assert db.fetchall()[0][0] == before
