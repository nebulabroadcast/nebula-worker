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


def test_nested_change_is_saved(db: nebula.DB, asset: nebula.Asset) -> None:
    asset.meta["test/list"] = [1]
    asset.save(notify=False)
    stale = nebula.Asset(asset.id, db=nebula.DB())
    other = nebula.Asset(asset.id, db=nebula.DB())
    other["title"] = "other"
    other.save(notify=False)
    stale.meta["test/list"].append(2)  # in place
    stale.save(notify=False)
    fresh = nebula.Asset(asset.id, db=db)
    assert fresh.meta["test/list"] == [1, 2]
    assert fresh["title"] == "other"


def ft_words(db: nebula.DB, asset: nebula.Asset) -> set[str]:
    db.query(
        "SELECT value FROM ft WHERE object_type = %s AND id = %s",
        [asset.object_type_id, asset.id],
    )
    return {value for (value,) in db.fetchall()}


def test_ft_index_follows_text_changes(db: nebula.DB, asset: nebula.Asset) -> None:
    asset["title"] = "Quetzalcoatl"
    asset.save(notify=False)
    assert "quetzalcoatl" in ft_words(db, asset)

    # A stale copy changing another text key keeps the newer title indexed
    stale = nebula.Asset(asset.id, db=nebula.DB())
    asset["title"] = "Xochipilli"
    asset.save(notify=False)
    stale["genre/original"] = "Tezcatlipoca"
    stale.save(notify=False)
    words = ft_words(db, asset)
    assert {"xochipilli", "tezcatlipoca"} <= words
    assert "quetzalcoatl" not in words

    del asset.meta["title"]
    asset.save(notify=False)
    assert "xochipilli" not in ft_words(db, asset)


def test_ft_index_untouched_without_text_changes(
    db: nebula.DB, asset: nebula.Asset
) -> None:
    db.query(
        "INSERT INTO ft (id, object_type, weight, value) VALUES (%s, %s, 1, 'marker')",
        [asset.id, asset.object_type_id],
    )
    db.commit()
    asset["status"] = ObjectStatus.OFFLINE
    asset.save(notify=False)
    assert "marker" in ft_words(db, asset)
