from nebula.objects.base import Meta


def test_tracks_every_kind_of_change() -> None:
    m = Meta({"a": 1, "b": 2, "c": 3})
    assert not m.changed and not m.deleted
    m["a"] = 10
    del m["b"]
    m.pop("c")
    m.pop("missing", None)
    m.update(d=4)
    m.setdefault("e", 5)
    m.setdefault("a", 0)  # existing key: no change
    assert m.changed == {"a", "d", "e"}
    assert m.deleted == {"b", "c"}


def test_readded_key_is_not_deleted() -> None:
    m = Meta({"a": 1})
    del m["a"]
    m["a"] = 2
    assert m.changed == {"a"} and not m.deleted


def test_saved_resets() -> None:
    m = Meta({"a": 1})
    m["a"] = 2
    m.clear()
    assert m.deleted == {"a"}
    m.saved()
    assert not m.changed and not m.deleted


def test_changes_that_bypass_item_assignment() -> None:
    m = Meta({"a": 1, "tracks": [{"ch": 2}], "status": {"x": 1}})
    m |= {"b": 2}
    dict.update(m, c=3)  # what a plugin might do
    m["tracks"].append({"ch": 1})  # in place, nested
    m["status"]["x"] = 2
    assert m.changed == {"b", "c", "tracks", "status"}


def test_same_value_is_no_change() -> None:
    m = Meta({"a": 1, "tracks": [1, 2]})
    m["a"] = 1
    m["tracks"] = [1, 2]
    assert not m.changed and not m.deleted


def test_assigning_a_plain_dict_keeps_what_was_stored() -> None:
    from nebula.objects import Asset

    asset = Asset(meta={"id": 1, "title": "a", "genre": "b", "media_type": 0})
    asset.meta = {"id": 1, "title": "new", "media_type": 0, "content_type": 0}
    assert isinstance(asset.meta, Meta)
    assert asset.meta.changed == {"title", "content_type"}
    assert asset.meta.deleted == {"genre"}
