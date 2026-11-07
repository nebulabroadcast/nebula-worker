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
