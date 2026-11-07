"""The database adapter (spec 009): DB() compatibility and the new API."""

import gc
import os
import threading
from collections.abc import Iterator

import psycopg
import pytest

import nebula
from nebula import db as nebula_db
from nebula.db import DB

TABLE = "_worker_test_db"


@pytest.fixture(autouse=True)
def table() -> Iterator[None]:
    db = DB()
    db.query(f"CREATE TABLE IF NOT EXISTS {TABLE} (id serial PRIMARY KEY, value text)")
    db.query(f"TRUNCATE {TABLE}")
    db.commit()
    yield
    db.query(f"DROP TABLE IF EXISTS {TABLE}")
    db.commit()


def in_use() -> int:
    stats = nebula_db.get_pool().get_stats()
    return stats["pool_size"] - stats["pool_available"]


def values() -> list[str]:
    db = DB()
    db.query(f"SELECT value FROM {TABLE} ORDER BY id")
    return [row[0] for row in db.fetchall()]


def terminate(pid: int) -> None:
    url = os.environ.get("NEBULA_POSTGRES", "postgres://nebula:nebula@postgres/nebula")
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("SELECT pg_terminate_backend(%s)", [pid])


#
# DB() compatibility
#


def test_read_returns_connection() -> None:
    db = DB()
    db.query("SELECT 1")
    assert db.fetchall() == [(1,)]
    assert in_use() == 0


def test_write_held_until_commit() -> None:
    db = DB()
    db.query(f"INSERT INTO {TABLE} (value) VALUES (%s)", ["a"])
    assert in_use() == 1
    db.query(f"SELECT value FROM {TABLE}")  # own write is visible
    assert db.fetchall() == [("a",)]
    assert values() == []  # not to others
    db.commit()
    assert in_use() == 0
    assert values() == ["a"]


def test_rollback() -> None:
    db = DB()
    db.query(f"INSERT INTO {TABLE} (value) VALUES ('a')")
    db.rollback()
    assert in_use() == 0
    assert values() == []


def test_dropped_handle_rolls_back() -> None:
    db = DB()
    db.query(f"INSERT INTO {TABLE} (value) VALUES ('a')")
    del db
    gc.collect()
    assert in_use() == 0
    assert values() == []


def test_failed_read_does_not_break_next_query() -> None:
    db = DB()
    with pytest.raises(nebula_db.Error):
        db.query("SELECT * FROM no_such_table")
    db.query("SELECT 2")
    assert db.fetchone() == (2,)
    assert in_use() == 0


def test_lastid_before_and_after_commit() -> None:
    db = DB()
    db.query(f"INSERT INTO {TABLE} (value) VALUES ('a')")
    first = db.lastid()
    db.query(f"INSERT INTO {TABLE} (value) VALUES ('b')")
    db.commit()
    assert db.lastid() == first + 1


def test_returning_rows() -> None:
    db = DB()
    db.query(f"INSERT INTO {TABLE} (value) VALUES ('a') RETURNING id")
    (id_new,) = db.fetchone()  # type: ignore[misc]
    db.commit()
    assert id_new


def test_fetchone_then_fetchall() -> None:
    db = DB()
    db.query("SELECT * FROM generate_series(1, 3)")
    assert db.fetchone() == (1,)
    assert db.fetchall() == [(2,), (3,)]
    assert db.fetchone() is None


def test_tuple_parameters_like_psycopg2() -> None:
    db = DB()
    db.query(f"INSERT INTO {TABLE} (id, value) VALUES %s, %s", [(1, "a"), (2, "b")])
    db.commit()
    db.query(f"SELECT value FROM {TABLE} WHERE id IN %s", [(1, 2)])
    assert sorted(db.fetchall()) == [("a",), ("b",)]


def test_literal_percent_without_parameters() -> None:
    db = DB()
    db.query("SELECT 'a%b' LIKE 'a%'")
    assert db.fetchone() == (True,)


def test_cursor_and_connection_access() -> None:
    # Plugins that used the psycopg2 objects directly
    db = DB()
    db.cur.execute(f"INSERT INTO {TABLE} (value) VALUES (%s)", ["a"])
    db.conn.commit()
    db.cur.execute(f"SELECT value FROM {TABLE}")
    assert db.cur.fetchall() == [("a",)]
    db.close()
    assert in_use() == 0


def test_one_handle_many_threads() -> None:
    db = DB()
    errors: list[str] = []

    def work(n: int) -> None:
        for i in range(200):
            db.query("SELECT %s::int", [n * 1000 + i])
            row = db.fetchone()
            if row != (n * 1000 + i,):
                errors.append(f"thread {n} got {row}")

    threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors[:5]


def test_lost_idle_connection_is_replaced() -> None:
    db = DB()
    db.query("SELECT pg_backend_pid()")
    (pid,) = db.fetchone()  # type: ignore[misc]
    terminate(pid)  # what a postgres restart does to the pool
    db.query("SELECT 1")
    assert db.fetchone() == (1,)


def test_lost_held_connection() -> None:
    db = DB()
    db.query(f"INSERT INTO {TABLE} (value) VALUES ('a')")
    db.query("SELECT pg_backend_pid()")
    (pid,) = db.fetchone()  # type: ignore[misc]
    terminate(pid)
    with pytest.raises(nebula_db.OperationalError):
        db.query("SELECT 1")  # the transaction is gone, as with psycopg2
    db.query("SELECT 1")  # but the handle works again
    assert db.fetchone() == (1,)
    assert values() == []


#
# New API
#


def test_fetch_and_execute() -> None:
    status = nebula.db.execute(
        f"INSERT INTO {TABLE} (value) VALUES (%s), (%s)", "a", "b"
    )
    assert status == "INSERT 0 2"
    rows = nebula.db.fetch(f"SELECT id, value FROM {TABLE} ORDER BY id")
    assert [row["value"] for row in rows] == ["a", "b"]
    assert nebula.db.fetchrow(f"SELECT value FROM {TABLE} WHERE value = %s", "b") == {
        "value": "b"
    }
    assert nebula.db.fetchrow(f"SELECT value FROM {TABLE} WHERE value = 'x'") is None
    assert in_use() == 0


def test_iterate() -> None:
    nebula.db.executemany(f"INSERT INTO {TABLE} (value) VALUES (%s)", [("a",), ("b",)])
    assert [
        row["value"] for row in nebula.db.iterate(f"SELECT value FROM {TABLE}")
    ] == [
        "a",
        "b",
    ]
    for _ in nebula.db.iterate(f"SELECT value FROM {TABLE}"):
        break  # leaving early returns the connection too
    gc.collect()
    assert in_use() == 0


def test_transaction_rolls_back_on_error() -> None:
    with pytest.raises(RuntimeError), nebula.db.transaction():
        nebula.db.execute(f"INSERT INTO {TABLE} (value) VALUES ('a')")
        with nebula.db.transaction():  # nested: joins the outer one
            nebula.db.execute(f"INSERT INTO {TABLE} (value) VALUES ('b')")
        raise RuntimeError
    assert values() == []


def test_db_handle_joins_transaction() -> None:
    # An object save inside a block must not commit the block early
    with pytest.raises(RuntimeError), nebula.db.transaction():
        db = DB()
        db.query(f"INSERT INTO {TABLE} (value) VALUES ('a')")
        db.commit()
        assert db.lastid()
        raise RuntimeError
    assert values() == []

    with nebula.db.transaction():
        db = DB()
        db.query(f"INSERT INTO {TABLE} (value) VALUES ('b')")
        db.commit()
    assert values() == ["b"]
