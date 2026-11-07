import pytest

from nebula import db as nebula_db
from nebula.config import config
from nebula.db import _TupleDumper


@pytest.mark.parametrize(
    ("url", "password", "host"),
    [
        ("postgres://nebula:50%off@db/nebula", "50%off", "db"),
        ("postgres://nebula:p@ss@db/nebula", "p@ss", "db"),
        ("postgres://nebula:plain@db:5432/nebula", "plain", "db"),
    ],
)
def test_password_taken_literally(
    monkeypatch: pytest.MonkeyPatch, url: str, password: str, host: str
) -> None:
    # As the psycopg2 version parsed it: no percent-decoding by libpq
    monkeypatch.setattr(config, "postgres", url)
    kwargs = nebula_db.connection_kwargs()
    assert (kwargs["password"], kwargs["host"], kwargs["dbname"]) == (
        password,
        host,
        "nebula",
    )


def test_tuples_render_like_psycopg2() -> None:
    dumper = _TupleDumper(tuple)
    assert dumper.quote((1, "x")) == b"(1, 'x')"
    assert dumper.quote(((1, "x"), (2, None))) == b"((1, 'x'), (2, NULL))"
