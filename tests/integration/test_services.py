from collections.abc import Callable
from xml.etree.ElementTree import fromstring

import pytest

import nebula
from nebula.jobs import send_to

FAKE_SERVICE = 9999  # no such row: service state updates do nothing


def test_unknown_encoder_fails_job(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    from services.conv import Service as Conv

    id_action = make_action('<task mode="ffmepg"/>')
    conv = Conv(FAKE_SERVICE, None)
    conv.actions = [a for a in conv.actions if a.id == id_action]
    id_job = send_to(asset.id, id_action, db=db)
    conv.on_main()
    db.query("SELECT status, retries, message FROM jobs WHERE id = %s", [id_job])
    status, retries, message = db.fetchall()[0]
    assert (status, retries) == (3, 3)  # failed, not retried
    assert "ffmepg" in message


def test_worker_runs_settings_scripts(capsys: pytest.CaptureFixture[str]) -> None:
    from services.worker import Service as Worker

    settings = (
        "<settings><init>print('init ran')</init>"
        "<main>print('main ran')</main></settings>"
    )
    worker = Worker(FAKE_SERVICE, fromstring(settings))
    worker.on_main()
    out = capsys.readouterr().out
    assert "init ran" in out and "main ran" in out
