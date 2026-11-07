from collections.abc import Callable

import nebula
from nebula.jobs import get_job, send_to
from nebula.jobs.actions import actions


def job_row(db: nebula.DB, id_job: int) -> tuple:
    db.query("SELECT status, id_service, message FROM jobs WHERE id = %s", [id_job])
    row = db.fetchall()[0]
    db.commit()
    return row


def test_start_if_false_keeps_job_pending(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("<start_if>asset['title'] == 'ready'</start_if>")
    id_job = send_to(asset.id, id_action, db=db)
    for _ in range(2):  # checked again on every pass, not parked as running
        assert get_job(901, [id_action], db=db) is None
        assert job_row(db, id_job) == (0, None, "Waiting")

    asset["title"] = "ready"
    asset.save(notify=False)
    actions.data.pop(id_action, None)
    job = get_job(901, [id_action], db=db)
    assert job and job.id == id_job
    assert job_row(db, id_job)[:2] == (1, 901)


def test_run_on_with_spaces(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("<run_on>901, 902</run_on>")
    send_to(asset.id, id_action, db=db)
    assert get_job(903, [id_action], db=db) is None
    assert get_job(902, [id_action], db=db)


def test_run_on_keeps_valid_ids(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("<run_on>901, x</run_on>")
    send_to(asset.id, id_action, db=db)
    assert get_job(902, [id_action], db=db) is None
    assert get_job(901, [id_action], db=db)


def test_failing_pre_script_blocks_job(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("<pre>raise ValueError('not now')</pre>")
    id_job = send_to(asset.id, id_action, db=db)
    assert get_job(901, [id_action], db=db) is None
    assert job_row(db, id_job) == (0, None, "Waiting: pre-script: not now")


def test_set_progress(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    send_to(asset.id, id_action, db=db)
    job = get_job(901, [id_action], db=db)
    assert job
    job.set_progress(42, "halfway")
    db.query("SELECT progress, message FROM jobs WHERE id = %s", [job.id])
    assert db.fetchall()[0] == (42, "halfway")


def test_run_on_odd_digits(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    # "²".isdigit() is True but int("²") fails: must not crash get_job
    id_action = make_action("<run_on>901,²</run_on>")
    send_to(asset.id, id_action, db=db)
    assert get_job(901, [id_action], db=db)
