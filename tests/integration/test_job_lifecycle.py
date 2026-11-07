"""Job states as services and the backend see them in the jobs table."""

from collections.abc import Callable

import nebula
from nebula.enum import JobState
from nebula.jobs import Job, get_job, send_to
from nebula.jobs.actions import Action, actions
from nebula.jobs.jobs import MAX_RETRIES


def job_row(db: nebula.DB, id_job: int) -> dict:
    row = nebula.db.fetchrow(
        "SELECT status, id_service, retries, priority, progress, message"
        " FROM jobs WHERE id = %s",
        id_job,
    )
    assert row
    return row


def take(id_action: int) -> Job:
    job = get_job(901, [id_action])
    assert job
    return job


def test_send_to_reuses_existing_job(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    first = send_to(asset.id, id_action, db=db)
    assert send_to(asset.id, id_action, restart_existing=False, db=db) == first
    # Different settings: a separate job
    other = send_to(asset.id, id_action, settings={"x": 1}, db=db)
    assert other != first


def test_send_to_restarts_finished_not_running(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    id_job = send_to(asset.id, id_action, db=db)
    job = take(id_action)
    # Running: not restarted unless asked
    send_to(asset.id, id_action, db=db)
    assert job_row(db, id_job)["status"] == JobState.IN_PROGRESS
    send_to(asset.id, id_action, restart_running=True, db=db)
    assert job_row(db, id_job)["status"] == JobState.RESTART

    job = take(id_action)
    job.done()
    send_to(asset.id, id_action, db=db)
    row = job_row(db, id_job)
    assert row["status"] == JobState.RESTART and row["id_service"] is None


def test_fail_retries_then_gives_up(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    id_job = send_to(asset.id, id_action, priority=3, db=db)
    for attempt in range(1, MAX_RETRIES + 1):
        job = take(id_action)
        job.fail("broken")
        row = job_row(db, id_job)
        assert row["status"] == JobState.FAILED
        assert row["retries"] == attempt
        assert row["id_service"] is None  # free for another service
    assert row["priority"] == 0  # lowered with each failure, not below 0
    assert get_job(901, [id_action]) is None  # out of retries


def test_critical_failure_is_not_retried(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    id_job = send_to(asset.id, id_action, db=db)
    take(id_action).fail("misconfigured", critical=True)
    assert job_row(db, id_job)["retries"] == MAX_RETRIES
    assert get_job(901, [id_action]) is None


def test_done_abort_restart(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    id_job = send_to(asset.id, id_action, db=db)
    job = take(id_action)
    job.set_progress(50)
    job.abort("stop it")
    row = job_row(db, id_job)
    assert (row["status"], row["progress"], row["message"]) == (
        JobState.ABORTED,
        0,
        "stop it",
    )
    assert get_job(901, [id_action]) is None  # aborted jobs aren't picked up

    job.restart()
    row = job_row(db, id_job)
    assert (row["status"], row["retries"], row["id_service"]) == (
        JobState.RESTART,
        0,
        None,
    )
    job = take(id_action)
    job.done("ok")
    row = job_row(db, id_job)
    assert (row["status"], row["progress"], row["message"]) == (
        JobState.COMPLETED,
        100,
        "ok",
    )


def test_only_one_service_takes_a_job(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    id_job = send_to(asset.id, id_action, db=db)
    job = Job(id_job)
    assert job.take(901)
    assert not Job(id_job).take(902)
    assert job_row(db, id_job)["id_service"] == 901


def test_skip_if(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("<skip_if>asset['title'] == 'integration test'</skip_if>")
    id_job = send_to(asset.id, id_action, db=db)
    assert get_job(901, [id_action]) is None
    row = job_row(db, id_job)
    assert (row["status"], row["message"]) == (JobState.SKIPPED, "Skipped")


def test_restarted_job_ignores_skip_if(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    # A user restart means "run it anyway"
    id_action = make_action("<skip_if>True</skip_if>")
    id_job = send_to(asset.id, id_action, db=db)
    Job(id_job).restart()
    assert get_job(901, [id_action])


def test_max_jobs(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    db.query(
        "UPDATE actions SET settings = %s WHERE id = %s",
        ['<settings max_jobs="1"/>', id_action],
    )
    db.commit()
    actions.data.pop(id_action, None)
    send_to(asset.id, id_action, db=db)
    send_to(asset.id, id_action, settings={"second": True}, db=db)
    assert get_job(901, [id_action])
    assert get_job(902, [id_action]) is None  # one is already running


def test_conditions_see_enums_not_builtins(asset: nebula.Asset) -> None:
    from xml.etree.ElementTree import fromstring

    ok = Action(
        1,
        "t",
        fromstring(
            "<settings><create_if>"
            "asset['status'] == ObjectStatus.ONLINE"
            "</create_if></settings>"
        ),
    )
    assert ok.should_create(asset)
    # No builtins: a condition can't import or open files; it just evaluates False
    bad = Action(
        1,
        "t",
        fromstring("<settings><create_if>__import__('os')</create_if></settings>"),
    )
    assert bad.should_create(asset) is False
    # A broken start_if doesn't start the job
    broken = Action(1, "t", fromstring("<settings><start_if>1/0</start_if></settings>"))
    assert broken.should_start(asset) is False
