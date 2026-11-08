"""Job states as services and the backend see them in the jobs table."""

from collections.abc import Callable
from xml.etree.ElementTree import fromstring

import pytest

import nebula
from nebula.enum import JobState
from nebula.jobs import Job, get_job, send_to
from nebula.jobs.actions import Action, actions
from nebula.jobs.jobs import MAX_RETRIES, wait
from nebula.log import Logger


def job_row(id_job: int, *cols: str) -> tuple:
    row = nebula.db.fetchrow(
        f"SELECT {', '.join(cols)} FROM jobs WHERE id = %s", id_job
    )
    assert row
    return tuple(row.values())


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
    assert job_row(id_job, "status") == (JobState.IN_PROGRESS,)
    send_to(asset.id, id_action, restart_running=True, db=db)
    assert job_row(id_job, "status") == (JobState.RESTART,)

    job = take(id_action)
    job.done()
    send_to(asset.id, id_action, db=db)
    assert job_row(id_job, "status", "id_service") == (JobState.RESTART, None)


def test_fail_retries_then_gives_up(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    # 2 with MAX_RETRIES 3: the last failure hits the floor
    id_job = send_to(asset.id, id_action, priority=2, db=db)
    for attempt in range(1, MAX_RETRIES + 1):
        take(id_action).fail("broken")
        # id_service cleared: free for another service
        assert job_row(id_job, "status", "retries", "id_service") == (
            JobState.FAILED,
            attempt,
            None,
        )
    assert job_row(id_job, "priority") == (0,)  # lowered with each failure, not < 0
    assert get_job(901, [id_action]) is None  # out of retries


def test_critical_failure_is_not_retried(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    id_job = send_to(asset.id, id_action, db=db)
    take(id_action).fail("misconfigured", critical=True)
    assert job_row(id_job, "retries") == (MAX_RETRIES,)
    assert get_job(901, [id_action]) is None


def test_done_abort_restart(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("")
    id_job = send_to(asset.id, id_action, db=db)
    job = take(id_action)
    job.set_progress(50, "halfway")
    assert job_row(id_job, "progress", "message") == (50, "halfway")
    job.abort("stop it")
    assert job_row(id_job, "status", "progress", "message") == (
        JobState.ABORTED,
        0,
        "stop it",
    )
    # A progress tick right after the user's abort must not undo it
    job.set_progress(60)
    assert job_row(id_job, "status") == (JobState.ABORTED,)
    assert get_job(901, [id_action]) is None  # aborted jobs aren't picked up

    job.restart()
    assert job_row(id_job, "status", "retries", "id_service") == (
        JobState.RESTART,
        0,
        None,
    )
    take(id_action).done("ok")
    assert job_row(id_job, "status", "progress", "message") == (
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
    assert job_row(id_job, "id_service") == (901,)
    # Another service that saw it pending doesn't mark it waiting
    assert not wait(db, Job(id_job), "Waiting")
    assert job_row(id_job, "message") != ("Waiting",)


def test_start_if_false_keeps_job_pending(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("<start_if>asset['title'] == 'ready'</start_if>")
    id_job = send_to(asset.id, id_action, db=db)
    for _ in range(2):  # checked again on every pass, not parked as running
        assert get_job(901, [id_action], db=db) is None
        assert job_row(id_job, "status", "id_service", "message") == (
            JobState.PENDING,
            None,
            "Waiting",
        )

    asset["title"] = "ready"
    asset.save(notify=False)
    actions.data.pop(id_action, None)
    job = get_job(901, [id_action], db=db)
    assert job and job.id == id_job
    assert job_row(id_job, "status", "id_service") == (JobState.IN_PROGRESS, 901)


def test_failing_pre_script_blocks_job(
    db: nebula.DB,
    asset: nebula.Asset,
    make_action: Callable[..., int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logged: list[str] = []
    monkeypatch.setattr(
        Logger, "__call__", lambda _, __, *args, **___: logged.append(str(args))
    )
    id_action = make_action("<pre>raise ValueError('not now')</pre>")
    id_job = send_to(asset.id, id_action, db=db)
    assert get_job(901, [id_action], db=db) is None
    assert job_row(id_job, "status", "id_service", "message") == (
        JobState.PENDING,
        None,
        "Waiting: pre-script: not now",
    )
    assert any("ValueError: not now" in msg for msg in logged)  # with traceback


def test_run_on(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    # Spaces are allowed; invalid entries ("x", and "²", for which isdigit() is
    # True but int() fails) are skipped, the valid ones kept
    id_action = make_action("<run_on>x, 901,²</run_on>")
    send_to(asset.id, id_action, db=db)
    assert get_job(902, [id_action], db=db) is None
    assert get_job(901, [id_action], db=db)


def test_skip_if(
    db: nebula.DB, asset: nebula.Asset, make_action: Callable[..., int]
) -> None:
    id_action = make_action("<skip_if>asset['title'] == 'integration test'</skip_if>")
    id_job = send_to(asset.id, id_action, db=db)
    assert get_job(901, [id_action]) is None
    assert job_row(id_job, "status", "message") == (JobState.SKIPPED, "Skipped")


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


def condition(tag: str, text: str) -> Action:
    return Action(1, "t", fromstring(f"<settings><{tag}>{text}</{tag}></settings>"))


def test_conditions_see_enums_not_builtins(asset: nebula.Asset) -> None:
    ok = condition("create_if", "asset['status'] == ObjectStatus.ONLINE")
    assert ok.should_create(asset)
    # No builtins: a condition can't import or open files; it just evaluates False
    assert condition("create_if", "__import__('os')").should_create(asset) is False
    # A broken start_if doesn't start the job
    assert condition("start_if", "1/0").should_start(asset) is False
