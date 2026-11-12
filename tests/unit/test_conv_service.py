from types import SimpleNamespace

import pytest

from nebula.enum import JobState
from services.conv import Service as Conv


def conv_with(status: JobState, lost: bool = False) -> tuple[SimpleNamespace, list]:
    calls: list[str] = []
    job = SimpleNamespace(
        status=status,
        lost=lost,
        get_status=lambda: status,
        abort=lambda: calls.append("abort"),
        restart=lambda: calls.append("restart"),
        set_progress=lambda *args: calls.append("set_progress"),
    )
    return SimpleNamespace(job=job), calls


@pytest.mark.parametrize("state", [JobState.ABORTED, JobState.RESTART])
def test_abort_and_restart_stop_the_encoder(state: JobState) -> None:
    conv, calls = conv_with(state)
    assert Conv.check_abort(conv)  # type: ignore[arg-type]
    Conv.job_aborted(conv)  # type: ignore[arg-type]
    assert calls == ["abort" if state == JobState.ABORTED else "restart"]


def test_running_job_is_not_aborted() -> None:
    conv, calls = conv_with(JobState.IN_PROGRESS)
    assert not Conv.check_abort(conv)  # type: ignore[arg-type]
    Conv.progress_handler(conv, 50)  # type: ignore[arg-type]
    assert calls == ["set_progress"]


def test_job_taken_by_another_service_is_left_alone() -> None:
    # Restarted and taken by another service: its status is in progress
    # again, but the job isn't ours any more
    conv, calls = conv_with(JobState.IN_PROGRESS, lost=True)
    assert Conv.check_abort(conv)  # type: ignore[arg-type]
    Conv.job_aborted(conv)  # type: ignore[arg-type]
    assert calls == []
