from types import SimpleNamespace

import pytest

from nebula.enum import JobState
from services.conv import Service as Conv


@pytest.mark.parametrize("state", [JobState.ABORTED, JobState.RESTART])
def test_abort_after_encoder_exited(state: JobState) -> None:
    # The user aborts just as ffmpeg exits: stop() then does nothing, but the
    # job must not be finalized and marked done
    calls: list[str] = []
    job = SimpleNamespace(
        get_status=lambda: state,
        lost=False,
        abort=lambda: calls.append("abort"),
        restart=lambda: calls.append("restart"),
    )
    encoder = SimpleNamespace(aborted=False, stop=lambda: None)  # process gone
    conv = SimpleNamespace(job=job, encoder=encoder)
    Conv.progress_handler(conv, 99)  # type: ignore[arg-type]
    assert encoder.aborted
    assert calls == ["abort" if state == JobState.ABORTED else "restart"]


def test_job_taken_by_another_service_is_left_alone() -> None:
    # Restarted and taken by another service between two progress ticks: its
    # status is in progress again, but the job isn't ours any more
    calls: list[str] = []
    job = SimpleNamespace(
        get_status=lambda: JobState.IN_PROGRESS,
        lost=True,
        abort=lambda: calls.append("abort"),
        restart=lambda: calls.append("restart"),
        set_progress=lambda *args: calls.append("set_progress"),
    )
    encoder = SimpleNamespace(aborted=False, stop=lambda: calls.append("stop"))
    conv = SimpleNamespace(job=job, encoder=encoder)
    Conv.progress_handler(conv, 50)  # type: ignore[arg-type]
    assert encoder.aborted
    assert calls == ["stop"]
