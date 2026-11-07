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
        abort=lambda: calls.append("abort"),
        restart=lambda: calls.append("restart"),
    )
    encoder = SimpleNamespace(aborted=False, stop=lambda: None)  # process gone
    conv = SimpleNamespace(job=job, encoder=encoder)
    Conv.progress_handler(conv, 99)  # type: ignore[arg-type]
    assert encoder.aborted
    assert calls == ["abort" if state == JobState.ABORTED else "restart"]
