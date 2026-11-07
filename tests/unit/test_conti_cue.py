from types import SimpleNamespace

import pytest

from services.play.conti import conti_controller
from services.play.conti.conti_controller import ContiController


class FakeSource:
    def __init__(self, parent, path, **kwargs) -> None:
        self.path = path
        self.item = kwargs["item"]

    def open(self) -> None:
        pass

    def stop(self, force: bool = False) -> None:
        pass


@pytest.fixture
def controller(monkeypatch: pytest.MonkeyPatch) -> ContiController:
    monkeypatch.setattr(conti_controller, "NebulaContiSource", FakeSource)
    ctrl = object.__new__(ContiController)  # no real Conti engine
    ctrl.conti = SimpleNamespace(playlist=[object()], started=True)
    ctrl.takes = 0
    ctrl.take = lambda layer=None: setattr(ctrl, "takes", ctrl.takes + 1)
    return ctrl


@pytest.mark.parametrize("play", [True, False])
def test_cue_play(controller: ContiController, play: bool) -> None:
    # play=True is how hard-timed events and recovery start the clip
    controller.cue("clip", item=SimpleNamespace(asset=None), play=play)
    assert controller.takes == int(play)
    assert controller.conti.playlist[-1].path == "clip"


def test_cue_play_starts_stopped_conti_without_take(
    controller: ContiController,
) -> None:
    # A stopped Conti starts with the cued clip: a take would skip it
    started: list[bool] = []
    controller.conti.started = False
    controller.conti.playlist = []
    controller.conti.start = lambda: started.append(True)
    controller.cue("clip", item=SimpleNamespace(asset=None), play=True)
    assert started == [True]
    assert controller.takes == 0
