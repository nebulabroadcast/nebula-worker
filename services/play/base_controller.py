from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .play import Service as PlayService


class BaseController:
    def __init__(self, parent: PlayService):
        self.props = {}
        self.state = {
            "position": 0,
            "duration": 0,
            "paused": False,
            "loop": False,
        }
