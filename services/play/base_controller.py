from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .play import Service as PlayService


class BaseController:
    parent: "PlayService"
    props: dict[str, Any]
    state: dict[str, Any]

    def __init__(self, parent: "PlayService"):
        self.parent = parent
        self.props = {}
        self.state = {
            "position": 0,
            "duration": 0,
            "paused": False,
            "loop": False,
        }
