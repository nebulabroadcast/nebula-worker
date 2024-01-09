from typing import TYPE_CHECKING

import nebula

if TYPE_CHECKING:
    from .play import Service as PlayService


class BaseController:
    parent: "PlayService"
    time_unit: str = "s"
    current_item: nebula.Item | None = None
