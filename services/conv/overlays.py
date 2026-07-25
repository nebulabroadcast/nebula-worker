#!/usr/bin/env python3

"""
FFmpeg Video Filter Generator for Proxies & Burn-in Overlays.

Allows setting up drawtext and drawbox filters using a fluent, method-chained API:
    filter_string = (
        Overlay.timecode(position=Position.TOP_LEFT, start_tc="10:00:00:00")
        .label("MY_CLIP_01.MXF", position=Position.TOP_RIGHT)
        .frame(position=Position.BOTTOM_RIGHT, start_number=1)
        .render()
    )

Positioning uses numeric keypad layout (1-9):
    7 (TOP_LEFT)     | 8 (TOP_CENTER)    | 9 (TOP_RIGHT)
    4 (MIDDLE_LEFT)  | 5 (CENTER)        | 6 (MIDDLE_RIGHT)
    1 (BOTTOM_LEFT)  | 2 (BOTTOM_CENTER)  | 3 (BOTTOM_RIGHT)
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Literal


class Position(IntEnum):
    """Numeric keypad positions representing screen alignment.

    7: Top-Left       8: Top-Center       9: Top-Right
    4: Middle-Left    5: Middle-Center    6: Middle-Right
    1: Bottom-Left    2: Bottom-Center    3: Bottom-Right
    """

    BOTTOM_LEFT = 1
    BOTTOM_CENTER = 2
    BOTTOM_RIGHT = 3
    MIDDLE_LEFT = 4
    CENTER = 5
    MIDDLE_CENTER = 5
    MIDDLE_RIGHT = 6
    TOP_LEFT = 7
    TOP_CENTER = 8
    TOP_RIGHT = 9

    # Shorthand aliases
    BL = 1
    BC = 2
    BR = 3
    ML = 4
    MC = 5
    MR = 6
    TL = 7
    TC = 8
    TR = 9


PositionType = Position | int | str


def parse_position(pos: PositionType) -> Position:
    """Parse a position argument into a valid Position IntEnum instance."""
    if isinstance(pos, Position):
        return pos
    if isinstance(pos, int):
        try:
            return Position(pos)
        except ValueError as e:
            raise ValueError(
                f"Invalid position integer: {pos}. Must be a numpad position (1-9)."
            ) from e
    if isinstance(pos, str):
        cleaned = pos.strip().upper().replace("-", "_").replace(" ", "_")
        if cleaned.isdigit():
            return parse_position(int(cleaned))
        # Handle string names or aliases
        if hasattr(Position, cleaned):
            return getattr(Position, cleaned)
        # Check mapping for common names
        name_map = {
            "TOPLEFT": Position.TOP_LEFT,
            "TOPCENTER": Position.TOP_CENTER,
            "TOPRIGHT": Position.TOP_RIGHT,
            "MIDDLELEFT": Position.MIDDLE_LEFT,
            "MIDDLERIGHT": Position.MIDDLE_RIGHT,
            "BOTTOMLEFT": Position.BOTTOM_LEFT,
            "BOTTOMCENTER": Position.BOTTOM_CENTER,
            "BOTTOMRIGHT": Position.BOTTOM_RIGHT,
        }
        if cleaned in name_map:
            return name_map[cleaned]

    raise ValueError(
        f"Invalid position value: {pos!r}. Expected integer 1-9 or Position enum."
    )


def escape_ffmpeg_text(text: str) -> str:
    """Escape special characters for ffmpeg drawtext filter text parameter."""
    if not text:
        return ""
    # In FFmpeg filter graph strings, backslashes,
    # single quotes, colons, and % must be escaped
    text = text.replace("\\", "\\\\")
    text = text.replace("'", "\\'")
    text = text.replace(":", "\\:")
    text = text.replace("%", "\\%")
    return text


def escape_ffmpeg_option(val: str) -> str:
    """Escape special characters in non-text option values like timecode."""
    if not val:
        return ""
    val = val.replace("\\", "\\\\")
    val = val.replace("'", "\\'")
    val = val.replace(":", "\\:")
    return val


@dataclass
class OverlayElement:
    kind: Literal["timecode", "label", "frame", "branding", "banner"]
    position: Position
    text: str = ""
    start_tc: str = ""
    rate: str = "25"
    start_number: int = 1
    font_size: int | None = None
    font_color: str | None = None
    font_file: str | None = None
    box: bool | None = None
    box_color: str | None = None
    box_border_w: int | None = None
    line_spacing: int | None = None
    offset_x: int = 0
    offset_y: int = 0
    extra_options: dict[str, Any] = field(default_factory=dict)


class _ClassOrInstanceMethod:
    """Descriptor allowing a method to be called on class
    or on an existing Overlay instance.
    """

    def __init__(self, func: Callable):
        self.func = func

    def __get__(self, instance: Any, owner: Any) -> Callable:
        if instance is None:

            def class_wrapper(*args, **kwargs):
                obj = owner()
                return self.func(obj, *args, **kwargs)

            return class_wrapper
        return lambda *args, **kwargs: self.func(instance, *args, **kwargs)


class Overlay:
    """FFmpeg filter generator for proxies and burn-in overlays.

    Supports method chaining:
        filter_str = (
            Overlay.timecode(position=7, start_tc="10:00:00:00")
            .label("CLIP_NAME.MXF", position=9)
            .frame(position=3, start_number=1)
            .render()
        )
    """

    def __init__(
        self,
        safe_area: int | tuple[int, int] = 20,
        font_size: int = 24,
        font_color: str = "white",
        font_file: str | None = None,
        box: bool = True,
        box_color: str = "black@0.5",
        box_border_w: int = 0,
        line_spacing: int = 6,
    ) -> None:
        if isinstance(safe_area, tuple):
            self.safe_area_x, self.safe_area_y = safe_area
        else:
            self.safe_area_x = self.safe_area_y = safe_area

        self.font_size = font_size
        self.font_color = font_color
        self.font_file = font_file
        self.box = box
        self.box_color = box_color
        self.box_border_w = box_border_w
        self.line_spacing = line_spacing
        self.elements: list[OverlayElement] = []

    def configure(
        self,
        safe_area: int | tuple[int, int] | None = None,
        font_size: int | None = None,
        font_color: str | None = None,
        font_file: str | None = None,
        box: bool | None = None,
        box_color: str | None = None,
        box_border_w: int | None = None,
        line_spacing: int | None = None,
    ) -> "Overlay":
        """Update global default settings for this Overlay instance."""
        if safe_area is not None:
            if isinstance(safe_area, tuple):
                self.safe_area_x, self.safe_area_y = safe_area
            else:
                self.safe_area_x = self.safe_area_y = safe_area
        if font_size is not None:
            self.font_size = font_size
        if font_color is not None:
            self.font_color = font_color
        if font_file is not None:
            self.font_file = font_file
        if box is not None:
            self.box = box
        if box_color is not None:
            self.box_color = box_color
        if box_border_w is not None:
            self.box_border_w = box_border_w
        if line_spacing is not None:
            self.line_spacing = line_spacing
        return self

    def set_safe_area(self, x: int, y: int | None = None) -> "Overlay":
        """Set horizontal and vertical safe area margins in pixels."""
        self.safe_area_x = x
        self.safe_area_y = x if y is None else y
        return self

    @_ClassOrInstanceMethod
    def timecode(
        self,
        position: PositionType = Position.BOTTOM_LEFT,
        start_tc: str = "00:00:00:00",
        rate: str | float | int = "25",
        prefix: str = "",
        font_size: int | None = None,
        font_color: str | None = None,
        font_file: str | None = None,
        box: bool | None = None,
        box_color: str | None = None,
        box_border_w: int | None = None,
        offset_x: int = 0,
        offset_y: int = 0,
        **kwargs,
    ) -> "Overlay":
        """Add timecode burn-in element."""
        pos = parse_position(position)
        # Handle parameter aliases if provided in kwargs or positional
        tc_val = kwargs.pop("tc", kwargs.pop("timecode", start_tc))
        fps_val = kwargs.pop("fps", rate)
        prefix_val = kwargs.pop("text", kwargs.pop("label", prefix))

        elem = OverlayElement(
            kind="timecode",
            position=pos,
            text=prefix_val,
            start_tc=str(tc_val),
            rate=str(fps_val),
            font_size=font_size,
            font_color=font_color,
            font_file=font_file,
            box=box,
            box_color=box_color,
            box_border_w=box_border_w,
            offset_x=offset_x,
            offset_y=offset_y,
            extra_options=kwargs,
        )
        self.elements.append(elem)
        return self

    @_ClassOrInstanceMethod
    def label(
        self,
        text: str = "",
        position: PositionType = Position.TOP_LEFT,
        font_size: int | None = None,
        font_color: str | None = None,
        font_file: str | None = None,
        box: bool | None = None,
        box_color: str | None = None,
        box_border_w: int | None = None,
        offset_x: int = 0,
        offset_y: int = 0,
        **kwargs,
    ) -> "Overlay":
        """Add text label element (e.g. clip name, project name)."""
        pos = parse_position(position)
        text_val = kwargs.pop("label", text)

        elem = OverlayElement(
            kind="label",
            position=pos,
            text=str(text_val),
            font_size=font_size,
            font_color=font_color,
            font_file=font_file,
            box=box,
            box_color=box_color,
            box_border_w=box_border_w,
            offset_x=offset_x,
            offset_y=offset_y,
            extra_options=kwargs,
        )
        self.elements.append(elem)
        return self

    @_ClassOrInstanceMethod
    def frame(
        self,
        position: PositionType = Position.BOTTOM_RIGHT,
        start_number: int = 1,
        prefix: str = "F: ",
        font_size: int | None = None,
        font_color: str | None = None,
        font_file: str | None = None,
        box: bool | None = None,
        box_color: str | None = None,
        box_border_w: int | None = None,
        offset_x: int = 0,
        offset_y: int = 0,
        **kwargs,
    ) -> "Overlay":
        """Add frame number burn-in element."""
        pos = parse_position(position)
        start_num = kwargs.pop("start_frame", kwargs.pop("frame", start_number))
        prefix_val = kwargs.pop("text", kwargs.pop("label", prefix))

        elem = OverlayElement(
            kind="frame",
            position=pos,
            text=prefix_val,
            start_number=int(start_num),
            font_size=font_size,
            font_color=font_color,
            font_file=font_file,
            box=box,
            box_color=box_color,
            box_border_w=box_border_w,
            offset_x=offset_x,
            offset_y=offset_y,
            extra_options=kwargs,
        )
        self.elements.append(elem)
        return self

    @_ClassOrInstanceMethod
    def branding(
        self,
        text: str = "",
        position: PositionType = Position.TOP_RIGHT,
        font_size: int | None = None,
        font_color: str | None = None,
        font_file: str | None = None,
        box: bool | None = None,
        box_color: str | None = None,
        box_border_w: int | None = None,
        offset_x: int = 0,
        offset_y: int = 0,
        **kwargs,
    ) -> "Overlay":
        """Add branding text or logo element."""
        pos = parse_position(position)
        elem = OverlayElement(
            kind="branding",
            position=pos,
            text=str(text),
            font_size=font_size,
            font_color=font_color,
            font_file=font_file,
            box=box,
            box_color=box_color,
            box_border_w=box_border_w,
            offset_x=offset_x,
            offset_y=offset_y,
            extra_options=kwargs,
        )
        self.elements.append(elem)
        return self

    @_ClassOrInstanceMethod
    def banner(
        self,
        position: PositionType = Position.TOP_CENTER,
        height: int = 40,
        color: str = "black@0.5",
    ) -> "Overlay":
        """Add background banner box (using drawbox filter)."""
        pos = parse_position(position)
        elem = OverlayElement(
            kind="banner",
            position=pos,
            font_size=height,
            box_color=color,
        )
        self.elements.append(elem)
        return self

    def render_list(self) -> list[str]:
        """Render all configured overlay elements into a
        list of FFmpeg filter strings."""
        if not self.elements:
            return []

        filters = []
        # Group elements by position to handle automatic line stacking
        elements_by_pos: dict[Position, list[OverlayElement]] = {}
        for elem in self.elements:
            elements_by_pos.setdefault(elem.position, []).append(elem)

        for elem in self.elements:
            if elem.kind == "banner":
                filters.append(self._render_banner(elem))
                continue

            pos_elements = elements_by_pos[elem.position]
            index_at_pos = pos_elements.index(elem)
            total_at_pos = len(pos_elements)

            filter_str = self._render_drawtext(elem, index_at_pos, total_at_pos)
            filters.append(filter_str)

        return filters

    def render(self) -> str:
        """Render all configured overlay elements into a single FFmpeg filter string."""
        return ",".join(self.render_list())

    def __str__(self) -> str:
        return self.render()

    def __repr__(self) -> str:
        return f"<Overlay elements={len(self.elements)} filter={self.render()!r}>"

    def _get_effective_val(self, val: Any, default: Any) -> Any:
        return val if val is not None else default

    def _render_banner(self, elem: OverlayElement) -> str:
        pos = elem.position
        height = elem.font_size or 40
        color = elem.box_color or "black@0.5"

        # Top banner
        if pos in (Position.TOP_LEFT, Position.TOP_CENTER, Position.TOP_RIGHT):
            y = "0"
        # Bottom banner
        elif pos in (
            Position.BOTTOM_LEFT,
            Position.BOTTOM_CENTER,
            Position.BOTTOM_RIGHT,
        ):
            y = f"h-{height}"
        # Middle banner
        else:
            y = f"(h-{height})/2"

        return f"drawbox=x=0:y={y}:w=iw:h={height}:color={color}:t=fill"

    def _render_drawtext(
        self, elem: OverlayElement, index_at_pos: int, total_at_pos: int
    ) -> str:
        pos = elem.position
        font_size = self._get_effective_val(elem.font_size, self.font_size)
        font_color = self._get_effective_val(elem.font_color, self.font_color)
        font_file = self._get_effective_val(elem.font_file, self.font_file)
        box = self._get_effective_val(elem.box, self.box)
        box_color = self._get_effective_val(elem.box_color, self.box_color)
        box_border_w = self._get_effective_val(elem.box_border_w, self.box_border_w)
        line_spacing = self._get_effective_val(elem.line_spacing, self.line_spacing)

        line_height = font_size + line_spacing

        # Calculate X coordinate based on position column
        # Left column: 7, 4, 1
        if pos in (Position.TOP_LEFT, Position.MIDDLE_LEFT, Position.BOTTOM_LEFT):
            x_base = f"{self.safe_area_x}"
            if elem.offset_x:
                x_expr = f"{self.safe_area_x + elem.offset_x}"
            else:
                x_expr = x_base

        # Center column: 8, 5, 2
        elif pos in (Position.TOP_CENTER, Position.CENTER, Position.BOTTOM_CENTER):
            x_base = "(w-text_w)/2"
            if elem.offset_x:
                x_expr = f"(w-text_w)/2+{elem.offset_x}"
            else:
                x_expr = x_base

        # Right column: 9, 6, 3
        else:
            x_base = f"w-text_w-{self.safe_area_x}"
            if elem.offset_x:
                x_expr = f"w-text_w-{self.safe_area_x + elem.offset_x}"
            else:
                x_expr = x_base

        # Calculate Y coordinate based on position row and stacking
        # Top row: 7, 8, 9 (stacks top down)
        if pos in (Position.TOP_LEFT, Position.TOP_CENTER, Position.TOP_RIGHT):
            y_offset = index_at_pos * line_height + elem.offset_y
            y_val = self.safe_area_y + y_offset
            y_expr = f"{y_val}"

        # Bottom row: 1, 2, 3 (stacks bottom up)
        elif pos in (
            Position.BOTTOM_LEFT,
            Position.BOTTOM_CENTER,
            Position.BOTTOM_RIGHT,
        ):
            y_offset = index_at_pos * line_height - elem.offset_y
            if y_offset == 0:
                y_expr = f"h-text_h-{self.safe_area_y}"
            else:
                y_expr = f"h-text_h-{self.safe_area_y + y_offset}"

        # Middle row: 4, 5, 6 (stacks vertically centered)
        else:
            if total_at_pos == 1:
                y_base = "(h-text_h)/2"
                if elem.offset_y:
                    y_expr = f"(h-text_h)/2+{elem.offset_y}"
                else:
                    y_expr = y_base
            else:
                center_offset = (
                    int((index_at_pos - (total_at_pos - 1) / 2) * line_height)
                    + elem.offset_y
                )
                if center_offset > 0:
                    y_expr = f"(h-text_h)/2+{center_offset}"
                elif center_offset < 0:
                    y_expr = f"(h-text_h)/2{center_offset}"
                else:
                    y_expr = "(h-text_h)/2"

        opts: dict[str, str] = {
            "x": x_expr,
            "y": y_expr,
            "fontsize": str(font_size),
            "fontcolor": str(font_color),
        }

        if font_file:
            opts["fontfile"] = str(font_file)

        if box:
            opts["box"] = "1"
            if box_color:
                opts["boxcolor"] = str(box_color)
            if box_border_w:
                opts["boxborderw"] = str(box_border_w)

        # Handle specific element kinds
        if elem.kind == "timecode":
            opts["timecode"] = escape_ffmpeg_option(elem.start_tc)
            opts["rate"] = str(elem.rate)
            if elem.text:
                opts["text"] = escape_ffmpeg_text(elem.text)

        elif elem.kind == "frame":
            prefix = escape_ffmpeg_text(elem.text)
            opts["text"] = f"{prefix}%{{n}}"
            if elem.start_number != 0:
                opts["start_number"] = str(elem.start_number)

        elif elem.kind in ("label", "branding"):
            opts["text"] = escape_ffmpeg_text(elem.text)

        # Include any extra drawtext options passed by user
        for k, v in elem.extra_options.items():
            opts[k] = str(v)

        filter_parts = [f"{k}='{v}'" for k, v in opts.items()]
        return f"drawtext={':'.join(filter_parts)}"
