import os
from typing import Any

from nxtools import tc2s
from nxtools.media import ffprobe

VIDEO_META_MAP = {
    "codec_name": "video/codec",
    "pix_fmt": "video/pixel_format",
    "width": "video/width",
    "height": "video/height",
    "index": "video/index",
    "color_range": "video/color_range",
    "color_space": "video/color_space",
}

# Codecs used for still pictures. MJPEG is also a (rare) motion video codec,
# so a stream with these is only treated as a picture if it has a single frame.
STILL_IMAGE_CODECS = {"mjpeg", "png", "bmp", "gif", "webp", "tiff", "jpegls"}


def is_cover_image(stream: dict[str, Any], stream_count: int) -> bool:
    """Return True for video streams that are pictures, not video tracks.

    Cover art of audio files, embedded thumbnails and similar. A standalone
    image file (a single stream) is not a cover: it is the content itself.
    """
    disposition = stream.get("disposition") or {}
    if disposition.get("attached_pic") or disposition.get("timed_thumbnails"):
        return True

    if stream.get("codec_name") not in STILL_IMAGE_CODECS or stream_count < 2:
        return False

    # Untagged still picture muxed next to other streams: a single frame.
    # Containers report this differently (mp4: nb_frames, mkv: DURATION tag).
    try:
        return int(stream["nb_frames"]) <= 1
    except (KeyError, ValueError):
        pass
    if (duration := stream_duration(stream)) is not None:
        return duration <= 1.0
    return stream.get("avg_frame_rate") in (None, "", "0/0")


def stream_duration(stream: dict[str, Any]) -> float | None:
    """Stream duration in seconds from `duration` or the mkv DURATION tag."""
    try:
        return float(stream["duration"])
    except (KeyError, ValueError):
        pass
    tag = (stream.get("tags") or {}).get("DURATION")
    if not tag:
        return None
    try:
        h, m, s = tag.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except ValueError:
        return None


class AudioTrack(dict[str, Any]):
    @property
    def id(self) -> int:
        return self["index"]

    def __repr__(self) -> str:
        return f"Audio track ({self.get('channel_layout', 'Unknown layout')})"


def parse_audio_track(**kwargs):
    result = {}
    for key in [
        "channels",
        "channel_layout",
        "bit_rate",
        "bits_per_sample",
        "duration",
        "index",
        "sample_fmt",
        "sample_rate",
        "start_pts",
        "start_time",
        "time_base",
    ]:
        if kwargs.get(key):
            result[key] = kwargs[key]

    if kwargs.get("codec_name"):
        result["codec"] = kwargs["codec_name"]
    result["language"] = kwargs.get("tags", {}).get("language", "eng")
    for r in kwargs.get("disposition", []):
        if kwargs["disposition"][r]:
            result["disposition"] = r
            break
    return result


def guess_aspect(w, h):
    if 0 in [w, h]:
        return 0
    valid_aspects = [
        (9, 16),  # Blasphemy
        (3, 4),
        (4, 5),
        (2, 3),
        (1, 1),  # Weird but OK I guess
        (6, 5),  # A.K.A. 1.2:1, Fox movietone
        (5, 4),
        (4, 3),
        (11, 8),  # Academy standard film ratio
        (1.43, 1),  # IMAX
        (3, 2),
        (14, 9),
        (16, 10),
        (5, 3),
        (16, 9),
        (1.85, 1),
        (2.35, 1),
        (2.39, 1),
        (2.4, 1),
        (21, 9),
        (2.76, 1),
    ]
    ratio = float(w) / float(h)
    return "{}/{}".format(
        *min(valid_aspects, key=lambda x: abs((float(x[0]) / x[1]) - ratio))
    )


def find_start_timecode(dump):
    tc_places = [
        dump["format"].get("tags", {}).get("timecode", "00:00:00:00"),
        dump["format"].get("timecode", "00:00:00:00"),
    ]
    tc = "00:00:00:00"
    for tcp in tc_places:
        if tcp != "00:00:00:00":
            tc = tcp
            break
    return tc


def noop(x: Any) -> Any:
    return x


def mediaprobe(source_file: str) -> dict[str, Any]:
    source_file = str(source_file)
    if not os.path.exists(source_file):
        return {}

    probe_result = ffprobe(source_file)
    if not probe_result:
        return {}

    meta: dict[str, Any] = {"audio_tracks": []}

    format_info = probe_result["format"]
    source_vdur: float = 0
    source_adur: float = 0

    # Track information

    for stream in probe_result["streams"]:
        if stream["codec_type"] == "video":
            if is_cover_image(stream, len(probe_result["streams"])):
                meta.setdefault("thumbnail_track", stream["index"])
                continue

            if "video/index" in meta and source_vdur:
                # We already have a video track with a duration
                continue

            # Frame rate detection
            fps_n, fps_d = (float(e) for e in stream["r_frame_rate"].split("/"))
            meta["video/fps_f"] = fps_n / fps_d
            meta["video/fps"] = f"{int(fps_n)}/{int(fps_d)}"

            # Aspect ratio detection
            try:
                dar_n, dar_d = (
                    float(e) for e in stream["display_aspect_ratio"].split(":")
                )
                if not (dar_n and dar_d):
                    raise Exception
            except Exception:
                dar_n, dar_d = float(stream["width"]), float(stream["height"])

            meta["video/aspect_ratio_f"] = float(dar_n) / dar_d
            meta["video/aspect_ratio"] = guess_aspect(dar_n, dar_d)

            try:
                source_vdur = float(stream["duration"])
            except Exception:
                pass

            for source_tag, target_tag in VIDEO_META_MAP.items():
                source_value = stream.get(source_tag)
                # 0 is a valid value (stream index), only skip missing ones
                if source_value is not None and source_value != "":
                    meta[target_tag] = source_value

        elif stream["codec_type"] == "audio":
            meta["audio_tracks"].append(parse_audio_track(**stream))
            try:
                source_adur = max(source_adur, float(stream["duration"]))
            except Exception:
                pass

    # Duration

    meta["duration"] = (
        float(format_info.get("duration", 0)) or source_vdur or source_adur
    )
    try:
        meta["num_frames"] = meta["duration"] * meta["video/fps_f"]
    except Exception:
        pass

    # Start timecode

    tc = find_start_timecode(probe_result)
    if tc != "00:00:00:00":
        meta["start_timecode"] = tc2s(tc)  # TODO: fps

    # Content type

    # Still images have no duration (png) or a single frame (jpg: one frame
    # at 25 fps). Pictures next to audio are covers, handled above.
    is_image = (
        "video/index" in meta
        and not meta["audio_tracks"]
        and (not meta.get("duration") or round(meta.get("num_frames", 0)) <= 1)
    )

    if is_image:
        meta["content_type"] = 3  # IMAGE
    elif meta.get("duration"):
        if "video/index" in meta:
            meta["content_type"] = 2  # VIDEO
        elif meta["audio_tracks"]:
            meta["content_type"] = 1  # AUDIO

    # Descriptive metadata

    if "tags" in format_info:
        tag_map = {
            "title": ("title", None),
            "artist": ("role/performer", None),
            "composer": ("role/composer", None),
            "album": ("album", None),
            "genre": ("genre", None),
            "comment": ("notes", None),
            "date": ("year", lambda x: int(x) if len(str(x)) == 4 else 0),
        }

        for tag, value in format_info["tags"].items():
            if tag.lower() in tag_map:
                target_tag, transform = tag_map[tag.lower()]
                if transform is None:
                    transform = noop
                meta[target_tag] = transform(value)

    # Clean-up

    keys = list(meta.keys())
    for k in keys:
        if meta[k] is None:
            del meta[k]

    return meta
