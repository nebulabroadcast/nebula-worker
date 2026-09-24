"""Export Nebula metadata as media container tags.

Every muxer uses its own vocabulary of metadata tags, so before object
metadata may be embedded to a rendered file, Nebula meta keys have to be
translated to tag names the target container understands.

This module is normally used via `BaseObject.metadata()`:

    >>> asset.metadata(format="mp4")
    {
        "title": "Woodpecker",
        "genre": "Documentary",
        "encoding_tool": "Nebula Worker 6.1.0"
    }

The resulting dict is ready to be handed over to a muxer - in case of FFmpeg,
each key-value pair is rendered as a separate `-metadata key=value` argument.

Two kinds of tags end up in the result:

  - content tags, derived from object metadata (title, genre, ...).
    See `DEFAULT_TAG_MAP`.
  - application tags, identifying Nebula as the software that produced
    the file. These are NOT derived from object metadata - they are
    always injected, the same way for every object. See `ContainerProfile`.
"""

from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any

from nebula.enum import MetaClass
from nebula.metadata.utils import get_cs_titles
from nebula.settings import settings
from nebula.version import __version__
from nxtools import format_time

# Identity of the software writing the file. Used to populate whatever
# "application" tag(s) the target container supports (see ContainerProfile
# below) - never derived from object metadata.

APPLICATION_NAME = "Nebula Broadcast"
APPLICATION_PRODUCT = "Nebula Worker"
APPLICATION_VERSION = __version__
APPLICATION_TAG = f"{APPLICATION_PRODUCT} {APPLICATION_VERSION}"

# Nebula meta key -> FFmpeg (generic) tag name.
# Mapping is evaluated in the order of definition, so when two meta keys
# share a tag, the one defined later wins (if it has a value).

DEFAULT_TAG_MAP: dict[str, str] = {
    "title": "title",
    "subtitle": "subtitle",
    "summary": "synopsis",
    "description": "description",
    "notes": "comment",
    "genre": "genre",
    "keywords": "keywords",
    "language": "language",
    "album": "album",
    "serie": "show",
    "serie/season": "season_number",
    "serie/episode": "episode_id",
    "role/director": "director",
    "role/performer": "artist",
    "role/composer": "composer",
    "role/cast": "cast",
    "source": "publisher",
    "rights": "license",
    "rights/attribution": "copyright",
    "year": "date",
    "date": "date",  # Overwrites year when both are set
}


@dataclass(frozen=True)
class ContainerProfile:
    """Describes how a container format handles metadata.

    `tags` - the set of -metadata keys this muxer actually understands.
    Anything else is silently dropped by FFmpeg, so it's not worth
    passing.

    `application` - tag(s) identifying Nebula as the producing software,
    pre-filled with fixed values (see APPLICATION_* above). Not every
    container has room for this: it's a dict rather than a single string
    because some (MXF) split it into several fields.
    """

    tags: set[str]
    application: dict[str, str] = dc_field(default_factory=dict)


# Tags written by the mov/mp4/3gp family of muxers.

MP4_TAGS = {
    "album",
    "album_artist",
    "artist",
    "comment",
    "composer",
    "copyright",
    "date",
    "description",
    "episode_id",
    "episode_sort",
    "genre",
    "grouping",
    "keywords",
    "location",
    "lyrics",
    "media_type",
    "network",
    "performer",
    "season_number",
    "show",
    "synopsis",
    "title",
}

MP4_PROFILE = ContainerProfile(
    tags=MP4_TAGS,
    application={"encoding_tool": APPLICATION_TAG},
)

# Tags mapped to ID3v2 frames.

ID3_TAGS = {
    "album",
    "album_artist",
    "artist",
    "comment",
    "composer",
    "copyright",
    "date",
    "encoded_by",
    "genre",
    "language",
    "performer",
    "publisher",
    "title",
    "track",
}

ID3_PROFILE = ContainerProfile(
    tags=ID3_TAGS,
    application={"encoded_by": APPLICATION_TAG},
)

# MXF doesn't use the generic key=value tag model at all: FFmpeg's mxfenc
# ignores title/genre/etc. entirely and instead writes a handful of fixed
# fields into the Preface/Identification set. These aren't documented
# muxer options, but FFmpeg does read them from the metadata dict:
# company_name and product_name default to "FFmpeg" / "OP1a Muxer" unless
# overridden - so this is the actual "application" field for MXF deliverables.
#
# Nebula doesn't currently have a way to embed descriptive (title/genre/...)
# metadata into MXF - FFmpeg's muxer has no support for it.

MXF_PROFILE = ContainerProfile(
    tags={"company_name", "product_name", "product_version"},
    application={
        "company_name": APPLICATION_NAME,
        "product_name": APPLICATION_PRODUCT,
        "product_version": APPLICATION_VERSION,
        "product_version_num": APPLICATION_VERSION,
    },
)

# Container format (muxer name or file extension) -> profile.
# Formats not listed here (mkv, webm, ogg, nut...) use free-form tags -
# everything in DEFAULT_TAG_MAP is passed through, and "encoded_by" is
# used to identify Nebula, same as ID3.

CONTAINERS: dict[str, ContainerProfile] = {
    "3gp": MP4_PROFILE,
    "m4a": MP4_PROFILE,
    "m4v": MP4_PROFILE,
    "mov": MP4_PROFILE,
    "mp3": ID3_PROFILE,
    "mp4": MP4_PROFILE,
    "mxf": MXF_PROFILE,
    "wav": ID3_PROFILE,
}

# Format aliases: muxer name or file extension -> key of CONTAINERS

CONTAINER_ALIASES: dict[str, str] = {
    "3g2": "3gp",
    "ipod": "mp4",
    "mj2": "mp4",
    "psp": "mp4",
    "qt": "mov",
}

# Fallback application tag for formats not listed in CONTAINERS
# (free-form tags, e.g. mkv/webm/ogg).

DEFAULT_APPLICATION_TAGS: dict[str, str] = {"encoding_tool": APPLICATION_TAG}


def get_container_profile(format: str | None) -> ContainerProfile | None:
    """Return the metadata profile of the given container format.

    Format may be a muxer name as well as a file extension.
    Returns None if the format is unknown or uses free-form tags.
    """
    if not format:
        return None
    key = format.strip().lower().lstrip(".")
    key = CONTAINER_ALIASES.get(key, key)
    return CONTAINERS.get(key)


def format_tag_value(key: str, value: Any) -> str:
    """Return a human readable representation of a metadata value."""

    meta_type = settings.metatypes.get(key)

    if meta_type is None:
        # Unknown meta type. Use the raw value.
        return str(value)

    match meta_type.metaclass:
        case MetaClass.DATETIME:
            # Containers expect a date or a full ISO timestamp,
            # not the human readable form used in the UI
            fmt = "%Y-%m-%d" if meta_type.mode == "date" else "%Y-%m-%dT%H:%M:%S"
            return format_time(value, fmt, never_placeholder="")

        case MetaClass.SELECT if meta_type.cs:
            return ", ".join(get_cs_titles(meta_type.cs, (value,)))

        case MetaClass.LIST if meta_type.cs:
            return ", ".join(get_cs_titles(meta_type.cs, tuple(value)))

        case MetaClass.LIST:
            return ", ".join(str(v) for v in value)

    return str(value)


def export_metadata(
    meta: dict[str, Any],
    format: str | None = None,
) -> dict[str, str]:
    """Convert Nebula metadata to a container tags dictionary.

    Keys of the resulting dict are tag names understood by the muxer
    of the given container format, values are human readable strings.
    Tags the container does not support, as well as empty values,
    are skipped.

    When no format is given, free-form tags are assumed
    and everything known is exported.

    The result always includes tag(s) identifying Nebula as the
    producing software (see ContainerProfile.application), on top of
    whatever content tags were derived from `meta`.
    """

    profile = get_container_profile(format)
    supported_tags = profile.tags if profile else None
    application_tags = profile.application if profile else DEFAULT_APPLICATION_TAGS

    result: dict[str, str] = {}

    for key, tag in DEFAULT_TAG_MAP.items():
        if supported_tags is not None and tag not in supported_tags:
            continue
        if not (value := meta.get(key)):
            continue
        if text := format_tag_value(key, value).strip():
            result[tag] = text

    result.update(application_tags)

    return result
