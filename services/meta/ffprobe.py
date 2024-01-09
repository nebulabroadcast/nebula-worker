import difflib

from nxtools import get_base_name, slugify

import nebula
from nebula.enum import MetaClass
from nebula.mediaprobe import mediaprobe
from nebula.objects import Asset
from nebula.settings import settings


def string2cs(key: str, value: str):
    """Return a CS best matching for the given string."""
    nebula.log.info(f"Parsing {key} value {value}")
    cs = settings.metatypes[key].cs
    if not cs:
        return None
    vslug = slugify(value, separator=" ")
    best_match = None
    max_ratio: float = 0
    for ckey, cals in settings.cs[cs].items():
        cval = cals.aliases["en"].title
        reflist = [v.strip() for v in cval.split("/")]
        for m in reflist:
            mslug = slugify(m, separator=" ")
            r = difflib.SequenceMatcher(None, vslug, mslug).ratio()
            if r < max_ratio:
                continue
            best_match = ckey
            max_ratio = r

    if max_ratio < 0.85:
        return None
    return best_match


def ffprobe_asset(asset: Asset):
    meta = mediaprobe(asset.file_path)
    if not meta:
        return False

    for key, value in meta.items():
        metatype = settings.metatypes.get(key)
        if not metatype:
            continue

        # Only update auto-generated title
        if key == "title":
            if value == get_base_name(asset.file_path):
                continue

        # Do not update descriptive metadata
        elif metatype.ns == "m" and asset[key]:
            continue

        if key == "genre" and settings.metatypes["genre"].metaclass == MetaClass.SELECT:
            try:
                if (new_val := string2cs("genre", value)) is None:
                    continue
            except Exception:
                nebula.log.traceback("Unable to parse genre")
                continue
            asset["genre/original"] = value
            value = new_val

        asset[key] = value

    return asset
