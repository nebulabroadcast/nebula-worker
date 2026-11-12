from xml.etree import ElementTree

from nebula.mediaprobe import mediaprobe
from nebula.metadata.probe import apply_probed_meta
from nebula.objects import Asset


def validate_package_asset(asset: Asset):
    """Ensure a package (e.g. MLT XML) asset file is well-formed XML."""
    try:
        ElementTree.parse(asset.file_path)
    except (ElementTree.ParseError, OSError):
        return False
    return asset


def ffprobe_asset(asset: Asset):
    meta = mediaprobe(asset.file_path)
    if not meta:
        return False
    apply_probed_meta(asset, meta)
    return asset
