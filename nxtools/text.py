__all__ = [
    "indent",
    "to_unicode",
    "unaccent",
    "slugify",
    "string2color",
    "fract2float",
    "format_filesize",
    "EMAIL_REGEXP",
    "GUID_REGEXP",
]


import string

import unidecode

#: E-mail address regular expression
EMAIL_REGEXP = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"

#: GUID/UUID regular expression
GUID_REGEXP = (
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

default_slug_whitelist = string.ascii_letters + string.digits
slug_separator_whitelist = " ,./\\;:!|*^#@~+-_="


def indent(src, length: int = 4):
    """Indent a multi-line text"""
    return (
        "\n".join([f"{length * ' '}{s.rstrip()}" for s in src.split("\n")]) + "\n"
        if src.endswith("\n")
        else ""
    )


def to_unicode(string: str) -> str:
    # DEPRECATED
    return string


def unaccent(string: str) -> str:
    """Remove accents and/or transliterate non-ascii characters"""
    return unidecode.unidecode(string)


def slugify(
    input_string: str,
    separator: str = "-",
    lower: bool = True,
    make_set: bool = False,
    min_length: int = 1,
    slug_whitelist: str = default_slug_whitelist,
    split_chars: str = slug_separator_whitelist,
) -> str:
    """Slugify a text string

    This function removes transliterates input string to ASCII,
    removes special characters
    and use join resulting elemets using specified separator.

    Args:
        input_string (str):
            Input string to slugify

        separator (str):
            A string used to separate returned elements (default: "-")

        lower (bool):
            Convert to lower-case (default: True)

        make_set (bool):
            Return "set" object instead of string

        min_length (int):
            Minimal length of an element (word)

        slug_whitelist (str):
            Characters allowed in the output
            (default: ascii letters, digits and the separator)

        split_chars (str):
            Set of characters used for word splitting (there is a sane default)

    """
    input_string = unaccent(input_string)
    if lower:
        input_string = input_string.lower()
    input_string = "".join(
        [ch if ch not in split_chars else " " for ch in input_string]
    )
    input_string = "".join(
        [ch if ch in slug_whitelist + " " else "" for ch in input_string]
    )
    elements = [
        elm.strip() for elm in input_string.split(" ") if len(elm.strip()) >= min_length
    ]
    return set(elements) if make_set else separator.join(elements)


def string2color(string: str) -> str:
    """Generate more or less unique color for a given string"""
    h = 0
    for char in string:
        h = ord(char) + ((h << 5) - h)
    return hex(h & 0x00FFFFFF)


def fract2float(fract) -> float:
    nd = fract.split("/")
    try:
        if len(nd) == 1 or nd[1] == "1":
            return float(nd[0])
        return float(nd[0]) / float(nd[1])
    except Exception:
        return 1


def format_filesize(value):
    """Return a human readable filesize for a given byte count"""
    if not value:
        return ""
    for x in ["bytes", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024.0:
            return f"{value:3.1f} {x}"
        value /= 1024.0
    return value
