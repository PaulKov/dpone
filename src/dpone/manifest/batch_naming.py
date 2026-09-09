from __future__ import annotations

from typing import Any

_RU_TRANSLIT = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "i",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "sch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}


def _translit_ru(text: str) -> str:
    out: list[str] = []
    for ch in text:
        low = ch.lower()
        if low in _RU_TRANSLIT:
            t = _RU_TRANSLIT[low]
            out.append(t)
        else:
            out.append(ch)
    return "".join(out)


def _to_snake(value: Any) -> str:
    import re

    s = str(value)
    s = _translit_ru(s)
    s = s.replace("-", "_").replace(" ", "_")
    # camelCase -> snake_case
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = s.lower()
    # replace forbidden chars
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    # collapse single-underscore runs (keep semantics of '__' untouched by not reducing below 1)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def _to_identifier(value: Any) -> str:
    """Normalize an identifier while preserving double-underscore separators."""
    import re

    s = str(value)
    s = _translit_ru(s)
    s = s.replace("-", "_").replace(" ", "_")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    # preserve __ by reducing 3+ underscores to '__', leaving '__' intact
    s = re.sub(r"_{3,}", "__", s)
    return s.strip("_")
