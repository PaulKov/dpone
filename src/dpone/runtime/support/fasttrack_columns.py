from __future__ import annotations

import re
from collections.abc import Mapping
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
        out.append(_RU_TRANSLIT.get(ch.lower(), ch))
    return "".join(out)


def sanitize_fasttrack_column_name(name: Any) -> str:
    value = _translit_ru(str(name)).replace("-", "_").replace(" ", "_")
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = value.lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        value = "column"
    if value[0].isdigit():
        value = f"column_{value}"
    return value


def normalize_fasttrack_record_columns(resource_name: str, record: Mapping[str, Any]) -> dict[str, Any]:
    del resource_name

    normalized: dict[str, Any] = {}
    collisions: dict[str, int] = {}
    for key, value in record.items():
        base = sanitize_fasttrack_column_name(key)
        current = collisions.get(base, 0)
        target = base if current == 0 else f"{base}_{current + 1}"
        collisions[base] = current + 1
        normalized[target] = value
    return normalized


__all__ = [
    "normalize_fasttrack_record_columns",
    "sanitize_fasttrack_column_name",
]
