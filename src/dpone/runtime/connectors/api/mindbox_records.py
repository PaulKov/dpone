from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Any


def normalize_mindbox_record(record: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in flatten_dict(dict(record)).items():
        clean_key = key.replace(".", "_")
        if isinstance(value, Decimal):
            value = float(value)
        elif isinstance(value, list | dict | tuple | set):
            value = json.dumps(value, ensure_ascii=False, default=str)
        elif value is not None and not isinstance(value, str | int | float | bool):
            value = str(value)
        result[clean_key] = value
    return result


def flatten_dict(data: dict[str, Any], parent_key: str = "", sep: str = "_") -> dict[str, Any]:
    items: list[tuple[str, Any]] = []
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep).items())
        else:
            items.append((new_key, value))
    return dict(items)
