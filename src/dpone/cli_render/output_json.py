"""Shared JSON rendering primitives for CLI adapters."""

from __future__ import annotations

import json
import sys
from typing import Any


def dumps_json(
    obj: Any,
    *,
    indent: int = 2,
    sort_keys: bool = False,
) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=indent, sort_keys=sort_keys) + "\n"


def write_json(
    obj: Any,
    *,
    indent: int = 2,
    sort_keys: bool = False,
) -> None:
    sys.stdout.write(dumps_json(obj, indent=indent, sort_keys=sort_keys))


__all__ = ["dumps_json", "write_json"]
