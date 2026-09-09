from __future__ import annotations

import sys
from typing import Any

import yaml


def dumps_yaml(
    obj: Any,
    *,
    sort_keys: bool = True,
) -> str:
    return yaml.safe_dump(obj, sort_keys=sort_keys, allow_unicode=True).rstrip() + "\n"


def write_yaml(
    obj: Any,
    *,
    sort_keys: bool = True,
) -> None:
    sys.stdout.write(dumps_yaml(obj, sort_keys=sort_keys))


__all__ = ["dumps_yaml", "write_yaml"]
