from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _is_mapping(obj: Any) -> bool:
    return isinstance(obj, Mapping)


def deep_merge(
    base: Any,
    override: Any,
    *,
    _path: tuple[str, ...] = (),
    _append_list_keys: frozenset[str] = frozenset({"depends_on", "transforms"}),
) -> Any:
    """Deep-merge two YAML-compatible objects.

    Rules:
    - dict + dict  -> recursive merge
    - list + list  -> replace, except for top-level keys in _append_list_keys
    - other types  -> override wins
    """

    if _is_mapping(base) and _is_mapping(override):
        result: dict[str, Any] = dict(base)  # shallow copy
        for k, v in override.items():
            if k in result:
                result[k] = deep_merge(
                    result[k],
                    v,
                    _path=_path + (str(k),),
                    _append_list_keys=_append_list_keys,
                )
            else:
                result[k] = v
        return result

    if isinstance(base, list) and isinstance(override, list):
        # concat only for specific, well-known top-level list keys
        if len(_path) == 1 and _path[0] in _append_list_keys:
            return [*base, *override]
        return override

    return override
