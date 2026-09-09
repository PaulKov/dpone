"""Small fail-closed snapshots for mutable interpreter-owned containers."""

from __future__ import annotations

from collections.abc import Iterable
from itertools import islice


def bounded_dict_items(
    values: object,
    maximum: int,
) -> tuple[tuple[object, object], ...] | None:
    """Snapshot an exact builtin dict without trusting its mutable length."""

    if type(values) is not dict:
        return None
    try:
        snapshot = _bounded_values(values.items(), maximum)
    except BaseException:
        return None
    if snapshot is None:
        return None
    items: list[tuple[object, object]] = []
    for item in snapshot:
        if type(item) is not tuple or len(item) != 2:
            return None
        key, value = item
        items.append((key, value))
    return tuple(items)


def bounded_list(values: object, maximum: int) -> tuple[object, ...] | None:
    """Snapshot an exact builtin list without trusting its mutable length."""

    if type(values) is not list:
        return None
    return _bounded_values(values, maximum)


def _bounded_values(values: Iterable[object], maximum: int) -> tuple[object, ...] | None:
    """Consume at most ``maximum + 1`` values and fail closed on mutation."""

    try:
        snapshot = tuple(islice(iter(values), maximum + 1))
    except BaseException:
        return None
    return snapshot if len(snapshot) <= maximum else None


__all__ = ["bounded_dict_items", "bounded_list"]
