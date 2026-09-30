"""Closed query-completeness settings shared by raw acquisition and recovery.

Numeric resource budgets stay positive and throwing. Result-shaping controls
are fixed separately: they must never silently omit or append business rows.
Access filters are inspected by the source, never cleared by this policy.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

RAW_READ_SETTINGS: Mapping[str, str | int] = MappingProxyType(
    {
        "final": 0,
        "use_query_cache": 0,
        "apply_mutations_on_fly": 0,
        "apply_patch_parts": 1,
        "apply_deleted_mask": 1,
        "max_parallel_replicas": 1,
        "skip_unavailable_shards": 0,
        "read_overflow_mode": "throw",
        "result_overflow_mode": "throw",
        "timeout_overflow_mode": "throw",
        "sort_overflow_mode": "throw",
        "limit": 0,
        "offset": 0,
        "extremes": 0,
    }
)
_BOUNDED_LIMIT_NAMES = frozenset(
    {
        "max_rows_to_read",
        "max_bytes_to_read",
        "max_result_rows",
        "max_result_bytes",
        "max_execution_time",
    }
)


def valid_raw_read_settings(value: object) -> bool:
    """Accept exactly the fixed profile plus optional positive resource bounds."""
    if type(value) is not tuple or not all(
        type(item) is tuple
        and len(item) == 2
        and type(item[0]) is str
        and bool(item[0])
        and type(item[1]) in (str, int)
        for item in value
    ):
        return False
    names = [item[0] for item in value]
    if len(names) != len(set(names)):
        return False
    values = dict(value)
    return all(
        name in values and type(values[name]) is type(expected) and values[name] == expected
        for name, expected in RAW_READ_SETTINGS.items()
    ) and all(
        name in RAW_READ_SETTINGS or (name in _BOUNDED_LIMIT_NAMES and type(limit) is int and limit > 0)
        for name, limit in value
    )
