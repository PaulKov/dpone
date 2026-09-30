"""Allocation and canonical byte bounds for raw snapshot JSON preimages.

Validate structure before document conversion or JSON serialization, then
apply the exact encoded-size cap. These guards share no model or I/O policy.
"""

from __future__ import annotations

from dpone.contracts.strict_json import canonical_json_bytes

_MAX_PREIMAGE_BYTES = 65_536


def validate_raw_snapshot_document(value: object) -> None:
    """Reject oversized or invalid documents before they become authority."""
    preflight_raw_snapshot_shape(value)
    try:
        size = len(canonical_json_bytes(value))
    except (TypeError, ValueError) as error:
        raise ValueError("mssql_native.source_snapshot_invalid") from error
    if size > _MAX_PREIMAGE_BYTES:
        raise ValueError("mssql_native.source_snapshot_invalid")


def preflight_raw_snapshot_shape(value: object) -> None:
    """Bound depth, elements, and string payload before JSON allocation.

    The lower-bound byte budget keeps even heavily escaped JSON input within a
    small constant multiple of the final cap. The exact encoded size is then
    checked by ``validate_raw_snapshot_document``.
    """
    remaining = _MAX_PREIMAGE_BYTES
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        remaining -= 1
        if remaining < 0 or depth > 8:
            raise ValueError("mssql_native.source_snapshot_invalid")
        if type(item) is str:
            remaining -= len(item)
        elif type(item) is dict:
            if len(item) * 2 > remaining:
                raise ValueError("mssql_native.source_snapshot_invalid")
            for key, nested in item.items():
                if type(key) is not str:
                    raise ValueError("mssql_native.source_snapshot_invalid")
                pending.append((key, depth + 1))
                pending.append((nested, depth + 1))
        elif isinstance(item, (list, tuple)):
            if len(item) > remaining:
                raise ValueError("mssql_native.source_snapshot_invalid")
            pending.extend((nested, depth + 1) for nested in item)
        elif type(item) is int:
            remaining -= len(str(item))
        elif item is not None and type(item) is not bool:
            raise ValueError("mssql_native.source_snapshot_invalid")
        if remaining < 0:
            raise ValueError("mssql_native.source_snapshot_invalid")
