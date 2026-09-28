"""Render safe ClickHouse types without losing projected nullability or precision."""

from __future__ import annotations

import re

from dpone.type_system.source_sink.clickhouse_mssql import is_probable_clickhouse_type


def render_clickhouse_type(dtype: str) -> str:
    physical_type = str(dtype).strip()
    if any(token in physical_type for token in (";", "--", "/*", "*/", "`", "#", '"')):
        raise ValueError("Unsafe ClickHouse schema-evolution type")
    # The sink projects physical types before comparison; never remap them.
    if is_probable_clickhouse_type(physical_type):
        _validate_single_clickhouse_type(physical_type)
        return physical_type
    normalized = str(dtype).lower()
    if "bigint" in normalized or normalized in {"int8", "int64"}:
        return "Int64"
    if "smallint" in normalized:
        return "Int16"
    if "tinyint" in normalized:
        return "Int8"
    if "int" in normalized:
        return "Int32"
    if "decimal" in normalized or "numeric" in normalized:
        return "Decimal(38, 10)"
    if "float" in normalized or "double" in normalized or "real" in normalized:
        return "Float64"
    if normalized == "date":
        return "Date"
    if "time" in normalized or "date" in normalized:
        return "DateTime64(6)"
    if "bool" in normalized or normalized == "bit":
        return "UInt8"
    return "String"


def _validate_single_clickhouse_type(value: str) -> None:
    """Reject extra ALTER clauses; the classifier alone only recognizes prefixes."""
    root = re.match(r"[A-Za-z][A-Za-z0-9_]*", value)
    suffix = value[root.end() :] if root else value
    if root and not suffix:
        return
    if not root or not suffix.startswith("("):
        raise ValueError("Unsafe ClickHouse schema-evolution type")
    depth = 0
    quoted = escaped = False
    for index, character in enumerate(suffix):
        if quoted:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == "'":
                quoted = False
            continue
        if character == "'":
            quoted = True
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth <= 0 and index != len(suffix) - 1:
                raise ValueError("Unsafe ClickHouse schema-evolution type")
    if depth != 0 or quoted:
        raise ValueError("Unsafe ClickHouse schema-evolution type")
