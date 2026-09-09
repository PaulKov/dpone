"""Target-type authority frozen by the pre-source MSSQL catalog plan.

The PostgreSQL projection owns business semantics, while an externally
provisioned SQL Server target may own an equivalent physical spelling for a
framework column (for example ``char(26)`` instead of ``varchar(26)`` for an
ULID).  The catalog preplan binds that live spelling once.  Native staging,
lineage, and strategy metadata must all consume the same decision rather than
re-deriving their own target type after source rows have been exported.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

MSSQL_SCHEMA_PREPLAN_OPTION = "__dpone_mssql_schema_preplan"


def preplanned_mssql_target_type(load_config: Any, column: str, default: str) -> str:
    """Return the catalog-bound type for ``column`` or its canonical default."""

    options = getattr(load_config, "options", {}) or {}
    if not isinstance(options, Mapping):
        return default
    preplan = options.get(MSSQL_SCHEMA_PREPLAN_OPTION)
    raw = getattr(preplan, "target_column_types", ())
    if not isinstance(raw, tuple):
        return default
    by_name = {str(name).casefold(): str(dtype) for name, dtype in raw}
    return by_name.get(str(column).casefold(), default)


__all__ = ["MSSQL_SCHEMA_PREPLAN_OPTION", "preplanned_mssql_target_type"]
