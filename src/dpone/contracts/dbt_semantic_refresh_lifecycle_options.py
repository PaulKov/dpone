"""Pinned mutation-sensitive dbt-sqlserver adapter option closure."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

EXPECTED_ADAPTER_OPTIONS: Mapping[str, object] = MappingProxyType(
    {
        "enabled": True,
        "materialized": "incremental",
        "incremental_strategy": "dpone_scope_merge",
        "on_schema_change": "fail",
        "contract_enforced": True,
        "pre_hook": (),
        "post_hook": (),
        "grants": {},
        "persist_docs": {},
        "as_columnstore": False,
        "indexes": (),
        "drop_unmanaged_indexes": False,
        "prefer_single_alter_column": False,
        "query_options": {},
        "query_options_raw": (),
        "sql_header": None,
        "incremental_predicates": (),
        "predicates": (),
        "column_types": {},
        "auto_provision_aad_principals": False,
        "column_type_expansion_max_rows": None,
    }
)

__all__ = ["EXPECTED_ADAPTER_OPTIONS"]
