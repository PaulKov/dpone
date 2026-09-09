"""Canonical inert dbt Core defaults admitted by the SQL Server graph policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

DBT_1_12_DEFAULT_CONFIG: Mapping[str, object] = {
    "latest_version_pointer": {"enabled": None, "alias": None},
    "on_error": None,
    "sql_header": None,
    "static_analysis": None,
}


def noncanonical_default_config_fields(config: Mapping[str, Any]) -> tuple[str, ...]:
    """Return emitted dbt Core 1.12 defaults whose values are not inert."""

    return tuple(
        field for field, expected in DBT_1_12_DEFAULT_CONFIG.items() if field in config and config[field] != expected
    )


def unknown_config_fields(config: Mapping[str, Any], *, allowed: frozenset[str]) -> tuple[str, ...]:
    """Return deterministic config fields outside the closed policy surface."""

    return tuple(sorted(set(config) - allowed))


def noncanonical_adapter_config_fields(config: Mapping[str, Any]) -> tuple[str, ...]:
    """Return SQL Server adapter settings that activate unsupported behaviour."""

    checks = (
        ("query_options", _empty_mapping(config.get("query_options"))),
        ("query_options_raw", _empty_sequence(config.get("query_options_raw"))),
        ("query_tag", config.get("query_tag") is None),
        ("sql_header", config.get("sql_header") is None),
        ("persist_docs", _empty_mapping(config.get("persist_docs"))),
        ("column_types", _empty_mapping(config.get("column_types"))),
        ("incremental_predicates", _empty_sequence(config.get("incremental_predicates"))),
        ("predicates", _empty_sequence(config.get("predicates"))),
        ("auto_provision_aad_principals", config.get("auto_provision_aad_principals") in (None, False)),
        ("column_type_expansion_max_rows", _default_expansion_limit(config.get("column_type_expansion_max_rows"))),
    )
    return tuple(field for field, passed in checks if not passed)


def _empty_mapping(value: object) -> bool:
    return value is None or isinstance(value, Mapping) and not value


def _empty_sequence(value: object) -> bool:
    return value is None or isinstance(value, Sequence) and not isinstance(value, str | bytes) and not value


def _default_expansion_limit(value: object) -> bool:
    return value is None or (type(value) is int and value == 1_000_000)


__all__ = [
    "DBT_1_12_DEFAULT_CONFIG",
    "noncanonical_adapter_config_fields",
    "noncanonical_default_config_fields",
    "unknown_config_fields",
]
