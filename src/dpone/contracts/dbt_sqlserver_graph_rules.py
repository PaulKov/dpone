"""Closed node-config, macro and constraint rules for the SQL Server graph."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.contracts import dbt_sqlserver_config_defaults as config_defaults
from dpone.contracts.dbt_contract_validation import canonical_fingerprint

DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED = "DPONE_DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED"
DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED = "DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED"

_PINNED_ADAPTER_PACKAGE = "dbt_sqlserver"
_PINNED_CORE_PACKAGE = "dbt"
_PINNED_ADAPTER_MACRO_NAMES_SHA256 = "sha256:a0cf3c021cf263dc50b2f23db93175e6bc778229d70ac0b7833c07446d3f38ee"
_MODEL_CONFIG_KEYS = frozenset(
    {
        "access",
        "alias",
        "as_columnstore",
        "auto_provision_aad_principals",
        "batch_size",
        "begin",
        "column_type_expansion_max_rows",
        "column_types",
        "concurrent_batches",
        "contract",
        "database",
        "docs",
        "drop_unmanaged_indexes",
        "enabled",
        "event_time",
        "freshness",
        "full_refresh",
        "grants",
        "group",
        "incremental_predicates",
        "incremental_strategy",
        "indexes",
        "lookback",
        "latest_version_pointer",
        "materialized",
        "meta",
        "on_configuration_change",
        "on_error",
        "on_schema_change",
        "packages",
        "persist_docs",
        "post-hook",
        "pre-hook",
        "predicates",
        "prefer_single_alter_column",
        "query_options",
        "query_options_raw",
        "query_tag",
        "quoting",
        "schema",
        "sql_header",
        "static_analysis",
        "table_refresh_method",
        "tags",
        "unique_key",
    }
)
_TEST_CONFIG_KEYS = frozenset(
    {
        "alias",
        "database",
        "enabled",
        "error_if",
        "fail_calc",
        "group",
        "limit",
        "materialized",
        "meta",
        "schema",
        "severity",
        "sql_header",
        "store_failures",
        "store_failures_as",
        "static_analysis",
        "tags",
        "warn_if",
        "where",
    }
)
_UNIT_TEST_CONFIG_KEYS = frozenset({"enabled", "group", "meta", "static_analysis", "tags"})
_COLUMN_NOT_NULL_KEYS = frozenset(
    {
        "expression",
        "name",
        "to",
        "to_columns",
        "type",
        "warn_unenforced",
        "warn_unsupported",
    }
)

DBT_SQLSERVER_GRAPH_RULES_PAYLOAD: Mapping[str, object] = {
    "config_keys": {
        "model": sorted(_MODEL_CONFIG_KEYS),
        "test": sorted(_TEST_CONFIG_KEYS),
        "unit_test": sorted(_UNIT_TEST_CONFIG_KEYS),
    },
    "adapter_config": {
        "query_options": "absent_null_or_empty_mapping",
        "query_options_raw": "absent_null_or_empty_array",
        "query_tag": "absent_or_null",
        "sql_header": "absent_or_null",
        "persist_docs": "absent_null_or_empty_mapping",
        "column_types": "absent_null_or_empty_mapping",
        "incremental_predicates": "absent_null_or_empty_array",
        "predicates": "absent_null_or_empty_array",
        "auto_provision_aad_principals": "absent_null_or_false",
        "column_type_expansion_max_rows": "absent_null_or_1000000",
    },
    "constraints": {
        "model": "absent_or_empty",
        "column": {
            "types": ["not_null"],
            "keys": sorted(_COLUMN_NOT_NULL_KEYS),
        },
    },
    "macros": {
        "adapter_package": _PINNED_ADAPTER_PACKAGE,
        "core_package": _PINNED_CORE_PACKAGE,
        "adapter_macro_names_sha256": _PINNED_ADAPTER_MACRO_NAMES_SHA256,
        "project_or_package_shadowing": False,
    },
}


@dataclass(frozen=True, slots=True)
class DbtSqlServerGraphRuleViolation:
    """One content-free rule violation translated by the public graph policy."""

    code: str
    unique_id: str
    field: str
    expectation: str
    node: Mapping[str, Any] | None


def model_rule_violations(
    unique_id: str,
    node: Mapping[str, Any],
    config: Mapping[str, Any],
) -> tuple[DbtSqlServerGraphRuleViolation, ...]:
    """Return closed model-config and physical-constraint violations."""

    violations = [
        *_closed_config_violations(
            unique_id,
            node,
            config,
            allowed=_MODEL_CONFIG_KEYS,
        ),
        *_config_violations(
            unique_id,
            node,
            config_defaults.noncanonical_adapter_config_fields(config),
            "use the canonical disabled/default SQL Server v1 value",
        ),
        *_physical_constraint_violations(unique_id, node),
    ]
    return tuple(violations)


def test_rule_violations(
    unique_id: str,
    node: Mapping[str, Any],
    config: Mapping[str, Any],
) -> tuple[DbtSqlServerGraphRuleViolation, ...]:
    """Return closed data-test config violations."""

    return tuple(
        _closed_config_violations(
            unique_id,
            node,
            config,
            allowed=_TEST_CONFIG_KEYS,
        )
    )


def unit_test_rule_violations(
    unique_id: str,
    node: Mapping[str, Any],
    config: Mapping[str, Any],
) -> tuple[DbtSqlServerGraphRuleViolation, ...]:
    """Return closed unit-test config violations."""

    return tuple(
        _closed_config_violations(
            unique_id,
            node,
            config,
            allowed=_UNIT_TEST_CONFIG_KEYS,
        )
    )


def macro_rule_violations(
    manifest: Mapping[str, Any],
) -> tuple[DbtSqlServerGraphRuleViolation, ...]:
    """Reject adapter macro-set drift and any project/package shadow."""

    raw_macros = manifest.get("macros")
    if raw_macros is None:
        return ()
    if not isinstance(raw_macros, Mapping):
        return (
            _violation(
                "manifest",
                None,
                "manifest.macros",
                "be an object",
            ),
        )

    macros = tuple(
        (str(key), value)
        for key, value in sorted(raw_macros.items(), key=lambda item: str(item[0]))
        if isinstance(value, Mapping)
    )
    adapter_names = tuple(
        sorted(
            {
                str(macro.get("name"))
                for _unique_id, macro in macros
                if macro.get("package_name") == _PINNED_ADAPTER_PACKAGE
                and isinstance(macro.get("name"), str)
                and macro.get("name")
            }
        )
    )
    violations: list[DbtSqlServerGraphRuleViolation] = []
    metadata = manifest.get("metadata")
    if (
        isinstance(metadata, Mapping)
        and metadata.get("adapter_type") == "sqlserver"
        and _macro_names_fingerprint(adapter_names) != _PINNED_ADAPTER_MACRO_NAMES_SHA256
    ):
        violations.append(
            _violation(
                "manifest",
                None,
                "manifest.macros",
                "contain the exact pinned dbt-sqlserver macro set",
            )
        )
    adapter_name_set = set(adapter_names)
    for unique_id, macro in macros:
        name = macro.get("name")
        package = macro.get("package_name")
        if (
            isinstance(name, str)
            and name in adapter_name_set
            and package not in {_PINNED_ADAPTER_PACKAGE, _PINNED_CORE_PACKAGE}
        ):
            violations.append(
                _violation(
                    unique_id,
                    macro,
                    f"macros.{name}",
                    "not shadow a pinned dbt-sqlserver adapter macro",
                )
            )
    return tuple(violations)


def _config_violations(
    unique_id: str,
    node: Mapping[str, Any],
    fields: Sequence[str],
    expectation: str,
) -> list[DbtSqlServerGraphRuleViolation]:
    return [
        _violation(
            unique_id,
            node,
            f"config.{field}",
            expectation,
        )
        for field in fields
    ]


def _closed_config_violations(
    unique_id: str,
    node: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    allowed: frozenset[str],
) -> list[DbtSqlServerGraphRuleViolation]:
    return [
        *_config_violations(
            unique_id,
            node,
            config_defaults.unknown_config_fields(config, allowed=allowed),
            "be absent because the pinned SQL Server config surface is closed",
        ),
        *_config_violations(
            unique_id,
            node,
            config_defaults.noncanonical_default_config_fields(config),
            "use the canonical dbt Core 1.12 default value",
        ),
    ]


def _physical_constraint_violations(
    unique_id: str,
    node: Mapping[str, Any],
) -> list[DbtSqlServerGraphRuleViolation]:
    violations: list[DbtSqlServerGraphRuleViolation] = []
    model_constraints = node.get("constraints")
    if not _empty_sequence(model_constraints):
        violations.append(
            _violation(
                unique_id,
                node,
                "constraints",
                "be absent or empty for SQL Server v1",
                code=DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED,
            )
        )
    columns = node.get("columns")
    if not isinstance(columns, Mapping):
        return violations
    for column_name, raw_column in sorted(
        columns.items(),
        key=lambda item: str(item[0]),
    ):
        if not isinstance(raw_column, Mapping):
            continue
        constraints = raw_column.get("constraints")
        if _admitted_column_constraints(constraints):
            continue
        violations.append(
            _violation(
                unique_id,
                node,
                f"columns.{column_name}.constraints",
                "contain only the physically provable not_null constraint",
                code=DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED,
            )
        )
    return violations


def _admitted_column_constraints(value: object) -> bool:
    if value is None:
        return True
    if not _is_sequence(value):
        return False
    return all(
        isinstance(item, Mapping)
        and item.get("type") == "not_null"
        and set(item) <= _COLUMN_NOT_NULL_KEYS
        and item.get("expression") is None
        and item.get("to") is None
        and _empty_sequence(item.get("to_columns"))
        for item in value
    )


def _empty_sequence(value: object) -> bool:
    return value is None or _is_sequence(value) and not value


def has_non_empty_unique_key(value: object) -> bool:
    """Return whether an incremental unique key is one or more named columns."""

    if isinstance(value, str):
        return bool(value.strip())
    return _is_sequence(value) and bool(value) and all(isinstance(item, str) and bool(item.strip()) for item in value)


def _is_sequence(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, str | bytes)


def _macro_names_fingerprint(names: tuple[str, ...]) -> str:
    return sqlserver_policy_fingerprint({"macro_names": list(names)})


def sqlserver_policy_fingerprint(value: Mapping[str, Any]) -> str:
    """Fingerprint one stable SQL Server policy projection canonically."""

    return canonical_fingerprint(value)


def _violation(
    unique_id: str,
    node: Mapping[str, Any] | None,
    field: str,
    expectation: str,
    *,
    code: str = DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED,
) -> DbtSqlServerGraphRuleViolation:
    return DbtSqlServerGraphRuleViolation(
        code=code,
        unique_id=unique_id,
        field=field,
        expectation=expectation,
        node=node,
    )


__all__ = [
    "DBT_SQLSERVER_GRAPH_RULES_PAYLOAD",
    "DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED",
    "DbtSqlServerGraphRuleViolation",
    "has_non_empty_unique_key",
    "macro_rule_violations",
    "model_rule_violations",
    "sqlserver_policy_fingerprint",
    "test_rule_violations",
    "unit_test_rule_violations",
]
