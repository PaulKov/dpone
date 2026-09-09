"""Canonical target-sensitive projection of a selected dbt graph."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.dbt_contract_validation import canonical_fingerprint

_EXECUTABLE_PREFIXES = ("model.", "seed.", "snapshot.", "test.", "unit_test.")


def dbt_graph_contract_sha256(
    manifest: Mapping[str, Any],
    selected_graph_unique_ids: tuple[str, ...],
    *,
    policy_projection: Mapping[str, object] | None = None,
) -> str:
    """Fingerprint stable selected-node semantics without volatile dbt metadata."""

    selected = tuple(sorted(_unique_ids(selected_graph_unique_ids)))
    nodes = _manifest_nodes(manifest)
    projection = []
    for unique_id in selected:
        node = nodes.get(unique_id)
        if not isinstance(node, Mapping):
            raise ValueError(f"selected dbt node is missing from the manifest: {unique_id}")
        depends_on = _mapping(node.get("depends_on"))
        dependencies = tuple(sorted(_strings(depends_on.get("nodes"))))
        macro_dependencies = tuple(sorted(_strings(depends_on.get("macros"))))
        config = _mapping(node.get("config"))
        projection.append(
            {
                "unique_id": unique_id,
                "resource_type": _required_text(
                    node.get("resource_type"),
                    f"{unique_id}.resource_type",
                ),
                "fqn": list(_strings(node.get("fqn"), required=True)),
                "depends_on": list(dependencies),
                "depends_on_macros": list(macro_dependencies),
                "materialized": _optional_text(config.get("materialized")),
                "policy_semantics": _graph_policy_semantics(
                    node,
                    config,
                ),
                "relation": {
                    "database": _optional_text(node.get("database")),
                    "schema": _optional_text(node.get("schema")),
                    "alias": _optional_text(node.get("alias") or node.get("name")),
                    "relation_name": _optional_text(node.get("relation_name")),
                },
                "columns": _column_contract(node.get("columns")),
            }
        )
    contract: dict[str, object] = {"selected_nodes": projection}
    if policy_projection is not None:
        contract["policy_projection"] = _stable_value(policy_projection)
    return canonical_fingerprint(contract)


def _graph_policy_semantics(
    node: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, object]:
    """Normalize mutation-relevant fields governed by the SQL Server policy."""

    resource_type = _optional_text(node.get("resource_type"))
    semantics: dict[str, object] = {
        "language": _optional_text(node.get("language")),
        "enabled": config.get("enabled"),
        "pre_hook": _stable_value(config.get("pre-hook", [])),
        "post_hook": _stable_value(config.get("post-hook", [])),
        "grants": _stable_value(config.get("grants", {})),
        "full_refresh": config.get("full_refresh", False),
        "contract": _stable_value(config.get("contract", {})),
        "node_contract": _stable_value(node.get("contract", {})),
    }
    if resource_type == "model":
        materialized = _optional_text(config.get("materialized"))
        semantics.update(
            {
                "as_columnstore": config.get("as_columnstore"),
                "indexes": _stable_value(config.get("indexes")),
                "drop_unmanaged_indexes": config.get("drop_unmanaged_indexes"),
                "prefer_single_alter_column": config.get("prefer_single_alter_column"),
                "query_options": _stable_value(config.get("query_options", {})),
                "query_options_raw": _stable_value(config.get("query_options_raw", [])),
                "query_tag": _optional_text(config.get("query_tag")),
                "sql_header": _optional_text(config.get("sql_header")),
                "persist_docs": _stable_value(config.get("persist_docs", {})),
                "column_types": _stable_value(config.get("column_types", {})),
                "incremental_predicates": _stable_value(config.get("incremental_predicates", [])),
                "predicates": _stable_value(config.get("predicates", [])),
                "auto_provision_aad_principals": config.get(
                    "auto_provision_aad_principals",
                    False,
                ),
                "column_type_expansion_max_rows": config.get(
                    "column_type_expansion_max_rows",
                    1_000_000,
                ),
                "constraints": _stable_value(node.get("constraints", [])),
            }
        )
        if materialized == "table":
            semantics["table_refresh_method"] = config.get(
                "table_refresh_method",
                "rename",
            )
        elif materialized == "incremental":
            semantics.update(
                {
                    "incremental_strategy": config.get("incremental_strategy"),
                    "unique_key": _stable_value(config.get("unique_key")),
                    "on_schema_change": config.get("on_schema_change"),
                }
            )
    elif resource_type == "test":
        semantics.update(
            {
                "store_failures": config.get("store_failures", False),
                "store_failures_as": _optional_text(config.get("store_failures_as")),
                "severity": _optional_text(config.get("severity")),
                "where": _optional_text(config.get("where")),
                "limit": config.get("limit"),
                "fail_calc": _optional_text(config.get("fail_calc")),
                "warn_if": _optional_text(config.get("warn_if")),
                "error_if": _optional_text(config.get("error_if")),
            }
        )
    return semantics


def expected_dbt_run_result_ids(
    manifest: Mapping[str, Any],
    selected_graph_unique_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """Exclude only selected ephemeral models from expected run-results."""

    nodes = _manifest_nodes(manifest)
    expected = []
    for unique_id in sorted(_unique_ids(selected_graph_unique_ids)):
        if not unique_id.startswith("model."):
            expected.append(unique_id)
            continue
        node = nodes.get(unique_id)
        if not isinstance(node, Mapping):
            raise ValueError(f"selected dbt model is missing from the manifest: {unique_id}")
        materialized = _optional_text(_mapping(node.get("config")).get("materialized"))
        if not materialized:
            raise ValueError("dbt selected model materialization is unavailable")
        if materialized != "ephemeral":
            expected.append(unique_id)
    if not expected:
        raise ValueError("dbt selection expects no run-results nodes")
    return tuple(expected)


def dbt_publish_logical_target(
    manifest: Mapping[str, Any],
    publish_model_unique_ids: tuple[str, ...],
) -> tuple[str, str]:
    """Return the one logical database/schema used by published models."""

    nodes = _manifest_nodes(manifest)
    targets = set()
    for unique_id in sorted(_unique_ids(publish_model_unique_ids)):
        if not unique_id.startswith("model."):
            raise ValueError("publish identities must reference dbt models")
        node = nodes.get(unique_id)
        if not isinstance(node, Mapping):
            raise ValueError(f"publish model is missing from the manifest: {unique_id}")
        materialized = _optional_text(_mapping(node.get("config")).get("materialized"))
        if materialized == "ephemeral":
            raise ValueError("a publish-enabled model cannot be ephemeral")
        database = _required_text(node.get("database"), f"{unique_id}.database")
        schema = _required_text(node.get("schema"), f"{unique_id}.schema")
        targets.add((database, schema))
    if len(targets) != 1:
        raise ValueError("one dbt workflow must use one logical database and schema")
    return targets.pop()


def _manifest_nodes(manifest: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for section in ("nodes", "unit_tests"):
        values = manifest.get(section)
        if values is None:
            continue
        if not isinstance(values, Mapping):
            raise ValueError(f"dbt manifest {section} must be an object")
        for unique_id, node in values.items():
            if not isinstance(unique_id, str) or unique_id in result:
                raise ValueError("dbt manifest contains invalid or duplicate node identities")
            result[unique_id] = node
    return result


def _column_contract(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    if not isinstance(value, Mapping):
        raise ValueError("dbt node columns must be an object")
    result: list[dict[str, object]] = []
    for name, raw in sorted(value.items(), key=lambda item: str(item[0])):
        if not isinstance(name, str) or not name or not isinstance(raw, Mapping):
            raise ValueError("dbt node column contract is invalid")
        constraints = raw.get("constraints")
        normalized_constraints: list[object]
        if constraints is None:
            normalized_constraints = []
        elif isinstance(constraints, Sequence) and not isinstance(
            constraints,
            str | bytes,
        ):
            normalized_constraints = [_stable_value(item) for item in constraints]
        else:
            raise ValueError("dbt node column constraints must be an array")
        result.append(
            {
                "name": name,
                "data_type": _optional_text(raw.get("data_type")),
                "constraints": normalized_constraints,
            }
        )
    return result


def _stable_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_value(item)
            for key, item in sorted(
                value.items(),
                key=lambda item: str(item[0]),
            )
        }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_stable_value(item) for item in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    raise ValueError("dbt graph contract contains an unsupported value")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: object, *, required: bool = False) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        if required:
            raise ValueError("dbt graph field must be an array")
        return ()
    items = tuple(value)
    if (required and not items) or any(not isinstance(item, str) or not item for item in items):
        raise ValueError("dbt graph field contains an invalid value")
    return items


def _unique_ids(values: tuple[str, ...]) -> tuple[str, ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(item, str) or not item.startswith(_EXECUTABLE_PREFIXES) for item in values)
        or len(values) != len(set(values))
    ):
        raise ValueError("dbt selected graph identities are invalid")
    return values


def _required_text(value: object, field: str) -> str:
    text = _optional_text(value)
    if not text:
        raise ValueError(f"dbt graph {field} is required")
    return text


def _optional_text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


__all__ = [
    "dbt_graph_contract_sha256",
    "dbt_publish_logical_target",
    "expected_dbt_run_result_ids",
]
