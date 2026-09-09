"""Platform-owned dbt publish-policy JSON Schema."""

from __future__ import annotations

from typing import Any

from dpone.contracts.dbt_publish_schema_contract_common import (
    NONBLANK_TOKEN,
    QUALITY_PRESETS,
    STRATEGIES,
    object_schema,
)
from dpone.contracts.dbt_publish_schema_semantic_refresh_policy import (
    semantic_refresh_policy as _semantic_refresh_policy,
)
from dpone.contracts.dbt_sqlserver_policy import (
    DBT_PROCESS_TIMEOUT_MAX_SECONDS,
    DBT_PROCESS_TIMEOUT_MIN_SECONDS,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED

_LEGACY_TOOLCHAIN_ID = "dbt-sqlserver-1.10-certified"


def policy_schema() -> dict[str, Any]:
    """Return canonical and legacy-facade policy shapes."""

    return {
        "oneOf": [
            _policy_root(
                "schema",
                "dpone.dbt-publish-policy.v1",
                _policy_profile(
                    _runtime_policy(toolchain_id=_LEGACY_TOOLCHAIN_ID),
                    require_strategy_policy=True,
                ),
            ),
            _policy_root(
                "kind",
                "dpone.dbt_publish_profiles.v1",
                _policy_profile(
                    _legacy_runtime_policy(),
                    require_strategy_policy=False,
                ),
            ),
        ]
    }


def policy_v2_schema() -> dict[str, Any]:
    """Return the additive semantic-refresh policy root.

    V1 stays a closed compatibility contract. Selecting V2 is therefore an
    explicit platform-owner decision rather than an interpretation change to
    an already published schema discriminator.
    """

    return _policy_root(
        "schema",
        "dpone.dbt-publish-policy.v2",
        _policy_profile(
            _runtime_policy(),
            require_strategy_policy=True,
            require_semantic_refresh=True,
        ),
    )


def policy_v3_schema() -> dict[str, Any]:
    """Return the non-semantic policy root for the certified 1.12 toolchain."""

    return _policy_root(
        "schema",
        "dpone.dbt-publish-policy.v3",
        _policy_profile(
            _runtime_policy(),
            require_strategy_policy=True,
        ),
    )


def _policy_profile(
    runtime: dict[str, Any],
    *,
    require_strategy_policy: bool,
    require_semantic_refresh: bool = False,
) -> dict[str, Any]:
    return object_schema(
        (
            "source",
            "sink",
            "runtime",
            *(("strategy_policy",) if require_strategy_policy else ()),
            *(("refresh",) if require_semantic_refresh else ()),
        ),
        {
            "source": _endpoint_policy(),
            "sink": _endpoint_policy(sink=True),
            "state": _state_policy(),
            "runtime": runtime,
            "certification": object_schema(
                (
                    "transport",
                    "schema_evolution",
                    "airflow_runtime_mode",
                ),
                {
                    "transport": NONBLANK_TOKEN,
                    "schema_evolution": NONBLANK_TOKEN,
                    "airflow_runtime_mode": NONBLANK_TOKEN,
                },
            ),
            "strategy_policy": _strategy_policy(),
            "physical_design": _physical_design_policy(),
            "execution": _execution_policy(),
            "quality": object_schema(
                (),
                {
                    "preset": {"enum": list(QUALITY_PRESETS)},
                    "dbt_warning_policy": {"enum": ["fail", "allow"]},
                },
            ),
            "lineage": object_schema(
                (),
                {
                    "enabled": {"type": "boolean"},
                    "preset": {"enum": ["standard"]},
                },
            ),
            **({"refresh": _semantic_refresh_policy()} if require_semantic_refresh else {}),
        },
    )


def _state_policy() -> dict[str, Any]:
    table = object_schema(
        ("schema", "name"),
        {
            "schema": NONBLANK_TOKEN,
            "name": NONBLANK_TOKEN,
        },
    )
    return object_schema(
        ("type", "connection_ref", "partition_checkpoint_table"),
        {
            "type": {"enum": ["mssql", "postgres", "bigquery"]},
            "connection_ref": NONBLANK_TOKEN,
            "table": table,
            "partition_checkpoint_table": table,
        },
    )


def _endpoint_policy(*, sink: bool = False) -> dict[str, Any]:
    properties = {
        "type": NONBLANK_TOKEN,
        "connection_ref": NONBLANK_TOKEN,
        "options": _sink_options_policy() if sink else _source_options_policy(),
    }
    if sink:
        properties.update(
            {
                "target_schema": NONBLANK_TOKEN,
                "staging_schema": NONBLANK_TOKEN,
            }
        )
    required = ("type", "connection_ref", *(("target_schema",) if sink else ()))
    return object_schema(required, properties)


def _source_options_policy() -> dict[str, Any]:
    return object_schema(
        (),
        {
            "native_transfer": object_schema(
                (),
                {"mode": {"enum": ["auto"]}},
            )
        },
    )


def _sink_options_policy() -> dict[str, Any]:
    audit = object_schema(
        (),
        {
            "enabled": {"type": "boolean"},
            "state_schema": NONBLANK_TOKEN,
        },
    )
    return object_schema(
        (),
        {"load_governance": object_schema((), {"audit": audit})},
    )


def _strategy_policy() -> dict[str, Any]:
    return object_schema(
        ("allowed_strategies",),
        {
            "allowed_strategies": {
                "type": "array",
                "minItems": 1,
                "items": {"enum": list(STRATEGIES)},
                "uniqueItems": True,
            },
            "full_refresh": object_schema(
                ("authorized",),
                {
                    "authorized": {"type": "boolean"},
                    "max_source_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 9223372036854775807,
                    },
                },
            ),
            "partition_replace": object_schema(
                (),
                {"require_atomic_capability": {"const": True}},
            ),
        },
    )


def _physical_design_policy() -> dict[str, Any]:
    clickhouse = {
        "profile": NONBLANK_TOKEN,
        "replica_count": {
            "type": "integer",
            "minimum": 1,
            "maximum": 1024,
        },
        "cluster": NONBLANK_TOKEN,
        "partition_by": NONBLANK_TOKEN,
    }
    return object_schema(
        (),
        {"clickhouse": object_schema(("profile",), clickhouse)},
    )


def _execution_policy() -> dict[str, Any]:
    return object_schema(
        (),
        {
            "profile": NONBLANK_TOKEN,
            "max_parallelism": {
                "type": "integer",
                "minimum": 1,
                "maximum": 1024,
            },
            "task_executor": {
                "enum": [
                    "KubernetesExecutor",
                    "CeleryExecutor",
                    "LocalExecutor",
                ]
            },
            "deferrable": {"type": "boolean"},
            "on_finish_action": {
                "enum": [
                    "delete_pod",
                    "delete_succeeded_pod",
                    "keep_pod",
                ]
            },
            "get_logs": {"type": "boolean"},
            "logging_interval_seconds": {
                "type": "integer",
                "minimum": 1,
                "maximum": 3600,
            },
        },
    )


def _runtime_policy(*, toolchain_id: str = DBT_SQLSERVER_1_12_CERTIFIED.contract_id) -> dict[str, Any]:
    return object_schema(
        (
            "image",
            "xcom_sidecar_image",
            "toolchain",
        ),
        {
            "image": NONBLANK_TOKEN,
            "xcom_sidecar_image": NONBLANK_TOKEN,
            "namespace": NONBLANK_TOKEN,
            "toolchain": {
                "const": toolchain_id,
            },
            "dbt_profile": NONBLANK_TOKEN,
            "dbt_target": NONBLANK_TOKEN,
            "dbt_threads": {
                "type": "integer",
                "minimum": 1,
                "maximum": 1024,
            },
            "dbt_timeout_seconds": {
                "type": "integer",
                "minimum": DBT_PROCESS_TIMEOUT_MIN_SECONDS,
                "maximum": DBT_PROCESS_TIMEOUT_MAX_SECONDS,
            },
            "airflow": object_schema(
                (),
                {"service_account_name": NONBLANK_TOKEN},
            ),
        },
    )


def _legacy_runtime_policy() -> dict[str, Any]:
    properties = dict(_runtime_policy(toolchain_id=_LEGACY_TOOLCHAIN_ID)["properties"])
    properties.update(
        {
            "dbt_core_version": NONBLANK_TOKEN,
            "dbt_adapter": NONBLANK_TOKEN,
            "dbt_adapter_version": NONBLANK_TOKEN,
            "dbt_project_dir": NONBLANK_TOKEN,
            "dbt_profiles_dir": NONBLANK_TOKEN,
        }
    )
    return object_schema(("image",), properties)


def _policy_root(
    discriminator: str,
    value: str,
    profile: dict[str, Any],
) -> dict[str, Any]:
    workflow = object_schema(
        ("owner",),
        {
            "schedule": {"type": ["string", "null"]},
            "start_date": {
                "type": "string",
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
            },
            "timezone": NONBLANK_TOKEN,
            "owner": NONBLANK_TOKEN,
            "tags": {
                "type": "array",
                "items": NONBLANK_TOKEN,
                "uniqueItems": True,
            },
            "catchup": {"type": "boolean"},
            "max_active_runs": {
                "type": "integer",
                "minimum": 1,
                "maximum": 1024,
            },
        },
    )
    return object_schema(
        (discriminator, "profiles", "workflows"),
        {
            discriminator: {"const": value},
            "profiles": {
                "type": "object",
                "minProperties": 1,
                "additionalProperties": profile,
            },
            "workflows": {
                "type": "object",
                "minProperties": 1,
                "additionalProperties": workflow,
            },
        },
    )


__all__ = ["policy_schema", "policy_v2_schema", "policy_v3_schema"]
