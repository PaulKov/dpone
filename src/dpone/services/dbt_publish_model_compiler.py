"""Compile one resolved dbt model intent into a dpone runtime manifest."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from dpone.contracts.dbt_identifiers import dbt_workload_id
from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishIssue,
    DbtPublishProfile,
    DbtPublishStrategyPolicy,
)
from dpone.contracts.dbt_semantic_refresh_project_overlay import (
    semantic_refresh_project_overlay,
)
from dpone.contracts.mssql_type_contract import mssql_logical_type
from dpone.governance.quality import QualityGatePolicy


class DbtPublishPlannerPort(Protocol):
    """Resolve strategy and physical design for a trusted publish intent."""

    def strategy(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        policy: DbtPublishStrategyPolicy,
        *,
        supported_strategies: tuple[str, ...] | None = None,
    ) -> tuple[dict[str, Any], tuple[Any, ...]]: ...

    def strategy_candidates(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        policy: DbtPublishStrategyPolicy,
    ) -> tuple[str, ...]: ...

    def physical_design(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        profile: DbtPublishProfile,
    ) -> tuple[dict[str, Any], tuple[Any, ...]]: ...


class DbtModelToWorkloadCompiler:
    """Pure model-to-workload adapter; profiles provide every trusted binding."""

    def __init__(self, *, planner: DbtPublishPlannerPort) -> None:
        self._planner = planner

    def compile(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        profile: DbtPublishProfile,
        strategy_policy: DbtPublishStrategyPolicy,
        *,
        supported_strategies: tuple[str, ...] | None = None,
    ) -> CompiledDbtModel:
        if profile.semantic_refresh is not None:
            return _semantic_refresh_model(
                model,
                intent,
                profile,
            )
        strategy, strategy_issues = self._planner.strategy(
            model,
            intent,
            strategy_policy,
            supported_strategies=supported_strategies,
        )
        physical_design, design_warnings = self._planner.physical_design(model, intent, profile)
        workload_id = dbt_workload_id(
            intent.workflow,
            model.alias,
            model.unique_id,
        )

        target_schema = intent.target_schema or profile.target_schema
        target_table = intent.target_table or model.alias
        source_options = dict(profile.source_options)
        source_options.update(_window_options(profile.source_type, strategy))
        sink_options = dict(profile.sink_options)
        sink_options["physical_design"] = physical_design
        schema_contract, type_issues = _schema_contract(model)
        sink_options["schema_contract"] = schema_contract
        sink_options["dbt_schema_readiness"] = {
            "enabled": True,
            "source_relation": model.relation,
            "column_order": list(model.columns),
            "required_constraints": {
                column.name: list(column.constraints) for column in model.column_contracts if column.constraints
            },
            "extra_columns": "block",
        }
        sink_options.setdefault("lineage", {"enabled": intent.lineage_enabled, "preset": "standard"})
        sink_table: dict[str, str] = {"schema": target_schema, "name": target_table}
        if profile.sink_type.lower() == "mssql" and model.database:
            sink_table["database"] = model.database
        manifest: dict[str, Any] = {
            "name": workload_id,
            "description": f"Generated from dbt model {model.unique_id}; do not edit",
            "runtime": dict(profile.runtime.get("dpone") or {}),
            "source": {
                "type": profile.source_type,
                "connection_ref": profile.source_connection_ref,
                "table": model.relation,
                "options": source_options,
            },
            "sink": {
                "type": profile.sink_type,
                "connection_ref": profile.sink_connection_ref,
                "table": sink_table,
                "mode": "replace" if strategy["mode"] != "incremental_merge" else "append",
                "strategy": _runtime_strategy(strategy),
                "options": sink_options,
            },
            "quality": _quality_policy(intent, profile),
            "gitops": {
                "airflow": {
                    "execution": {
                        "outlets": _release_outlets(
                            sink_type=profile.sink_type,
                            connection_ref=profile.sink_connection_ref,
                            database=model.database,
                            schema=target_schema,
                            table=target_table,
                        ),
                    }
                }
            },
        }
        state_issues = _state_policy_issues(model, strategy, profile)
        if profile.state:
            manifest["state"] = dict(profile.state)
        if profile.staging_schema:
            manifest["sink"]["staging"] = {"schema": profile.staging_schema}
        return CompiledDbtModel(
            model=model,
            intent=intent,
            profile=profile,
            strategy=strategy,
            physical_design=physical_design,
            workload_id=workload_id,
            manifest=manifest,
            warnings=(
                *strategy_issues,
                *design_warnings,
                *state_issues,
                *type_issues,
            ),
        )

    def strategy_candidates(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        policy: DbtPublishStrategyPolicy,
    ) -> tuple[str, ...]:
        """Expose deterministic candidates to the capability orchestrator."""

        return self._planner.strategy_candidates(model, intent, policy)


def _semantic_refresh_model(
    model: DbtModelArtifact,
    intent: DbtPublishIntent,
    profile: DbtPublishProfile,
) -> CompiledDbtModel:
    """Build a proof/template input that cannot execute as a transfer workload."""

    target_schema = intent.target_schema or profile.target_schema
    target_table = intent.target_table or model.alias
    strategy = {
        "mode": "semantic_refresh_v2",
        "model_archetype": "scope_stable_event_fact",
        "mutation_protocol": "update_insert_v1",
        "deletes": "ignore_missing",
        "dbt_incremental_strategy": "dpone_scope_merge",
        "lifecycle_authority": "platform_profile",
        "project_config_overlay": semantic_refresh_project_overlay((model.fqn,)),
    }
    _schema, type_issues = _schema_contract(model)
    return CompiledDbtModel(
        model=model,
        intent=intent,
        profile=profile,
        strategy=strategy,
        physical_design={},
        workload_id=dbt_workload_id(intent.workflow, model.alias, model.unique_id),
        manifest={
            "schema": "dpone.dbt-semantic-refresh-model-template.v1",
            "mode": "semantic_refresh_v2",
            "model_unique_id": model.unique_id,
            "logical_output_asset_uri": f"{profile.sink_type}://{target_schema}/{target_table}",
        },
        warnings=type_issues,
    )


def _release_outlets(
    *,
    sink_type: str,
    connection_ref: str,
    database: str | None,
    schema: str,
    table: str,
) -> list[dict[str, Any] | str]:
    """Emit environment-neutral outlets for immutable dbt release packs.

    MSSQL keeps a logical ``asset_ref`` (no host/port). Other engines keep the
    compact ``engine://schema/table`` form, which is already environment-neutral.
    """

    engine = sink_type.strip().lower()
    if engine == "mssql":
        if not database:
            return []
        return [
            {
                "asset_ref": {
                    "engine": "mssql",
                    "connection_ref": connection_ref,
                    "database": database,
                    "schema": schema,
                    "table": table,
                }
            }
        ]
    return [f"{engine}://{schema}/{table}"]


def _window_options(source_type: str, strategy: Mapping[str, Any]) -> dict[str, Any]:
    if source_type.lower() != "mssql" or strategy.get("mode") != "partition_replace":
        return {}
    partition = _mapping(strategy.get("partition"))
    column = str(partition.get("column") or "")
    days = int(strategy.get("window_days") or 0)
    if not column or not days:
        return {}
    escaped = column.replace("]", "]]")
    lookback_days = max(days - 1, 0)
    return {
        "partition_column": column,
        "source_custom_predicate": (
            f"[{escaped}] >= DATEADD(day, -{lookback_days}, "
            "CONVERT(datetime2, '{{{{ data_interval_start }}}}', 127)) "
            f"AND [{escaped}] < CONVERT(datetime2, '{{{{ data_interval_end }}}}', 127)"
        ),
    }


def _state_policy_issues(
    model: DbtModelArtifact,
    strategy: Mapping[str, Any],
    profile: DbtPublishProfile,
) -> tuple[DbtPublishIssue, ...]:
    if strategy.get("mode") not in {"incremental_merge", "partition_replace"}:
        return ()
    state = profile.state
    if (
        not isinstance(state, Mapping)
        or not state
        or not isinstance(state.get("connection_ref"), str)
        or not state.get("connection_ref")
        or not isinstance(state.get("partition_checkpoint_table"), Mapping)
    ):
        return (
            DbtPublishIssue(
                code="DPONE_DBT_STATE_POLICY_REQUIRED",
                message=(
                    "Stateful dbt publishing requires a platform-owned state connection and partition checkpoint table"
                ),
                path=model.original_file_path,
                remediation=(
                    "Add state.connection_ref and state.partition_checkpoint_table to the selected dbt publish profile."
                ),
            ),
        )
    return ()


def _runtime_strategy(strategy: Mapping[str, Any]) -> dict[str, Any]:
    payload = {key: value for key, value in strategy.items() if key not in {"decision_reason", "window_days"}}
    return payload


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _schema_contract(
    model: DbtModelArtifact,
) -> tuple[dict[str, Any], tuple[DbtPublishIssue, ...]]:
    columns: dict[str, dict[str, Any]] = {}
    issues = []
    for column in model.column_contracts:
        try:
            logical_type = mssql_logical_type(column.data_type)
        except ValueError:
            logical_type = {"type": "unsupported"}
            issues.append(
                DbtPublishIssue(
                    code="DPONE_DBT_CONTRACT_TYPE_UNSUPPORTED",
                    message=(f"dbt contract column has no safe canonical MSSQL mapping: {column.name}"),
                    path=(f"{model.original_file_path}#columns.{column.name}"),
                    remediation=(
                        "Use a supported MSSQL contract type or define an approved explicit type-fidelity profile."
                    ),
                )
            )
        columns[column.name] = {
            **logical_type,
            "nullable": column.nullable,
        }
    return (
        {
            "enforcement": "strict",
            "columns": columns,
        },
        tuple(issues),
    )


def _quality_policy(
    intent: DbtPublishIntent,
    profile: DbtPublishProfile,
) -> dict[str, Any]:
    configured = dict(profile.quality)
    profile_preset = str(configured.get("preset") or "standard").strip().lower()
    intent_preset = intent.quality_preset.strip().lower()
    ranks = {"standard": 0, "strict": 1}
    if profile_preset not in ranks or intent_preset not in ranks:
        raise ValueError("dbt quality preset must be standard or strict")
    effective_preset = max(
        (profile_preset, intent_preset),
        key=ranks.__getitem__,
    )
    raw_gates = configured.get("gates")
    if raw_gates is None:
        tolerance = 0.0 if effective_preset == "strict" else 0.1
        raw_gates = [
            {
                "id": "source_target_rows",
                "type": "row_count_reconciliation",
                "severity": "error",
                "tolerance": {"mode": "pct", "value": tolerance},
            }
        ]
    policy = QualityGatePolicy.from_config({"gates": raw_gates})
    return {
        "acceptance": {
            "enabled": True,
            "mode": "required",
        },
        "gates": [dict(item.raw) for item in policy.gates],
    }


__all__ = ["DbtModelToWorkloadCompiler"]
