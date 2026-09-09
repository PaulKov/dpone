from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft7Validator

from dpone.commands.plan_cmd import _render_text as render_plan_text
from dpone.config.state import resolve_mssql_state_location
from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.readiness.managed_planning import ExecutionPlanService
from dpone.readiness.managed_planning_snapshot import state_plan
from dpone.runtime.credentials.config import CredentialsConfig

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATHS = {
    "flat": ROOT / "src/dpone/schema/etl-config.schema.json",
    "batch": ROOT / "src/dpone/schema/etl-batch-manifest.schema.json",
    "flow": ROOT / "src/dpone/schema/etl-flow-manifest.schema.json",
    "flow_fragment": ROOT / "src/dpone/schema/etl-flow-fragment-manifest.schema.json",
}


def _typed_reconciliation() -> dict[str, Any]:
    return {
        "enabled": True,
        "mode": "key_snapshot",
        "cadence": "every_run",
        "consistency": "same_source_snapshot",
        "delete_policy": "soft_delete",
        "empty_snapshot": {"policy": "fail"},
        "guards": {"max_delete_ratio": 0.05, "max_delete_rows": 100_000},
    }


def _canonical_state() -> dict[str, Any]:
    return {
        "type": "mssql",
        "connection_ref": "mssql_sample_metrics_state",
        "atomicity": "target_atomic",
        "provisioning": "external",
        "table": {"name": "dpone_source_state"},
        "run_table": {"name": "dpone_run_state"},
        "receipt_table": {"name": "dpone_commit_receipt"},
        "repair_authority_table": {"name": "dpone_repair_authority"},
        "repair_consumption_table": {"name": "dpone_repair_authority_consumption"},
        "audit_table": {"name": "dpone_load_audit"},
    }


def _canonical_process(*, legacy_connections: bool = False) -> dict[str, Any]:
    if legacy_connections:
        source_connection = {"connection_type": "airflow", "connection_id": "source"}
        sink_connection = {"connection_type": "airflow", "connection_id": "target"}
    else:
        source_connection = {"connection_ref": "postgres_sample_metrics_source"}
        sink_connection = {"connection_ref": "mssql_sample_metrics_target"}
    return {
        "name": "sample_metrics_metrics_value",
        "source": {
            "type": "postgres",
            **source_connection,
            "table": {"schema": "public", "name": "metrics_value"},
            "options": {"incremental_strategy": "xmin"},
        },
        "sink": {
            "type": "mssql",
            **sink_connection,
            "table": {"schema": "sample_metrics", "name": "metrics_value"},
            "strategy": {
                "mode": "incremental_merge",
                "unique_key": ["guid"],
                "merge_policy": "update_insert",
            },
            "options": {
                "technical_columns": "required",
                "soft_delete": {"mode": "timestamp_only"},
                "physical_design": {
                    "apply_runtime": False,
                    "columns": {
                        "metric_code": {
                            "target_type": {"mssql": "nvarchar(450)"},
                        }
                    },
                },
            },
        },
        "reconciliation": _typed_reconciliation(),
        "state": _canonical_state(),
    }


def _manifest(schema_name: str) -> dict[str, Any]:
    if schema_name == "flat":
        process = _canonical_process(legacy_connections=True)
        unique_key = list(process["sink"]["strategy"]["unique_key"])
        process["source"]["options"]["unique_key"] = unique_key
        return process
    process = _canonical_process()
    if schema_name == "batch":
        process.pop("name")
        return {
            "kind": "dpone.batch.v1",
            "defaults": process,
            "schemas": {"public": {"tables": ["metrics_value"]}},
        }
    if schema_name == "flow":
        return {
            "kind": "dpone.flow.v1",
            "authoring": {"mode": "flow", "source": "pipelines/sample_metrics/pipeline.yaml"},
            "metadata": {"id": "sample_metrics", "domain": "platform"},
            "processes": [process],
        }
    return {"kind": "dpone.flow-fragment.v1", "processes": [process]}


def _metrics_config_flow_manifest() -> dict[str, Any]:
    """Representative production flow for the text-key snapshot contract."""

    process = _canonical_process()
    process["name"] = "sample_metrics_metrics_config"
    process["source"]["table"]["name"] = "metrics_config"
    process["source"]["options"].update(
        {
            "batch_commit_mode": "whole",
            "export_format": "csv",
            "compress_export": False,
        }
    )
    process["sink"]["table"]["name"] = "metrics_config"
    process["sink"]["strategy"]["unique_key"] = ["metric_code"]
    process["sink"]["options"]["schema_contract"] = {
        "columns": {
            "metric_code": {"logical_type": "string", "nullable": False},
            "name": {"logical_type": "string", "nullable": False},
            "weight": {"logical_type": "float", "nullable": False},
        }
    }
    return {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": "sample_metrics_metrics_config/pipeline.yaml"},
        "metadata": {"id": "sample_metrics_metrics_config", "domain": "platform"},
        "processes": [process],
    }


def _schema_errors(schema_name: str, manifest: dict[str, Any]) -> list[Any]:
    schema = json.loads(SCHEMA_PATHS[schema_name].read_text(encoding="utf-8"))
    Draft7Validator.check_schema(schema)
    return list(Draft7Validator(schema).iter_errors(manifest))


def _physical_design(schema_name: str, manifest: dict[str, Any]) -> dict[str, Any]:
    if schema_name == "flat":
        return manifest["sink"]["options"]["physical_design"]
    if schema_name == "batch":
        return manifest["defaults"]["sink"]["options"]["physical_design"]
    return manifest["processes"][0]["sink"]["options"]["physical_design"]


def _builder_process(reconciliation: object) -> dict[str, Any]:
    process = _canonical_process()
    process["reconciliation"] = reconciliation
    process.pop("state")
    return process


def _state_connection(database: str, schema: str) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(database="credential_db", schema="credential_schema"),
        safe_metadata={"connection_ref": "mssql_sample_metrics_state"},
        descriptor=ResolvedConnectionDescriptor(
            connection_type="mssql",
            properties={"database": database, "schema": schema},
        ),
    )


def test_typed_reconciliation_does_not_enable_legacy_service() -> None:
    load_config = LoadConfigBuilder().build(_builder_process(_typed_reconciliation()))

    assert load_config.reconciliation is False
    assert load_config.reconciliation_policy is not None
    assert load_config.reconciliation_policy.key_snapshot_enabled is True
    assert load_config.options["reconciliation"] == _typed_reconciliation()


def test_plan_exposes_key_snapshot_state_wire_and_no_runtime_ddl(tmp_path: Path) -> None:
    manifest_path = tmp_path / "sample_metrics.flow.yaml"
    manifest = _manifest("flow")
    manifest["authoring"]["source"] = manifest_path.name
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    plan = ExecutionPlanService().plan_manifest(manifest_path)

    assert plan["reconciliation"] == _typed_reconciliation()
    assert plan["state"] == {
        "backend": "mssql",
        "connection_ref": "mssql_sample_metrics_state",
        "connection_id": "mssql_sample_metrics_state",
        "atomicity": "target_atomic",
        "provisioning": "external",
        "location_authority": "environment_registry",
        "tables": {
            "table": "dpone_source_state",
            "run_table": "dpone_run_state",
            "receipt_table": "dpone_commit_receipt",
            "repair_authority_table": "dpone_repair_authority",
            "repair_consumption_table": "dpone_repair_authority_consumption",
            "audit_table": "dpone_load_audit",
        },
    }
    assert plan["staging"]["shadow_or_swap"] is False
    assert plan["staging"]["finalization"] == "in_place"
    assert plan["physical_design"]["apply_runtime"] is False
    assert plan["physical_design"]["runtime_ddl"] == "disabled"
    assert plan["native_transfer_bulk_wire"]["selected_route"] == "postgres_mssql_bulk_text_codec"
    assert plan["native_transfer_bulk_wire"]["effective_export_format"] == "mssql-delimited"
    assert plan["native_transfer_bulk_wire"]["lossless"] is True


@pytest.mark.parametrize(
    "state_type",
    (
        "mssql",
        "MSSQL",
        "microsoft mssql",
        "microsoft_mssql",
        "odbc",
        "sqlserver",
        "sql_server",
        "sql-server",
    ),
)
def test_state_plan_projects_every_mssql_alias_as_one_canonical_backend(state_type: str) -> None:
    state = _canonical_state()
    state["type"] = state_type

    planned = state_plan({"state": state}, "mssql")

    assert planned["backend"] == "mssql"
    assert planned["atomicity"] == "target_atomic"
    assert planned["provisioning"] == "external"
    assert planned["tables"] == state_plan({"state": _canonical_state()}, "mssql")["tables"]


def test_plan_and_runtime_share_omitted_target_atomic_state_defaults(tmp_path: Path) -> None:
    manifest_path = tmp_path / "state-defaults.flow.yaml"
    manifest = _manifest("flow")
    manifest["authoring"]["source"] = manifest_path.name
    state = manifest["processes"][0]["state"]
    for field in (
        "provisioning",
        "run_table",
        "receipt_table",
        "repair_authority_table",
        "repair_consumption_table",
        "audit_table",
    ):
        state.pop(field)
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    plan = ExecutionPlanService().plan_manifest(manifest_path)
    resolved = resolve_mssql_state_location(state, _state_connection("DWH_Dev", "system"))

    assert plan["state"]["atomicity"] == resolved.atomicity == "target_atomic"
    assert plan["state"]["provisioning"] == resolved.provisioning == "external"
    assert plan["state"]["tables"] == {
        "table": resolved.location.table,
        "run_table": resolved.run_table,
        "receipt_table": resolved.location.receipt_table,
        "repair_authority_table": resolved.location.repair_authority_table,
        "repair_consumption_table": resolved.location.repair_consumption_table,
        "audit_table": resolved.audit_table,
    }


def test_plan_preserves_snapshot_finalizer_and_external_text_key_contract(tmp_path: Path) -> None:
    manifest_path = tmp_path / "sample_metrics_metrics_config.flow.yaml"
    manifest = _metrics_config_flow_manifest()
    manifest["authoring"]["source"] = manifest_path.name
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    plan = ExecutionPlanService().plan_manifest(manifest_path, explain_strategy=True)

    assert plan["strategy"]["merge_policy"] == "update_insert"
    assert plan["strategy_intelligence"]["decision"]["merge_policy"] == "update_insert"
    assert plan["schema_evolution"] == {
        "enabled": False,
        "configured_enabled": True,
        "mode": "external_contract",
        "apply_safe": False,
        "ddl_preview": [],
        "on_type_change": "fail",
        "runtime_execution": "bypassed",
        "provisioning": "external",
        "authority": "one_time_installer",
        "bypass_reason": "postgres_xmin_key_snapshot_to_mssql",
    }
    physical = plan["physical_design"]
    assert physical["ddl"] == []
    assert physical["generated_ddl_suppressed"] is True
    assert physical["provisioning"] == "external"
    assert physical["ddl_authority"] == "one_time_installer"
    assert physical["external_contract"] == {
        "route": "postgres_xmin_key_snapshot_to_mssql",
        "target_must_exist": True,
        "schema_must_exist": True,
        "unique_index": {"required": True, "columns": ["metric_code"]},
        "required_text_key_collation": "Latin1_General_100_BIN2",
        "text_key_collations": {"metric_code": "Latin1_General_100_BIN2"},
    }
    serialized = json.dumps(plan)
    assert "CREATE TABLE" not in serialized
    assert "ALTER TABLE" not in serialized
    rendered = render_plan_text(plan)
    assert "- merge_policy: update_insert" in rendered
    assert "- schema_evolution_execution: bypassed provisioning=external authority=one_time_installer" in rendered
    assert "- target_unique_index: required=True columns=metric_code" in rendered
    assert "- target_text_key_collations: metric_code=Latin1_General_100_BIN2" in rendered


def test_legacy_true_reconciliation_keeps_legacy_service_contract() -> None:
    load_config = LoadConfigBuilder().build(_builder_process(True))

    assert load_config.reconciliation is True
    assert load_config.reconciliation_policy is None
    assert "reconciliation" not in load_config.options


@pytest.mark.parametrize(
    ("database", "schema"),
    [
        pytest.param("DWH_Dev", "system", id="dev"),
        pytest.param("Example_System", "dbo", id="prod"),
    ],
)
def test_registry_defaults_resolve_all_state_tables_to_three_part_names(
    database: str,
    schema: str,
) -> None:
    resolved = resolve_mssql_state_location(_canonical_state(), _state_connection(database, schema))

    def qualified(table: str) -> str:
        return MSSQLObjectName.from_parts(
            database=database,
            schema=schema,
            table=table,
            strict=True,
        ).quoted()

    assert resolved.location.state_table_name == qualified("dpone_source_state")
    assert resolved.location.receipt_table_name == qualified("dpone_commit_receipt")
    assert resolved.location.repair_authority_table_name == qualified("dpone_repair_authority")
    assert resolved.location.repair_consumption_table_name == qualified("dpone_repair_authority_consumption")
    assert qualified(resolved.run_table) == qualified("dpone_run_state")
    assert qualified(resolved.audit_table) == qualified("dpone_load_audit")
    assert resolved.atomicity == "target_atomic"
    assert resolved.provisioning == "external"


def test_matching_explicit_state_location_remains_compatible() -> None:
    state = _canonical_state()
    state["table"].update(database="DWH_Dev", schema="system")

    resolved = resolve_mssql_state_location(state, _state_connection("DWH_Dev", "system"))

    assert resolved.location.database == "DWH_Dev"
    assert resolved.location.schema == "system"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("database", "Example_System", id="database"),
        pytest.param("schema", "dbo", id="schema"),
    ],
)
def test_explicit_state_location_overrides_only_omitted_registry_default(
    field: str,
    value: str,
) -> None:
    state = _canonical_state()
    state["table"][field] = value

    resolved = resolve_mssql_state_location(state, _state_connection("DWH_Dev", "system"))

    assert getattr(resolved.location, field) == value
    assert resolved.location.database == (value if field == "database" else "DWH_Dev")
    assert resolved.location.schema == (value if field == "schema" else "system")


@pytest.mark.parametrize("schema_name", SCHEMA_PATHS)
def test_all_authoring_schemas_accept_canonical_snapshot_contract(schema_name: str) -> None:
    assert _schema_errors(schema_name, _manifest(schema_name)) == []


@pytest.mark.parametrize("schema_name", SCHEMA_PATHS)
def test_all_authoring_schemas_reject_unknown_reconciliation_mode(schema_name: str) -> None:
    manifest = _manifest(schema_name)
    if schema_name == "flat":
        reconciliation = manifest["reconciliation"]
    elif schema_name == "batch":
        reconciliation = manifest["defaults"]["reconciliation"]
    else:
        reconciliation = manifest["processes"][0]["reconciliation"]
    reconciliation["mode"] = "snapshot"

    assert _schema_errors(schema_name, manifest)


@pytest.mark.parametrize("schema_name", SCHEMA_PATHS)
def test_all_authoring_schemas_type_runtime_physical_design_switch(schema_name: str) -> None:
    manifest = _manifest(schema_name)
    _physical_design(schema_name, manifest)["apply_runtime"] = "false"

    assert _schema_errors(schema_name, manifest)


@pytest.mark.parametrize("schema_name", SCHEMA_PATHS)
def test_all_authoring_schemas_type_mssql_column_override(schema_name: str) -> None:
    manifest = _manifest(schema_name)
    metric_code = _physical_design(schema_name, manifest)["columns"]["metric_code"]
    assert metric_code["target_type"]["mssql"] == "nvarchar(450)"

    metric_code["target_type"]["mssql"] = 450
    assert _schema_errors(schema_name, manifest)


@pytest.mark.parametrize(
    "example",
    [
        "landing_postgres_xmin_state_mssql.batch.yaml",
        "landing_kafka_to_postgres.batch.yaml",
    ],
)
def test_batch_schema_keeps_legacy_state_examples_valid(example: str) -> None:
    manifest = yaml.safe_load((ROOT / "examples/batch" / example).read_text(encoding="utf-8"))

    assert _schema_errors("batch", manifest) == []
