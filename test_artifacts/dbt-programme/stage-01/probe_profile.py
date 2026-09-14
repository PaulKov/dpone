"""Reproduce the stage-01 profile boundary without network or database I/O.

Run from the repository root with ``uv run --frozen python -B <this file>``.
The JSON report distinguishes expected rejection from a corrected defect;
matching the known baseline defect is not a successful data-delivery claim.
Only synthetic policy, model and credential values are constructed.
"""

from __future__ import annotations

import json
import subprocess
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from dpone.adapters.dbt_runtime_profile import RuntimeDbtProfileRenderer
from dpone.contracts.dbt_project_artifacts import DbtWorkflowReleasePlan
from dpone.contracts.dbt_publish_models import (
    CompiledDbtWorkflow,
    DbtColumnArtifact,
    DbtModelArtifact,
    DbtPublishIntent,
)
from dpone.contracts.dbt_publishing import DbtProfileSpec, DbtPublishingError
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import (
    DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS,
    DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS,
    DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
)
from dpone.contracts.dbt_sqlserver_policy import DbtSqlServerRuntimePolicy
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.run_interval import RunInterval
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler
from dpone.services.dbt_publish_planning import DbtPublishPlanner
from dpone.services.interval_context import IntervalContextService


def synthetic_policy() -> dict:
    """Return a complete current policy using generic immutable image identities."""
    return {
        "schema": "dpone.dbt-publish-policy.v3",
        "profiles": {
            "synthetic_route": {
                "source": {
                    "type": "mssql",
                    "connection_ref": "mssql_source",
                    "options": {"native_transfer": {"mode": "auto"}},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "clickhouse_sink",
                    "target_schema": "analytics",
                    "staging_schema": "staging",
                    "options": {"load_governance": {"audit": {"enabled": True, "state_schema": "state"}}},
                },
                "state": {
                    "type": "mssql",
                    "connection_ref": "mssql_state",
                    "partition_checkpoint_table": {"schema": "state", "name": "checkpoints"},
                },
                "strategy_policy": {
                    "allowed_strategies": ["partition_replace"],
                    "partition_replace": {"require_atomic_capability": True},
                },
                "runtime": {
                    "image": "example/dbt@sha256:" + "a" * 64,
                    "xcom_sidecar_image": "example/xcom@sha256:" + "b" * 64,
                    "toolchain": DBT_SQLSERVER_1_12_CERTIFIED.contract_id,
                },
            },
        },
        "workflows": {"synthetic_events": {"owner": "data_platform"}},
    }


def registry_observations(policy: dict) -> list[dict]:
    variants = (
        ("baseline_auto", (), None, True),
        ("empty_native_transfer", ("source", "options", "native_transfer"), {}, True),
        ("required_mode", ("source", "options", "native_transfer", "mode"), "required", False),
        ("off_mode", ("source", "options", "native_transfer", "mode"), "off", False),
        (
            "native_execution",
            ("source", "options", "native_transfer", "execution"),
            {"mode": "bounded_parallel"},
            False,
        ),
        ("native_wire", ("source", "options", "native_transfer", "wire"), {"format": "tsv"}, False),
        ("source_fetch_size", ("source", "options", "fetch_size"), 1000, False),
        ("source_encrypt", ("source", "options", "encrypt"), True, False),
        ("sink_settings", ("sink", "options", "settings"), {"max_threads": 2}, False),
        ("sink_bulk", ("sink", "options", "clickhouse_bulk"), {"mode": "http"}, False),
        ("sink_database", ("sink", "database"), "analytics", False),
        ("old_toolchain_v3", ("runtime", "toolchain"), "dbt-sqlserver-1.10-certified", False),
    )
    results = []
    for name, path, value, expected in variants:
        candidate = deepcopy(policy)
        cursor = candidate["profiles"]["synthetic_route"]
        for key in path[:-1]:
            cursor = cursor[key]
        if path:
            cursor[path[-1]] = value
        registry, issues = DbtPublishProfileRegistry.from_mapping(candidate, source_path="synthetic-policy.yml")
        accepted = registry is not None
        assert accepted == expected, name
        results.append(
            {
                "case": name,
                "accepted": accepted,
                "expected": expected,
                "issues": [{"code": issue.code, "message": issue.message} for issue in issues],
            }
        )
    return results


def compiler_observations(policy: dict) -> dict:
    registry, issues = DbtPublishProfileRegistry.from_mapping(policy, source_path="synthetic-policy.yml")
    assert registry is not None and not issues
    profile = registry.profile("synthetic_route")
    strategy_policy = registry.strategy_policy("synthetic_route")
    model = DbtModelArtifact(
        unique_id="model.synthetic.events",
        name="events",
        original_file_path="models/events.sql",
        database="warehouse",
        schema="mart",
        alias="events_view",
        materialized="table",
        contract_enforced=True,
        columns=("event_id", "event_date"),
        column_contracts=(
            DbtColumnArtifact("event_id", "int", False, ("not_null",)),
            DbtColumnArtifact("event_date", "date", False, ("not_null",)),
        ),
        group="synthetic",
        tags=(),
        meta={},
        unique_key=(),
        depends_on=(),
        fqn=("synthetic", "events"),
    )
    intent = DbtPublishIntent(
        enabled=True,
        profile="synthetic_route",
        workflow="synthetic_events",
        strategy_mode="partition_replace",
        partition_key="event_date",
        window_days=2,
        target_schema="reporting",
        target_table="events_target",
    )
    compiler = DbtModelToWorkloadCompiler(planner=DbtPublishPlanner())
    compiled = compiler.compile(model, intent, profile, strategy_policy, supported_strategies=("partition_replace",))
    assert not compiled.warnings
    source, sink = compiled.manifest["source"], compiled.manifest["sink"]
    assert source["options"]["native_transfer"] == {"mode": "auto"}
    assert source["table"] == {"database": "warehouse", "schema": "mart", "name": "events_view"}
    assert sink["table"] == {"schema": "reporting", "name": "events_target"}
    assert sink["staging"] == {"schema": "staging"}
    assert sink["options"]["load_governance"] == profile.sink_options["load_governance"]
    assert profile.source_options == {"native_transfer": {"mode": "auto"}}
    workflow = CompiledDbtWorkflow(
        "synthetic_events", registry.workflow("synthetic_events"), (compiled,), "synthetic_events"
    )
    plan = DbtWorkflowReleasePlan.prepare(workflow)
    assert (plan.database, plan.schema) == ("warehouse", "mart")
    interval = RunInterval(interval_start="2026-09-01T00:00:00Z", interval_end="2026-09-02T00:00:00Z")
    load = SimpleNamespace(options=deepcopy(source["options"]), custom_predicate=None)
    IntervalContextService(interval).apply(load)
    predicate = load.options["source_custom_predicate"]
    mssql = compiler.compile(
        model,
        intent,
        replace(profile, sink_type="mssql", sink_connection_ref="mssql_sink"),
        strategy_policy,
        supported_strategies=("partition_replace",),
    )
    assert mssql.manifest["sink"]["table"] == {"database": "warehouse", "schema": "reporting", "name": "events_target"}
    return {
        "source": source,
        "sink_table": sink["table"],
        "sink_staging": sink["staging"],
        "admitted_options_preserved": True,
        "transformation_pack_plan": {
            "database": plan.database,
            "schema": plan.schema,
            "connection_ref": plan.profile.source_connection_ref,
            "adapter_runtime": plan.adapter_runtime.to_dict(),
        },
        "mssql_sink_projection_only": mssql.manifest["sink"]["table"],
        "interval_runtime_predicate": predicate,
        "interval_behavior_status": "FAIL" if "{{" in predicate else "PASS",
        "interval_bug_reproduced": "{{2026-09-01T00:00:00Z}}" in predicate,
    }


def renderer_observations() -> list[dict]:
    """Probe complete synthetic identity, missing schema, and literal TLS types."""
    results = []
    profile = DbtProfileSpec(
        profile_name="synthetic",
        target_name="dev",
        connection_ref="mssql_source",
        adapter_type="sqlserver",
        database="warehouse",
        schema="mart",
        threads=4,
    )
    for name, schema, encrypt, trust in (
        ("complete_boolean_tls", "mart", True, False),
        ("missing_schema", None, True, False),
        ("string_encrypt", "mart", "yes", False),
        ("wrong_schema", "other", True, False),
    ):
        connection = ResolvedBindingConnection(
            credentials=CredentialsConfig(
                host="mssql.example",
                database="warehouse",
                schema=schema,
                username="synthetic",
                password="synthetic-unused-value",
                encrypt=encrypt,
                trust_server_certificate=trust,
            ),
            descriptor=ResolvedConnectionDescriptor(connection_type="mssql", properties={}),
            safe_metadata={},
        )
        renderer = RuntimeDbtProfileRenderer(SimpleNamespace(resolve=lambda ref: connection))
        try:
            rendered = renderer.render(profile, DbtSqlServerRuntimePolicy.for_process_timeout(3600))
            assert rendered.content
            result = {"case": name, "accepted": True}
        except DbtPublishingError as error:
            result = {"case": name, "accepted": False, "error_code": error.code}
        results.append(result)
    return results


def main() -> None:
    root = Path(__file__).resolve().parents[3]
    # The probe lives three artifact-directory levels beneath the repository.
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    policy = synthetic_policy()
    print(
        json.dumps(
            {
                "schema": "stage-01-baseline-observation.v1",
                "source_commit": head,
                "macro_authority": {
                    "framework_records": len(DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS),
                    "invocation_records": len(DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS),
                    "sha256": DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
                },
                "scope": "synthetic; no dbt subprocess, database, network, or live certification",
                "registry": registry_observations(policy),
                "compiler": compiler_observations(policy),
                "renderer": renderer_observations(),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
