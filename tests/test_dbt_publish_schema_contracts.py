from __future__ import annotations

import hashlib
import json
from pathlib import Path

import jsonschema
import pytest
import yaml

from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples" / "dbt-inline-publishing"


def test_published_dbt_schemas_match_canonical_producers() -> None:
    for contract_id, schema in dbt_schema_contracts().items():
        path = Path("docs/schemas/dbt") / f"{contract_id}.schema.json"
        assert json.loads(path.read_text(encoding="utf-8")) == schema
        jsonschema.Draft202012Validator.check_schema(schema)


def test_v1_policy_and_explain_schema_bytes_remain_frozen() -> None:
    expected = {
        "dpone.dbt-publish-policy.v1": "808a23a62d454de5a737fdd464b82b96b3d400ec95b811948bee18e317873e26",
        "dpone.dbt-publish-explain.v1": "dcc51b9a230e00fab9440e95c36eae68c978f3faf77e6d8af282cd2c18483d46",
    }

    for contract_id, expected_sha256 in expected.items():
        payload = (ROOT / "docs" / "schemas" / "dbt" / f"{contract_id}.schema.json").read_bytes()
        assert hashlib.sha256(payload).hexdigest() == expected_sha256


def test_semantic_refresh_policy_uses_a_closed_v2_root() -> None:
    policy = yaml.safe_load((DEMO / "dpone" / "dbt-publish-profiles.yml").read_text(encoding="utf-8"))
    policy["schema"] = "dpone.dbt-publish-policy.v2"
    profile = next(iter(policy["profiles"].values()))
    profile["refresh"] = {
        "schema": "dpone.semantic-refresh-profile.v1",
        "enabled": True,
        "capability": "scope_stable_event_fact",
        "scope": {"grain": "day", "timezone": "UTC", "interval": "half_open"},
        "mutation": {"protocol": "update_insert_v1", "deletes": "ignore_missing"},
        "initial_load": "require_existing_complete_relation",
        "concurrency": "exclusive_workflow",
        "source_snapshot": "snapshot",
        "publication": {
            "database_engine": "Atomic",
            "table_engine": "MergeTree",
            "replica_count": 1,
            "strategy": "full_table_exchange",
        },
        "workflow_publish_atomicity": "none",
        "automatic_sql_retry": False,
    }

    schemas = dbt_schema_contracts()
    jsonschema.validate(policy, schemas["dpone.dbt-publish-policy.v2"])
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(policy, schemas["dpone.dbt-publish-policy.v1"])


def test_semantic_refresh_explain_has_a_closed_v2_schema() -> None:
    payload = {
        "schema": "dpone.dbt-publish-explain.v2",
        "model": "model.project.events",
        "model_path": "models/events.sql",
        "source_relation": {"database": "DWH", "schema": "mart", "name": "events"},
        "intent": {},
        "resolved_strategy": {"mode": "semantic_refresh_v2"},
        "resolved_physical_design": {},
        "route_capability": {"status": "UNVERIFIED"},
        "workload_id": "events",
        "warnings": [],
        "semantic_refresh": {
            "workflow_mode": "normal",
            "model_archetype": "scope_stable_event_fact",
            "scope": "UTC day [start,end)",
            "static_dependency_closure": "UNVERIFIED",
            "runtime_dependency_closure": "RUNTIME_REQUIRED",
            "ephemeral_nodes": "none",
            "adapter_lifecycle_policy": "UNVERIFIED",
            "runtime_adapter_lifecycle": "RUNTIME_REQUIRED",
            "dbt_core": "1.12.3",
            "dbt_sqlserver": "1.11.1",
            "writer_assurance": "RUNTIME_REQUIRED",
            "source_side_pruning": "NOT_PROVEN",
            "event_time_policy": "immutable_effective_key_member",
            "effective_key_policy": "non_null_injective_exact",
            "utc_assurance": "RUNTIME_REQUIRED",
            "publication": "sequential_model_atomic",
            "recovery": "evidence_driven",
            "replay_mutation": "upsert_only",
            "row_removal": "unsupported",
            "profile_sha256": "sha256:" + "a" * 64,
        },
    }

    schema = dbt_schema_contracts()["dpone.dbt-publish-explain.v2"]
    jsonschema.validate(payload, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**payload, "unexpected": True}, schema)


def test_dev_evidence_request_schema_bounds_workflow_inventory() -> None:
    schema = dbt_schema_contracts()["dpone.dbt-dev-evidence-request.v1"]

    assert schema["properties"]["workflows"]["maxItems"] == 200


@pytest.mark.parametrize(
    ("legacy_id", "exact_id"),
    (
        (
            "dpone.dbt-airflow-attempt-evidence.v1",
            "dpone.dbt-airflow-attempt-evidence.v2",
        ),
        (
            "dpone.dbt-workflow-evidence-outcome.v1",
            "dpone.dbt-workflow-evidence-outcome.v2",
        ),
    ),
)
def test_exact_evidence_uses_v2_without_widening_closed_v1(
    legacy_id: str,
    exact_id: str,
) -> None:
    schemas = dbt_schema_contracts()

    assert "deployment_identity" not in schemas[legacy_id]["properties"]
    assert "deployment_identity" in schemas[exact_id]["properties"]
    assert "deployment_identity" in schemas[exact_id]["required"]


def test_runtime_dbt_contract_examples_validate() -> None:
    from dpone.contracts.dbt_invocation import DbtInvocationContext
    from dpone.contracts.dbt_publishing import (
        DbtExecutionEvidence,
        DbtExecutionPack,
        DbtNodeOutcome,
        DbtProfileSpec,
        DbtSelectionLock,
        DbtSqlServerAdapterPolicy,
        DbtSqlServerRuntimePolicy,
    )
    from dpone.contracts.dbt_sqlserver_graph_policy import (
        DBT_SQLSERVER_GRAPH_POLICY_ID,
        DBT_SQLSERVER_GRAPH_POLICY_SHA256,
    )
    from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED

    digest = "sha256:" + "a" * 64
    invocation = DbtInvocationContext.canonical()
    lock = DbtSelectionLock.build(
        manifest_sha256=digest,
        toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
        invocation_context_sha256=invocation.invocation_context_sha256,
        graph_contract_sha256=digest,
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=("model.project.orders",),
        selected_graph_unique_ids=("model.project.orders",),
        expected_run_result_unique_ids=("model.project.orders",),
        publish_model_unique_ids=("model.project.orders",),
    )
    pack = DbtExecutionPack.build(
        workflow_id="daily_marts",
        project_bundle_sha256=digest,
        project_subdir="dbt-project",
        target_path="target",
        profile=DbtProfileSpec(
            profile_name="dpone_runtime",
            target_name="runtime",
            connection_ref="mssql_prod",
            adapter_type="sqlserver",
            database="analytics",
            schema="mart",
            threads=4,
        ),
        selection_lock=lock,
        invocation_context=invocation,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(3600),
        adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
        dbt_warning_policy="fail",
        timeout_seconds=3600,
    )
    schemas = dbt_schema_contracts()
    jsonschema.validate(lock.to_dict(), schemas["dpone.dbt-selection-lock.v1"])
    jsonschema.validate(pack.to_dict(), schemas["dpone.dbt-execution-pack.v1"])
    evidence = json.loads(
        json.dumps(
            DbtExecutionEvidence(
                status="passed",
                code="DPONE_DBT_EXECUTION_PASSED",
                workflow_id="daily_marts",
                release_id=digest,
                deployment_id=digest,
                workload_pack_sha256=digest,
                project_bundle_sha256=digest,
                manifest_sha256=digest,
                selection_sha256=digest,
                toolchain_sha256=digest,
                invocation_context_sha256=digest,
                logical_target_sha256=digest,
                target_binding_sha256=digest,
                adapter_runtime=pack.adapter_runtime,
                adapter_policy_sha256=pack.adapter_policy.adapter_policy_sha256,
                graph_policy_sha256=lock.graph_policy_sha256,
                preflight_status="passed",
                build_started=True,
                dbt_exit_code=0,
                dbt_warning_policy="fail",
                dbt_warning_count=0,
                dbt_schema_version="https://schemas.getdbt.com/dbt/run-results/v6.json",
                dbt_version="1.12.3",
                invocation_id="invocation-1",
                started_at="2026-07-28T10:00:00Z",
                finished_at="2026-07-28T10:01:00Z",
                airflow={
                    "dag_id": "daily_marts",
                    "task_id": "dbt_build",
                    "run_id": "scheduled__2026-07-28T10:00:00Z",
                    "try_number": 1,
                    "map_index": -1,
                },
                credential_versions=(),
                nodes=(DbtNodeOutcome("model.project.orders", "success", 1.0),),
            ).to_dict()
        )
    )
    evidence_schema = schemas["dpone.dbt-execution-evidence.v1"]
    jsonschema.validate(evidence, evidence_schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**evidence, "nodes": []}, evidence_schema)
    for invalid_state in (
        {"preflight_status": "failed"},
        {"build_started": False},
    ):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate({**evidence, **invalid_state}, evidence_schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            {
                **evidence,
                "dbt_warning_count": 1,
                "nodes": [
                    {
                        "unique_id": "model.project.orders",
                        "status": "warn",
                        "execution_time": 1.0,
                    }
                ],
            },
            evidence_schema,
        )
    commit_unknown = {
        **evidence,
        "status": "failed",
        "code": "COMMIT_UNKNOWN",
        "dbt_exit_code": None,
        "dbt_schema_version": None,
        "dbt_version": None,
        "invocation_id": None,
        "nodes": [],
        "recovery": {
            "status": "COMMIT_UNKNOWN",
            "failure_boundary": "target_invocation",
            "target_state": "unknown",
            "checkpoint_state": "not_advanced",
            "source_state": "not_advanced",
            "safe_to_retry": False,
            "operator_verification_required": True,
            "recovery_action": "operator_verification_required",
        },
    }
    jsonschema.validate(commit_unknown, evidence_schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            {key: value for key, value in commit_unknown.items() if key != "recovery"},
            evidence_schema,
        )


@pytest.mark.parametrize(
    "path",
    (
        ROOT / "docs" / "dbt-self-service-runtime-identity.md",
        ROOT / "docs" / "feature-design-dbt-pre-mutation-identity-remediation-v1.md",
    ),
)
def test_documented_dbt_invocation_context_is_canonical(path: Path) -> None:
    from dpone.contracts.dbt_invocation import DbtInvocationContext

    marker = "```yaml\nschema: dpone.dbt-invocation-context.v1\n"
    text = path.read_text(encoding="utf-8")
    start = text.index(marker) + len("```yaml\n")
    end = text.index("\n```", start)
    payload = yaml.safe_load(text[start:end])

    assert payload == DbtInvocationContext.canonical().to_dict()
    jsonschema.validate(
        payload,
        dbt_schema_contracts()["dpone.dbt-invocation-context.v1"],
    )


def test_documented_dbt_logical_target_matches_runtime_contract() -> None:
    from dpone.contracts.dbt_execution_pack import DbtProfileSpec

    path = ROOT / "docs" / "dbt-self-service-runtime-identity.md"
    text = path.read_text(encoding="utf-8")
    marker = "```yaml\nprofile:\n"
    start = text.index(marker) + len("```yaml\n")
    end = text.index("\n```", start)
    payload = yaml.safe_load(text[start:end])

    profile = DbtProfileSpec.from_mapping(payload["profile"])

    assert profile.threads == 4
    assert "hash(profile_name, target_name, connection_ref, adapter_type, database, schema)" in text
    assert "hash(profile, target, connection_ref, adapter, database, schema, graph)" not in text


def test_execution_pack_schema_rejects_unsafe_workflow_identity() -> None:
    schema = dbt_schema_contracts()["dpone.dbt-execution-pack.v1"]

    workflow = schema["properties"]["workflow_id"]

    jsonschema.validate("daily_marts", workflow)
    for invalid in ("../prod", "sales/daily", "Daily", "-daily", "a" * 65):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(invalid, workflow)


def test_cross_item_dbt_schema_invariants_are_explicit_annotations() -> None:
    schemas = dbt_schema_contracts()

    assert schemas["dpone.dbt-selection-lock.v1"]["x-dpone-semantic-invariants"] == [
        "expected_run_result_unique_ids_subset_of_selected_graph_unique_ids",
        "publish_model_unique_ids_subset_of_expected_run_result_unique_ids",
        "selection_sha256_matches_canonical_payload",
    ]
    assert schemas["dpone.dbt-execution-evidence.v1"]["x-dpone-semantic-invariants"] == [
        "dbt_warning_count_equals_warn_node_count",
        "node_unique_ids_are_unique",
        "commit_unknown_requires_non_retryable_recovery",
    ]


def test_dev_evidence_public_schemas_are_closed_and_strict() -> None:
    digest = "sha256:" + "a" * 64
    schemas = dbt_schema_contracts()
    jsonschema.validate(
        {
            "schema": "dpone.dbt-dev-evidence-bundle.v1",
            "passed": True,
            "release_id": digest,
            "deployment_id": digest,
            "output_root": "/tmp/trusted-evidence",
            "subject_path": "evidence-subjects.sha256",
            "subject_sha256": digest,
            "file_count": 4,
            "total_bytes": 1024,
            "verified_workloads": ["dbt__daily_marts"],
            "no_op": False,
        },
        schemas["dpone.dbt-dev-evidence-bundle.v1"],
    )
    jsonschema.validate(
        {
            "schema": "dpone.dbt-dev-evidence-provenance.v1",
            "release_id": digest,
            "deployment_id": digest,
            "producer_repository": "PaulKov/airflow-dev",
            "producer_workflow": "dbt-self-service-dev-evidence.yml",
            "source_commit": "b" * 40,
        },
        schemas["dpone.dbt-dev-evidence-provenance.v1"],
    )


def test_dev_evidence_bundle_v1_preserves_path_compatibility_and_safety() -> None:
    digest = "sha256:" + "a" * 64
    schema = dbt_schema_contracts()["dpone.dbt-dev-evidence-bundle.v1"]
    payload = {
        "schema": "dpone.dbt-dev-evidence-bundle.v1",
        "passed": True,
        "release_id": digest,
        "deployment_id": digest,
        "output_root": "/" + "a" * 2999,
        "subject_path": "evidence-subjects.sha256",
        "subject_sha256": digest,
        "file_count": 4,
        "total_bytes": 1024,
        "verified_workloads": ["dbt__daily_marts"],
        "no_op": False,
    }

    jsonschema.validate(payload, schema)
    for unsafe_path in ("../escape", "nested/../escape", "/absolute", r"nested\escape"):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate({**payload, "subject_path": unsafe_path}, schema)


def test_dev_evidence_bundle_v2_rejects_unsafe_subject_paths() -> None:
    digest = "sha256:" + "a" * 64
    schema = dbt_schema_contracts()["dpone.dbt-dev-evidence-bundle.v2"]
    payload = {
        "schema": "dpone.dbt-dev-evidence-bundle.v2",
        "passed": True,
        "release_id": digest,
        "deployment_id": digest,
        "evidence_set_id": digest,
        "campaign_request_sha256": digest,
        "output_root": "/tmp/trusted-evidence",
        "subject_path": "evidence-subjects.sha256",
        "subject_sha256": digest,
        "file_count": 4,
        "total_bytes": 1024,
        "verified_workloads": ["dbt__daily_marts"],
        "no_op": False,
    }

    for unsafe_path in ("../escape", "nested/../escape", "/absolute", r"nested\escape"):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate({**payload, "subject_path": unsafe_path}, schema)


def test_public_ci_report_schemas_validate_success_and_blocked_shapes() -> None:
    digest = "sha256:" + "a" * 64
    schemas = dbt_schema_contracts()
    payloads = {
        "dpone.dbt-release-integrity.v1": {
            "schema": "dpone.dbt-release-integrity.v1",
            "passed": True,
            "subject_path": "release-subjects.sha256",
            "subject_sha256": digest,
            "file_count": 2,
            "total_bytes": 1024,
            "no_op": False,
        },
        "dpone.dbt-release-materialization.v1": {
            "schema": "dpone.dbt-release-materialization.v1",
            "passed": True,
            "release_id": digest,
            "release_dir": "/cache/releases/sha256-a",
            "no_op": False,
        },
        "dpone.dbt-prod-mirror-prepare.v1": {
            "schema": "dpone.dbt-prod-mirror-prepare.v1",
            "passed": True,
            "release_id": digest,
            "promotion_id": digest,
            "mirror_path": "dbt",
            "source_snapshot_path": ".dpone/dbt/source-snapshot.json",
            "descriptor_path": ".dpone/dbt/promotion.json",
            "no_op": False,
        },
        "dpone.dbt-promotion-verification.v1": {
            "schema": "dpone.dbt-promotion-verification.v1",
            "status": "failed",
            "passed": False,
            "code": "DPONE_DBT_PROMOTION_SOURCE_DRIFT",
            "release_id": digest,
            "source_snapshot_sha256": digest,
            "expected_project_bundle_sha256": digest,
            "observed_project_bundle_sha256": None,
        },
        "dpone.dbt-dev-evidence-verification.v1": {
            "schema": "dpone.dbt-dev-evidence-verification.v1",
            "status": "unverified",
            "passed": False,
            "code": "DPONE_DBT_DEV_EVIDENCE_UNVERIFIED",
            "release_id": digest,
            "deployment_id": digest,
            "required_workloads": ["publish_orders"],
            "verified_workloads": [],
            "verified_dbt_workflows": [],
            "reason_codes": ["airflow_workload_coverage_incomplete"],
        },
    }

    for contract_id, payload in payloads.items():
        jsonschema.validate(payload, schemas[contract_id])


@pytest.mark.parametrize(
    ("contract_id", "payload"),
    (
        (
            "dpone.dbt-dev-evidence-verification.v1",
            {
                "schema": "dpone.dbt-dev-evidence-verification.v1",
                "status": "passed",
                "passed": False,
                "code": "DPONE_DBT_DEV_EVIDENCE_VERIFIED",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "a" * 64,
                "required_workloads": ["publish_orders"],
                "verified_workloads": ["publish_orders"],
                "verified_dbt_workflows": ["daily_marts"],
                "reason_codes": [],
            },
        ),
        (
            "dpone.dbt-promotion-verification.v1",
            {
                "schema": "dpone.dbt-promotion-verification.v1",
                "status": "passed",
                "passed": False,
                "code": "DPONE_DBT_PROMOTION_VERIFIED",
                "release_id": "sha256:" + "a" * 64,
                "source_snapshot_sha256": "sha256:" + "a" * 64,
                "expected_project_bundle_sha256": "sha256:" + "a" * 64,
                "observed_project_bundle_sha256": "sha256:" + "a" * 64,
            },
        ),
        (
            "dpone.dbt-dev-evidence-campaign.v1",
            {
                "schema": "dpone.dbt-dev-evidence-campaign.v1",
                "status": "passed",
                "passed": False,
                "code": "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED",
                "evidence_set_id": "sha256:" + "a" * 64,
                "release_id": "sha256:" + "b" * 64,
                "deployment_id": "sha256:" + "c" * 64,
                "workflow_states": [{"workflow_id": "daily", "state": "failed"}],
                "elapsed_seconds": 0,
            },
        ),
        (
            "dpone.dbt-dev-evidence-campaign-outcome.v1",
            {
                "schema": ("dpone.dbt-dev-evidence-campaign-outcome.v1"),
                "status": "passed",
                "passed": False,
                "code": "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED",
                "evidence_set_id": "sha256:" + "a" * 64,
                "release_id": "sha256:" + "b" * 64,
                "deployment_id": "sha256:" + "c" * 64,
                "workflow_states": [{"workflow_id": "daily", "state": "failed"}],
                "closed": True,
                "campaign_request_sha256": "sha256:" + "d" * 64,
            },
        ),
        (
            "dpone.dbt-workflow-evidence-outcome.v1",
            {
                "schema": "dpone.dbt-workflow-evidence-outcome.v1",
                "status": "passed",
                "code": "DPONE_DBT_WORKFLOW_PASSED",
                "workflow_id": "daily",
                "release_id": "sha256:" + "b" * 64,
                "deployment_id": "sha256:" + "c" * 64,
                "dag_run_id": "run",
                "tasks": [{"task_id": "publish_orders", "state": "failed"}],
                "evidence_set_id": "sha256:" + "a" * 64,
                "artifacts": [
                    {
                        "category": "airflow",
                        "logical_id": "publish_orders",
                        "sha256": "sha256:" + "d" * 64,
                        "bytes": 1,
                    }
                ],
            },
        ),
    ),
)
def test_verification_schemas_reject_contradictory_success(
    contract_id: str,
    payload: dict[str, object],
) -> None:
    errors = tuple(jsonschema.Draft202012Validator(dbt_schema_contracts()[contract_id]).iter_errors(payload))

    assert errors


def test_exact_airflow_attempt_schema_rejects_empty_identity_and_xcom_objects() -> None:
    schema = dbt_schema_contracts()["dpone.dbt-airflow-attempt-evidence.v2"]
    payload = {
        "schema": "dpone.dbt-airflow-attempt-evidence.v2",
        "status": "passed",
        "evidence_set_id": "sha256:" + "a" * 64,
        "run_identity": {},
        "attempt": {
            "dag_id": "DAG__dbt__publish__refresh",
            "task_id": "publish",
            "run_id": "manual__1",
            "try_number": 1,
            "map_index": -1,
        },
        "xcom_summary_sha256": "sha256:" + "b" * 64,
        "xcom_summary": {},
        "deployment_identity": {
            "schema": "dpone.airflow-deployment-identity.v1",
            "release_id": "sha256:" + "c" * 64,
            "deployment_id": "sha256:" + "d" * 64,
            "activation_id": "4f60628e-ef48-48b0-84c3-a9e27a82a7f1",
        },
    }

    errors = tuple(jsonschema.Draft202012Validator(schema).iter_errors(payload))

    assert errors
    assert schema["x-dpone-semantic-validator"].endswith("validate_dbt_airflow_attempt_evidence")


def test_exact_workflow_schema_declares_mandatory_semantic_identity_validator() -> None:
    schema = dbt_schema_contracts()["dpone.dbt-workflow-evidence-outcome.v2"]

    assert schema["x-dpone-semantic-validator"].endswith("validate_workflow_outcome_contract")


@pytest.mark.parametrize("field", ("models", "workflows", "warnings", "blockers"))
def test_compile_report_arrays_reject_primitive_items(field: str) -> None:
    schema = dbt_schema_contracts()["dpone.dbt-publish-compile.v2"]
    payload: dict[str, object] = {
        "schema": "dpone.dbt-publish-compile.v2",
        "manifest_path": "target/manifest.json",
        "manifest_schema_version": 12,
        "manifest_sha256": "sha256:" + "a" * 64,
        "dbt_version": "1.12.3",
        "dbt_adapter": "sqlserver",
        "dbt_adapter_version": "1.11.1",
        "release_id": None,
        "passed": False,
        "models": [],
        "workflows": [],
        "warnings": [],
        "blockers": [],
        "artifacts": {},
        "compile_fingerprint": "sha256:" + "b" * 64,
    }
    payload[field] = ["invalid"]

    errors = tuple(jsonschema.Draft202012Validator(schema).iter_errors(payload))

    assert errors


def test_explain_schema_validates_compiled_model_projection() -> None:
    report = json.loads((DEMO / "fixtures" / "manifest.v12.json").read_text(encoding="utf-8"))
    publish = report["nodes"]["model.dpone_dbt_demo.competitive_pricing"]["config"]["meta"]["dpone"]["publish"]
    payload = {
        "schema": "dpone.dbt-publish-explain.v1",
        "model": "model.dpone_dbt_demo.competitive_pricing",
        "model_path": "models/competitive_pricing.sql",
        "source_relation": {
            "database": "analytics",
            "schema": "dbo",
            "name": "competitive_pricing",
        },
        "intent": {
            "schema": "dpone.dbt-publish-intent.v2",
            **publish,
        },
        "resolved_strategy": {"mode": "partition_replace"},
        "resolved_physical_design": {"profile": "clickhouse_mart"},
        "route_capability": {"support": "supported"},
        "workload_id": "competitive_pricing",
        "warnings": [],
    }

    jsonschema.validate(
        payload,
        dbt_schema_contracts()["dpone.dbt-publish-explain.v1"],
    )


def test_shipped_authoring_and_policy_examples_match_public_schemas() -> None:
    manifest = json.loads((DEMO / "fixtures" / "manifest.v12.json").read_text(encoding="utf-8"))
    publish = manifest["nodes"]["model.dpone_dbt_demo.competitive_pricing"]["config"]["meta"]["dpone"]["publish"]
    policy = yaml.safe_load((DEMO / "dpone" / "dbt-publish-profiles.yml").read_text(encoding="utf-8"))
    schemas = dbt_schema_contracts()

    jsonschema.validate(publish, schemas["dpone.dbt-publish-authoring.v1"])
    jsonschema.validate(policy, schemas["dpone.dbt-publish-policy.v3"])


def test_canonical_policy_has_one_toolchain_authority() -> None:
    policy = yaml.safe_load((DEMO / "dpone" / "dbt-publish-profiles.yml").read_text(encoding="utf-8"))
    runtime = next(iter(policy["profiles"].values()))["runtime"]

    assert runtime["toolchain"] == "dbt-sqlserver-1.11-core-1.12-certified"
    assert {
        "dbt_core_version",
        "dbt_adapter",
        "dbt_adapter_version",
    }.isdisjoint(runtime)


@pytest.mark.parametrize(
    "field",
    ("dbt_core_version", "dbt_adapter", "dbt_adapter_version"),
)
def test_canonical_policy_rejects_duplicate_toolchain_fields(
    field: str,
) -> None:
    policy = yaml.safe_load((DEMO / "dpone" / "dbt-publish-profiles.yml").read_text(encoding="utf-8"))
    runtime = next(iter(policy["profiles"].values()))["runtime"]
    runtime[field] = "contradictory"

    errors = tuple(
        jsonschema.Draft202012Validator(dbt_schema_contracts()["dpone.dbt-publish-policy.v3"]).iter_errors(policy)
    )

    assert errors


def test_canonical_policy_requires_explicit_strategy_authority() -> None:
    policy = yaml.safe_load((DEMO / "dpone" / "dbt-publish-profiles.yml").read_text(encoding="utf-8"))
    del next(iter(policy["profiles"].values()))["strategy_policy"]

    errors = tuple(
        jsonschema.Draft202012Validator(dbt_schema_contracts()["dpone.dbt-publish-policy.v3"]).iter_errors(policy)
    )

    assert errors


def test_policy_schema_accepts_bounded_environment_neutral_strategy_policy() -> None:
    policy = yaml.safe_load((DEMO / "dpone" / "dbt-publish-profiles.yml").read_text(encoding="utf-8"))
    profile = next(iter(policy["profiles"].values()))
    profile["strategy_policy"] = {
        "allowed_strategies": ["full_refresh", "incremental_merge"],
        "full_refresh": {
            "authorized": True,
            "max_source_bytes": 1_000_000_000,
        },
        "partition_replace": {"require_atomic_capability": True},
    }
    profile["quality"]["dbt_warning_policy"] = "allow"

    jsonschema.validate(
        policy,
        dbt_schema_contracts()["dpone.dbt-publish-policy.v3"],
    )


@pytest.mark.parametrize(
    "strategy_policy",
    [
        {"allowed_strategies": []},
        {"allowed_strategies": ["auto"]},
        {
            "allowed_strategies": ["full_refresh"],
            "full_refresh": {"authorized": True, "max_source_bytes": "1000"},
        },
        {
            "allowed_strategies": ["partition_replace"],
            "partition_replace": {"require_atomic_capability": False},
        },
    ],
)
def test_policy_schema_rejects_unsafe_strategy_policy(
    strategy_policy: dict,
) -> None:
    policy = yaml.safe_load((DEMO / "dpone" / "dbt-publish-profiles.yml").read_text(encoding="utf-8"))
    next(iter(policy["profiles"].values()))["strategy_policy"] = strategy_policy

    errors = tuple(
        jsonschema.Draft202012Validator(dbt_schema_contracts()["dpone.dbt-publish-policy.v3"]).iter_errors(policy)
    )

    assert errors


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload.update({"unexpected": True}),
        lambda payload: payload["physical_design"].update({"engine": "MergeTree()"}),
        lambda payload: payload["physical_design"].update({"partition_by": "toYYYYMM(date_id)"}),
        lambda payload: payload["execution"].update({"max_parallelism": "2"}),
        lambda payload: payload["quality"].update({"dbt_warning_policy": "allow"}),
        lambda payload: payload["lineage"].update({"enabled": "false"}),
    ],
)
def test_authoring_schema_is_closed_and_strict(mutator) -> None:
    manifest = json.loads((DEMO / "fixtures" / "manifest.v12.json").read_text(encoding="utf-8"))
    publish = manifest["nodes"]["model.dpone_dbt_demo.competitive_pricing"]["config"]["meta"]["dpone"]["publish"]
    mutator(publish)

    errors = tuple(
        jsonschema.Draft202012Validator(dbt_schema_contracts()["dpone.dbt-publish-authoring.v1"]).iter_errors(publish)
    )

    assert errors
