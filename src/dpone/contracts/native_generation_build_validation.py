"""Validate actual build observations against the invocation's fixed authorities.

This validation step does not publish originals or grant source completion. The
writer must acquire bounded bytes from captured roots and authenticate the pack,
run identity and trusted termination before constructing the positive cohort.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.dbt_execution_evidence import DbtExecutionEvidence
from dpone.contracts.dbt_execution_pack import DbtExecutionPack, dbt_target_identity_sha256
from dpone.contracts.dbt_run_results import ParsedDbtRunResults, parse_dbt_run_results
from dpone.contracts.dbt_runtime import dbt_target_binding_sha256, validate_dbt_workload_identity
from dpone.contracts.dbt_sqlserver_graph_policy_contract import dbt_sqlserver_graph_contract_sha256
from dpone.contracts.native_generation_invocation import TrustedDbtCommandPlan, TrustedDbtInvocationCompletion
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import NativeSourceCustodyError, SourceExecutorBinding
from dpone.ports.dbt_publishing import DbtManifestSchemaValidator, DbtRunResultsSchemaValidator


def validate_native_build_evidence(
    evidence: DbtExecutionEvidence,
    *,
    pack: DbtExecutionPack,
    run_identity: AirflowRunIdentity,
    manifest: Mapping[str, Any],
    run_results: Mapping[str, Any],
    manifest_validator: DbtManifestSchemaValidator,
    results_validator: DbtRunResultsSchemaValidator,
) -> ParsedDbtRunResults:
    """Require complete observed outcomes, exact release/pack and explicit dbt ID.

    Fixed authorities come from the admitted bootstrap, never from the result
    list. Schema validators are the existing injected official validators; unit
    fakes cannot qualify a live route. No equality with a pre-dispatch UUID is
    assumed: the caller binds this observed ID to its trusted executor separately.
    """
    if (
        type(evidence) is not DbtExecutionEvidence
        or type(pack) is not DbtExecutionPack
        or type(run_identity) is not AirflowRunIdentity
    ):
        raise NativeSourceCustodyError("build validation requires exact fixed records")
    evidence.__post_init__()
    pack.__post_init__()
    validate_dbt_workload_identity(pack, run_identity)
    if evidence.status != "passed":
        raise NativeSourceCustodyError("failed build evidence cannot become positive completion")
    expected = {
        "workflow_id": pack.workflow_id,
        "release_id": run_identity.release_id,
        "deployment_id": run_identity.deployment_id,
        "workload_pack_sha256": run_identity.workload_pack.sha256,
        "project_bundle_sha256": pack.project_bundle_sha256,
        "manifest_sha256": pack.selection_lock.manifest_sha256,
        "selection_sha256": pack.selection_lock.selection_sha256,
        "toolchain_sha256": pack.selection_lock.toolchain_sha256,
        "invocation_context_sha256": pack.invocation_context.invocation_context_sha256,
        "logical_target_sha256": dbt_target_identity_sha256(pack.profile),
        "target_binding_sha256": dbt_target_binding_sha256(pack, run_identity),
        "adapter_runtime": pack.adapter_runtime,
        "adapter_policy_sha256": pack.adapter_policy.adapter_policy_sha256,
        "graph_policy_sha256": pack.selection_lock.graph_policy_sha256,
        "dbt_warning_policy": pack.dbt_warning_policy,
    }
    if any(getattr(evidence, field) != value for field, value in expected.items()):
        raise NativeSourceCustodyError("build evidence differs from the admitted pack or run identity")
    if manifest_validator.validate(manifest, version=int(pack.manifest_schema_version.removeprefix("v"))):
        raise NativeSourceCustodyError("build manifest violates the pinned official schema")
    if results_validator.validate(run_results, version=int(pack.run_results_schema_version.removeprefix("v"))):
        raise NativeSourceCustodyError("build run results violate the pinned official schema")
    parsed = parse_dbt_run_results(
        run_results,
        expected_run_result_unique_ids=pack.selection_lock.expected_run_result_unique_ids,
        expected_dbt_version=pack.dbt_core_version,
        expected_schema_version=pack.run_results_schema_version,
    )
    metadata = manifest.get("metadata")
    if not isinstance(metadata, Mapping) or (
        metadata.get("invocation_id") != parsed.invocation_id
        or metadata.get("dbt_version") != pack.dbt_core_version
        or metadata.get("dbt_schema_version")
        != f"https://schemas.getdbt.com/dbt/manifest/{pack.manifest_schema_version}.json"
    ):
        raise NativeSourceCustodyError("build manifest does not belong to the observed dbt invocation")
    if (
        dbt_sqlserver_graph_contract_sha256(manifest, pack.selection_lock.selected_graph_unique_ids)
        != pack.selection_lock.graph_contract_sha256
    ):
        raise NativeSourceCustodyError("actual build graph differs from locked graph membership")
    observed = tuple(sorted((node.unique_id, node.status, node.execution_time) for node in parsed.nodes))
    reported = tuple(sorted((node.unique_id, node.status, node.execution_time) for node in evidence.nodes))
    if (
        not parsed.passes(pack.dbt_warning_policy)
        or observed != reported
        or evidence.invocation_id != parsed.invocation_id
        or evidence.dbt_version != parsed.dbt_version
        or evidence.dbt_schema_version != parsed.schema_version
        or evidence.dbt_warning_count != parsed.warning_count
    ):
        raise NativeSourceCustodyError("build evidence differs from complete actual successful results")
    return parsed


def validate_native_build_termination(
    terminal: TrustedDbtInvocationCompletion,
    *,
    plan: TrustedDbtCommandPlan,
    executor: SourceExecutorBinding,
    toolchain: OriginalRef,
    qualification: OriginalRef,
) -> None:
    """Match the whole normal-return observation and its admitted monotonic budget.

    Wall timestamps are correlation only. Equality at the deadline follows the
    trusted recorder; a positive record above that limit contradicts its plan.
    """
    terminal.__post_init__()
    plan.__post_init__()
    if (
        plan.phase != "BUILD"
        or plan.executor_invocation_id != executor.invocation_id
        or (terminal.executor, terminal.command, terminal.toolchain, terminal.qualification, terminal.command_count)
        != (executor, executor.command, toolchain, qualification, len(plan.commands))
    ):
        raise NativeSourceCustodyError("termination differs from the complete admitted build")
    if terminal.elapsed_microseconds > plan.total_termination_budget_seconds * 1000000:
        raise NativeSourceCustodyError("build exceeded its admitted termination budget")
