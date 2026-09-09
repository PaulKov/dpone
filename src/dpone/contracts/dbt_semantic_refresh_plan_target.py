"""Protected target projection for semantic-refresh plans."""

from __future__ import annotations

from dpone.contracts.dbt_semantic_refresh_plan_assurance import RuntimeAssuranceIndex
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshDeploymentModelAuthority,
    SemanticRefreshPlanTarget,
    SemanticRefreshPreReleaseModelInput,
    SemanticRefreshPreReleaseProofBundle,
)
from dpone.contracts.dbt_semantic_refresh_plan_recovery import SemanticRefreshPlanModeContext
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError, semantic_refresh_sha256
from dpone.contracts.semantic_refresh_operation_plan import SemanticRefreshOperationPlan
from dpone.contracts.semantic_refresh_route_certification import (
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_runtime_assurance import RuntimeAssuranceKind


def plan_target(
    operation: SemanticRefreshOperationPlan,
    model: SemanticRefreshPreReleaseModelInput,
    deployment: SemanticRefreshDeploymentModelAuthority,
    scope_start: str,
    scope_end: str,
    pre_release: SemanticRefreshPreReleaseProofBundle,
    route: SemanticRefreshRouteLiveCertificationReceipt,
    assurances: RuntimeAssuranceIndex,
    mode: SemanticRefreshPlanModeContext,
) -> SemanticRefreshPlanTarget:
    """Bind one operation to its protected deployment target and predecessor."""

    model_id = operation.model_unique_id
    writer = assurances[(model_id, RuntimeAssuranceKind.WRITER_EXCLUSIVITY)]
    ddl = assurances[(model_id, RuntimeAssuranceKind.DDL_FREEZE)]
    if ddl.ddl_epoch is None:
        raise SemanticRefreshContractError("DDL freeze assurance omitted its protected epoch")
    utc = assurances.get((model_id, RuntimeAssuranceKind.UTC_SEMANTICS))
    lineage = mode.lineage_for(model_id) if mode.lineages else None
    if lineage is None:
        target_generation = deployment.baseline_receipt.clickhouse_generation
        target_generation_id = deployment.target_predecessor_generation_id
        target_uuid = deployment.baseline_receipt.clickhouse_target_uuid
        target_operation_id = deployment.baseline_receipt.baseline_adoption_receipt_sha256
        terminal_receipt = None
        head_receipt = deployment.baseline_receipt.baseline_adoption_receipt_sha256
    else:
        if lineage.clickhouse_target_authority_id != deployment.clickhouse_target_authority_id:
            raise SemanticRefreshContractError("recovery target head differs from deployment target authority")
        values = (
            lineage.target_predecessor_generation,
            lineage.target_predecessor_generation_id,
            lineage.target_predecessor_uuid,
            lineage.target_predecessor_operation_id,
            lineage.target_head_authority_receipt_sha256,
        )
        if any(value is None for value in values):
            raise SemanticRefreshContractError("recovery target head is incomplete")
        target_generation = lineage.target_predecessor_generation
        target_generation_id = lineage.target_predecessor_generation_id
        target_uuid = lineage.target_predecessor_uuid
        target_operation_id = lineage.target_predecessor_operation_id
        terminal_receipt = lineage.target_head_terminal_receipt_sha256
        head_receipt = lineage.target_head_authority_receipt_sha256
    assert target_generation is not None
    assert target_generation_id is not None
    assert target_uuid is not None
    assert target_operation_id is not None
    assert head_receipt is not None
    scope_id = f"{scope_start}/{scope_end}"
    strategy_template = semantic_refresh_sha256(
        {
            "artifact_authority": deployment.artifact_authority.to_dict(),
            "baseline_receipt_sha256": deployment.baseline_receipt.baseline_adoption_receipt_sha256,
            "clickhouse_cluster_authority_id": deployment.clickhouse_cluster_authority_id,
            "clickhouse_target_authority_id": deployment.clickhouse_target_authority_id,
            "clickhouse_target_uuid": target_uuid,
            "ddl_freeze_assurance_receipt_sha256": ddl.runtime_assurance_receipt_sha256,
            "ddl_epoch": ddl.ddl_epoch,
            "effective_key_mapping_sha256": operation.effective_key_mapping_sha256,
            "effective_key_template_sha256": operation.effective_key_template_sha256,
            "model_definition_proof_sha256": model.model_definition_proof.model_definition_proof_sha256,
            "mssql_connection_authority_id": deployment.mssql_connection_authority_id,
            "mssql_control_database": deployment.mssql_control_database,
            "mssql_control_schema": deployment.mssql_control_schema,
            "mssql_image_schema": deployment.mssql_image_schema,
            "mssql_target_authority_id": deployment.mssql_target_authority_id,
            "operation_id": operation.operation_id,
            "operation_plan_sha256": operation.operation_plan_sha256,
            "publication_database": deployment.publication_database,
            "publication_scope_id": scope_id,
            "publication_target_table": deployment.publication_target_table,
            "resource_policy_sha256": pre_release.resource_policy_digest,
            "route_certification_receipt_sha256": route.route_certification_receipt_sha256,
            "schema": "dpone.semantic-refresh-strategy-template.v1",
            "scope_image_namespace_policy_sha256": deployment.scope_image_namespace_policy_sha256,
            "sqlserver_lifecycle_policy_sha256": pre_release.lifecycle_policy.sqlserver_lifecycle_policy_sha256,
            "target_resource_id": deployment.target_resource_id,
            "target_predecessor_generation": target_generation,
            "target_predecessor_generation_id": target_generation_id,
            "target_predecessor_operation_id": target_operation_id,
            "target_head_terminal_receipt_sha256": terminal_receipt,
            "target_head_authority_receipt_sha256": head_receipt,
            "utc_semantics_assurance_receipt_sha256": (
                utc.runtime_assurance_receipt_sha256 if utc is not None else None
            ),
            "writable_schema_sha256": model.writable_schema_sha256,
            "writer_exclusivity_assurance_receipt_sha256": writer.runtime_assurance_receipt_sha256,
        }
    )
    return SemanticRefreshPlanTarget(
        model_unique_id=model_id,
        target_resource_id=deployment.target_resource_id,
        mssql_connection_authority_id=deployment.mssql_connection_authority_id,
        mssql_target_authority_id=deployment.mssql_target_authority_id,
        mssql_control_database=deployment.mssql_control_database,
        mssql_control_schema=deployment.mssql_control_schema,
        mssql_image_schema=deployment.mssql_image_schema,
        scope_image_namespace_policy_sha256=deployment.scope_image_namespace_policy_sha256,
        clickhouse_cluster_authority_id=deployment.clickhouse_cluster_authority_id,
        clickhouse_target_authority_id=deployment.clickhouse_target_authority_id,
        clickhouse_target_uuid=target_uuid,
        target_predecessor_generation=target_generation,
        target_predecessor_generation_id=target_generation_id,
        target_predecessor_operation_id=target_operation_id,
        target_head_terminal_receipt_sha256=terminal_receipt,
        target_head_authority_receipt_sha256=head_receipt,
        publication_database=deployment.publication_database,
        publication_target_table=deployment.publication_target_table,
        publication_scope_id=scope_id,
        baseline_receipt=deployment.baseline_receipt,
        baseline_receipt_sha256=deployment.baseline_receipt.baseline_adoption_receipt_sha256,
        baseline_status=deployment.baseline_receipt.status,
        strategy_template_sha256=strategy_template,
        model_definition_proof_sha256=model.model_definition_proof.model_definition_proof_sha256,
        effective_key_template_sha256=operation.effective_key_template_sha256,
        effective_key_mapping_sha256=operation.effective_key_mapping_sha256,
        writable_columns=model.writable_columns,
        writable_schema_sha256=model.writable_schema_sha256,
        resource_policy=pre_release.resource_policy,
        resource_policy_sha256=pre_release.resource_policy_digest,
        route_certification_receipt_sha256=route.route_certification_receipt_sha256,
        writer_exclusivity_assurance_receipt_sha256=writer.runtime_assurance_receipt_sha256,
        ddl_freeze_assurance_receipt_sha256=ddl.runtime_assurance_receipt_sha256,
        ddl_epoch=ddl.ddl_epoch,
        artifact_authority=deployment.artifact_authority,
        utc_semantics_assurance_receipt_sha256=(utc.runtime_assurance_receipt_sha256 if utc is not None else None),
    )
