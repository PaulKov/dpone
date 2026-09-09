"""Pure selection, policy, workflow, and identity helpers for dbt compilation."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection
from typing import Any, Protocol

from dpone.contracts.dbt_identifiers import dbt_dag_id, dbt_workflow_id
from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel,
    CompiledDbtWorkflow,
    DbtModelArtifact,
    DbtPublishIssue,
    DbtPublishProfile,
    DbtWorkflowProfile,
)
from dpone.contracts.dbt_semantic_refresh_certification import (
    SemanticRefreshCertificationRequest,
    SemanticRefreshCertificationVerifierPort,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED


class DbtWorkflowRegistry(Protocol):
    """Small workflow-only view needed while assembling compiled DAGs."""

    def workflow(self, name: str) -> DbtWorkflowProfile | None: ...


def semantic_refresh_capability(
    *,
    model: DbtModelArtifact,
    manifest_sha256: str,
    profile_sha256: str,
    coordinate_sha256: str | None,
    verifier: SemanticRefreshCertificationVerifierPort | None,
    verification_time: str | None,
) -> tuple[dict[str, Any], DbtPublishIssue | None]:
    """Return CERTIFIED only for one current request-bound protected receipt."""

    unverified: dict[str, Any] = {
        "activation": "blocked_until_exact_environment_certification",
        "capability": "scope_stable_event_fact",
        "status": "UNVERIFIED",
    }
    if coordinate_sha256 is None or verifier is None or verification_time is None:
        return unverified, _certification_issue(model)
    try:
        request = SemanticRefreshCertificationRequest.build(
            certification_coordinate_sha256=coordinate_sha256,
            manifest_sha256=manifest_sha256,
            profile_sha256=profile_sha256,
            toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
            verification_time=verification_time,
        )
        decision = verifier.verify(request)
    except Exception:
        return unverified, _certification_issue(model)
    if decision.request_sha256 != request.request_sha256 or not decision.valid_at(request.verification_time):
        return unverified, _certification_issue(model)
    return (
        {
            "capability": "scope_stable_event_fact",
            "certification_coordinate_sha256": coordinate_sha256,
            "certification_receipt_sha256": decision.receipt_sha256,
            "status": "CERTIFIED",
        },
        None,
    )


def _certification_issue(model: DbtModelArtifact) -> DbtPublishIssue:
    return DbtPublishIssue(
        code="DPONE_DBT_V2_LIVE_UNVERIFIED",
        message="Semantic refresh V2 has no protected receipt for this exact certification coordinate",
        path=model.original_file_path,
        remediation=(
            "Run the required live fault matrix and bind its protected exact-coordinate receipt; "
            "local Docker evidence cannot activate a production route."
        ),
    )


def model_policy(
    model: DbtModelArtifact,
    *,
    require_contracts: bool,
) -> tuple[DbtPublishIssue, ...]:
    """Return model-level blockers before route compilation."""

    issues = []
    if model.materialized == "ephemeral":
        issues.append(
            model_issue(
                model,
                "DPONE_DBT_EPHEMERAL_UNSUPPORTED",
                "ephemeral models have no publishable relation",
            )
        )
    if require_contracts and not model.contract_enforced:
        issues.append(
            model_issue(
                model,
                "DPONE_DBT_CONTRACT_REQUIRED",
                "Production publishing requires contract.enforced: true",
            )
        )
    if require_contracts and not model.columns:
        issues.append(
            model_issue(
                model,
                "DPONE_DBT_CONTRACT_COLUMNS_MISSING",
                "The dbt contract has no columns",
            )
        )
    if not model.schema or not model.alias:
        issues.append(
            model_issue(
                model,
                "DPONE_DBT_RELATION_INVALID",
                "Resolved database/schema/alias is incomplete",
            )
        )
    return tuple(issues)


def build_workflows(
    models: tuple[CompiledDbtModel, ...],
    registry: DbtWorkflowRegistry,
) -> tuple[tuple[CompiledDbtWorkflow, ...], tuple[DbtPublishIssue, ...]]:
    """Group compiled models and enforce workflow-wide invariants."""

    groups: dict[str, list[CompiledDbtModel]] = defaultdict(list)
    for model in models:
        groups[model.intent.workflow].append(model)
    workflows = []
    issues: list[DbtPublishIssue] = []
    for workflow, grouped in sorted(groups.items()):
        publish_profiles = {item.intent.profile for item in grouped}
        parallelism = {
            item.intent.max_parallelism or int(item.profile.execution.get("max_parallelism", 2) or 2)
            for item in grouped
        }
        target_ids = {
            (
                item.intent.target_schema or item.profile.target_schema,
                item.intent.target_table or item.model.alias,
            )
            for item in grouped
        }
        if len(publish_profiles) != 1:
            issues.append(
                workflow_issue(
                    workflow,
                    "DPONE_DBT_WORKFLOW_PROFILE_CONFLICT",
                    publish_profiles,
                )
            )
        if len(parallelism) != 1:
            issues.append(
                workflow_issue(
                    workflow,
                    "DPONE_DBT_WORKFLOW_PARALLELISM_CONFLICT",
                    parallelism,
                )
            )
        if len(target_ids) != len(grouped):
            issues.append(
                workflow_issue(
                    workflow,
                    "DPONE_DBT_WORKFLOW_TARGET_COLLISION",
                    target_ids,
                )
            )
        profile = registry.workflow(workflow)
        if profile is None:
            continue
        workflow_id_issue = workflow_identifier_issue(
            workflow,
            path=f"workflow:{workflow}",
        )
        if workflow_id_issue is not None:
            issues.append(workflow_id_issue)
            continue
        workflows.append(
            CompiledDbtWorkflow(
                workflow=workflow,
                profile=profile,
                models=tuple(sorted(grouped, key=lambda item: item.workload_id)),
                dag_id=dbt_dag_id(
                    grouped[0].model.group or "data",
                    workflow,
                ),
            )
        )
    return tuple(workflows), tuple(issues)


def identity_issues(
    models: tuple[CompiledDbtModel, ...],
    workflows: tuple[CompiledDbtWorkflow, ...],
) -> tuple[DbtPublishIssue, ...]:
    """Reject generated workload or DAG identity collisions."""

    workload_ids = [item.workload_id for item in models]
    dbt_workload_ids = [f"dbt__{item.workflow}" for item in workflows]
    dag_ids = [item.dag_id for item in workflows]
    issues = []
    for label, values in (("workload", workload_ids + dbt_workload_ids), ("dag", dag_ids)):
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            issues.append(
                DbtPublishIssue(
                    code="DPONE_DBT_INTENT_INVALID",
                    message=f"Generated {label} identities collide: {duplicates}",
                    path=f"generated:{label}",
                    remediation="Use distinct model aliases/workflow names; no artifact was written.",
                )
            )
    return tuple(issues)


def select_models(
    models: tuple[DbtModelArtifact, ...],
    selector: str | None,
    *,
    path: str,
) -> tuple[tuple[DbtModelArtifact, ...], tuple[DbtPublishIssue, ...]]:
    """Select one exact model without falling back to unrelated models."""

    if selector is None:
        return models, ()
    exact = tuple(model for model in models if selector in {model.unique_id, model.fqn_selector})
    if len(exact) == 1:
        return exact, ()
    short = tuple(model for model in models if selector in {model.name, model.alias})
    if len(short) == 1:
        return short, ()
    if len(exact) > 1 or len(short) > 1:
        return (), (
            DbtPublishIssue(
                code="DPONE_DBT_MODEL_AMBIGUOUS",
                message=(f"dbt model selector {selector!r} matches multiple models"),
                path=path,
                remediation="Use the exact dbt unique_id or FQN selector.",
            ),
        )
    return (), (
        DbtPublishIssue(
            code="DPONE_DBT_MODEL_NOT_FOUND",
            message=f"dbt model selector {selector!r} matched no model",
            path=path,
            remediation=("Run dpone dbt check and use an exact unique_id, FQN, name or alias."),
        ),
    )


def model_issue(
    model: DbtModelArtifact,
    code: str,
    message: str,
    *,
    remediation: str | None = None,
) -> DbtPublishIssue:
    """Build one model-scoped public issue."""

    return DbtPublishIssue(
        code=code,
        message=message,
        path=model.original_file_path,
        remediation=remediation,
    )


def semantic_refresh_profile_issues(
    model: DbtModelArtifact,
    profile: DbtPublishProfile,
) -> tuple[DbtPublishIssue, ...]:
    """Validate the platform-owned V2 route selection.

    The trusted profile selects V2. Authors neither have to repeat nor may
    independently activate the protected incremental strategy in model config.
    """

    enabled = profile.semantic_refresh is not None
    author_selected = model.incremental_strategy == "dpone_scope_merge"
    if author_selected and not enabled:
        return (
            model_issue(
                model,
                "DPONE_DBT_V2_PROFILE_STRATEGY_MISMATCH",
                "The model selects a protected V2 strategy without a platform V2 profile",
                remediation=(
                    "Remove the author-owned incremental_strategy override; a platform owner must bind "
                    "the versioned semantic-refresh profile and injected lifecycle."
                ),
            ),
        )
    if enabled and (profile.source_type.casefold(), profile.sink_type.casefold()) != ("mssql", "clickhouse"):
        return (
            model_issue(
                model,
                "DPONE_DBT_V2_ROUTE_UNSUPPORTED",
                "Semantic refresh V2 supports only the MSSQL to ClickHouse capability cell",
                remediation="Select the platform-owned MSSQL to ClickHouse V2 profile.",
            ),
        )
    return ()


def workflow_identifier_issue(
    workflow: str,
    *,
    path: str,
) -> DbtPublishIssue | None:
    """Return a stable issue when a workflow could escape artifact paths."""

    try:
        dbt_workflow_id(workflow)
    except ValueError:
        return DbtPublishIssue(
            code="DPONE_DBT_WORKFLOW_ID_INVALID",
            message=(f"Workflow {workflow!r} must match [a-z][a-z0-9_]{{0,63}}"),
            path=path,
            remediation=("Use a lowercase path-safe workflow id; no artifact was written."),
        )
    return None


def one_certified_toolchain(models: list[CompiledDbtModel]) -> bool:
    """Return whether every compiled model names the certified toolchain."""

    return {item.profile.toolchain_id for item in models} == {DBT_SQLSERVER_1_12_CERTIFIED.contract_id}


def distinct_issues(
    issues: list[DbtPublishIssue],
) -> tuple[DbtPublishIssue, ...]:
    """Deduplicate equivalent capability failures deterministically."""

    result: dict[tuple[str, str, str], DbtPublishIssue] = {}
    for issue in issues:
        result.setdefault((issue.code, issue.message, issue.path), issue)
    return tuple(result.values())


def workflow_issue(
    workflow: str,
    code: str,
    values: Collection[object],
) -> DbtPublishIssue:
    """Build one workflow-scoped conflict."""

    return DbtPublishIssue(
        code=code,
        message=(f"Workflow {workflow!r} has conflicting values: {sorted(values, key=str)!r}"),
        path=f"workflow:{workflow}",
        remediation=("Use one effective workflow-level value for every published model."),
    )


__all__ = [
    "build_workflows",
    "distinct_issues",
    "identity_issues",
    "model_issue",
    "model_policy",
    "one_certified_toolchain",
    "select_models",
    "semantic_refresh_capability",
    "semantic_refresh_profile_issues",
    "workflow_identifier_issue",
]
