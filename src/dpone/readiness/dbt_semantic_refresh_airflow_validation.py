"""Provider authentication and compatibility adapters for template proof binding."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone_airflow_pack.pack_identity import verify_pack_fingerprint
from dpone_airflow_pack.semantic_refresh_topology import SemanticRefreshTopologyTemplate

from dpone.contracts import dbt_semantic_refresh_template_binding as _binding
from dpone.contracts.dbt_semantic_refresh_template_binding import (
    SemanticRefreshTemplateProofAuthority as SemanticRefreshTemplateProofAuthority,
)
from dpone.contracts.dbt_semantic_refresh_template_binding import (
    digest as digest,
)
from dpone.contracts.dbt_semantic_refresh_template_binding import (
    expected_assurance_receipts as expected_assurance_receipts,
)
from dpone.contracts.dbt_semantic_refresh_template_binding import (
    validate_pre_release_template as validate_pre_release_template,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_publishing import DbtExecutionPack
    from dpone.contracts.dbt_semantic_refresh_activation import SemanticRefreshActivationAuthorityReceipt
    from dpone.contracts.dbt_semantic_refresh_plan_contracts import SemanticRefreshPlanBundle
    from dpone.contracts.dbt_semantic_refresh_run_contracts import SemanticRefreshRunExecutionBundle

_TEMPLATE_FIELDS = frozenset(
    {
        "activation",
        "dbt_execution_pack",
        "executable",
        "kind",
        "pack_fingerprint",
        "pack_identity",
        "producer",
        "runtime_command",
        "runtime_payload_ids",
        "schema_version",
        "semantic_refresh",
        "workload",
    }
)


def protected_template(
    template_pack: Mapping[str, Any],
) -> tuple[SemanticRefreshTopologyTemplate, DbtExecutionPack, str, str]:
    """Parse and authenticate one closed, run-neutral template."""

    if not isinstance(template_pack, Mapping) or set(template_pack) != _TEMPLATE_FIELDS:
        raise ValueError("semantic-refresh template fields are not closed")
    verify_pack_fingerprint(template_pack)
    semantic = template_pack.get("semantic_refresh")
    if (
        template_pack.get("kind") != "gitops.airflow_pack"
        or template_pack.get("schema_version") != "3"
        or template_pack.get("activation") != "POST_DEPLOYMENT_AUTHORITY_REQUIRED"
        or template_pack.get("executable") is not False
        or not isinstance(semantic, Mapping)
        or set(semantic) != {"mode", "package_artifacts_sha256", "pre_release_bundle_sha256", "topology"}
        or semantic.get("mode") != "semantic_refresh_v2_template"
        or not isinstance(semantic.get("topology"), Mapping)
    ):
        raise ValueError("semantic-refresh release input is not a non-executable V2 template")
    topology = SemanticRefreshTopologyTemplate.from_mapping(semantic["topology"])
    execution, pre_release_sha256, package_sha256 = _binding.parse_template_execution(
        template_pack.get("dbt_execution_pack"),
        semantic.get("pre_release_bundle_sha256"),
        semantic.get("package_artifacts_sha256"),
    )
    return topology, execution, pre_release_sha256, package_sha256


def validate_activation_inputs(
    plan: SemanticRefreshPlanBundle,
    run: SemanticRefreshRunExecutionBundle,
    receipt: SemanticRefreshActivationAuthorityReceipt,
    topology: SemanticRefreshTopologyTemplate,
    execution_pack: DbtExecutionPack,
    pre_release_sha256: str,
    package_sha256: str,
) -> None:
    """Validate one run-bound authority against the immutable template."""

    _binding.require_activation_receipt(receipt)
    _binding.require_plan_run_bundles(plan, run)
    _binding.validate_activation_inputs(
        plan, run, receipt, _topology_coordinates(topology), execution_pack, pre_release_sha256, package_sha256
    )


def validate_plan_run_template(
    plan: SemanticRefreshPlanBundle,
    run: SemanticRefreshRunExecutionBundle,
    topology: SemanticRefreshTopologyTemplate,
    execution_pack: DbtExecutionPack,
    pre_release_sha256: str,
    package_sha256: str,
) -> None:
    """Validate a run execution against its plan and template."""

    _binding.require_plan_run_bundles(plan, run)
    _binding.validate_plan_run_template(
        plan, run, _topology_coordinates(topology), execution_pack, pre_release_sha256, package_sha256
    )


def validate_plan_template(
    plan: SemanticRefreshPlanBundle,
    topology: SemanticRefreshTopologyTemplate,
    execution_pack: DbtExecutionPack,
    pre_release_sha256: str,
    package_sha256: str,
) -> None:
    """Validate a deployment plan against its run-neutral template."""

    _binding.require_plan_bundle(plan)
    _binding.validate_plan_template(
        plan, _topology_coordinates(topology), execution_pack, pre_release_sha256, package_sha256
    )


def _topology_coordinates(topology: SemanticRefreshTopologyTemplate) -> _binding.SemanticRefreshTemplateCoordinates:
    """Project only verified primitive coordinates into the pure proof policy."""

    return _binding.SemanticRefreshTemplateCoordinates(topology.workflow_name, topology.model_unique_ids)
