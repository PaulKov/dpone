"""Pure proof binding for immutable semantic-refresh release templates.

Provider adapters authenticate the complete pack and parse topology before
passing its workflow and ordered model coordinates here. These contracts bind
native execution, pre-release proofs, deployment plans, logical runs and protected
receipts without importing the provider or inspecting framework objects.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.dbt_publishing import DbtExecutionPack
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1
from dpone.contracts.dbt_semantic_refresh_activation import SemanticRefreshActivationAuthorityReceipt
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshPlanBundle,
    SemanticRefreshPreReleaseProofBundle,
)
from dpone.contracts.dbt_semantic_refresh_run_contracts import SemanticRefreshRunExecutionBundle
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256


@dataclass(frozen=True, slots=True)
class SemanticRefreshTemplateCoordinates:
    """Workflow/model coordinates projected from an authenticated topology.

    The provider owns topology authentication and ordering. Projection preserves
    its model order verbatim; proof binding never reparses a provider object or
    treats these coordinates as independent topology authentication.
    """

    workflow_name: str
    model_unique_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SemanticRefreshTemplateProofAuthority:
    """Deployment-neutral proof coordinates bound into one release template."""

    workflow_name: str
    manifest_sha256: str
    profile_sha256: str
    toolchain_sha256: str
    model_unique_ids: tuple[str, ...]
    pre_release_bundle_sha256: str
    package_artifacts_sha256: str

    @classmethod
    def from_pre_release(
        cls,
        value: SemanticRefreshPreReleaseProofBundle,
    ) -> SemanticRefreshTemplateProofAuthority:
        if not isinstance(value, SemanticRefreshPreReleaseProofBundle):
            raise TypeError("semantic-refresh release requires a typed pre-release proof bundle")
        payload = value.to_dict()
        supplied = payload.pop("pre_release_bundle_sha256", None)
        if supplied != value.pre_release_bundle_sha256 or semantic_refresh_sha256(payload) != supplied:
            raise ValueError("semantic-refresh pre-release proof bundle digest differs")
        model_ids = tuple(item.model_unique_id for item in value.models)
        if value.mutation_closure.selected_mutating_node_ids != model_ids:
            raise ValueError("semantic-refresh pre-release mutation closure differs from its models")
        return cls(
            workflow_name=value.workflow_name,
            manifest_sha256=value.manifest_sha256,
            profile_sha256=value.profile_sha256,
            toolchain_sha256=value.toolchain_sha256,
            model_unique_ids=model_ids,
            pre_release_bundle_sha256=value.pre_release_bundle_sha256,
            package_artifacts_sha256=value.lifecycle_policy.package_artifacts_digest,
        )

    def __post_init__(self) -> None:
        if not isinstance(self.workflow_name, str) or not self.workflow_name:
            raise ValueError("semantic-refresh proof workflow identity is absent")
        for field_name in (
            "manifest_sha256",
            "profile_sha256",
            "toolchain_sha256",
            "pre_release_bundle_sha256",
            "package_artifacts_sha256",
        ):
            digest(getattr(self, field_name), field_name)
        if (
            not self.model_unique_ids
            or self.model_unique_ids != tuple(sorted(set(self.model_unique_ids)))
            or any(not isinstance(item, str) or not item for item in self.model_unique_ids)
        ):
            raise ValueError("semantic-refresh proof model closure must be canonical")


def parse_template_execution(
    execution_pack: object,
    pre_release_sha256: object,
    package_sha256: object,
) -> tuple[DbtExecutionPack, str, str]:
    """Admit native execution and wire before checking the two proof digest pins.

    Call after provider fingerprint, semantic shape and topology admission. The
    order deliberately retains native parse/wire errors ahead of malformed pins.
    """

    execution = DbtExecutionPack.from_mapping(execution_pack)
    execution.require_wire_contract(DBT_RUNTIME_WIRE_V1)
    return (
        execution,
        digest(pre_release_sha256, "pre_release_bundle_sha256"),
        digest(package_sha256, "package_artifacts_sha256"),
    )


def require_activation_receipt(receipt: object) -> None:
    """Admit receipt type before adapters access provider coordinates."""

    if not isinstance(receipt, SemanticRefreshActivationAuthorityReceipt):
        raise TypeError("semantic-refresh activation requires a protected persistence receipt")


def require_plan_run_bundles(plan: object, run: object) -> None:
    """Admit plan/run types before adapters access provider coordinates."""

    if not isinstance(plan, SemanticRefreshPlanBundle) or not isinstance(run, SemanticRefreshRunExecutionBundle):
        raise TypeError("semantic-refresh activation requires typed plan and run bundles")


def require_plan_bundle(plan: object) -> None:
    """Admit the deployment plan type before provider coordinate projection."""

    if not isinstance(plan, SemanticRefreshPlanBundle):
        raise TypeError("semantic-refresh deployment requires a typed plan bundle")


def validate_activation_inputs(
    plan: SemanticRefreshPlanBundle,
    run: SemanticRefreshRunExecutionBundle,
    receipt: SemanticRefreshActivationAuthorityReceipt,
    topology: SemanticRefreshTemplateCoordinates,
    execution_pack: DbtExecutionPack,
    pre_release_sha256: str,
    package_sha256: str,
) -> None:
    """Validate one run-bound authority against the immutable template."""

    require_activation_receipt(receipt)
    validate_plan_run_template(plan, run, topology, execution_pack, pre_release_sha256, package_sha256)
    authority = plan.release_deployment_authority
    expected_baselines = tuple(sorted((item.model_unique_id, item.baseline_receipt_sha256) for item in plan.targets))
    route_receipts = {item.route_certification_receipt_sha256 for item in plan.targets}
    if (
        receipt.plan_bundle_sha256 != plan.plan_bundle_sha256
        or receipt.release_id != authority.release_id
        or receipt.deployment_id != authority.deployment_id
        or receipt.baseline_receipts != expected_baselines
        or receipt.runtime_assurance_receipts != expected_assurance_receipts(plan)
        or route_receipts != {receipt.route_certification_receipt_sha256}
    ):
        raise ValueError("semantic-refresh activation authority differs from template/plan/run closure")


def validate_plan_run_template(
    plan: SemanticRefreshPlanBundle,
    run: SemanticRefreshRunExecutionBundle,
    topology: SemanticRefreshTemplateCoordinates,
    execution_pack: DbtExecutionPack,
    pre_release_sha256: str,
    package_sha256: str,
) -> None:
    """Validate a run execution against its plan and template."""

    require_plan_run_bundles(plan, run)
    validate_plan_template(plan, topology, execution_pack, pre_release_sha256, package_sha256)
    binding = run.workflow_execution_binding
    model_ids = tuple(item.model_unique_id for item in plan.operation_plans)
    if (
        run.plan_bundle_sha256 != plan.plan_bundle_sha256
        or binding.workflow_plan_sha256 != plan.workflow_plan.workflow_plan_sha256
        or tuple(binding.selected_mutating_node_ids) != tuple(sorted(model_ids))
    ):
        raise ValueError("semantic-refresh activation authority differs from template/plan/run closure")


def validate_plan_template(
    plan: SemanticRefreshPlanBundle,
    topology: SemanticRefreshTemplateCoordinates,
    execution_pack: DbtExecutionPack,
    pre_release_sha256: str,
    package_sha256: str,
) -> None:
    """Validate a deployment plan against its run-neutral template."""

    require_plan_bundle(plan)
    model_ids = tuple(item.model_unique_id for item in plan.operation_plans)
    if (
        plan.pre_release_bundle_sha256 != pre_release_sha256
        or plan.package_artifacts_sha256 != package_sha256
        or set(model_ids) != set(topology.model_unique_ids)
        or execution_pack.workflow_id != topology.workflow_name
        or set(execution_pack.selection_lock.publish_model_unique_ids) != set(model_ids)
    ):
        raise ValueError("semantic-refresh deployment authority differs from template/plan closure")


def validate_pre_release_template(
    execution_pack: DbtExecutionPack,
    topology: Mapping[str, Any],
    authority: SemanticRefreshTemplateProofAuthority,
) -> None:
    """Validate exact pre-release coordinates before template construction."""

    execution_pack.require_wire_contract(DBT_RUNTIME_WIRE_V1)
    if not isinstance(authority, SemanticRefreshTemplateProofAuthority):
        raise TypeError("semantic-refresh release requires typed proof coordinates")
    model_ids = authority.model_unique_ids
    topology_models = topology.get("model_unique_ids")
    if (
        execution_pack.workflow_id != authority.workflow_name
        or execution_pack.selection_lock.manifest_sha256 != authority.manifest_sha256
        or execution_pack.selection_lock.toolchain_sha256 != authority.toolchain_sha256
        or execution_pack.selection_lock.publish_model_unique_ids != model_ids
        or execution_pack.selection_lock.selected_graph_unique_ids != model_ids
        or topology.get("workflow_name") != authority.workflow_name
        or topology.get("profile_sha256") != authority.profile_sha256
        or not isinstance(topology_models, list)
        or tuple(topology_models) != model_ids
    ):
        raise ValueError("semantic-refresh template differs from the exact pre-release proof closure")


def expected_assurance_receipts(
    plan: SemanticRefreshPlanBundle,
) -> tuple[tuple[str, str, str], ...]:
    """Project the exact ordered runtime-assurance receipt closure."""

    return tuple(
        sorted(
            (target.model_unique_id, kind, value)
            for target in plan.targets
            for kind, value in (
                ("ddl_freeze", target.ddl_freeze_assurance_receipt_sha256),
                ("utc_semantics", target.utc_semantics_assurance_receipt_sha256),
                ("writer_exclusivity", target.writer_exclusivity_assurance_receipt_sha256),
            )
            if value is not None
        )
    )


def digest(value: object, field_name: str) -> str:
    """Require and return a canonical SHA-256 digest."""

    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a canonical sha256 digest")
    return value
