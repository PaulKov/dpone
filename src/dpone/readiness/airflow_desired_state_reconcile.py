"""Composition adapters for one exact Airflow desired-state watcher cycle."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.adapters.airflow_desired_state_checkpoint import (
    AtomicAirflowDesiredStateSnapshotWriter,
    FileDesiredStateCheckpointStore,
    FileDesiredStateReconcileEvidenceStore,
    FileDesiredStateRecoveryRecordStore,
)
from dpone.app.composition_activation import build_composition_activation_coordinator
from dpone.app.dbt_workspace_activation_composition import (
    build_deployment_cache_workspace_activation_coordinator,
)
from dpone.contracts.airflow_desired_state import (
    MAX_AIRFLOW_DESIRED_STATE_BYTES,
    AirflowDesiredDeployment,
)
from dpone.ports.airflow_desired_state import (
    ActiveDesiredDeployment,
    DesiredStateActivationResult,
    DesiredStateReconcilePortError,
)
from dpone.readiness.airflow_artifact_trust_material import (
    AirflowArtifactTrustMaterialError,
)
from dpone.readiness.airflow_deployment_attestation_verifier import (
    optional_airflow_deployment_attestation_verifier,
)
from dpone.readiness.airflow_desired_state_materializer import (
    ArtifactRegistryDesiredDeploymentMaterializer,
)
from dpone.readiness.airflow_desired_state_values import (
    cache_reconcile_error as _cache_reconcile_error,
)
from dpone.readiness.airflow_desired_state_values import (
    commit_failure_status as _commit_failure_status,
)
from dpone.readiness.airflow_desired_state_values import (
    commit_running_status as _commit_running_status,
)
from dpone.readiness.airflow_desired_state_values import (
    commit_success_status as _commit_success_status,
)
from dpone.readiness.airflow_desired_state_values import (
    optional_text as _optional_text,
)
from dpone.readiness.airflow_desired_state_values import (
    required_text as _required_text,
)
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactDeliveryError,
    MaterializeRequest,
)
from dpone.runtime.airflow_artifact_delivery_support import require_registry_scope
from dpone.runtime.deployment_cache import (
    DeploymentCacheCurrentState,
    DeploymentCacheError,
    DeploymentCacheMaterializer,
)
from dpone.runtime.deployment_cache_common import (
    promotion_lock,
    read_regular_json_object,
    reconcile_lock,
    resolve_relative_current_symlink,
)
from dpone.services.airflow_desired_state_reconcile import (
    AirflowDesiredStateReconciler,
    DesiredStateReconcileError,
    DesiredStateReconcileRequest,
)

if TYPE_CHECKING:
    from dpone.contracts.airflow_desired_state_reconcile import (
        DesiredStateReconcileEvidence,
    )
    from dpone.ports.composition_activation import CompositionActivationCoordinatorPort
    from dpone.ports.dbt_workspace_activation import DbtWorkspaceActivationCoordinatorPort
    from dpone.readiness.airflow_artifact_delivery import ArtifactRegistryOptions
    from dpone.readiness.airflow_desired_state_authority import (
        AirflowDesiredStateAuthority,
    )
    from dpone.readiness.airflow_desired_state_store import DesiredStateStoreOptions
    from dpone.runtime.deployment_cache_models import CurrentDeployment


class DeploymentCacheDesiredDeploymentActivator:
    """Activate a verified projection using local CAS and remote precommit CAS."""

    def __init__(
        self,
        *,
        cache_root: Path,
        promoted_by: str,
        workspace_activation: DbtWorkspaceActivationCoordinatorPort | None = None,
        composition_activation: CompositionActivationCoordinatorPort | None = None,
        workspace_authority_connection_ref: str | None = None,
    ) -> None:
        self._cache_root = cache_root
        self._promoted_by = promoted_by
        self._workspace_authority_connection_ref = workspace_authority_connection_ref
        self._state = DeploymentCacheCurrentState(cache_root)
        self._materializer = DeploymentCacheMaterializer(
            cache_root,
            allowed_promoters=(promoted_by,),
            workspace_activation=workspace_activation,
            composition_activation_coordinator=composition_activation,
        )

    def current(self, *, environment: str) -> ActiveDesiredDeployment | None:
        try:
            deployment_id = self._state.active_deployment_id(expected_environment=environment)
            if deployment_id is None:
                return None
            projection = self._materializer.validate_current_details(
                resolve_relative_current_symlink(self._cache_root),
                environment=environment,
            )
            pointer = read_regular_json_object(
                self._cache_root / "current-pointer.json",
                missing_code="DPONE_CURRENT_POINTER_NOT_FOUND",
                invalid_code="DPONE_CURRENT_POINTER_INVALID",
                label="current pointer",
                root=self._cache_root,
            )
            release_id = _required_text(pointer, "release_id")
            if projection.deployment_id != deployment_id or projection.release_id != release_id:
                raise DeploymentCacheError(
                    "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED",
                    "active deployment projection differs from its current pointer",
                )
            return ActiveDesiredDeployment(
                release_id=release_id,
                deployment_id=deployment_id,
                activation_id=_required_text(pointer, "activation_id"),
                attestation_ref=_optional_text(pointer.get("attestation_ref")),
            )
        except DeploymentCacheError as exc:
            raise _cache_port_error(exc) from exc

    def commit_if_current(
        self,
        expected: ActiveDesiredDeployment,
        *,
        environment: str,
        post_validation_commit: Callable[[], None],
    ) -> None:
        try:
            with promotion_lock(self._cache_root):
                observed = self.current(environment=environment)
                if observed != expected:
                    raise DesiredStateReconcilePortError(
                        "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED",
                        "active deployment changed before control evidence commit",
                        state_may_have_changed=True,
                    )
                post_validation_commit()
        except DeploymentCacheError as exc:
            raise _cache_port_error(exc) from exc

    def activate(
        self,
        desired: AirflowDesiredDeployment,
        *,
        expected_current_deployment_id: str | None,
        remote_precondition: Callable[[], bool],
        post_activation_commit: Callable[[DesiredStateActivationResult], None] | None = None,
    ) -> DesiredStateActivationResult:
        request = MaterializeRequest(
            cache_root=self._cache_root,
            release_id=desired.promotion.release_id,
            deployment_id=desired.promotion.deployment_id,
            environment=desired.environment,
            artifact_registry_ref="desired-state-activation",
        )

        def enforce_remote_revision() -> None:
            if not remote_precondition():
                raise DeploymentCacheError(
                    "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED",
                    "desired state changed before local cache activation",
                )

        def commit_activation(current: CurrentDeployment) -> None:
            if post_activation_commit is None:
                return
            if current.activation_id is None:
                raise DeploymentCacheError(
                    "DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID",
                    "cache activation did not produce an activation identity",
                )
            post_activation_commit(
                DesiredStateActivationResult(
                    activation_id=current.activation_id,
                    previous_deployment_id=current.previous_deployment_id,
                )
            )

        try:
            current = self._materializer.promote(
                self._cache_root / "deployments" / desired.environment / request.deployment_dir_name,
                environment=desired.environment,
                promoted_by=self._promoted_by,
                expected_current_deployment_id=expected_current_deployment_id,
                expect_current_absent=expected_current_deployment_id is None,
                source_commit=desired.source.git_sha,
                attestation_ref=desired.sha256,
                activation_id=desired.source.occurrence_id,
                precommit_check=enforce_remote_revision,
                postcommit_action=commit_activation if post_activation_commit is not None else None,
                workspace_authority_connection_ref=self._workspace_authority_connection_ref,
            )
        except DeploymentCacheError as exc:
            raise _cache_port_error(exc) from exc
        if current.activation_id is None:
            raise DesiredStateReconcilePortError(
                "DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID",
                "cache activation did not produce an activation identity",
                state_may_have_changed=True,
            )
        return DesiredStateActivationResult(
            activation_id=current.activation_id,
            previous_deployment_id=current.previous_deployment_id,
        )


def reconcile_desired_state(
    *,
    authority: AirflowDesiredStateAuthority,
    desired_store_options: DesiredStateStoreOptions,
    artifact_registry_options: ArtifactRegistryOptions,
    cache_root: Path,
    max_object_bytes: int,
    max_total_bytes: int,
    trust_policy_path: Path = Path("/etc/dpone/artifact-trust/policy.json"),
    trust_key_root: Path = Path("/etc/dpone/artifact-trust"),
) -> DesiredStateReconcileEvidence:
    """Run one serialized reconcile cycle and return credential-free evidence."""

    root = cache_root.resolve(strict=False)
    status_root = root / "status"
    evidence_store = FileDesiredStateReconcileEvidenceStore(status_root, root=root)
    try:
        with reconcile_lock(root):
            _commit_running_status(evidence_store)
            try:
                registry = artifact_registry_options.build()
                require_registry_scope(registry, authority.registry_scope_id)
                deployment_attestation_verifier = optional_airflow_deployment_attestation_verifier(
                    registry=registry,
                    policy_path=trust_policy_path,
                    key_root=trust_key_root,
                )
                service = AirflowDesiredStateReconciler(
                    reader=desired_store_options.build(),
                    snapshot_writer=AtomicAirflowDesiredStateSnapshotWriter(
                        status_root / "desired-state.json",
                        root=root,
                        max_bytes=MAX_AIRFLOW_DESIRED_STATE_BYTES,
                    ),
                    checkpoint_store=FileDesiredStateCheckpointStore(
                        status_root / "desired-state-checkpoint.json",
                        root=root,
                    ),
                    activation_receipt_store=evidence_store,
                    recovery_record_store=FileDesiredStateRecoveryRecordStore(
                        status_root / "active-recovery-record.json",
                        root=root,
                    ),
                    materializer=ArtifactRegistryDesiredDeploymentMaterializer(
                        cache_root=root,
                        artifact_registry_ref=authority.artifact_registry_ref,
                        registry=registry,
                        max_object_bytes=max_object_bytes,
                        max_total_bytes=max_total_bytes,
                        deployment_attestation_verifier=deployment_attestation_verifier,
                    ),
                    activator=DeploymentCacheDesiredDeploymentActivator(
                        cache_root=root,
                        promoted_by=authority.watcher_identity,
                        workspace_activation=(
                            build_deployment_cache_workspace_activation_coordinator(
                                cache_root=root,
                                authority_connection_ref=authority.workspace_authority_connection_ref,
                            )
                            if authority.workspace_authority_connection_ref is not None
                            else None
                        ),
                        composition_activation=(
                            build_composition_activation_coordinator(
                                cache_root=root,
                                authority_connection_ref=authority.workspace_authority_connection_ref,
                            )
                            if authority.workspace_authority_connection_ref is not None
                            else None
                        ),
                        workspace_authority_connection_ref=authority.workspace_authority_connection_ref,
                    ),
                    success_status_committer=lambda evidence: _commit_success_status(evidence_store, evidence),
                )
                evidence = service.reconcile(
                    DesiredStateReconcileRequest(
                        environment=authority.environment,
                        registry_scope_id=authority.registry_scope_id,
                        source_project=authority.source_project,
                        source_ref=authority.source_ref,
                    )
                )
            except AirflowArtifactDeliveryError as exc:
                error = DesiredStateReconcileError(
                    exc.code,
                    "artifact registry is outside the trusted desired-state authority",
                )
                _commit_failure_status(evidence_store, error)
                raise error from exc
            except AirflowArtifactTrustMaterialError as exc:
                error = DesiredStateReconcileError(
                    exc.code,
                    "Airflow deployment trust material is invalid",
                )
                _commit_failure_status(evidence_store, error)
                raise error from exc
            except DesiredStateReconcileError as exc:
                _commit_failure_status(evidence_store, exc)
                raise
            except DeploymentCacheError as exc:
                error = _cache_reconcile_error(exc)
                _commit_failure_status(evidence_store, error)
                raise error from exc
            return evidence
    except DeploymentCacheError as exc:
        error = _cache_reconcile_error(exc)
        _commit_failure_status(evidence_store, error)
        raise error from exc


def _cache_port_error(exc: DeploymentCacheError) -> DesiredStateReconcilePortError:
    return DesiredStateReconcilePortError(
        exc.code,
        "local deployment cache operation failed",
        state_may_have_changed=exc.details.get("state_may_have_changed") is True,
    )


__all__ = [
    "ArtifactRegistryDesiredDeploymentMaterializer",
    "DeploymentCacheDesiredDeploymentActivator",
    "reconcile_desired_state",
]
