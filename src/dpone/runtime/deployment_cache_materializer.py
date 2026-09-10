"""Verified ordered promotion for the local Airflow deployment cache."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.runtime.deployment_cache_activation import DeploymentCacheActivationSnapshotter
from dpone.runtime.deployment_cache_audit import append_promotion_audit
from dpone.runtime.deployment_cache_commit import DeploymentCacheCommitter
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    cache_error_after_mutation,
    promotion_lock,
    read_regular_json_object,
    resolve_relative_current_symlink,
)
from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState
from dpone.runtime.deployment_cache_integrity import (
    DEFAULT_MAX_CACHE_ARTIFACT_BYTES,
)
from dpone.runtime.deployment_cache_models import CurrentDeployment, ValidatedDeploymentProjection
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.deployment_cache_promotion_policy import DeploymentCachePromotionPolicy
from dpone.runtime.deployment_cache_workspace_activation import DeploymentCacheWorkspaceActivation

if TYPE_CHECKING:
    from dpone.ports.composition_activation import CompositionActivationCoordinatorPort
    from dpone.ports.dbt_workspace_activation import DbtWorkspaceActivationCoordinatorPort


class DeploymentCacheMaterializer:
    """Promote one verified local projection with explicit recovery semantics."""

    def __init__(
        self,
        cache_root: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        activation_id_factory: Callable[[], str] | None = None,
        allowed_promoters: tuple[str, ...] | None = None,
        max_artifact_bytes: int = DEFAULT_MAX_CACHE_ARTIFACT_BYTES,
        workspace_activation: DbtWorkspaceActivationCoordinatorPort | None = None,
        composition_activation_coordinator: CompositionActivationCoordinatorPort | None = None,
    ) -> None:
        self._cache_root = Path(cache_root).resolve(strict=False)
        self._promotion_policy = DeploymentCachePromotionPolicy(
            clock=clock,
            activation_id_factory=activation_id_factory,
            allowed_promoters=allowed_promoters,
            error_factory=DeploymentCacheError,
        )
        self._workspace_activation = DeploymentCacheWorkspaceActivation(
            workspace_activation, composition_coordinator=composition_activation_coordinator
        )
        self._current_state = DeploymentCacheCurrentState(self._cache_root)
        self._projection_validator = DeploymentCacheProjectionValidator(
            self._cache_root,
            max_artifact_bytes=max_artifact_bytes,
        )
        self._activation_snapshotter = DeploymentCacheActivationSnapshotter(
            self._cache_root,
            validator=self._projection_validator,
        )
        self._committer = DeploymentCacheCommitter(self._cache_root)

    def promote(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
        promoted_by: str = "local://dpone",
        expected_current_deployment_id: str | None = None,
        expect_current_absent: bool = False,
        source_commit: str | None = None,
        attestation_ref: str | None = None,
        activation_id: str | None = None,
        precommit_check: Callable[[], None] | None = None,
        postcommit_action: Callable[[CurrentDeployment], None] | None = None,
        workspace_authority_connection_ref: str | None = None,
    ) -> CurrentDeployment:
        if not promoted_by:
            raise DeploymentCacheError(
                "DPONE_CURRENT_POINTER_PROMOTER_MISSING",
                "current pointer promotion requires promoted_by",
            )
        self._promotion_policy.authorize(promoted_by)
        if expect_current_absent and expected_current_deployment_id is not None:
            raise DeploymentCacheError(
                "DPONE_CURRENT_POINTER_CAS_INVALID",
                "expected current deployment id and expected-absent guard are mutually exclusive",
            )
        resolved_activation_id = self._promotion_policy.activation_id(activation_id)
        deployment_path = Path(deployment_dir).absolute()
        mutation_started = False
        try:
            with promotion_lock(self._cache_root):
                activation = self._activation_snapshotter.prepare(deployment_path, environment=environment)
                mutation_started = True
                deployment = activation.projection.identity()
                deployment_path = activation.path
                previous_deployment_id = self._current_state.active_deployment_id()
                self._check_cas(
                    previous_deployment_id,
                    expected_current_deployment_id=expected_current_deployment_id,
                    expect_current_absent=expect_current_absent,
                )
                pointer = self._promotion_policy.pointer(
                    deployment=deployment,
                    environment=environment,
                    promoted_by=promoted_by,
                    previous_deployment_id=previous_deployment_id,
                    source_commit=source_commit,
                    attestation_ref=attestation_ref,
                    activation_id=resolved_activation_id,
                    workspace_authority_connection_ref=workspace_authority_connection_ref,
                )
                if precommit_check is not None:
                    precommit_check()
                workspace_occurrence = self._workspace_activation.prepare_occurrence(
                    projection_root=deployment_path,
                    dbt_wire=activation.projection.dbt_runtime_wire_contract,
                    activation_id=resolved_activation_id,
                    environment=environment,
                    release_id=str(deployment["release_id"]),
                    deployment_id=str(deployment["deployment_id"]),
                    previous_deployment_id=previous_deployment_id,
                )
                current, pointer_path = self._commit_promotion(
                    deployment_path=deployment_path,
                    pointer=pointer,
                )
                self._workspace_activation.activate_occurrence(workspace_occurrence, projection_root=deployment_path)
                result = CurrentDeployment(
                    activation_id=str(pointer["activation_id"]),
                    deployment_id=str(deployment["deployment_id"]),
                    release_id=str(deployment["release_id"]),
                    environment=environment,
                    current_path=current,
                    pointer_path=pointer_path,
                    promoted_by=promoted_by,
                    promoted_at=str(pointer["promoted_at"]),
                    previous_deployment_id=previous_deployment_id,
                    source_commit=source_commit,
                    attestation_ref=attestation_ref,
                    workspace_authority_connection_ref=workspace_authority_connection_ref,
                )
                if postcommit_action is not None:
                    postcommit_action(result)
        except DeploymentCacheError as exc:
            if mutation_started and exc.details.get("state_may_have_changed") is not True:
                raise cache_error_after_mutation(exc) from exc
            raise
        return result

    def validate(self, deployment_dir: str | Path, *, environment: str) -> dict[str, Any]:
        """Validate one immutable deployment projection without mutating cache state."""

        return self._projection_validator.validate(deployment_dir, environment=environment)

    def validate_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
    ) -> ValidatedDeploymentProjection:
        """Return the same verified projection payloads used for promotion."""

        return self._projection_validator.validate_details(deployment_dir, environment=environment)

    def validate_current_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
    ) -> ValidatedDeploymentProjection:
        """Validate active bytes from activation or legacy deployment layout."""

        return self._projection_validator.validate_current_details(deployment_dir, environment=environment)

    def recover(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
        promoted_by: str,
        expected_current_deployment_id: str | None,
    ) -> CurrentDeployment:
        """Repair inconsistent control state with an authorized, CAS-bound promotion."""

        if not promoted_by:
            raise DeploymentCacheError(
                "DPONE_CURRENT_POINTER_PROMOTER_MISSING",
                "cache recovery requires promoted_by",
            )
        self._promotion_policy.authorize(promoted_by)
        activation_id = self._promotion_policy.activation_id(None)
        deployment_path = Path(deployment_dir).absolute()
        with promotion_lock(self._cache_root):
            previous_deployment_id = self._current_state.actual_deployment_id()
            if previous_deployment_id != expected_current_deployment_id:
                raise DeploymentCacheError(
                    "DPONE_CURRENT_POINTER_CAS_MISMATCH",
                    "active current deployment changed after the recovery plan was reviewed",
                    path=(self._cache_root / "current").as_posix(),
                )
            activation = self._activation_snapshotter.prepare(deployment_path, environment=environment)
            deployment = activation.projection.identity()
            deployment_path = activation.path
            pointer = self._promotion_policy.pointer(
                deployment=deployment,
                environment=environment,
                promoted_by=promoted_by,
                previous_deployment_id=previous_deployment_id,
                source_commit=None,
                attestation_ref=None,
                activation_id=activation_id,
            )
            workspace_occurrence = self._workspace_activation.prepare_occurrence(
                projection_root=deployment_path,
                dbt_wire=activation.projection.dbt_runtime_wire_contract,
                activation_id=activation_id,
                environment=environment,
                release_id=str(deployment["release_id"]),
                deployment_id=str(deployment["deployment_id"]),
                previous_deployment_id=previous_deployment_id,
            )
            current, pointer_path = self._commit_promotion(deployment_path=deployment_path, pointer=pointer)
            self._workspace_activation.activate_occurrence(workspace_occurrence, projection_root=deployment_path)
        return CurrentDeployment(
            activation_id=str(pointer["activation_id"]),
            deployment_id=str(deployment["deployment_id"]),
            release_id=str(deployment["release_id"]),
            environment=environment,
            current_path=current,
            pointer_path=pointer_path,
            promoted_by=promoted_by,
            promoted_at=str(pointer["promoted_at"]),
            previous_deployment_id=previous_deployment_id,
        )

    def repair_audit(
        self,
        *,
        environment: str,
        recovery_actor: str,
        expected_current_deployment_id: str,
    ) -> CurrentDeployment:
        """Append the existing pointer to audit without rewriting its provenance."""

        if not recovery_actor:
            raise DeploymentCacheError("DPONE_CURRENT_POINTER_PROMOTER_MISSING", "cache recovery requires promoted_by")
        self._promotion_policy.authorize(recovery_actor)
        with promotion_lock(self._cache_root):
            active_id = self._current_state.active_deployment_id(expected_environment=environment)
            if active_id != expected_current_deployment_id:
                raise DeploymentCacheError(
                    "DPONE_CURRENT_POINTER_CAS_MISMATCH",
                    "active current deployment changed after the recovery plan was reviewed",
                    path=(self._cache_root / "current").as_posix(),
                )
            pointer_path = self._cache_root / "current-pointer.json"
            pointer = read_regular_json_object(
                pointer_path,
                missing_code="DPONE_CURRENT_POINTER_NOT_FOUND",
                invalid_code="DPONE_CURRENT_POINTER_INVALID",
                label="current pointer",
                root=self._cache_root,
            )
            current_target = resolve_relative_current_symlink(self._cache_root)
            current_projection = self.validate_current_details(
                current_target,
                environment=environment,
            )
            if (
                current_projection.deployment_id != active_id
                or pointer.get("release_id") != current_projection.release_id
            ):
                raise _recovery_required(self._cache_root / "current")
            self._workspace_activation.require_existing(
                projection_root=current_target,
                dbt_wire=current_projection.dbt_runtime_wire_contract,
                activation_id=_optional_string(pointer.get("activation_id")),
                environment=environment,
                release_id=current_projection.release_id,
                deployment_id=current_projection.deployment_id,
                previous_deployment_id=_optional_string(pointer.get("previous_deployment_id")),
            )
            audit_payload = dict(pointer)
            audit_payload["recovery"] = {
                "actor": recovery_actor,
                "repaired_at": self._promotion_policy.timestamp(),
            }
            try:
                append_promotion_audit(self._cache_root / "current-pointer-audit.jsonl", audit_payload)
            except OSError as exc:
                raise DeploymentCacheError(
                    "DPONE_CACHE_PROMOTION_WRITE_FAILED",
                    "promotion audit repair could not be written",
                    path=(self._cache_root / "current-pointer-audit.jsonl").as_posix(),
                ) from exc
        return CurrentDeployment(
            activation_id=_optional_string(pointer.get("activation_id")),
            deployment_id=active_id,
            release_id=str(pointer.get("release_id") or ""),
            environment=environment,
            current_path=self._cache_root / "current",
            pointer_path=pointer_path,
            promoted_by=str(pointer.get("promoted_by") or ""),
            promoted_at=str(pointer.get("promoted_at") or ""),
            previous_deployment_id=_optional_string(pointer.get("previous_deployment_id")),
            source_commit=_optional_string(pointer.get("source_commit")),
            attestation_ref=_optional_string(pointer.get("attestation_ref")),
            workspace_authority_connection_ref=_optional_string(pointer.get("workspace_authority_connection_ref")),
        )

    def _check_cas(
        self,
        current_deployment_id: str | None,
        *,
        expected_current_deployment_id: str | None,
        expect_current_absent: bool,
    ) -> None:
        mismatch = (
            expect_current_absent
            and current_deployment_id is not None
            or expected_current_deployment_id is not None
            and current_deployment_id != expected_current_deployment_id
        )
        if mismatch:
            raise DeploymentCacheError(
                "DPONE_CURRENT_POINTER_CAS_MISMATCH",
                "current deployment changed before promotion",
                path=(self._cache_root / "current-pointer.json").as_posix(),
            )

    def _commit_promotion(self, *, deployment_path: Path, pointer: dict[str, Any]) -> tuple[Path, Path]:
        """Delegate the commit protocol through the stable test seam."""

        return self._committer.commit(deployment_path=deployment_path, pointer=pointer)


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _recovery_required(path: Path) -> DeploymentCacheError:
    return DeploymentCacheError(
        "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED",
        "current deployment control files are inconsistent; run cache recovery before promotion",
        path=path.as_posix(),
    )


__all__ = ["CurrentDeployment", "DeploymentCacheMaterializer"]
