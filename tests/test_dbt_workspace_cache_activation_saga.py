"""Workspace pointer mutation is subordinate to durable occurrence readback."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionActivationReceipt
from dpone.contracts.dbt_workspace_activation import (
    DbtWorkspaceActivationReceipt,
    DbtWorkspaceActivationRequest,
    DbtWorkspaceActiveActivation,
    DbtWorkspaceGuardEpoch,
    DbtWorkspacePhysicalResource,
    DbtWorkspacePreparedActivation,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_materializer import DeploymentCacheMaterializer
from tests.test_composition_activation_contract import request as composition_request
from tests.test_dbt_compact_wire_v2 import _development_authority
from tests.test_dbt_workspace_cache_installation import (
    projected_workspace as projected_workspace,
)
from tests.test_dbt_workspace_cache_installation import workspace as workspace
from tests.test_development_remote_delivery_authority import (
    _admission,
    _admission_verifier,
    _clock,
    _development_projection,
)


def _digest(character: str) -> str:
    return "sha256:" + character * 64


class _Coordinator:
    def __init__(self, *, activate_failure=False, foreign_prepare=False):
        self.activate_failure = activate_failure
        self.foreign_prepare = foreign_prepare
        self.events = []
        self.requests = {}

    def _request(self, **coordinates):
        resource = DbtWorkspacePhysicalResource(
            guard_id="mssql://platform-service/warehouse/mart/orders",
            connector="mssql",
            service_authority_sha256=_digest("1"),
            target_authority_sha256=_digest("2"),
            observation_sha256=_digest("3"),
            write_subjects=(_digest("4"),),
        )
        return DbtWorkspaceActivationRequest.build(
            activation_id=coordinates["activation_id"],
            environment=coordinates["environment"],
            release_id=coordinates["release_id"],
            deployment_id=_digest("f") if self.foreign_prepare else coordinates["deployment_id"],
            previous_deployment_id=coordinates["previous_deployment_id"],
            source_inventory_sha256=_digest("5"),
            runtime_context_sha256=_digest("6"),
            write_subjects=resource.write_subjects,
            resources=(resource,),
        )

    @staticmethod
    def _receipt(request, state):
        return DbtWorkspaceActivationReceipt.build(
            activation_id=request.activation_id,
            request_sha256=request.request_sha256,
            state=state,
            guard_epochs=(DbtWorkspaceGuardEpoch(request.resources[0].guard_id, 1),),
        )

    def prepare(self, *, projection_root, **coordinates):
        assert projection_root.parts[-3:-1] == ("activations", coordinates["environment"])
        request = self._request(**coordinates)
        self.requests[request.activation_id] = request
        self.events.append(("prepare", request.activation_id))
        return DbtWorkspacePreparedActivation(request, self._receipt(request, "PREPARED"))

    def activate(self, prepared, *, projection_root):
        assert projection_root.is_dir()
        self.events.append(("activate", prepared.request.activation_id))
        if self.activate_failure:
            raise RuntimeError("secret adapter diagnostic")
        return DbtWorkspaceActiveActivation(prepared.request, self._receipt(prepared.request, "ACTIVE"))

    def require_active(self, *, projection_root, **coordinates):
        assert projection_root.is_dir()
        request = self.requests[coordinates["activation_id"]]
        self.events.append(("require_active", request.activation_id))
        return DbtWorkspaceActiveActivation(request, self._receipt(request, "ACTIVE"))


class _CompositionCoordinator:
    def __init__(self) -> None:
        self.events = []
        self.requests = {}

    @staticmethod
    def _occurrence(request, state):
        return CompositionActivationOccurrence(
            request,
            CompositionActivationReceipt(
                request.request_sha256,
                state,
                tuple((resource.guard_id, 1) for resource in request.resources),
            ),
        )

    def prepare(self, **coordinates):
        template = composition_request()
        context = replace(
            template.context,
            activation_id=coordinates["activation_id"],
            environment=coordinates["environment"],
            release_id=coordinates["release_id"],
            deployment_id=coordinates["deployment_id"],
            previous_deployment_id=coordinates["previous_deployment_id"],
        )
        request = replace(template, context=context)
        self.requests[request.activation_id] = request
        self.events.append(("prepare", request.activation_id))
        return self._occurrence(request, "PREPARED")

    def activate(self, prepared, *, projection_root):
        assert projection_root.is_dir()
        self.events.append(("activate", prepared.request.activation_id))
        return self._occurrence(prepared.request, "ACTIVE")

    def require_active(self, *, projection_root, **coordinates):
        assert projection_root.is_dir()
        request = self.requests[coordinates["activation_id"]]
        self.events.append(("require_active", request.activation_id))
        return self._occurrence(request, "ACTIVE")


def test_promote_prepares_before_pointer_and_requires_active_after_commit(projected_workspace):
    cache, projection = projected_workspace
    coordinator = _Coordinator()
    current = DeploymentCacheMaterializer(cache, workspace_activation=coordinator).promote(
        projection.deployment_dir,
        environment="prod",
    )

    assert coordinator.events == [("prepare", current.activation_id), ("activate", current.activation_id)]
    assert (cache / "current").is_symlink()
    assert (cache / "current-pointer.json").is_file()


def test_foreign_prepared_occurrence_cannot_mutate_pointer(projected_workspace):
    cache, projection = projected_workspace
    with pytest.raises(DeploymentCacheError) as caught:
        DeploymentCacheMaterializer(cache, workspace_activation=_Coordinator(foreign_prepare=True)).promote(
            projection.deployment_dir,
            environment="prod",
        )
    assert caught.value.code == "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE"
    assert not (cache / "current").exists()
    assert not (cache / "current-pointer.json").exists()


def test_unknown_active_commit_leaves_pointer_for_explicit_recovery(projected_workspace):
    cache, projection = projected_workspace
    with pytest.raises(DeploymentCacheError) as caught:
        DeploymentCacheMaterializer(
            cache,
            workspace_activation=_Coordinator(activate_failure=True),
        ).promote(projection.deployment_dir, environment="prod")

    assert caught.value.code == "DPONE_DBT_WORKSPACE_ACTIVATION_COMMIT_UNKNOWN"
    assert caught.value.details == {"state_may_have_changed": True, "recovery_required": True}
    assert (cache / "current").is_symlink()
    assert (cache / "current-pointer.json").is_file()
    assert "secret" not in str(caught.value)


def test_audit_repair_reads_existing_active_occurrence_without_reserving_again(projected_workspace):
    cache, projection = projected_workspace
    coordinator = _Coordinator()
    materializer = DeploymentCacheMaterializer(cache, workspace_activation=coordinator)
    current = materializer.promote(projection.deployment_dir, environment="prod")
    (cache / "current-pointer-audit.jsonl").unlink()
    coordinator.events.clear()

    repaired = materializer.repair_audit(
        environment="prod",
        recovery_actor="test://platform",
        expected_current_deployment_id=current.deployment_id,
    )

    assert repaired.activation_id == current.activation_id
    assert coordinator.events == [("require_active", current.activation_id)]
    assert (cache / "current-pointer-audit.jsonl").is_file()


def test_recovery_uses_a_fresh_occurrence_and_reservation(projected_workspace):
    from tests.airflow_cache_promotion_test_support import write_cache_fixture

    cache, projection = projected_workspace
    previous = write_cache_fixture(cache)
    DeploymentCacheMaterializer(cache).promote(previous.deployment, environment="dev")
    coordinator = _Coordinator()
    fresh_id = "53fdb2bf-2245-43bd-b8bd-04769012508a"
    recovered = DeploymentCacheMaterializer(
        cache,
        activation_id_factory=lambda: fresh_id,
        workspace_activation=coordinator,
    ).recover(
        projection.deployment_dir,
        environment="prod",
        promoted_by="test://platform",
        expected_current_deployment_id=previous.deployment_id,
    )

    assert recovered.activation_id == fresh_id
    assert recovered.previous_deployment_id == previous.deployment_id
    assert coordinator.events == [("prepare", fresh_id), ("activate", fresh_id)]


def test_legacy_release_never_calls_workspace_coordinator(tmp_path: Path):
    from tests.airflow_cache_promotion_test_support import write_cache_fixture

    cache = tmp_path / ".dpone-cache"
    fixture = write_cache_fixture(cache)
    coordinator = _Coordinator()
    DeploymentCacheMaterializer(cache, workspace_activation=coordinator).promote(
        fixture.deployment,
        environment="dev",
    )
    assert coordinator.events == []


@pytest.mark.parametrize("composed", [False, True])
def test_development_activation_requires_exact_nonproduction_admission(tmp_path: Path, composed: bool) -> None:
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=composed)
    deployment = cache / "deployments" / "development" / deployment_id.replace(":", "-", 1)
    coordinator = _CompositionCoordinator() if composed else _Coordinator()

    with pytest.raises(DeploymentCacheError) as denied:
        DeploymentCacheMaterializer(
            cache,
            workspace_activation=None if composed else coordinator,
            composition_activation_coordinator=coordinator if composed else None,
        ).promote(
            deployment,
            environment="development",
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_ACTIVATION_AUTHORITY_REQUIRED"
    assert coordinator.events == []
    assert not (cache / "current").exists()

    current = DeploymentCacheMaterializer(
        cache,
        workspace_activation=None if composed else coordinator,
        composition_activation_coordinator=coordinator if composed else None,
        development_admission=_admission(
            _development_authority(),
            "activate",
            release_id,
            deployment_id,
        ),
        development_admission_verifier=_admission_verifier(_development_authority()),
        clock=_clock,
    ).promote(deployment, environment="development")

    assert current.release_id == release_id
    assert coordinator.events == [("prepare", current.activation_id), ("activate", current.activation_id)]


@pytest.mark.parametrize("composed", [False, True])
def test_development_activation_rejects_release_changed_after_snapshot(tmp_path: Path, composed: bool) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=composed)
    deployment = cache / "deployments" / "development" / deployment_id.replace(":", "-", 1)
    release_path = cache / "releases" / release_id.replace(":", "-", 1) / "release-set.json"
    coordinator = _CompositionCoordinator() if composed else _Coordinator()

    def remove_development_authority() -> None:
        release_path.chmod(0o644)
        release = json.loads(release_path.read_text(encoding="utf-8"))
        if composed:
            native = next(item for item in release["constituents"] if item["id"] == "native")
            native["release"].pop("development_authority")
        else:
            release.pop("development_authority")
        release_path.write_text(json.dumps(release, sort_keys=True), encoding="utf-8")

    with pytest.raises(DeploymentCacheError) as denied:
        DeploymentCacheMaterializer(
            cache,
            workspace_activation=None if composed else coordinator,
            composition_activation_coordinator=coordinator if composed else None,
            development_admission=_admission(authority, "activate", release_id, deployment_id),
            development_admission_verifier=_admission_verifier(authority, revoke_after_successes=1),
            clock=_clock,
        ).promote(
            deployment,
            environment="development",
            precommit_check=remove_development_authority,
        )

    assert denied.value.code == "DPONE_RELEASE_FINGERPRINT_MISMATCH"
    assert denied.value.details == {"state_may_have_changed": True, "recovery_required": True}
    assert not (cache / "current").exists()
    assert not (cache / "current-pointer.json").exists()


@pytest.mark.parametrize("composed", [False, True])
def test_development_recovery_rechecks_target_operation_before_pointer_mutation(
    tmp_path: Path,
    composed: bool,
) -> None:
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=composed)
    deployment = cache / "deployments" / "development" / deployment_id.replace(":", "-", 1)
    wrong_operation = _admission(
        _development_authority(),
        "publish",
        release_id,
        deployment_id,
    )

    with pytest.raises(DeploymentCacheError) as denied:
        DeploymentCacheMaterializer(
            cache,
            workspace_activation=None if composed else _Coordinator(),
            composition_activation_coordinator=_CompositionCoordinator() if composed else None,
            development_admission=wrong_operation,
            development_admission_verifier=_admission_verifier(_development_authority()),
            clock=_clock,
        ).recover(
            deployment,
            environment="development",
            promoted_by="test://platform",
            expected_current_deployment_id=None,
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_ACTIVATION_AUTHORITY_REQUIRED"
    assert not (cache / "current-pointer.json").exists()


def test_development_recovery_reports_prepared_state_when_final_admission_is_revoked(tmp_path: Path) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=False)
    deployment = cache / "deployments" / "development" / deployment_id.replace(":", "-", 1)
    coordinator = _Coordinator()

    with pytest.raises(DeploymentCacheError) as denied:
        DeploymentCacheMaterializer(
            cache,
            workspace_activation=coordinator,
            development_admission=_admission(authority, "activate", release_id, deployment_id),
            development_admission_verifier=_admission_verifier(authority, revoke_after_successes=1),
            clock=_clock,
        ).recover(
            deployment,
            environment="development",
            promoted_by="test://platform",
            expected_current_deployment_id=None,
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_ACTIVATION_AUTHORITY_REQUIRED"
    assert denied.value.details == {"state_may_have_changed": True, "recovery_required": True}
    assert len(coordinator.events) == 1
    assert coordinator.events[0][0] == "prepare"
    assert (cache / "activations" / "development" / deployment_id.replace(":", "-", 1)).is_dir()
    assert not (cache / "current").exists()


@pytest.mark.parametrize("composed", [False, True])
def test_development_audit_repair_rechecks_admission_before_readback(tmp_path: Path, composed: bool) -> None:
    authority = _development_authority()
    cache, release_id, deployment_id = _development_projection(tmp_path, composed=composed)
    deployment = cache / "deployments" / "development" / deployment_id.replace(":", "-", 1)
    coordinator = _CompositionCoordinator() if composed else _Coordinator()
    admission = _admission(authority, "activate", release_id, deployment_id)
    verifier = _admission_verifier(authority)
    materializer = DeploymentCacheMaterializer(
        cache,
        workspace_activation=None if composed else coordinator,
        composition_activation_coordinator=coordinator if composed else None,
        development_admission=admission,
        development_admission_verifier=verifier,
        clock=_clock,
    )
    current = materializer.promote(deployment, environment="development")
    (cache / "current-pointer-audit.jsonl").unlink()
    coordinator.events.clear()

    denied_materializer = DeploymentCacheMaterializer(
        cache,
        workspace_activation=None if composed else coordinator,
        composition_activation_coordinator=coordinator if composed else None,
        development_admission=admission,
        development_admission_verifier=_admission_verifier(
            authority,
            current_revocation_epoch=authority.revocation_epoch + 1,
        ),
        clock=_clock,
    )
    with pytest.raises(DeploymentCacheError) as denied:
        denied_materializer.repair_audit(
            environment="development",
            recovery_actor="test://platform",
            expected_current_deployment_id=current.deployment_id,
        )

    assert denied.value.code == "DPONE_DEVELOPMENT_ACTIVATION_AUTHORITY_REQUIRED"
    assert coordinator.events == []
    assert not (cache / "current-pointer-audit.jsonl").exists()

    repaired = materializer.repair_audit(
        environment="development",
        recovery_actor="test://platform",
        expected_current_deployment_id=current.deployment_id,
    )
    assert repaired.deployment_id == deployment_id
    assert coordinator.events == [("require_active", current.activation_id)]
    assert (cache / "current-pointer-audit.jsonl").is_file()
