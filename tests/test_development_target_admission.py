"""Target admission is exact, current, operation-bound, and non-production."""

import inspect
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from dpone.contracts.development_delivery_authority import DevelopmentAuthorityError
from dpone.contracts.development_target_admission import DevelopmentTargetAdmission
from dpone.runtime import (
    airflow_artifact_materialization,
    airflow_artifact_publication,
    deployment_cache_materializer,
)
from dpone.runtime.airflow_artifact_materialization import AirflowArtifactMaterializer
from dpone.runtime.airflow_artifact_publication import AirflowArtifactPublisher
from dpone.runtime.deployment_cache_materializer import DeploymentCacheMaterializer
from tests.test_development_delivery_authority import authority

RELEASE_ID = "sha256:" + "1" * 64
DEPLOYMENT_ID = "sha256:" + "2" * 64
POLICY_ID = "sha256:" + "3" * 64
CHECKED_AT = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)


def admission(operation="publish") -> DevelopmentTargetAdmission:
    return DevelopmentTargetAdmission(
        authority=authority(),
        operation=operation,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        target_environment="development",
        target_trust_tier="non_production",
        target_policy_sha256=POLICY_ID,
        checked_at=CHECKED_AT,
        current_revocation_epoch=7,
    )


def test_exact_target_operation_is_admitted_and_has_stable_identity() -> None:
    receipt = admission()

    receipt.require(
        authority_projection=receipt.authority.release_projection(),
        operation="publish",
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        target_environment="development",
        target_trust_tier="non_production",
        now=CHECKED_AT,
    )

    assert receipt.admission_id == admission().admission_id


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"operation": "materialize"}, "target_identity_mismatch"),
        ({"release_id": "sha256:" + "4" * 64}, "target_identity_mismatch"),
        ({"deployment_id": "sha256:" + "5" * 64}, "target_identity_mismatch"),
        ({"target_environment": "qa"}, "target_identity_mismatch"),
        ({"target_trust_tier": "production"}, "production_target_forbidden"),
        ({"authority_projection": {}}, "artifact_scope_mismatch"),
    ],
)
def test_target_scope_mismatch_fails_closed(changes, reason) -> None:
    receipt = admission()
    arguments = {
        "authority_projection": receipt.authority.release_projection(),
        "operation": "publish",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "target_environment": "development",
        "target_trust_tier": "non_production",
        "now": CHECKED_AT,
    }
    arguments.update(changes)

    with pytest.raises(DevelopmentAuthorityError, match=reason):
        receipt.require(**arguments)


def test_expired_or_revoked_operation_receipt_cannot_be_constructed() -> None:
    with pytest.raises(DevelopmentAuthorityError, match="revoked"):
        replace(admission(), current_revocation_epoch=8)
    with pytest.raises(DevelopmentAuthorityError, match="expired"):
        replace(admission(), checked_at=datetime(2026, 9, 18, 11, 0, tzinfo=UTC))


def test_operation_rechecks_current_time_at_use() -> None:
    receipt = admission()

    with pytest.raises(DevelopmentAuthorityError, match="expired"):
        receipt.require(
            authority_projection=receipt.authority.release_projection(),
            operation="publish",
            release_id=RELEASE_ID,
            deployment_id=DEPLOYMENT_ID,
            target_environment="development",
            target_trust_tier="non_production",
            now=datetime(2026, 9, 18, 11, 0, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("target_policy_sha256", "current_revocation_epoch", "reason"),
    [
        ("sha256:" + "9" * 64, 7, "target_policy_changed"),
        (POLICY_ID, 8, "revoked"),
    ],
)
def test_current_target_state_is_independently_reopened(
    target_policy_sha256: str,
    current_revocation_epoch: int,
    reason: str,
) -> None:
    with pytest.raises(DevelopmentAuthorityError, match=reason):
        admission().require_current_target(
            target_policy_sha256=target_policy_sha256,
            current_revocation_epoch=current_revocation_epoch,
            now=CHECKED_AT,
        )


def test_authority_environment_cannot_be_relabelled_by_target() -> None:
    with pytest.raises(DevelopmentAuthorityError, match="target_environment_mismatch"):
        replace(admission(), target_environment="qa")


def test_runtime_entrypoints_accept_the_canonical_admission_type() -> None:
    for module, constructor in (
        (airflow_artifact_publication, AirflowArtifactPublisher.__init__),
        (airflow_artifact_materialization, AirflowArtifactMaterializer.__init__),
        (deployment_cache_materializer, DeploymentCacheMaterializer.__init__),
    ):
        assert module.DevelopmentTargetAdmission is DevelopmentTargetAdmission
        annotations = inspect.get_annotations(constructor, eval_str=False)
        assert annotations["development_admission"] == "DevelopmentTargetAdmission | None"
        if constructor is not DeploymentCacheMaterializer.__init__:
            assert annotations["development_authority"] == "DevelopmentDeliveryAuthority | None"
