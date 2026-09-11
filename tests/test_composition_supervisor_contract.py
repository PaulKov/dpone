from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from dpone.contracts.airflow_deployment import deployment_id
from dpone.contracts.airflow_deployment_projection import deployment_projection_violation
from dpone.contracts.composition_supervisor import (
    CompositionSupervisorProjection,
    supervisor_projection_for_release,
)


def valid_supervisor_payload() -> dict[str, object]:
    return {
        "schema": "dpone.composition-supervisor.v1",
        "persistent_volume_claim": "dpone-composition-supervisor",
        "child_uid_start": 1_000_000_000,
        "child_gid_start": 1_000_000_000,
        "child_identity_count": 1_000_000,
    }


def test_v3_requires_complete_supervisor_projection() -> None:
    value = CompositionSupervisorProjection.from_mapping(valid_supervisor_payload())

    assert value.child_uid_stop == 1_001_000_000
    assert value.child_gid_stop == 1_001_000_000
    assert value.to_dict() == valid_supervisor_payload()


@pytest.mark.parametrize(
    "field",
    [
        "schema",
        "persistent_volume_claim",
        "child_uid_start",
        "child_gid_start",
        "child_identity_count",
    ],
)
def test_v3_rejects_missing_supervisor_field(field: str) -> None:
    payload = valid_supervisor_payload()
    payload.pop(field)

    with pytest.raises(ValueError, match="composition_supervisor"):
        CompositionSupervisorProjection.from_mapping(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", "dpone.composition-supervisor.v2"),
        ("persistent_volume_claim", "Invalid_PVC"),
        ("child_uid_start", True),
        ("child_uid_start", 999_999),
        ("child_gid_start", True),
        ("child_gid_start", 999_999),
        ("child_identity_count", True),
        ("child_identity_count", 999_999),
        ("child_uid_start", 2**31 - 1_000_000),
        ("child_gid_start", 2**31 - 1_000_000),
    ],
)
def test_v3_rejects_invalid_supervisor_field(field: str, value: object) -> None:
    payload = valid_supervisor_payload()
    payload[field] = value

    with pytest.raises(ValueError):
        CompositionSupervisorProjection.from_mapping(payload)


def test_v3_supervisor_projection_is_immutable() -> None:
    value = CompositionSupervisorProjection.from_mapping(valid_supervisor_payload())

    with pytest.raises(FrozenInstanceError):
        setattr(value, "child_uid_start", 1_000_000)


def test_v3_schema_requires_supervisor_projection() -> None:
    with pytest.raises(ValueError, match="composition_supervisor_required"):
        supervisor_projection_for_release(release_schema="dpone.release-set.v3", value=None)


@pytest.mark.parametrize(
    "release_schema",
    ["dpone.release-set.v1", "dpone.release-set.v2"],
)
def test_v1_v2_schema_forbids_supervisor_projection(release_schema: str) -> None:
    with pytest.raises(ValueError, match="composition_supervisor_forbidden"):
        supervisor_projection_for_release(
            release_schema=release_schema,
            value=valid_supervisor_payload(),
        )


@pytest.mark.parametrize(
    "release_schema",
    ["dpone.release-set.v1", "dpone.release-set.v2"],
)
def test_v1_v2_schema_preserves_absent_supervisor_projection(release_schema: str) -> None:
    assert supervisor_projection_for_release(release_schema=release_schema, value=None) is None


def test_release_v3_projection_requires_supervisor_across_shared_mirror_contract() -> None:
    release_id = "sha256:" + "a" * 64
    deployment = {
        "schema": "dpone.deployment-set.v3",
        "deployment_id": "",
        "deployment_type": "preview",
        "runnable": False,
        "environment": "dev",
        "release_ref": release_id,
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }
    deployment["deployment_id"] = deployment_id(deployment)
    index = {
        "schema": "dpone.airflow-deployment-index.v3",
        "release_id": release_id,
        "deployment_id": deployment["deployment_id"],
        "runtime_artifact_delivery": deployment["runtime_artifact_delivery"],
    }

    violation = deployment_projection_violation(
        deployment,
        index,
        release_schema="dpone.release-set.v3",
    )

    assert violation is not None
    assert violation.code == "DPONE_COMPOSITION_SUPERVISOR_REQUIRED"
