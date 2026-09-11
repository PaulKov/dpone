from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from dpone_airflow_pack.deployment_index_contract import (
    _composition_supervisor_from_payload,
)
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError

from dpone.contracts.composition_supervisor import (
    CompositionSupervisorProjection,
    supervisor_projection_for_release,
)
from dpone.readiness.airflow_deployment_projection import (
    _seal_composition_supervisor_projection,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_projection_validator import (
    _validate_supervisor_projection_mirror,
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


def test_v3_projection_seals_supervisor_into_both_authenticated_documents() -> None:
    deployment = {
        "schema": "dpone.deployment-set.v3",
        "deployment_id": "sha256:" + "0" * 64,
        "environment": "prod",
    }
    airflow_index = {
        "schema": "dpone.airflow-deployment-index.v3",
        "deployment_id": deployment["deployment_id"],
        "deployment": {
            "artifact_ref": "cache://deployments/prod/sha256-old/deployment.json",
            "sha256": "sha256:" + "0" * 64,
            "bytes": 1,
        },
    }
    projection = CompositionSupervisorProjection.from_mapping(valid_supervisor_payload())

    deployment_bytes = _seal_composition_supervisor_projection(
        deployment=deployment,
        airflow_index=airflow_index,
        projection=projection,
    )

    assert deployment["composition_supervisor"] == valid_supervisor_payload()
    assert airflow_index["composition_supervisor"] == valid_supervisor_payload()
    assert airflow_index["deployment_id"] == deployment["deployment_id"]
    assert airflow_index["deployment"]["artifact_ref"] == (
        f"cache://deployments/prod/{str(deployment['deployment_id']).replace(':', '-')}/deployment.json"
    )
    assert airflow_index["deployment"]["bytes"] == len(deployment_bytes)


@pytest.mark.parametrize(
    ("deployment_projection", "index_projection"),
    [
        (valid_supervisor_payload(), None),
        (None, valid_supervisor_payload()),
        (
            valid_supervisor_payload(),
            {**valid_supervisor_payload(), "child_gid_start": 1_100_000_000},
        ),
    ],
)
def test_cache_projection_rejects_missing_or_mismatched_supervisor_mirror(
    deployment_projection: object,
    index_projection: object,
) -> None:
    deployment = {
        "schema": "dpone.deployment-set.v3",
        "composition_supervisor": deployment_projection,
    }
    index = {
        "schema": "dpone.airflow-deployment-index.v3",
        "composition_supervisor": index_projection,
    }

    with pytest.raises(DeploymentCacheError) as exc_info:
        _validate_supervisor_projection_mirror(deployment, index)

    assert exc_info.value.code == "DPONE_COMPOSITION_SUPERVISOR_MISMATCH"


def test_cache_projection_rejects_supervisor_on_v2_wire() -> None:
    projection = valid_supervisor_payload()

    with pytest.raises(DeploymentCacheError) as exc_info:
        _validate_supervisor_projection_mirror(
            {
                "schema": "dpone.deployment-set.v2",
                "composition_supervisor": projection,
            },
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "composition_supervisor": projection,
            },
        )

    assert exc_info.value.code == "DPONE_COMPOSITION_SUPERVISOR_FORBIDDEN"


def test_provider_index_preserves_valid_v3_supervisor_projection(tmp_path: Path) -> None:
    projection = _composition_supervisor_from_payload(
        {
            "schema": "dpone.airflow-deployment-index.v3",
            "composition_supervisor": valid_supervisor_payload(),
        },
        path=tmp_path / "airflow-index.json",
    )

    assert projection == valid_supervisor_payload()


@pytest.mark.parametrize(
    "schema",
    ["dpone.airflow-deployment-index.v1", "dpone.airflow-deployment-index.v2"],
)
def test_provider_index_rejects_supervisor_on_v1_v2(schema: str, tmp_path: Path) -> None:
    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        _composition_supervisor_from_payload(
            {
                "schema": schema,
                "composition_supervisor": valid_supervisor_payload(),
            },
            path=tmp_path / "airflow-index.json",
        )

    assert exc_info.value.code == "DPONE_COMPOSITION_SUPERVISOR_FORBIDDEN"


def test_provider_index_rejects_invalid_v3_supervisor_projection(tmp_path: Path) -> None:
    invalid = valid_supervisor_payload()
    invalid.pop("child_gid_start")

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        _composition_supervisor_from_payload(
            {
                "schema": "dpone.airflow-deployment-index.v3",
                "composition_supervisor": invalid,
            },
            path=tmp_path / "airflow-index.json",
        )

    assert exc_info.value.code == "DPONE_COMPOSITION_SUPERVISOR_INVALID"


def test_provider_index_rejects_noncanonical_pvc_whitespace(tmp_path: Path) -> None:
    invalid = {
        **valid_supervisor_payload(),
        "persistent_volume_claim": " dpone-composition-supervisor",
    }

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        _composition_supervisor_from_payload(
            {
                "schema": "dpone.airflow-deployment-index.v3",
                "composition_supervisor": invalid,
            },
            path=tmp_path / "airflow-index.json",
        )

    assert exc_info.value.code == "DPONE_COMPOSITION_SUPERVISOR_INVALID"
