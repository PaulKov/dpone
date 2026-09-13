from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest
from dpone_airflow_pack.mssql_asset_ref_codec import require_asset_ref_sha256
from dpone_airflow_pack.mssql_outlet_projection_contract import compute_projection_sha256

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


def _valid_mssql_projection() -> dict[str, Any]:
    asset_ref = {
        "engine": "mssql",
        "connection_ref": "writer",
        "database": "DWH",
        "schema": "dbo",
        "table": "orders",
    }
    projection: dict[str, Any] = {
        "schema": "dpone.mssql-asset-outlet-projection.v1",
        "environment": "dev",
        "binding_set_ref": "sha256:" + "b" * 64,
        "connection_registry_ref": "sha256:" + "c" * 64,
        "entries": [
            {
                "workload_ids": ["orders"],
                "asset_ref": asset_ref,
                "asset_ref_sha256": require_asset_ref_sha256(asset_ref),
                "registry_connection_ref": "writer",
                "resolved_binding": {"registry_connection_ref": "writer"},
                "uri": "mssql://sql.internal:1433/DWH/dbo/orders",
            }
        ],
    }
    projection["projection_sha256"] = compute_projection_sha256(projection)
    return projection


def _projection_pair(
    *,
    deployment_supervisor: object = None,
    index_supervisor: object = None,
    with_mssql: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    release_id = "sha256:" + "a" * 64
    mirrored = {
        "binding_set_ref": "sha256:" + "b" * 64,
        "connection_registry_ref": "sha256:" + "c" * 64,
        "credential_runtime_ref": "sha256:" + "d" * 64,
        "runtime_image_digest": "sha256:" + "e" * 64,
        "airflow_bundle_ref": "git:" + "f" * 40,
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }
    deployment: dict[str, Any] = {
        "schema": "dpone.deployment-set.v3",
        "deployment_id": "",
        "deployment_type": "preview",
        "runnable": False,
        "environment": "dev",
        "release_ref": release_id,
        **mirrored,
    }
    index: dict[str, Any] = {
        "schema": "dpone.airflow-deployment-index.v3",
        "release_id": release_id,
        **mirrored,
    }
    if deployment_supervisor is not None:
        deployment["composition_supervisor"] = deployment_supervisor
    if index_supervisor is not None:
        index["composition_supervisor"] = index_supervisor
    if with_mssql:
        projection = _valid_mssql_projection()
        deployment["mssql_asset_outlet_projection"] = projection
        index["mssql_asset_outlet_projection"] = projection
    deployment["deployment_id"] = deployment_id(deployment)
    index["deployment_id"] = deployment["deployment_id"]
    return deployment, index


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
    deployment, index = _projection_pair(with_mssql=True)

    violation = deployment_projection_violation(
        deployment,
        index,
        release_schema="dpone.release-set.v3",
    )

    assert violation is not None
    assert violation.code == "DPONE_COMPOSITION_SUPERVISOR_REQUIRED"


def test_shared_mirror_forbids_supervisor_on_non_composition_release() -> None:
    supervisor = valid_supervisor_payload()
    deployment, index = _projection_pair(
        deployment_supervisor=supervisor,
        index_supervisor=supervisor,
        with_mssql=True,
    )

    violation = deployment_projection_violation(
        deployment,
        index,
        release_schema="dpone.release-set.v2",
    )

    assert violation is not None
    assert violation.code == "DPONE_COMPOSITION_SUPERVISOR_FORBIDDEN"


def test_shared_mirror_rejects_malformed_supervisor() -> None:
    malformed = valid_supervisor_payload()
    malformed.pop("child_gid_start")
    deployment, index = _projection_pair(
        deployment_supervisor=malformed,
        index_supervisor=malformed,
    )

    violation = deployment_projection_violation(
        deployment,
        index,
        release_schema="dpone.release-set.v3",
    )

    assert violation is not None
    assert violation.code == "DPONE_COMPOSITION_SUPERVISOR_INVALID"


def test_shared_mirror_rejects_one_sided_supervisor() -> None:
    deployment, index = _projection_pair(
        deployment_supervisor=valid_supervisor_payload(),
    )

    violation = deployment_projection_violation(
        deployment,
        index,
        release_schema="dpone.release-set.v3",
    )

    assert violation is not None
    assert violation.code == "DPONE_COMPOSITION_SUPERVISOR_MISMATCH"
