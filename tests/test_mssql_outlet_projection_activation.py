"""P0/P1 activation integrity for MSSQL outlet projection mirrors."""

from __future__ import annotations

import copy
from typing import Any

import pytest
from dpone_airflow_pack.asset_outlets import OutletUriSpec, build_asset_outlets
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.init_fetch_validation import exact_mapping
from dpone_airflow_pack.mssql_asset_ref_codec import require_asset_ref_sha256
from dpone_airflow_pack.mssql_outlet_projection_contract import (
    PROJECTION_INVALID,
    PROJECTION_MISMATCH,
    compute_projection_sha256,
    mirror_projections_or_raise,
)

from dpone.contracts.airflow_deployment import deployment_id
from dpone.contracts.airflow_deployment_projection import deployment_projection_violation
from dpone.gitops.schema_contracts import get_gitops_schema_contract

_ASSET_REF = {
    "engine": "mssql",
    "connection_ref": "mssql_marts",
    "database": "DWH",
    "schema": "dbo",
    "table": "orders",
}


def _projection(*, environment: str, host: str, instance: str | None = None) -> dict[str, Any]:
    path = f"{instance}/DWH/dbo/orders" if instance else "DWH/dbo/orders"
    uri = f"mssql://{host}:1433/{path}"
    entry = {
        "workload_ids": ["demo"],
        "asset_ref": dict(_ASSET_REF),
        "asset_ref_sha256": require_asset_ref_sha256(_ASSET_REF),
        "registry_connection_ref": "mssql_writer",
        "resolved_binding": {"registry_connection_ref": "mssql_writer"},
        "uri": uri,
    }
    body = {
        "schema": "dpone.mssql-asset-outlet-projection.v1",
        "environment": environment,
        "binding_set_ref": "sha256:" + "b" * 64,
        "connection_registry_ref": "sha256:" + "c" * 64,
        "entries": [entry],
    }
    body["projection_sha256"] = compute_projection_sha256(body)
    return body


def _deployment_pair(projection: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    refs = {
        "binding_set_ref": projection["binding_set_ref"],
        "connection_registry_ref": projection["connection_registry_ref"],
        "credential_runtime_ref": "sha256:" + "d" * 64,
        "runtime_image_digest": "sha256:" + "e" * 64,
        "airflow_bundle_ref": "git:" + "a" * 40,
        "runtime_artifact_delivery": {
            "mode": "init_fetch",
            "artifact_registry_ref": "dpone-dev-artifacts",
            "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone"},
            "source": {"artifact_registry_ref": "dpone-dev-artifacts"},
            "verify": {"checksums": "required", "attestations": "optional"},
            "trust_tier": "non_production",
        },
    }
    deployment = {
        "schema": "dpone.deployment-set.v3",
        "deployment_id": "",
        "deployment_type": "environment",
        "runnable": True,
        "environment": projection["environment"],
        "trust_tier": "non_production",
        "release_ref": "sha256:" + "2" * 64,
        **refs,
        "mssql_asset_outlet_projection": projection,
    }
    deployment["deployment_id"] = deployment_id(deployment)
    index = {
        "schema": "dpone.airflow-deployment-index.v3",
        "release_id": deployment["release_ref"],
        "deployment_id": deployment["deployment_id"],
        **refs,
        "mssql_asset_outlet_projection": copy.deepcopy(projection),
    }
    return deployment, index


def test_independently_valid_projection_a_and_b_mismatch() -> None:
    projection_a = _projection(environment="prod", host="sql-a.internal", instance="prod01")
    projection_b = _projection(environment="prod", host="sql-b.internal", instance="prod01")
    assert projection_a["projection_sha256"] != projection_b["projection_sha256"]
    with pytest.raises(InitFetchProviderError) as exc:
        mirror_projections_or_raise(
            deployment_projection=projection_a,
            index_projection=projection_b,
            expected_environment="prod",
            expected_binding_set_ref=projection_a["binding_set_ref"],
            expected_connection_registry_ref=projection_a["connection_registry_ref"],
        )
    assert exc.value.code == PROJECTION_MISMATCH


def test_deployment_projection_violation_surfaces_mssql_mismatch_code() -> None:
    projection_a = _projection(environment="prod", host="sql-a.internal")
    projection_b = _projection(environment="prod", host="sql-b.internal")
    deployment, index = _deployment_pair(projection_a)
    index["mssql_asset_outlet_projection"] = projection_b
    violation = deployment_projection_violation(deployment, index)
    assert violation is not None
    assert violation.code == PROJECTION_MISMATCH


def test_v3_wire_requires_projection_v2_stays_exact() -> None:
    v2 = get_gitops_schema_contract("dpone.airflow-deployment-index.v2")
    v3 = get_gitops_schema_contract("dpone.airflow-deployment-index.v3")
    assert "mssql_asset_outlet_projection" not in v2.schema["properties"]
    assert "mssql_asset_outlet_projection" in v3.schema["required"]


def test_old_v2_exact_mapping_rejects_mssql_projection_field() -> None:
    """Closed v2 readers must not accept additive mssql projection (forces v3)."""

    v2_keys = frozenset(
        {
            "schema",
            "release_id",
            "deployment_id",
            "trust_tier",
            "dag_specs",
            "workload_packs",
            "binding_set_ref",
            "connection_registry_ref",
            "credential_runtime_ref",
            "binding_set",
            "connection_registry",
            "credential_runtime",
            "runtime_image_ref",
            "runtime_image_digest",
            "airflow_bundle_ref",
            "runtime_artifact_delivery",
            "release",
            "deployment",
        }
    )
    payload = {key: "x" for key in v2_keys}
    payload["schema"] = "dpone.airflow-deployment-index.v2"
    payload["mssql_asset_outlet_projection"] = _projection(environment="prod", host="sql-prod.internal")
    with pytest.raises(InitFetchProviderError) as exc:
        exact_mapping(payload, "airflow deployment index v2", v2_keys, optional=frozenset(), path=None)
    assert "unknown fields" in str(exc.value).lower()
    assert "mssql_asset_outlet_projection" in str(exc.value)


def test_projected_outlet_value_error_is_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Boom:
        def __init__(self, uri: str) -> None:
            raise ValueError("provider rejected uri")

    monkeypatch.setattr(
        "dpone_airflow_pack.asset_outlets._asset_class",
        lambda: _Boom,
    )
    with pytest.raises(InitFetchProviderError) as projected:
        build_asset_outlets(
            [OutletUriSpec(uri="mssql://sql-prod.internal:1433/DWH/dbo/orders", provenance="deployment_projection")]
        )
    assert projected.value.code == PROJECTION_INVALID

    assert (
        build_asset_outlets(
            [OutletUriSpec(uri="mssql://sql-prod.internal:1433/DWH/dbo/orders", provenance="legacy_authoring")]
        )
        == []
    )
