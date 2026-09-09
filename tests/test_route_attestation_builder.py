from __future__ import annotations

import json

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.services.route_attestation_builder import RouteAttestationBuilder, RouteAttestationBuildError

_RELEASE_ID = "sha256:" + "a" * 64
_DEPLOYMENT_ID = "sha256:" + "b" * 64
_IMAGE_DIGEST = "sha256:" + "c" * 64


def _certification_bundle(*, passed: bool = True, level: str = "certified") -> dict[str, object]:
    return {
        "schema_version": "dpone.route_certification_bundle.v1",
        "evidence_status": "PASS",
        "release": "0.72.6",
        "profile": "vendor_live",
        "route": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "incremental_merge",
            "pair_id": "mssql_to_clickhouse",
            "case_id": "mssql_to_clickhouse__incremental_merge",
            "colon_id": "mssql:clickhouse:incremental_merge",
        },
        "passed": passed,
        "level": level,
        "score": 100.0,
        "blockers": [],
        "artifact_index": {},
    }


def _deployment() -> dict[str, object]:
    return {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": _DEPLOYMENT_ID,
        "deployment_type": "environment",
        "runnable": True,
        "environment": "prod",
        "release_ref": _RELEASE_ID,
        "runtime_image_digest": _IMAGE_DIGEST,
        "runtime_artifact_delivery": {"mode": "init_fetch"},
    }


def _raw(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def test_builder_binds_certification_and_deployment_into_deterministic_claims() -> None:
    certification = _certification_bundle()
    artifact = RouteAttestationBuilder().build(
        route_id="mssql_clickhouse_incremental_merge_airflow_kpo",
        certification_bundle=certification,
        certification_bundle_bytes=_raw(certification),
        deployment=_deployment(),
        authorization_profile="safe_sample_production",
        issued_at="2026-07-15T10:00:00Z",
        not_before="2026-07-15T10:00:00Z",
        expires_at="2026-07-16T10:00:00Z",
    )

    payload = artifact.to_dict()
    assert payload["schema"] == "dpone.route-attestation.v1"
    assert payload["attestation_id"] == canonical_fingerprint(payload["claims"])
    assert payload["claims"]["route"] == {
        "route_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "incremental_merge",
        "transport": "native_bcp_to_clickhouse",
        "schema_evolution": "widening",
        "airflow_runtime_mode": "kpo",
        "sampling_mode": "pushdown",
    }
    assert payload["claims"]["subject"] == {
        "release_id": _RELEASE_ID,
        "deployment_id": _DEPLOYMENT_ID,
        "environment": "production",
        "runtime_image_digest": _IMAGE_DIGEST,
        "authorization_profile": "safe_sample_production",
    }
    assert (
        artifact.to_bytes()
        == RouteAttestationBuilder()
        .build(
            route_id="mssql_clickhouse_incremental_merge_airflow_kpo",
            certification_bundle=certification,
            certification_bundle_bytes=_raw(certification),
            deployment=_deployment(),
            authorization_profile="safe_sample_production",
            issued_at="2026-07-15T10:00:00Z",
            not_before="2026-07-15T10:00:00Z",
            expires_at="2026-07-16T10:00:00Z",
        )
        .to_bytes()
    )


@pytest.mark.parametrize(
    ("certification", "deployment", "code"),
    [
        (_certification_bundle(passed=False), _deployment(), "DPONE_ROUTE_CERTIFICATION_NOT_CERTIFIED"),
        (_certification_bundle(level="route_ready"), _deployment(), "DPONE_ROUTE_CERTIFICATION_NOT_CERTIFIED"),
        (
            {key: value for key, value in _certification_bundle().items() if key != "evidence_status"},
            _deployment(),
            "DPONE_ROUTE_CERTIFICATION_NOT_CERTIFIED",
        ),
        (
            {**_certification_bundle(), "evidence_status": " pass "},
            _deployment(),
            "DPONE_ROUTE_CERTIFICATION_NOT_CERTIFIED",
        ),
        (
            {**_certification_bundle(), "blockers": ["forged"]},
            _deployment(),
            "DPONE_ROUTE_CERTIFICATION_NOT_CERTIFIED",
        ),
        (
            {
                **_certification_bundle(),
                "stages": [{"name": "live", "required": True, "passed": False, "blockers": []}],
            },
            _deployment(),
            "DPONE_ROUTE_CERTIFICATION_NOT_CERTIFIED",
        ),
        (
            {
                **_certification_bundle(),
                "route": {"source": "postgres", "sink": "clickhouse", "strategy": "incremental_merge"},
            },
            _deployment(),
            "DPONE_ROUTE_CERTIFICATION_ROUTE_MISMATCH",
        ),
        (
            _certification_bundle(),
            {**_deployment(), "runnable": False},
            "DPONE_ROUTE_ATTESTATION_DEPLOYMENT_NOT_RUNNABLE",
        ),
    ],
)
def test_builder_rejects_uncertified_or_mismatched_inputs(
    certification: dict[str, object], deployment: dict[str, object], code: str
) -> None:
    with pytest.raises(RouteAttestationBuildError) as exc:
        RouteAttestationBuilder().build(
            route_id="mssql_clickhouse_incremental_merge_airflow_kpo",
            certification_bundle=certification,
            certification_bundle_bytes=_raw(certification),
            deployment=deployment,
            authorization_profile="safe_sample_production",
            issued_at="2026-07-15T10:00:00Z",
            not_before="2026-07-15T10:00:00Z",
            expires_at="2026-07-16T10:00:00Z",
        )

    assert exc.value.code == code


def test_builder_rejects_invalid_validity_order() -> None:
    certification = _certification_bundle()
    with pytest.raises(RouteAttestationBuildError) as exc:
        RouteAttestationBuilder().build(
            route_id="mssql_clickhouse_incremental_merge_airflow_kpo",
            certification_bundle=certification,
            certification_bundle_bytes=_raw(certification),
            deployment=_deployment(),
            authorization_profile="safe_sample_production",
            issued_at="2026-07-16T10:00:00Z",
            not_before="2026-07-16T10:00:00Z",
            expires_at="2026-07-15T10:00:00Z",
        )

    assert exc.value.code == "DPONE_ROUTE_ATTESTATION_VALIDITY_INVALID"
