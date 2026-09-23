from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from dpone.contracts.airflow_credential_promotion import CREDENTIAL_PROMOTION_SCHEMA, CredentialPromotionEvidence
from dpone.readiness.airflow_desired_state_authority import AirflowDesiredStateAuthority
from dpone.readiness.airflow_desired_state_store import PromotionInput, load_promotion_input
from dpone.services.airflow_desired_state_preparation import AirflowDesiredStatePreparationService


def _authority(workspace: bool = True) -> AirflowDesiredStateAuthority:
    return AirflowDesiredStateAuthority(
        environment="dev",
        desired_state_uri="s3://example/control/dev/desired.json",
        certified_s3_endpoint_url="https://storage.example.com",
        artifact_registry_uri="s3://example/artifacts/dev/repository",
        artifact_registry_ref="artifacts-dev",
        watcher_identity="pack-watcher",
        source_project="example/repository",
        source_ref="master",
        workspace_authority_connection_ref="workspace_control" if workspace else None,
    )


@pytest.mark.parametrize("schema", [[], {}, None, True, 6])
def test_malformed_promotion_schema_is_a_closed_error(tmp_path, schema):
    path = tmp_path / "promotion.json"
    path.write_text(json.dumps({"schema_version": schema}))
    with pytest.raises(ValueError, match="schema is unsupported"):
        load_promotion_input(path)


def _promotion(authority: AirflowDesiredStateAuthority) -> PromotionInput:
    return PromotionInput(
        environment=authority.environment,
        registry_scope_id=authority.registry_scope_id,
        release_id="sha256:" + "a" * 64,
        deployment_id="sha256:" + "b" * 64,
        source_git_sha="c" * 40,
        runtime_image_digest="sha256:" + "d" * 64,
        airflow_index_sha256="sha256:" + "e" * 64,
        expected_dag_ids=("DAG__example__orders__sync",),
        evidence_sha256="sha256:" + "f" * 64,
    )


def _candidate(authority: AirflowDesiredStateAuthority, promotion: PromotionInput, *, legacy: bool = False):
    service = AirflowDesiredStatePreparationService(
        clock=lambda: datetime(2026, 9, 23, tzinfo=UTC),
        occurrence_id_factory=lambda: "123e4567-e89b-42d3-a456-426614174000",
    )
    if legacy:
        return service.legacy_candidate(
            authority=authority, promotion=promotion, pipeline_id="10", job_id="11", expected_revision=None
        )
    return service.candidate(
        authority=authority, promotion=promotion, pipeline_id="10", preparation_job_id="11", expected_revision=None
    )


@pytest.mark.parametrize("legacy", [False, True])
def test_workspace_publication_requires_projection_evidence(legacy: bool) -> None:
    authority = _authority()
    with pytest.raises(ValueError, match="projection"):
        _candidate(authority, _promotion(authority), legacy=legacy)


def test_non_workspace_publication_preserves_legacy_behavior() -> None:
    authority = _authority(False)
    assert _candidate(authority, _promotion(authority)).environment == "dev"


def test_workspace_authority_change_is_not_just_environment_parity() -> None:
    authority = replace(_authority(), workspace_authority_connection_ref="other_control")
    with pytest.raises(ValueError, match="projection"):
        _candidate(authority, _promotion(authority))


def _evidence(authority: AirflowDesiredStateAuthority) -> CredentialPromotionEvidence:
    return CredentialPromotionEvidence(
        artifact_ref="cache://runtime-credential-projections/sha256-" + "1" * 64 + "/credential-projection.json",
        sha256="sha256:" + "1" * 64,
        bytes=321,
        workspace_authority_connection_ref="workspace_control",
        publish_authority_sha256=authority.publish_authority_sha256,
    )


def test_exact_projection_authority_allows_preparation_and_publish_validation() -> None:
    authority = _authority()
    promotion = replace(_promotion(authority), credential_projection=_evidence(authority))
    assert _candidate(authority, promotion).publication_evidence_sha256 == promotion.evidence_sha256
    assert _candidate(authority, promotion, legacy=True).publication_evidence_sha256 == promotion.evidence_sha256


@pytest.mark.parametrize("drift", ["ref", "digest", "disabled"])
def test_projection_authority_drift_is_rejected(drift: str) -> None:
    authority = _authority()
    promotion = replace(_promotion(authority), credential_projection=_evidence(authority))
    if drift == "ref":
        authority = replace(authority, workspace_authority_connection_ref="other_control")
    elif drift == "digest":
        authority = replace(authority, watcher_identity="other-watcher")
    else:
        authority = replace(authority, workspace_authority_connection_ref=None)
    with pytest.raises(ValueError, match="projection"):
        _candidate(authority, promotion)


def _receipt(authority: AirflowDesiredStateAuthority) -> dict:
    promotion = _promotion(authority)
    return {
        **{
            name: getattr(promotion, name)
            for name in (
                "environment",
                "registry_scope_id",
                "release_id",
                "deployment_id",
                "source_git_sha",
                "runtime_image_digest",
                "airflow_index_sha256",
            )
        },
        "schema_version": CREDENTIAL_PROMOTION_SCHEMA,
        "expected_dag_ids": list(promotion.expected_dag_ids),
        "expected_dag_count": len(promotion.expected_dag_ids),
        "status": "passed",
        "warnings": [],
        "blockers": [],
        "created_at": "2026-09-23T00:00:00Z",
        **_evidence(authority).to_dict(),
    }


def test_neutral_receipt_roundtrip(tmp_path) -> None:
    authority = _authority()
    path = tmp_path / "promotion.json"
    path.write_text(json.dumps(_receipt(authority)))
    promotion = load_promotion_input(path)
    assert promotion.credential_projection == _evidence(authority)
    assert _candidate(authority, promotion).environment == "dev"


@pytest.mark.parametrize("corruption", ["missing", "extra", "path", "boolean_size", "duplicate", "not_passed"])
def test_neutral_receipt_fails_closed(tmp_path, corruption: str) -> None:
    receipt = _receipt(_authority())
    if corruption == "missing":
        receipt.pop("publish_authority_sha256")
    elif corruption == "extra":
        receipt["credential_projection"]["uri_value"] = "forbidden"
    elif corruption == "path":
        receipt["credential_projection"]["artifact_ref"] = "cache://other/credential-projection.json"
    elif corruption == "boolean_size":
        receipt["credential_projection"]["bytes"] = True
    elif corruption == "not_passed":
        receipt["status"] = "failed"
    raw = json.dumps(receipt)
    if corruption == "duplicate":
        raw = raw[:-1] + ', "publish_authority_sha256": "sha256:' + "2" * 64 + '"}'
    path = tmp_path / "promotion.json"
    path.write_text(raw)
    with pytest.raises(ValueError):
        load_promotion_input(path)


def _built_inputs(tmp_path):
    from tests.test_airflow_credential_projection_delivery import build_native_deployment

    deployment, _cache, authority = build_native_deployment(tmp_path)
    root = deployment.deployment_dir
    return {
        "authority": authority,
        "deployment_payload": (root / "deployment.json").read_bytes(),
        "index_payload": (root / "airflow-index.json").read_bytes(),
        "projection_payload": (root / "credential-projection.json").read_bytes(),
        "binding_set_payload": (root / "binding-set.json").read_bytes(),
        "registry_payload": (root / "connection-registry.ref").read_bytes(),
    }


def test_real_deployment_produces_authority_bound_promotion(tmp_path) -> None:
    from dpone.readiness.airflow_credential_promotion import build_credential_promotion_evidence

    inputs = _built_inputs(tmp_path)
    evidence = build_credential_promotion_evidence(**inputs)
    authority = inputs["authority"]
    assert evidence.publish_authority_sha256 == authority.publish_authority_sha256
    assert evidence.workspace_authority_connection_ref == "workspace_control"
    assert evidence.descriptor() == json.loads(inputs["deployment_payload"])["credential_projection"]


@pytest.mark.parametrize(
    "field", ["deployment_payload", "index_payload", "projection_payload", "binding_set_payload", "registry_payload"]
)
def test_real_promotion_rejects_mutated_artifacts(tmp_path, field: str) -> None:
    from dpone.readiness.airflow_credential_promotion import build_credential_promotion_evidence

    inputs = _built_inputs(tmp_path)
    if field == "index_payload":
        value = json.loads(inputs[field])
        value["credential_projection"]["sha256"] = "sha256:" + "0" * 64
        inputs[field] = json.dumps(value).encode()
    else:
        inputs[field] += b" "
    with pytest.raises(ValueError):
        build_credential_promotion_evidence(**inputs)
