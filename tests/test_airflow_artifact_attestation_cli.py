from __future__ import annotations

import json
from pathlib import Path

import pytest

import dpone.readiness.airflow_deployment_attestation as attestation_module
from dpone.adapters.object_storage_artifact_registry import ObjectStorageArtifactRegistry
from dpone.cli import main as cli_main
from dpone.contracts.airflow_artifact_attestation import sha256_bytes
from dpone.contracts.airflow_deployment_trust_policy import (
    POLICY_SCHEMA,
    AirflowDeploymentTrustPolicy,
)
from dpone.contracts.blob_signature import BlobSignatureVerification
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.runtime.airflow_artifact_delivery import AirflowArtifactPublisher
from dpone.runtime.airflow_artifact_delivery_models import PublishRequest
from dpone.storage import LocalObjectStorageClient, ObjectStorageUri
from tests.support.airflow_artifact_projection import write_exact_test_projection


def test_prepare_cli_is_deterministic_and_refuses_different_existing_bytes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root, evidence_path, scope_id, release_id, deployment_id = _published_projection(tmp_path)
    output = tmp_path / "artifact-attestation.json"
    command = _prepare_command(
        cache_root=cache_root,
        evidence_path=evidence_path,
        scope_id=scope_id,
        release_id=release_id,
        deployment_id=deployment_id,
        output=output,
    )

    first_code, first = _run_cli(command, capsys)
    first_bytes = output.read_bytes()
    second_code, second = _run_cli(command, capsys)

    assert first_code == second_code == 0
    assert first["attestation_id"] == second["attestation_id"]
    assert output.read_bytes() == first_bytes

    output.write_text('{"different":true}', encoding="utf-8")
    failed_code, failed = _run_cli(command, capsys)

    assert failed_code == 2
    assert failed["passed"] is False
    assert failed["errors"][0]["message"] == ("output already contains different immutable bytes")
    assert failed["errors"][0]["fixes"][0]["id"] == ("choose_new_output_or_restore_exact_bytes")
    assert output.read_text(encoding="utf-8") == '{"different":true}'


def test_prepare_cli_rejects_non_offset_timestamp_before_writing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root, evidence_path, scope_id, release_id, deployment_id = _published_projection(tmp_path)
    output = tmp_path / "artifact-attestation.json"
    command = _prepare_command(
        cache_root=cache_root,
        evidence_path=evidence_path,
        scope_id=scope_id,
        release_id=release_id,
        deployment_id=deployment_id,
        output=output,
    )
    command[command.index("2026-07-29T10:00:00Z")] = "2026-07-29T10:00:00"

    code, payload = _run_cli(command, capsys)

    assert code == 2
    assert payload["errors"][0]["code"] == "DPONE_ARTIFACT_ATTESTATION_PREPARE_FAILED"
    assert payload["errors"][0]["message"] == ("issued_at must be an RFC 3339 timestamp with a UTC offset")
    assert payload["errors"][0]["fixes"][0]["id"] == "use_ci_pipeline_created_at"
    assert payload["errors"][0]["docs_url"] == ("docs/airflow-artifact-attestation-operations.md")
    assert not output.exists()


@pytest.mark.parametrize(
    ("case", "expected_code", "expected_exit"),
    [
        ("malformed_policy", "DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID", 2),
        ("verifier_unavailable", "DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE", 3),
        ("invalid_signature", "DPONE_ARTIFACT_ATTESTATION_SIGNATURE_INVALID", 4),
    ],
)
def test_publish_cli_preserves_stable_failure_classification_without_remote_writes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_code: str,
    expected_exit: int,
) -> None:
    command, registry_root, policy_path = _publish_fixture(tmp_path, capsys)
    if case == "malformed_policy":
        policy_path.write_text("{}", encoding="utf-8")
    else:
        status = "unverified" if case == "verifier_unavailable" else "invalid"
        monkeypatch.setattr(
            attestation_module,
            "CosignPublicKeyBlobSignatureVerifier",
            lambda: _SignatureVerifier(status),
        )

    code, payload = _run_cli(command, capsys)

    assert code == expected_exit
    assert payload["errors"][0]["code"] == expected_code
    assert not list(registry_root.rglob("artifact-attestation.json"))


def test_publish_cli_receipt_matches_schema_and_is_idempotent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command, _, _ = _publish_fixture(tmp_path, capsys)
    monkeypatch.setattr(
        attestation_module,
        "CosignPublicKeyBlobSignatureVerifier",
        lambda: _SignatureVerifier("verified"),
    )

    first_code, first = _run_cli(command, capsys)
    second_code, second = _run_cli(command, capsys)

    assert first_code == second_code == 0
    assert (
        GitOpsSchemaValidator().validate(
            first,
            expected_kind="dpone.airflow-artifact-attestation-publish.v1",
        )
        == ()
    )
    assert (
        GitOpsSchemaValidator().validate(
            second,
            expected_kind="dpone.airflow-artifact-attestation-publish.v1",
        )
        == ()
    )
    assert (first["status"], first["created_objects"], first["existing_equal_objects"]) == (
        "published",
        3,
        0,
    )
    assert (second["status"], second["created_objects"], second["existing_equal_objects"]) == (
        "already_published",
        0,
        3,
    )
    assert first["verified_objects"] == second["verified_objects"] == 3


def _published_projection(tmp_path: Path) -> tuple[Path, Path, str, str, str]:
    cache_root = tmp_path / "cache"
    registry_root = tmp_path / "registry"
    release_id, deployment_id = write_exact_test_projection(cache_root)
    registry = ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(registry_root),
        root=ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/prod/example-workloads"),
    )
    report = AirflowArtifactPublisher(registry=registry).publish(
        PublishRequest(
            cache_root=cache_root,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="dev",
            artifact_registry_ref="local-test",
            registry_scope_id=registry.authority_scope_id,
            publication_mode="exact",
        )
    )
    evidence_path = tmp_path / "airflow-artifact-publish.json"
    evidence_path.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    return cache_root, evidence_path, registry.authority_scope_id, release_id, deployment_id


def _publish_fixture(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> tuple[list[str], Path, Path]:
    cache_root, evidence_path, scope_id, release_id, deployment_id = _published_projection(tmp_path)
    statement_path = tmp_path / "artifact-attestation.json"
    code, _ = _run_cli(
        _prepare_command(
            cache_root=cache_root,
            evidence_path=evidence_path,
            scope_id=scope_id,
            release_id=release_id,
            deployment_id=deployment_id,
            output=statement_path,
        ),
        capsys,
    )
    assert code == 0
    trust_root = tmp_path / "trust"
    trust_root.mkdir()
    key = b"public-key"
    (trust_root / "current.pub").write_bytes(key)
    policy_path = trust_root / "policy.json"
    policy = AirflowDeploymentTrustPolicy.from_mapping(
        {
            "schema": POLICY_SCHEMA,
            "trust_tier": "production",
            "attestations": "required_for_prod",
            "backend": "cosign_public_key_v1",
            "trusted_public_keys": {
                "current": {
                    "file": "current.pub",
                    "sha256": sha256_bytes(key),
                }
            },
            "cosign": {
                "minimum_version": "3.0.4",
                "maximum_version_exclusive": "4.0.0",
                "timeout_seconds": 10,
            },
            "allowed_environments": ["dev"],
            "allowed_artifact_registry_refs": ["local-test"],
            "allowed_registry_scope_ids": [scope_id],
            "allowed_source_projects": ["platform/example-workloads"],
            "allowed_source_refs": ["refs/heads/master"],
            "revoked_attestation_ids": [],
            "revoked_public_key_ids": [],
        }
    )
    policy_path.write_text(
        json.dumps(policy.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    bundle_path = tmp_path / "artifact-attestation.sigstore.json"
    bundle_path.write_text('{"bundle":"test"}', encoding="utf-8")
    registry_root = tmp_path / "registry"
    return (
        [
            "airflow",
            "artifact-attestation",
            "publish",
            "--cache-root",
            str(cache_root),
            "--publication-evidence",
            str(evidence_path),
            "--statement",
            str(statement_path),
            "--sigstore-bundle",
            str(bundle_path),
            "--trust-policy-path",
            str(policy_path),
            "--trust-key-root",
            str(trust_root),
            "--registry-uri",
            "s3://example-data-bucket/dpone-artifacts/prod/example-workloads",
            "--local-registry-root",
            str(registry_root),
            "--format",
            "json",
        ],
        registry_root,
        policy_path,
    )


class _SignatureVerifier:
    def __init__(self, status: str) -> None:
        self._status = status

    def verify_blob(self, **_: object) -> BlobSignatureVerification:
        if self._status == "verified":
            return BlobSignatureVerification.verified("3.0.4")
        if self._status == "unverified":
            return BlobSignatureVerification.unverified("verifier_unavailable")
        return BlobSignatureVerification.invalid("3.0.4")


def _prepare_command(
    *,
    cache_root: Path,
    evidence_path: Path,
    scope_id: str,
    release_id: str,
    deployment_id: str,
    output: Path,
) -> list[str]:
    return [
        "airflow",
        "artifact-attestation",
        "prepare",
        "--cache-root",
        str(cache_root),
        "--release-id",
        release_id,
        "--deployment-id",
        deployment_id,
        "--environment",
        "dev",
        "--artifact-registry-ref",
        "local-test",
        "--registry-scope-id",
        scope_id,
        "--publication-evidence",
        str(evidence_path),
        "--source-project",
        "platform/example-workloads",
        "--source-ref",
        "refs/heads/master",
        "--source-git-sha",
        "1" * 40,
        "--issued-at",
        "2026-07-29T10:00:00Z",
        "--output",
        str(output),
        "--format",
        "json",
    ]


def _run_cli(
    command: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, dict[str, object]]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(command)
    captured = capsys.readouterr()
    assert not captured.err
    return int(exc.value.code), json.loads(captured.out)
