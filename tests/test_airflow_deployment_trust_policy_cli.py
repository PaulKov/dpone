from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from dpone.cli import main as cli_main
from dpone.contracts.airflow_artifact_attestation import sha256_bytes
from dpone.contracts.airflow_deployment_trust_policy import (
    POLICY_SCHEMA,
    AirflowDeploymentTrustPolicy,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.readiness.airflow_artifact_delivery import ArtifactRegistryOptions
from dpone.readiness.airflow_deployment_attestation import publish_artifact_attestation

_SHA = "sha256:" + "a" * 64


def test_policy_render_writes_canonical_validated_bytes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "policy.pretty.json"
    output = tmp_path / "policy.json"
    source.write_text(json.dumps(_policy_payload(), indent=2), encoding="utf-8")

    code = _run_cli(
        [
            "airflow",
            "artifact-attestation",
            "policy-render",
            "--input",
            str(source),
            "--output",
            str(output),
            "--format",
            "json",
        ]
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert AirflowDeploymentTrustPolicy.from_bytes(output.read_bytes()).to_bytes() == output.read_bytes()
    assert result["policy_sha256"] == sha256_bytes(output.read_bytes())
    assert (
        GitOpsSchemaValidator().validate(
            result,
            expected_kind="dpone.airflow-deployment-trust-policy-render.v1",
        )
        == ()
    )


def test_policy_render_refuses_path_traversal_and_does_not_write(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _policy_payload()
    payload["trusted_public_keys"]["dpone-prod-2026-07"]["file"] = "../escape.pub"
    source = tmp_path / "policy.pretty.json"
    output = tmp_path / "policy.json"
    source.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    code = _run_cli(
        [
            "airflow",
            "artifact-attestation",
            "policy-render",
            "--input",
            str(source),
            "--output",
            str(output),
            "--format",
            "json",
        ]
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)

    assert code == 2
    assert captured.err == ""
    assert result["errors"][0]["code"] == "DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID"
    assert not output.exists()


def test_policy_schema_matches_runtime_structural_limits() -> None:
    validator = GitOpsSchemaValidator()
    unsafe = _policy_payload()
    unsafe["trusted_public_keys"]["dpone-prod-2026-07"]["file"] = "../escape.pub"
    empty_scope = _policy_payload()
    empty_scope["allowed_registry_scope_ids"] = []

    assert validator.validate(
        unsafe,
        expected_kind="dpone.airflow-deployment-trust-policy.v1",
    )
    assert validator.validate(
        empty_scope,
        expected_kind="dpone.airflow-deployment-trust-policy.v1",
    )
    AirflowDeploymentTrustPolicy.from_mapping(_policy_payload())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("allowed_environments", [" prod"]),
        ("allowed_source_refs", ["refs/heads/master "]),
        ("allowed_environments", ["prod\n"]),
        ("allowed_environments", ["pro\nd"]),
        ("allowed_environments", ["pro\rd"]),
        ("allowed_environments", ["pro\td"]),
    ],
)
def test_policy_schema_and_runtime_reject_whitespace_drift(
    field: str,
    value: list[str],
) -> None:
    payload = _policy_payload()
    payload[field] = value

    _assert_rejected_by_schema_validators(payload)
    with pytest.raises(ValueError):
        AirflowDeploymentTrustPolicy.from_mapping(payload)


def test_policy_schema_and_runtime_reject_cosign_version_whitespace() -> None:
    payload = _policy_payload()
    payload["cosign"]["minimum_version"] = " 3.0.4"

    assert GitOpsSchemaValidator().validate(
        payload,
        expected_kind="dpone.airflow-deployment-trust-policy.v1",
    )
    with pytest.raises(ValueError):
        AirflowDeploymentTrustPolicy.from_mapping(payload)


def test_policy_key_map_is_bounded_and_key_id_is_structurally_unique() -> None:
    payload = _policy_payload()
    payload["trusted_public_keys"] = {
        f"key-{index}": {
            "file": f"key-{index}.pub",
            "sha256": "sha256:" + f"{index + 1:064x}",
        }
        for index in range(9)
    }

    assert GitOpsSchemaValidator().validate(
        payload,
        expected_kind="dpone.airflow-deployment-trust-policy.v1",
    )
    with pytest.raises(ValueError):
        AirflowDeploymentTrustPolicy.from_mapping(payload)


@pytest.mark.parametrize(
    ("key_id", "file"),
    [
        ("dpone-prod\n", "dpone-prod.pub"),
        ("dpone-prod", "dpone-prod.pub\n"),
        ("dpone\rprod", "dpone-prod.pub"),
        ("dpone-prod", "dpone\rprod.pub"),
        ("dpone\tprod", "dpone-prod.pub"),
        ("dpone-prod", "dpone\tprod.pub"),
    ],
)
def test_policy_schema_and_runtime_reject_control_characters_in_key_identity(
    key_id: str,
    file: str,
) -> None:
    payload = _policy_payload()
    payload["trusted_public_keys"] = {
        key_id: {
            "file": file,
            "sha256": _SHA,
        }
    }

    _assert_rejected_by_schema_validators(payload)
    with pytest.raises(ValueError):
        AirflowDeploymentTrustPolicy.from_mapping(payload)


def test_policy_allows_explicit_aliases_for_the_same_public_key() -> None:
    payload = _policy_payload()
    payload["trusted_public_keys"]["dpone-prod-previous"] = {
        "file": "dpone-prod-2026-07.pub",
        "sha256": _SHA,
    }

    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.airflow-deployment-trust-policy.v1",
        )
        == ()
    )
    assert len(AirflowDeploymentTrustPolicy.from_mapping(payload).trusted_public_keys) == 2


def test_policy_render_text_explains_next_step(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "policy.pretty.json"
    output = tmp_path / "policy.json"
    source.write_text(json.dumps(_policy_payload(), indent=2), encoding="utf-8")

    code = _run_cli(
        [
            "airflow",
            "artifact-attestation",
            "policy-render",
            "--input",
            str(source),
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert "policy sha256:" in captured.out
    assert "next: mount these exact bytes" in captured.out
    assert captured.err == ""


def test_policy_render_refuses_different_immutable_output_with_action(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "policy.pretty.json"
    output = tmp_path / "policy.json"
    source.write_text(json.dumps(_policy_payload(), indent=2), encoding="utf-8")
    output.write_text('{"different":true}', encoding="utf-8")

    code = _run_cli(
        [
            "airflow",
            "artifact-attestation",
            "policy-render",
            "--input",
            str(source),
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert code == 4
    assert "DPONE_ARTIFACT_ATTESTATION_IMMUTABILITY_CONFLICT" in captured.out
    assert "action: choose_new_output_or_restore_exact_bytes" in captured.out
    assert "runbook: docs/airflow-artifact-attestation-operations.md" in captured.out
    assert captured.err == ""
    assert output.read_text(encoding="utf-8") == '{"different":true}'


def test_publish_requires_explicit_connection_type_before_reading_inputs(
    tmp_path: Path,
) -> None:
    result = publish_artifact_attestation(
        cache_root=str(tmp_path / "cache"),
        publication_evidence_path=str(tmp_path / "missing-evidence.json"),
        statement_path=str(tmp_path / "missing-statement.json"),
        sigstore_bundle_path=str(tmp_path / "missing-bundle.json"),
        trust_policy_path=str(tmp_path / "missing-policy.json"),
        trust_key_root=str(tmp_path / "trust"),
        registry_options=ArtifactRegistryOptions(
            registry_uri="s3://example-data-bucket/dpone-artifacts/prod/example-workloads",
            connection_id="s3_dpone_artifacts_writer",
        ),
    )

    assert result.exit_code == 2
    error = result.to_dict()["errors"][0]
    assert error["code"] == "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID"
    assert error["message"] == "connection_type is required when connection_id is configured"
    assert error["fixes"][0]["id"] == "set_connection_type_env_airflow_or_vault"
    assert error["docs_url"] == "docs/airflow-artifact-attestation-operations.md"


def _policy_payload() -> dict[str, Any]:
    return {
        "schema": POLICY_SCHEMA,
        "trust_tier": "production",
        "attestations": "required_for_prod",
        "backend": "cosign_public_key_v1",
        "trusted_public_keys": {
            "dpone-prod-2026-07": {
                "file": "dpone-prod-2026-07.pub",
                "sha256": _SHA,
            }
        },
        "cosign": {
            "minimum_version": "3.0.4",
            "maximum_version_exclusive": "4.0.0",
            "timeout_seconds": 10,
        },
        "allowed_environments": ["prod"],
        "allowed_artifact_registry_refs": ["dpone_prod"],
        "allowed_registry_scope_ids": [_SHA],
        "allowed_source_projects": ["platform/example-workloads"],
        "allowed_source_refs": ["refs/heads/master"],
        "revoked_attestation_ids": [],
        "revoked_public_key_ids": [],
    }


def _assert_rejected_by_schema_validators(payload: dict[str, Any]) -> None:
    assert GitOpsSchemaValidator().validate(
        payload,
        expected_kind="dpone.airflow-deployment-trust-policy.v1",
    )
    schema_path = Path(__file__).parents[1] / "docs/schemas/gitops/airflow-deployment-trust-policy.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert tuple(Draft202012Validator(schema).iter_errors(payload))


def _run_cli(command: list[str]) -> int:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(command)
    return int(exc.value.code)
