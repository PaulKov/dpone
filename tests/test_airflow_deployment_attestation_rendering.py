from dpone.commands.airflow_deployment_attestation_rendering import (
    airflow_attestation_prepare_text,
    airflow_attestation_publish_text,
)

_SHA = "sha256:" + "a" * 64


def test_prepare_text_exposes_statement_identity_and_next_action() -> None:
    text = airflow_attestation_prepare_text(
        {
            "passed": True,
            "attestation_id": _SHA,
            "statement_sha256": _SHA,
            "output_path": ".ci/out/artifact-attestation.json",
        }
    )

    assert f"attestation id: {_SHA}" in text
    assert "statement: .ci/out/artifact-attestation.json" in text
    assert "next: sign the exact statement" in text


def test_publish_text_exposes_verification_without_secret_material() -> None:
    text = airflow_attestation_publish_text(
        {
            "passed": True,
            "status": "published",
            "deployment_id": _SHA,
            "attestation_id": _SHA,
            "verified_objects": 3,
            "verification": {"decision": "verified"},
        }
    )

    assert "verification: verified" in text
    assert "verified immutable objects: 3" in text


def test_failure_text_exposes_action_and_runbook() -> None:
    text = airflow_attestation_publish_text(
        {
            "passed": False,
            "errors": [
                {
                    "code": "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                    "message": "connection_type is required when connection_id is configured",
                    "fixes": [{"id": "set_connection_type_env_airflow_or_vault"}],
                    "docs_url": "docs/airflow-artifact-attestation-operations.md",
                }
            ],
        }
    )

    assert "connection_type is required" in text
    assert "action: set_connection_type_env_airflow_or_vault" in text
    assert "runbook: docs/airflow-artifact-attestation-operations.md" in text
