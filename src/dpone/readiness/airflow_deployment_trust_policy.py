"""CLI-facing canonicalization of Airflow deployment trust policies."""

from __future__ import annotations

from pathlib import Path

from dpone.contracts.airflow_artifact_attestation import sha256_bytes
from dpone.contracts.airflow_deployment_trust_policy import AirflowDeploymentTrustPolicy
from dpone.readiness.airflow_attestation_files import (
    read_bounded_regular_file,
    write_immutable_file,
)
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error, manual_fix

_MAX_POLICY_BYTES = 256 * 1024


def render_airflow_deployment_trust_policy(
    *,
    input_path: str,
    output_path: str,
) -> SelfServiceResult:
    """Validate human-authored JSON and atomically write canonical policy bytes."""

    try:
        policy = AirflowDeploymentTrustPolicy.from_json_bytes(
            read_bounded_regular_file(
                Path(input_path),
                maximum=_MAX_POLICY_BYTES,
                label="artifact trust policy input",
            )
        )
        payload = policy.to_bytes()
        destination = Path(output_path)
        write_immutable_file(
            destination,
            payload,
            label="artifact trust policy output",
        )
        return SelfServiceResult(
            passed=True,
            details={
                "schema": "dpone.airflow-deployment-trust-policy-render.v1",
                "status": "rendered",
                "policy_fingerprint": policy.fingerprint,
                "policy_sha256": sha256_bytes(payload),
                "policy_bytes": len(payload),
                "output_path": destination.as_posix(),
                "errors": [],
                "warnings": [],
            },
            exit_code=0,
        )
    except Exception as exc:
        immutable_conflict = isinstance(exc, ValueError) and "already contains different bytes" in str(exc)
        return SelfServiceResult(
            passed=False,
            details={
                "schema": "dpone.airflow-artifact-attestation-error.v1",
                "status": "failed",
                "errors": [
                    dpone_error(
                        (
                            "DPONE_ARTIFACT_ATTESTATION_IMMUTABILITY_CONFLICT"
                            if immutable_conflict
                            else "DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID"
                        ),
                        (
                            "output already contains different immutable bytes"
                            if immutable_conflict
                            else "artifact trust policy could not be rendered"
                        ),
                        stage="artifact_attestation",
                        fixes=[
                            manual_fix(
                                "choose_new_output_or_restore_exact_bytes"
                                if immutable_conflict
                                else "inspect_trust_policy_contract"
                            )
                        ],
                        docs_url="docs/airflow-artifact-attestation-operations.md",
                    )
                ],
                "warnings": [],
            },
            exit_code=4 if immutable_conflict else 2,
        )


__all__ = ["render_airflow_deployment_trust_policy"]
