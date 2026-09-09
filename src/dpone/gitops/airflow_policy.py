from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_POLICY_PROFILES = ("advisory", "pr", "release")


@dataclass(frozen=True, slots=True)
class AirflowRunnerPolicyFinding:
    name: str
    passed: bool
    severity: str
    message: str
    path: str
    issue_code: str


class GitOpsAirflowRunnerPolicyEvaluator:
    """Evaluates profile-specific Airflow runner release gates."""

    def evaluate(
        self,
        *,
        profile: str,
        bundle_path: str,
        bundle: object,
        pod_template_path: str,
        pod_template: object,
        image_contract_path: str,
        image_contract: object,
    ) -> tuple[AirflowRunnerPolicyFinding, ...]:
        normalized = normalize_airflow_runner_policy(profile)
        if normalized not in _POLICY_PROFILES:
            return (
                _finding(
                    name="runner_policy_profile",
                    passed=False,
                    severity="blocker",
                    message=f"Unknown Airflow runner policy profile: {profile}",
                    path=str(profile),
                    issue_code="invalid_runner_policy",
                ),
            )
        if normalized == "advisory":
            return ()

        severity = "blocker" if normalized == "release" else "warning"
        return (
            _bundle_attestation_finding(bundle_path=bundle_path, bundle=bundle, severity=severity),
            _image_digest_finding(
                image_contract_path=image_contract_path,
                image_contract=image_contract,
                severity=severity,
            ),
            _service_account_finding(
                pod_template_path=pod_template_path,
                pod_template=pod_template,
                severity=severity,
            ),
            _resources_finding(
                pod_template_path=pod_template_path,
                pod_template=pod_template,
                severity=severity,
            ),
            _non_root_finding(
                pod_template_path=pod_template_path,
                pod_template=pod_template,
                severity=severity,
            ),
        )


def airflow_runner_policy_names() -> tuple[str, ...]:
    return _POLICY_PROFILES


def normalize_airflow_runner_policy(profile: object) -> str:
    text = str(profile or "advisory").strip().lower()
    return text or "advisory"


def _bundle_attestation_finding(*, bundle_path: str, bundle: object, severity: str) -> AirflowRunnerPolicyFinding:
    present = isinstance(_mapping(bundle).get("attestation"), Mapping)
    return _finding(
        name="runner_policy_bundle_attestation",
        passed=present,
        severity=severity,
        message="Bundle attestation is present"
        if present
        else "Release Airflow runner policy requires bundle attestation",
        path=bundle_path,
        issue_code="bundle_attestation_required",
    )


def _image_digest_finding(
    *,
    image_contract_path: str,
    image_contract: object,
    severity: str,
) -> AirflowRunnerPolicyFinding:
    digest = str(_mapping(image_contract).get("image_digest") or "").strip()
    return _finding(
        name="runner_policy_image_digest",
        passed=bool(digest),
        severity=severity,
        message="Image contract has an immutable image digest"
        if digest
        else "Release Airflow runner policy requires image_contract.image_digest",
        path=image_contract_path,
        issue_code="image_digest_required",
    )


def _service_account_finding(
    *,
    pod_template_path: str,
    pod_template: object,
    severity: str,
) -> AirflowRunnerPolicyFinding:
    service_account = str(_pod_spec(pod_template).get("serviceAccountName") or "").strip()
    return _finding(
        name="runner_policy_service_account",
        passed=bool(service_account),
        severity=severity,
        message="Pod template declares spec.serviceAccountName"
        if service_account
        else "Release Airflow runner policy requires spec.serviceAccountName",
        path=pod_template_path,
        issue_code="pod_template_service_account_required",
    )


def _resources_finding(
    *,
    pod_template_path: str,
    pod_template: object,
    severity: str,
) -> AirflowRunnerPolicyFinding:
    resources = _mapping(_first_container(pod_template).get("resources"))
    passed = bool(_mapping(resources.get("requests"))) and bool(_mapping(resources.get("limits")))
    return _finding(
        name="runner_policy_resources",
        passed=passed,
        severity=severity,
        message="Base container declares resource requests and limits"
        if passed
        else "Release Airflow runner policy requires resource requests and limits",
        path=pod_template_path,
        issue_code="pod_template_resources_required",
    )


def _non_root_finding(
    *,
    pod_template_path: str,
    pod_template: object,
    severity: str,
) -> AirflowRunnerPolicyFinding:
    security_context = _mapping(_pod_spec(pod_template).get("securityContext"))
    run_as_user = security_context.get("runAsUser")
    run_as_non_root = security_context.get("runAsNonRoot")
    passed = run_as_non_root is True and run_as_user not in {0, "0", None}
    return _finding(
        name="runner_policy_non_root",
        passed=passed,
        severity=severity,
        message="Pod template declares a non-root security context"
        if passed
        else "Release Airflow runner policy requires a non-root pod security context",
        path=pod_template_path,
        issue_code="pod_template_non_root_required",
    )


def _pod_spec(pod_template: object) -> Mapping[str, Any]:
    return _mapping(_mapping(pod_template).get("spec"))


def _first_container(pod_template: object) -> Mapping[str, Any]:
    containers = _pod_spec(pod_template).get("containers")
    if isinstance(containers, list) and containers and isinstance(containers[0], Mapping):
        return containers[0]
    return {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finding(
    *,
    name: str,
    passed: bool,
    severity: str,
    message: str,
    path: str,
    issue_code: str,
) -> AirflowRunnerPolicyFinding:
    return AirflowRunnerPolicyFinding(
        name=name,
        passed=passed,
        severity=severity,
        message=message,
        path=path,
        issue_code=issue_code,
    )


__all__ = [
    "AirflowRunnerPolicyFinding",
    "GitOpsAirflowRunnerPolicyEvaluator",
    "airflow_runner_policy_names",
    "normalize_airflow_runner_policy",
]
