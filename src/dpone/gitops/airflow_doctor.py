from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.gitops.airflow_models import GitOpsAirflowCheck
from dpone.gitops.models import GitOpsIssue


class GitOpsAirflowDoctor:
    """Validates Airflow/Kubernetes runner contracts without importing Airflow."""

    def validate_bundle(
        self,
        *,
        bundle_path: str,
        bundle: object,
        require_attestation: bool,
    ) -> tuple[tuple[GitOpsAirflowCheck, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
        checks: list[GitOpsAirflowCheck] = []
        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = []
        bundle_mapping = _mapping(bundle)
        bundle_passed = bool(bundle_mapping) and bundle_mapping.get("kind") == "gitops.bundle"
        checks.append(
            _check(
                name="bundle_json",
                passed=bundle_passed,
                severity="blocker",
                message="Bundle JSON has kind gitops.bundle"
                if bundle_passed
                else "Bundle JSON must have kind gitops.bundle",
                path=bundle_path,
                source="dpone gitops airflow doctor",
            )
        )
        if not bundle_passed:
            blockers.append(
                _issue(
                    code="bundle_json_invalid",
                    message="Bundle JSON must parse to an object with kind gitops.bundle",
                    path=bundle_path,
                )
            )

        attestation = bundle_mapping.get("attestation") if bundle_mapping else None
        attestation_present = isinstance(attestation, Mapping)
        attestation_passed = attestation_present or not require_attestation
        checks.append(
            _check(
                name="bundle_attestation",
                passed=attestation_passed,
                severity="blocker" if require_attestation else "warning",
                message="Bundle attestation is present"
                if attestation_present
                else "Bundle attestation is required for this runner gate",
                path=bundle_path,
                source="dpone gitops airflow doctor",
            )
        )
        if require_attestation and not attestation_present:
            blockers.append(
                _issue(
                    code="bundle_attestation_required",
                    message="Bundle attestation is required for Airflow runner doctor",
                    path=bundle_path,
                )
            )
        elif not require_attestation and not attestation_present:
            warnings.append(
                _issue(
                    code="bundle_attestation_missing",
                    message="Bundle attestation is absent; use --require-attestation for release gates",
                    path=bundle_path,
                )
            )
        return tuple(checks), tuple(warnings), tuple(blockers)

    def validate_pod_template(
        self,
        *,
        pod_template_path: str,
        pod_template: object,
    ) -> tuple[tuple[GitOpsAirflowCheck, ...], tuple[GitOpsIssue, ...]]:
        checks: list[GitOpsAirflowCheck] = []
        blockers: list[GitOpsIssue] = []
        payload = _mapping(pod_template)
        metadata = _mapping(payload.get("metadata"))
        pod_name = str(metadata.get("name") or "").strip()
        checks.append(
            _check(
                name="pod_template_metadata_name",
                passed=bool(pod_name),
                severity="blocker",
                message="Pod template metadata.name is set"
                if pod_name
                else "Airflow KubernetesExecutor requires metadata.name",
                path=pod_template_path,
                source="pod_template_file",
            )
        )
        if not pod_name:
            blockers.append(
                _issue(
                    code="pod_template_name_missing",
                    message="Airflow KubernetesExecutor pod_template_file requires metadata.name",
                    path=pod_template_path,
                )
            )

        first_container = _first_container(payload)
        first_name = str(first_container.get("name") or "").strip()
        base_passed = first_name == "base"
        checks.append(
            _check(
                name="pod_template_base_container",
                passed=base_passed,
                severity="blocker",
                message="First pod template container is named base"
                if base_passed
                else "Airflow KubernetesExecutor requires spec.containers[0].name to be base",
                path=pod_template_path,
                source="pod_template_file",
            )
        )
        if not base_passed:
            blockers.append(
                _issue(
                    code="pod_template_base_container_missing",
                    message="Airflow KubernetesExecutor requires spec.containers[0].name to be base",
                    path=pod_template_path,
                )
            )

        image = str(first_container.get("image") or "").strip()
        checks.append(
            _check(
                name="pod_template_image",
                passed=bool(image),
                severity="blocker",
                message="First pod template container image is set"
                if image
                else "Airflow KubernetesExecutor requires spec.containers[0].image",
                path=pod_template_path,
                source="pod_template_file",
            )
        )
        if not image:
            blockers.append(
                _issue(
                    code="pod_template_image_missing",
                    message="Airflow KubernetesExecutor requires spec.containers[0].image",
                    path=pod_template_path,
                )
            )
        return tuple(checks), tuple(blockers)

    def validate_image_contract(
        self,
        *,
        image_contract_path: str,
        image_contract: object,
        expected_image: str | None,
    ) -> tuple[tuple[GitOpsAirflowCheck, ...], tuple[GitOpsIssue, ...]]:
        checks: list[GitOpsAirflowCheck] = []
        blockers: list[GitOpsIssue] = []
        payload = _mapping(image_contract)
        image = str(payload.get("image") or "").strip()
        image_passed = bool(image) and (expected_image is None or image == expected_image)
        image_message = "Image contract image matches the runner image"
        if not image:
            image_message = "Image contract must include a non-empty image"
        elif expected_image is not None and image != expected_image:
            image_message = "Image contract image must match the runner image"
        checks.append(
            _check(
                name="image_contract_image",
                passed=image_passed,
                severity="blocker",
                message=image_message,
                path=image_contract_path,
                source="image_contract",
            )
        )
        if not image_passed:
            blockers.append(
                _issue(
                    code="image_contract_image_mismatch" if image else "image_contract_image_missing",
                    message=image_message,
                    path=image_contract_path,
                )
            )

        tools = payload.get("tools")
        tools_passed = isinstance(tools, list) and "dpone" in {str(tool) for tool in tools}
        checks.append(
            _check(
                name="image_contract_tools",
                passed=tools_passed,
                severity="blocker",
                message="Image contract declares the dpone executable"
                if tools_passed
                else "Image contract tools must include dpone",
                path=image_contract_path,
                source="image_contract",
            )
        )
        if not tools_passed:
            blockers.append(
                _issue(
                    code="image_contract_dpone_tool_missing",
                    message="Image contract tools must include dpone",
                    path=image_contract_path,
                )
            )
        return tuple(checks), tuple(blockers)


def _first_container(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    spec = _mapping(payload.get("spec"))
    containers = spec.get("containers")
    if isinstance(containers, list) and containers and isinstance(containers[0], Mapping):
        return containers[0]
    return {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _check(
    *,
    name: str,
    passed: bool,
    severity: str,
    message: str,
    path: str,
    source: str,
) -> GitOpsAirflowCheck:
    return GitOpsAirflowCheck(
        name=name,
        passed=passed,
        severity=severity,
        message=message,
        path=path,
        source=source,
    )


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source="dpone gitops airflow doctor")


__all__ = ["GitOpsAirflowDoctor"]
