from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_models import GitOpsAirflowCheck
from dpone.gitops.airflow_pod_doctor_connection_bridge import connection_bridge_pod_doctor_checks
from dpone.gitops.airflow_pod_doctor_git_sync import git_sync_pod_doctor_checks
from dpone.gitops.models import GitOpsIssue

AIRFLOW_XCOM_RETURN_PATH = "/airflow/xcom/return.json"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodDoctorReport:
    artifact_dir: str | None
    pod_contract_path: str
    pod_spec_path: str
    kpo_kwargs_path: str
    runner_policy: str
    checks: tuple[GitOpsAirflowCheck, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_pod_doctor"
    schema_version: str = "1"
    producer: str = "dpone gitops airflow pod-doctor"

    @property
    def passed(self) -> bool:
        return not self.blockers and all(check.passed or check.severity == "warning" for check in self.checks)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "artifact_dir": self.artifact_dir,
            "pod_contract_path": self.pod_contract_path,
            "pod_spec_path": self.pod_spec_path,
            "kpo_kwargs_path": self.kpo_kwargs_path,
            "runner_policy": self.runner_policy,
            "checks": [check.to_jsonable() for check in self.checks],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


class GitOpsAirflowPodDoctor:
    """Validate pod contract artifacts without importing Airflow or Kubernetes."""

    def validate(
        self,
        *,
        pod_contract_path: str,
        pod_contract: object,
        pod_spec_path: str,
        pod_spec: object,
        kpo_kwargs_path: str,
        kpo_kwargs: object,
        runner_policy: str,
    ) -> tuple[tuple[GitOpsAirflowCheck, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
        checks: list[GitOpsAirflowCheck] = []
        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = []
        contract = _mapping(pod_contract)
        spec = _mapping(pod_spec)
        kwargs = _mapping(kpo_kwargs)
        _append_contract_kind(checks, blockers, path=pod_contract_path, contract=contract)
        _append_base_container_check(checks, blockers, pod_spec_path=pod_spec_path, pod_spec=spec)
        _append_image_check(checks, blockers, pod_spec_path=pod_spec_path, pod_spec=spec, contract=contract)
        _append_xcom_checks(
            checks,
            blockers,
            pod_contract_path=pod_contract_path,
            contract=contract,
            kpo_kwargs_path=kpo_kwargs_path,
            kpo_kwargs=kwargs,
        )
        _append_pod_template_check(
            checks,
            blockers,
            contract=contract,
            kpo_kwargs_path=kpo_kwargs_path,
            kpo_kwargs=kwargs,
        )
        git_sync_checks, git_sync_warnings, git_sync_blockers = git_sync_pod_doctor_checks(
            contract=contract,
            pod_spec_path=pod_spec_path,
            pod_spec=spec,
            runner_policy=runner_policy,
        )
        checks.extend(git_sync_checks)
        warnings.extend(git_sync_warnings)
        blockers.extend(git_sync_blockers)
        bridge_checks, bridge_warnings, bridge_blockers = connection_bridge_pod_doctor_checks(
            contract=contract,
            pod_spec_path=pod_spec_path,
            pod_spec=spec,
            runner_policy=runner_policy,
            check_factory=_check,
            issue_factory=_issue,
        )
        checks.extend(bridge_checks)
        warnings.extend(bridge_warnings)
        blockers.extend(bridge_blockers)
        if runner_policy == "release":
            _append_release_checks(checks, blockers, pod_spec_path=pod_spec_path, pod_spec=spec)
        return tuple(checks), tuple(warnings), tuple(blockers)


def _append_contract_kind(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    path: str,
    contract: Mapping[str, Any],
) -> None:
    passed = contract.get("kind") == "gitops.airflow_pod_contract"
    checks.append(_check("pod_contract_json", passed, "blocker", "Pod contract kind is valid", path))
    if not passed:
        blockers.append(
            _issue("pod_contract_json_invalid", "Pod contract kind must be gitops.airflow_pod_contract", path)
        )


def _append_base_container_check(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    pod_spec_path: str,
    pod_spec: Mapping[str, Any],
) -> None:
    passed = _first_container(pod_spec).get("name") == "base"
    checks.append(
        _check(
            "pod_contract_base_container",
            passed,
            "blocker",
            "Pod spec first container must be named base",
            pod_spec_path,
        )
    )
    if not passed:
        blockers.append(
            _issue(
                "pod_contract_base_container_missing",
                "Pod contract requires spec.containers[0].name to be base",
                pod_spec_path,
            )
        )


def _append_image_check(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    pod_spec_path: str,
    pod_spec: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> None:
    image = _text(_first_container(pod_spec).get("image"))
    expected = _text(contract.get("image"))
    passed = bool(image) and image == expected
    checks.append(_check("pod_contract_image", passed, "blocker", "Pod spec image matches pod contract", pod_spec_path))
    if not passed:
        blockers.append(
            _issue("pod_contract_image_mismatch", "Pod spec image must match pod contract image", pod_spec_path)
        )


def _append_xcom_checks(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    pod_contract_path: str,
    contract: Mapping[str, Any],
    kpo_kwargs_path: str,
    kpo_kwargs: Mapping[str, Any],
) -> None:
    xcom = _mapping(contract.get("xcom"))
    return_path_passed = xcom.get("return_path") == AIRFLOW_XCOM_RETURN_PATH and bool(xcom.get("enabled"))
    checks.append(
        _check(
            "pod_contract_xcom_return_path",
            return_path_passed,
            "blocker",
            "Pod contract writes Airflow XCom return.json",
            pod_contract_path,
        )
    )
    if not return_path_passed:
        blockers.append(
            _issue(
                "pod_contract_xcom_return_path_invalid",
                "Pod contract must enable /airflow/xcom/return.json output",
                pod_contract_path,
            )
        )
    push_passed = kpo_kwargs.get("do_xcom_push") is True
    checks.append(
        _check(
            "pod_contract_xcom_push",
            push_passed,
            "blocker",
            "KubernetesPodOperator kwargs enable do_xcom_push",
            kpo_kwargs_path,
        )
    )
    if not push_passed:
        blockers.append(
            _issue(
                "pod_contract_xcom_push_required",
                "KubernetesPodOperator kwargs must set do_xcom_push=true",
                kpo_kwargs_path,
            )
        )


def _append_pod_template_check(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    contract: Mapping[str, Any],
    kpo_kwargs_path: str,
    kpo_kwargs: Mapping[str, Any],
) -> None:
    expected = _text(contract.get("pod_spec_path"))
    passed = bool(expected) and kpo_kwargs.get("pod_template_file") == expected
    checks.append(
        _check(
            "pod_contract_pod_template_file",
            passed,
            "blocker",
            "KPO kwargs reference the generated pod spec",
            kpo_kwargs_path,
        )
    )
    if not passed:
        blockers.append(
            _issue(
                "pod_contract_pod_template_mismatch",
                "KPO kwargs pod_template_file must match pod contract pod_spec_path",
                kpo_kwargs_path,
            )
        )


def _append_release_checks(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    pod_spec_path: str,
    pod_spec: Mapping[str, Any],
) -> None:
    spec = _mapping(pod_spec.get("spec"))
    service_account = _text(spec.get("serviceAccountName"))
    service_account_passed = bool(service_account) and service_account != "default"
    checks.append(
        _check(
            "pod_contract_release_service_account",
            service_account_passed,
            "blocker",
            "Release pod contract uses a dedicated service account",
            pod_spec_path,
        )
    )
    if not service_account_passed:
        blockers.append(
            _issue(
                "pod_contract_service_account_required",
                "Release pod contract requires a non-default service account",
                pod_spec_path,
            )
        )
    resources_passed = bool(_mapping(_first_container(pod_spec).get("resources")))
    checks.append(
        _check(
            "pod_contract_release_resources",
            resources_passed,
            "blocker",
            "Release pod contract sets resources",
            pod_spec_path,
        )
    )
    if not resources_passed:
        blockers.append(
            _issue("pod_contract_resources_required", "Release pod contract requires resources", pod_spec_path)
        )


def _first_container(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    spec = _mapping(payload.get("spec"))
    containers = spec.get("containers")
    if isinstance(containers, list) and containers and isinstance(containers[0], Mapping):
        return containers[0]
    return {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _check(name: str, passed: bool, severity: str, message: str, path: str) -> GitOpsAirflowCheck:
    return GitOpsAirflowCheck(
        name=name,
        passed=passed,
        severity=severity,
        message=message,
        path=path,
        source="dpone gitops airflow pod-doctor",
    )


def _issue(code: str, message: str, path: str) -> GitOpsIssue:
    return build_pod_doctor_issue(code=code, message=message, path=path)


def build_pod_doctor_issue(
    *,
    code: str,
    message: str,
    path: str,
    source: str = "dpone gitops airflow pod-doctor",
) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=source)


__all__ = ["GitOpsAirflowPodDoctor", "GitOpsAirflowPodDoctorReport", "build_pod_doctor_issue"]
