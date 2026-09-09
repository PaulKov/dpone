from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_SOURCE = "dpone gitops airflow pod-doctor"


def connection_bridge_pod_doctor_checks(
    *,
    contract: Mapping[str, Any],
    pod_spec_path: str,
    pod_spec: Mapping[str, Any],
    runner_policy: str,
    check_factory: Any,
    issue_factory: Any,
) -> tuple[tuple[Any, ...], tuple[Any, ...], tuple[Any, ...]]:
    bridge = _mapping(contract.get("connection_bridge"))
    required_ids = tuple(str(item) for item in bridge.get("required_connection_ids", ()) if str(item))

    def check(name: str, passed: bool, severity: str, path: str) -> Any:
        return _check(check_factory, name, passed, severity, path)

    def issue(code: str, message: str, path: str) -> Any:
        return _issue(issue_factory, code, message, path)

    if not required_ids:
        return (
            (check("pod_contract_airflow_connection_bridge_not_required", True, "warning", pod_spec_path),),
            (),
            (),
        )
    mode = str(bridge.get("mode") or "disabled")
    runtime_mode = str(bridge.get("runtime_mode") or "runtime_only")
    checks: list[Any] = []
    warnings: list[Any] = []
    blockers: list[Any] = []
    if runtime_mode == "airflow_image":
        checks.append(check("pod_contract_airflow_runtime_image", True, "blocker", pod_spec_path))
        return tuple(checks), tuple(warnings), tuple(blockers)
    if mode == "k8s_secret":
        _append_secret_bridge_checks(
            checks,
            blockers,
            bridge=bridge,
            pod_spec=pod_spec,
            path=pod_spec_path,
            check=check,
            issue=issue,
        )
    elif mode == "env":
        _append_env_bridge_checks(
            checks,
            blockers,
            bridge=bridge,
            pod_spec=pod_spec,
            path=pod_spec_path,
            check=check,
            issue=issue,
        )
    else:
        passed = runner_policy != "release"
        checks.append(check("pod_contract_airflow_connection_bridge", passed, "blocker", pod_spec_path))
        bridge_issue = issue(
            "pod_contract_airflow_connection_bridge_required",
            "Runtime-only pods using connection_type=airflow require k8s_secret/env bridge or airflow_image mode",
            pod_spec_path,
        )
        (warnings if passed else blockers).append(bridge_issue)
    return tuple(checks), tuple(warnings), tuple(blockers)


def _append_secret_bridge_checks(
    checks: list[Any],
    blockers: list[Any],
    *,
    bridge: Mapping[str, Any],
    pod_spec: Mapping[str, Any],
    path: str,
    check: Any,
    issue: Any,
) -> None:
    env_by_name = _base_env_by_name(pod_spec)
    missing: list[str] = []
    for ref in _env_refs(bridge):
        env_name = str(ref.get("env_name") or "")
        secret = _mapping(ref.get("secret_ref"))
        if not env_name or env_name not in env_by_name:
            missing.append(env_name or "<missing>")
            continue
        actual_ref = _mapping(_mapping(env_by_name[env_name].get("valueFrom")).get("secretKeyRef"))
        if actual_ref.get("name") != secret.get("name") or actual_ref.get("key") != secret.get("key"):
            missing.append(env_name)
    passed = not missing
    checks.append(check("pod_contract_airflow_connection_secret_env", passed, "blocker", path))
    if not passed:
        blockers.append(
            issue(
                "pod_contract_airflow_connection_secret_env_missing",
                "Missing or mismatched AIRFLOW_CONN_* Secret env refs: " + ", ".join(missing),
                path,
            )
        )


def _append_env_bridge_checks(
    checks: list[Any],
    blockers: list[Any],
    *,
    bridge: Mapping[str, Any],
    pod_spec: Mapping[str, Any],
    path: str,
    check: Any,
    issue: Any,
) -> None:
    env_names = set(_base_env_by_name(pod_spec))
    required = [str(ref.get("env_name") or "") for ref in _env_refs(bridge) if str(ref.get("env_name") or "")]
    missing = [name for name in required if name not in env_names]
    passed = not missing
    checks.append(check("pod_contract_airflow_connection_env", passed, "blocker", path))
    if not passed:
        blockers.append(
            issue(
                "pod_contract_airflow_connection_env_missing",
                "Missing AIRFLOW_CONN_* env vars for env bridge mode: " + ", ".join(missing),
                path,
            )
        )


def _env_refs(bridge: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_env = bridge.get("env")
    if not isinstance(raw_env, list):
        return ()
    return tuple(item for item in raw_env if isinstance(item, Mapping))


def _base_env_by_name(pod_spec: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    container = _first_container(pod_spec)
    raw_env = container.get("env")
    if not isinstance(raw_env, list):
        return {}
    return {str(item.get("name")): item for item in raw_env if isinstance(item, Mapping) and item.get("name")}


def _first_container(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    spec = _mapping(payload.get("spec"))
    containers = spec.get("containers")
    if isinstance(containers, list) and containers and isinstance(containers[0], Mapping):
        return containers[0]
    return {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _check(factory: Any, name: str, passed: bool, severity: str, path: str) -> Any:
    return factory(name, passed, severity, name.replace("_", " "), path)


def _issue(factory: Any, code: str, message: str, path: str) -> Any:
    return factory(code, message, path)


__all__ = ["connection_bridge_pod_doctor_checks"]
