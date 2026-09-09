"""Kubernetes base-container env channel for Airflow launch envelopes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.launch_pin_codes import PIN_INVALID
from dpone_airflow_pack.xcom_sidecar import XCOM_SIDECAR_CONTAINER_NAME


def base_container_env_map(pod: Any) -> dict[str, str]:
    """Read literal values from the selected Pod's base-container env."""

    containers = _pod_containers(pod)
    if not containers:
        return {}
    base = next((item for item in containers if _container_name(item) == "base"), None)
    if base is None:
        non_sidecar = [item for item in containers if _container_name(item) != XCOM_SIDECAR_CONTAINER_NAME]
        base = non_sidecar[0] if len(non_sidecar) == 1 else (containers[0] if len(containers) == 1 else None)
    if base is None:
        raise RuntimeError(
            f"{PIN_INVALID}: selected pod must expose a base container or a single non-sidecar container"
        )
    env = base.get("env") if isinstance(base, Mapping) else getattr(base, "env", None)
    result: dict[str, str] = {}
    for item in env or ():
        if isinstance(item, Mapping):
            name, value = item.get("name"), item.get("value")
        else:
            name, value = getattr(item, "name", None), getattr(item, "value", None)
        # Envelope authority reads literal ``value`` only; valueFrom secrets are
        # preserved on merge but are not launch-envelope identity carriers.
        if isinstance(name, str) and value is not None:
            result[name] = str(value)
    return result


def merge_base_container_env(pod: Any, env_vars: Mapping[str, str]) -> None:
    """Merge literal envelope values without dropping unrelated valueFrom env."""

    containers = _pod_containers(pod)
    if not containers:
        raise RuntimeError(f"{PIN_INVALID}: cannot persist launch envelope; pod has no containers")
    target = next((item for item in containers if _container_name(item) == "base"), None)
    if target is None:
        non_sidecar = [item for item in containers if _container_name(item) != XCOM_SIDECAR_CONTAINER_NAME]
        target = non_sidecar[0] if non_sidecar else containers[0]
    existing = _env_items(target)
    by_name = {item["name"]: item for item in existing if isinstance(item.get("name"), str)}
    for name, value in env_vars.items():
        by_name[name] = {"name": name, "value": value}
    merged = list(by_name.values())
    if isinstance(target, dict):
        target["env"] = merged
    else:
        target.env = merged


def _pod_containers(pod: Any) -> list[Any]:
    spec = getattr(pod, "spec", None)
    if spec is None and isinstance(pod, Mapping):
        spec = pod.get("spec")
    if isinstance(spec, Mapping):
        containers = spec.get("containers") or []
    else:
        containers = getattr(spec, "containers", None) or []
    return list(containers)


def _container_name(container: Any) -> str:
    if isinstance(container, Mapping):
        return str(container.get("name") or "")
    return str(getattr(container, "name", "") or "")


def _env_items(container: Any) -> list[dict[str, Any]]:
    env = container.get("env") if isinstance(container, Mapping) else getattr(container, "env", None)
    items: list[dict[str, Any]] = []
    for item in env or ():
        if isinstance(item, Mapping):
            name = item.get("name")
            if not isinstance(name, str) or not name:
                continue
            if "valueFrom" in item and item.get("value") is None:
                items.append({"name": name, "valueFrom": _mapping_value_from(item.get("valueFrom"))})
                continue
            if item.get("value") is not None:
                entry: dict[str, Any] = {"name": name, "value": str(item.get("value"))}
                if "valueFrom" in item:
                    entry["valueFrom"] = _mapping_value_from(item.get("valueFrom"))
                items.append(entry)
            continue
        name = getattr(item, "name", None)
        if not isinstance(name, str) or not name:
            continue
        value = getattr(item, "value", None)
        value_from = getattr(item, "value_from", None)
        if value_from is not None and value is None:
            items.append({"name": name, "valueFrom": _mapping_value_from(value_from)})
            continue
        if value is not None:
            entry = {"name": name, "value": str(value)}
            if value_from is not None:
                entry["valueFrom"] = _mapping_value_from(value_from)
            items.append(entry)
    return items


def _mapping_value_from(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "to_dict"):
        try:
            payload = value.to_dict()
            return dict(payload) if isinstance(payload, Mapping) else {"raw": str(value)}
        except Exception:  # noqa: BLE001
            return {"raw": str(value)}
    return {"raw": str(value)}


__all__ = ["base_container_env_map", "merge_base_container_env"]
