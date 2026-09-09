"""Parse-safe operator diagnostics for self-service Airflow explain output."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_sha256_digest
from dpone.contracts.runtime_artifact_delivery import missing_init_fetch_delivery_paths
from dpone.readiness.airflow_bundle_identity import airflow_bundle_identity, airflow_bundle_versioning_check
from dpone.readiness.airflow_indexed_artifacts import (
    IndexedAirflowArtifactChecksumMismatch,
    IndexedAirflowArtifactReadError,
    read_indexed_airflow_artifact,
)

if TYPE_CHECKING:
    from dpone.readiness.airflow_active_index import ActiveAirflowIndexSnapshot

_INDEX_RELATIVE_PATH = ".dpone-cache/current/airflow-index.json"
_PARSE_SIDE_EFFECTS = {
    "network": False,
    "metadata_db": False,
    "airflow_variables": False,
    "airflow_connections": False,
    "vault": False,
    "kubernetes": False,
    "cache_refresh": False,
}


def build_operator_diagnostics(
    root: Path,
    *,
    snapshot: ActiveAirflowIndexSnapshot | None = None,
) -> dict[str, Any]:
    """Return local Airflow operator diagnostics without parse-time side effects."""

    active_snapshot = snapshot
    if active_snapshot is None:
        from dpone.readiness.airflow_active_index import load_active_index_snapshot

        active_snapshot = load_active_index_snapshot(root)
    diagnostics: dict[str, Any] = {
        "kind": "dpone.airflow-operator-diagnostics.v1",
        "status": "planned",
        "index_path": _INDEX_RELATIVE_PATH,
        "parse_side_effects": dict(_PARSE_SIDE_EFFECTS),
        "operator_pinning": "planned",
        "checks": [
            _check(
                "DPONE_OPERATOR_PARSE_SAFE",
                "passed",
                "Diagnostics are derived from local files only.",
            )
        ],
    }
    if active_snapshot.source == "missing":
        return _finalize_diagnostics(diagnostics)
    index = active_snapshot.index
    if index is None:
        diagnostics["status"] = "invalid"
        diagnostics["checks"].append(
            _check(
                active_snapshot.error_code or "DPONE_AIRFLOW_INDEX_INVALID",
                "failed",
                active_snapshot.error_message or "airflow-index.json is invalid.",
            )
        )
        return _finalize_diagnostics(diagnostics)
    delivery_issue = _runtime_artifact_delivery_issue(index)
    if delivery_issue is not None:
        diagnostics["status"] = "invalid"
        diagnostics["checks"].append(delivery_issue)
        return _finalize_diagnostics(diagnostics)
    bundle_identity = airflow_bundle_identity(index.get("airflow_bundle_ref"))
    identity_refs = {
        "release_id": index.get("release_id"),
        "deployment_id": index.get("deployment_id"),
        "binding_set_ref": index.get("binding_set_ref"),
        "connection_registry_ref": index.get("connection_registry_ref"),
        "credential_runtime_ref": index.get("credential_runtime_ref"),
        "runtime_image_digest": index.get("runtime_image_digest"),
    }
    invalid_identity_ref_names = tuple(
        name for name, value in identity_refs.items() if value is not None and not is_sha256_digest(value)
    )
    release_id = _digest_or_none(identity_refs["release_id"])
    deployment_id = _digest_or_none(identity_refs["deployment_id"])
    diagnostics.update(
        {
            "status": "materialized",
            "release_id": release_id,
            "deployment_id": deployment_id,
            "runtime_image_digest": _digest_or_none(identity_refs["runtime_image_digest"]),
            "binding_set_ref": _digest_or_none(identity_refs["binding_set_ref"]),
            "connection_registry_ref": _digest_or_none(identity_refs["connection_registry_ref"]),
            "credential_runtime_ref": _digest_or_none(identity_refs["credential_runtime_ref"]),
            "airflow_bundle_ref": index.get("airflow_bundle_ref"),
            "airflow_bundle": bundle_identity.to_dict() if bundle_identity is not None else None,
            "runtime_artifact_delivery": _mapping(index.get("runtime_artifact_delivery")),
            "operator_pinning": "pinned" if release_id and deployment_id else "incomplete",
        }
    )
    if invalid_identity_ref_names:
        diagnostics["checks"].append(
            _check(
                "DPONE_OPERATOR_DEPLOYMENT_IDENTITY_INVALID",
                "failed",
                "Airflow deployment index contains non-digest deployment identity refs: "
                f"{', '.join(invalid_identity_ref_names)}.",
            )
        )
    bundle_check = airflow_bundle_versioning_check(bundle_identity)
    if bundle_check is not None:
        diagnostics["checks"].append(bundle_check)
    workload_operators, workload_checks = _workload_operator_diagnostics(root, index)
    diagnostics["workload_operators"] = workload_operators
    diagnostics["checks"].extend(workload_checks)
    diagnostics["checks"].append(
        _check(
            "DPONE_OPERATOR_PINNING",
            "passed" if release_id and deployment_id else "failed",
            "release_id and deployment_id are present in the deployment index.",
        )
    )
    return _finalize_diagnostics(diagnostics)


def _workload_operator_diagnostics(
    root: Path, index: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    operators: list[dict[str, Any]] = []
    checks: list[dict[str, str]] = []
    for item in _mappings(index.get("workload_packs")):
        workload_id = str(item.get("id") or "")
        artifact_ref = str(item.get("artifact_ref") or "")
        try:
            data = read_indexed_airflow_artifact(root, item)
            pack = json.loads(data.decode("utf-8"))
        except IndexedAirflowArtifactChecksumMismatch:
            checks.append(
                _check(
                    "DPONE_OPERATOR_PACK_CHECKSUM_MISMATCH",
                    "failed",
                    f"workload pack checksum mismatch for {workload_id or artifact_ref}",
                )
            )
            continue
        except (
            IndexedAirflowArtifactReadError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            checks.append(
                _check(
                    "DPONE_OPERATOR_PACK_UNREADABLE",
                    "failed",
                    f"workload pack is not readable for {workload_id or artifact_ref}: {exc}",
                )
            )
            continue
        if not isinstance(pack, dict):
            checks.append(
                _check(
                    "DPONE_OPERATOR_PACK_INVALID",
                    "failed",
                    f"workload pack must be a JSON object for {workload_id or artifact_ref}",
                )
            )
            continue
        operators.append(_workload_operator_summary(workload_id=workload_id, pack=pack))
    return operators, checks


def _workload_operator_summary(*, workload_id: str, pack: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "workload_id": workload_id or _text(_mapping(pack.get("workload")).get("workload_id")),
        "task_id": _text(_mapping(pack.get("kpo_kwargs")).get("task_id")),
        "operator": "PinnedXComSidecarKubernetesPodOperator",
    }
    projection = _mapping(pack.get("connection_projection"))
    if _is_airflow_connection_secret_volume_projection(projection):
        bridge = _airflow_connection_bridge_summary(pack=pack, projection=projection)
        summary["operator"] = bridge["operator"]
        summary["airflow_connection_bridge"] = bridge
    return summary


def _airflow_connection_bridge_summary(*, pack: dict[str, Any], projection: dict[str, Any]) -> dict[str, Any]:
    cleanup_policy = _text(projection.get("cleanup_policy")) or "after_execute"
    deferrable = _pack_deferrable(pack)
    checks = [
        _check(
            "DPONE_AIRFLOW_CONNECTION_SECRET_VOLUME_OPERATOR",
            "passed",
            "Workload uses the operator-side Airflow Connection Secret-volume bridge.",
        ),
        _check(
            "DPONE_AIRFLOW_CONNECTION_SECRET_VALUES_ABSENT",
            "passed" if projection.get("secret_values") is False else "warning",
            "Projection contains Secret references only; URI values are resolved at task execution.",
        ),
    ]
    if deferrable and cleanup_policy == "after_execute":
        checks.append(
            _check(
                "DPONE_AIRFLOW_CONNECTION_DEFERRABLE_CLEANUP_POLICY",
                "failed",
                "Deferrable KPO execution must use cleanup_policy: retain with platform-managed cleanup; "
                "after_execute cleanup can remove the Secret before the deferred pod completes.",
            )
        )
    else:
        checks.append(
            _check(
                "DPONE_AIRFLOW_CONNECTION_DEFERRABLE_CLEANUP_POLICY",
                "passed",
                "Secret cleanup policy is compatible with the declared KPO execution mode.",
            )
        )
    return {
        "operator": "AirflowConnectionSecretVolumeKubernetesPodOperator",
        "projection_mode": _text(projection.get("mode")),
        "payload_format": _text(projection.get("payload_format")),
        "topology": "digest_only",
        "secret_ref_fingerprint": _secret_ref_fingerprint(
            {
                "secret_name": projection.get("secret_name"),
                "mount_path": projection.get("mount_path"),
            }
        ),
        "cleanup_policy": cleanup_policy,
        "deferrable": deferrable,
        "connections": [_projection_connection_summary(item) for item in _mappings(projection.get("connections"))],
        "checks": checks,
    }


def _projection_connection_summary(item: dict[str, Any]) -> dict[str, Any]:
    fields = _mapping(item.get("fields"))
    return {
        "connection_ref": _text(item.get("connection_ref")),
        "registry_connection_ref": _text(item.get("registry_connection_ref")),
        "connection_id": _text(item.get("connection_id")),
        "secret_ref_fingerprint": _secret_ref_fingerprint(
            {
                "secret_key": item.get("secret_key"),
                "mount_path": item.get("mount_path"),
            }
        ),
        "fields": sorted(str(key) for key in fields),
    }


def _is_airflow_connection_secret_volume_projection(projection: dict[str, Any]) -> bool:
    return (
        projection.get("mode") == "kubernetes_secret_volume"
        and projection.get("payload_format") == "airflow_connection_uri"
    )


def _pack_deferrable(pack: dict[str, Any]) -> bool:
    kpo_value = _mapping(pack.get("kpo_kwargs")).get("deferrable")
    if kpo_value is not None:
        return bool(kpo_value)
    return bool(_mapping(_mapping(pack.get("airflow")).get("execution")).get("deferrable"))


def _attach_check_summary(diagnostics: dict[str, Any]) -> None:
    checks = [*_mappings(diagnostics.get("checks")), *_nested_operator_checks(diagnostics)]
    summary = {
        "passed": sum(1 for check in checks if check.get("status") == "passed"),
        "warning": sum(1 for check in checks if check.get("status") == "warning"),
        "failed": sum(1 for check in checks if check.get("status") == "failed"),
    }
    diagnostics["summary"] = summary
    diagnostics["next_actions"] = _next_actions(checks)
    current_status = _text(diagnostics.get("status"))
    if current_status in {"invalid", "planned"}:
        return
    if summary["failed"]:
        diagnostics["status"] = "operator_issues"
    elif summary["warning"]:
        diagnostics["status"] = "operator_warnings"


def _finalize_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    _attach_check_summary(diagnostics)
    return diagnostics


def failed_operator_checks(diagnostics: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    """Return failed top-level and nested checks for aggregate explain health."""

    checks = [*_mappings(diagnostics.get("checks")), *_nested_operator_checks(dict(diagnostics))]
    return tuple(check for check in checks if check.get("status") == "failed")


def _nested_operator_checks(diagnostics: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for operator in _mappings(diagnostics.get("workload_operators")):
        bridge = _mapping(operator.get("airflow_connection_bridge"))
        checks.extend(_mappings(bridge.get("checks")))
    return checks


def _next_actions(checks: list[dict[str, Any]]) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    seen: set[str] = set()
    for check in checks:
        if check.get("status") != "failed":
            continue
        code = _text(check.get("code"))
        if not code or code in seen:
            continue
        action = _next_action_for_code(code)
        if action:
            actions.append({"code": code, "action": action, "safety": "manual"})
            seen.add(code)
    return actions


def _next_action_for_code(code: str) -> str:
    if code == "DPONE_AIRFLOW_INDEX_DELIVERY_INVALID":
        return "Regenerate the Airflow deployment index with a complete init_fetch delivery contract."
    if code == "DPONE_AIRFLOW_CONNECTION_DEFERRABLE_CLEANUP_POLICY":
        return (
            "Set connection_projection.cleanup_policy: retain for deferrable KPO tasks and configure "
            "platform-managed Secret cleanup."
        )
    if code == "DPONE_AIRFLOW_INDEX_INVALID":
        return "Regenerate the local Airflow preview/deployment index or resync the deployment cache."
    if code in {
        "DPONE_AIRFLOW_INDEX_TOO_LARGE",
        "DPONE_AIRFLOW_INDEX_UNSAFE",
        "DPONE_AIRFLOW_INDEX_UNAVAILABLE",
    }:
        return "Rebuild or resync the local deployment cache from a verified deployment."
    if code == "DPONE_OPERATOR_PINNING":
        return "Rebuild or republish the Airflow deployment index so release_id and deployment_id are both pinned."
    if code == "DPONE_OPERATOR_PACK_CHECKSUM_MISMATCH":
        return "Run cache sync or rebuild the local deployment cache; do not run from a modified workload pack."
    if code == "DPONE_OPERATOR_PACK_UNREADABLE":
        return (
            "Run cache sync or rebuild the local deployment cache so listed workload packs are present and valid JSON."
        )
    if code == "DPONE_OPERATOR_PACK_INVALID":
        return "Rebuild the release-set so every workload pack is a JSON object."
    if code == "DPONE_OPERATOR_DEPLOYMENT_IDENTITY_INVALID":
        return "Rebuild the Airflow deployment index so deployment identity refs are sha256 digests or null."
    return ""


def _runtime_artifact_delivery_issue(index: Mapping[str, Any]) -> dict[str, str] | None:
    missing_paths = missing_init_fetch_delivery_paths(_mapping(index.get("runtime_artifact_delivery")))
    if not missing_paths:
        return None
    return _check(
        "DPONE_AIRFLOW_INDEX_DELIVERY_INVALID",
        "failed",
        "Airflow deployment index has incomplete init_fetch delivery fields: " + ", ".join(missing_paths) + ".",
    )


def _mapping(value: Any) -> dict[str, Any]:
    return _json_copy(value) if isinstance(value, Mapping) else {}


def _mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [_mapping(item) for item in value if isinstance(item, Mapping)]


def _json_copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_copy(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_copy(item) for item in value]
    return value


def _check(code: str, status: str, message: str) -> dict[str, str]:
    return {"code": code, "status": status, "message": message}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _digest_or_none(value: Any) -> str | None:
    return str(value) if is_sha256_digest(value) else None


def _secret_ref_fingerprint(payload: dict[str, Any]) -> str:
    return canonical_fingerprint({key: _text(value) for key, value in payload.items() if _text(value)})


__all__ = ["build_operator_diagnostics", "failed_operator_checks"]
