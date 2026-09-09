"""Human-readable rendering for Airflow deployment build output."""

from __future__ import annotations

import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path


def self_service_airflow_build_text(payload: Mapping[str, object]) -> str | None:
    """Render environment deployment projection output for platform operators."""

    deployment = _mapping(payload.get("deployment"))
    airflow_index = _mapping(payload.get("airflow_index"))
    if deployment is None or airflow_index is None:
        return _failed_build_text(payload)

    environment = _optional_text(deployment.get("environment")) or "unknown"
    deployment_id = _optional_text(deployment.get("deployment_id"))
    release_id = _optional_text(deployment.get("release_ref")) or _optional_text(payload.get("release_id"))
    runtime_delivery = (
        airflow_index.get("runtime_artifact_delivery")
        if isinstance(airflow_index.get("runtime_artifact_delivery"), Mapping)
        else {}
    )
    assert isinstance(runtime_delivery, Mapping)
    deployment_dir = _repo_relative_path(payload.get("deployment_dir"))
    airflow_index_path = f"{deployment_dir}/airflow-index.json" if deployment_dir else ""
    lines = [
        "dpone airflow build: OK",
        f"- environment: {environment}",
    ]
    if release_id:
        lines.append(f"- release: {release_id}")
    if deployment_id:
        lines.append(f"- deployment: {deployment_id}")
    lines.extend(
        [
            f"- runnable: {_yes_no(deployment.get('runnable'))}",
            f"- dag specs: {len(_items(airflow_index.get('dag_specs')))}",
            f"- workload packs: {len(_items(airflow_index.get('workload_packs')))}",
        ]
    )
    delivery_mode = _optional_text(runtime_delivery.get("mode"))
    if delivery_mode:
        lines.append(f"- runtime delivery: {delivery_mode}")
    artifact_registry_ref = _optional_text(runtime_delivery.get("artifact_registry_ref"))
    if artifact_registry_ref:
        lines.append(f"- artifact registry: {artifact_registry_ref}")
    airflow_bundle_ref = _optional_text(airflow_index.get("airflow_bundle_ref"))
    if airflow_bundle_ref:
        lines.append(f"- airflow bundle: {airflow_bundle_ref}")
    if deployment_dir:
        lines.append(f"- output: {deployment_dir}")
    if airflow_index_path:
        lines.append(f"- airflow index: {airflow_index_path}")
    if deployment_dir:
        lines.append(_cache_sync_action(deployment_dir, environment))
    lines.append("- details: rerun with --format json for fingerprints and full deployment projection")
    return "\n".join(lines) + "\n"


def _failed_build_text(payload: Mapping[str, object]) -> str | None:
    errors = _error_items(payload.get("errors"))
    if not errors:
        return None
    lines = [
        "dpone airflow build: FAILED",
        f"- environment: {_optional_text(payload.get('environment')) or 'unknown'}",
    ]
    release_id = _release_id(payload)
    if release_id:
        lines.append(f"- release: {release_id}")
    lines.extend(_issue_lines(errors))
    lines.append(_failure_action(errors))
    lines.append("- details: rerun with --format json for structured errors and projection diagnostics")
    return "\n".join(lines) + "\n"


def _release_id(payload: Mapping[str, object]) -> str:
    release_id = _optional_text(payload.get("release_id"))
    if release_id:
        return release_id
    for error in _error_items(payload.get("errors")):
        entity = _mapping(error.get("entity"))
        if entity is not None and entity.get("kind") == "release":
            return _optional_text(entity.get("id"))
    return ""


def _issue_lines(errors: tuple[Mapping[str, object], ...]) -> list[str]:
    lines: list[str] = []
    for error in errors:
        code = _optional_text(error.get("code")) or "DPONE_AIRFLOW_BUILD_ERROR"
        message = _optional_text(error.get("message"))
        lines.append(f"- issue: {code}: {message}" if message else f"- issue: {code}")
    return lines


def _failure_action(errors: tuple[Mapping[str, object], ...]) -> str:
    fix_ids = {fix_id for error in errors for fix_id in _fix_ids(error.get("fixes"))}
    if "materialize_release_set" in fix_ids:
        return "- action: materialize release-set, then rerun dpone airflow build"
    if "create_binding_set" in fix_ids:
        return "- action: create binding-set for the environment, then rerun dpone airflow build"
    if "create_connection_registry" in fix_ids:
        return "- action: create connection registry for the environment, then rerun dpone airflow build"
    if "create_credential_runtime" in fix_ids:
        return "- action: create credential-runtime for the environment, then rerun dpone airflow build"
    return "- action: fix the reported projection input, then rerun dpone airflow build"


def _error_items(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _fix_ids(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(
        fix_id for item in value if isinstance(item, Mapping) for fix_id in [_optional_text(item.get("id"))] if fix_id
    )


def _items(value: object) -> tuple[object, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(value)


def _mapping(value: object) -> Mapping[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    return value


def _repo_relative_path(value: object) -> str:
    raw = _optional_text(value)
    if not raw:
        return ""
    path = Path(raw)
    try:
        return path.resolve(strict=False).relative_to(Path.cwd().resolve(strict=False)).as_posix()
    except ValueError:
        return path.name


def _cache_sync_action(deployment_dir: str, environment: str) -> str:
    identity = '"${DPONE_CI_IDENTITY:?set DPONE_CI_IDENTITY}"'
    return (
        "- action: dpone airflow cache-sync"
        f" --deployment-dir {shlex.quote(deployment_dir)}"
        f" --environment {shlex.quote(environment)}"
        f" --promoted-by {identity}"
        f" --allowed-promoter {identity}"
        " --expect-current-absent"
        " --confirm-promote"
    )


def _yes_no(value: object) -> str:
    return "yes" if value is True else "no"


def _optional_text(value: object) -> str:
    return str(value or "").strip()


__all__ = ["self_service_airflow_build_text"]
