"""Bounded validation of exact desired-state reconcile diagnostics."""

from __future__ import annotations

import json
import re
import time
import unicodedata
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_artifact_contract import (
    read_confined_cache_file,
    read_confined_cache_file_with_identity,
)
from dpone_airflow_pack.deployment_index_contract import AirflowDeploymentIndexError

_MAX_RECONCILE_STATUS_BYTES = 8 * 1024 * 1024
_SUCCESS_SCHEMA = "dpone.airflow-desired-state-reconcile.v1"
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_ENVIRONMENT = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$")
_PROJECT_SEGMENT = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?$")
_DAG_ID = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,248}[A-Za-z0-9])?$")
_MAX_EXPECTED_DAGS = 5000
DEFAULT_MAX_RECONCILE_AGE_SECONDS = 180
_SUCCESS_FIELDS = frozenset(
    {
        "schema",
        "passed",
        "status",
        "environment",
        "observed_revision",
        "desired_state_sha256",
        "registry_scope_id",
        "source_project",
        "source_ref",
        "release_id",
        "deployment_id",
        "occurrence_id",
        "source_git_sha",
        "airflow_index_sha256",
        "runtime_image_digest",
        "expected_dag_ids",
        "activation_id",
        "previous_deployment_id",
        "predecessor_status",
        "materialized",
        "activated",
    }
)


def read_reconcile_status(
    root: Path,
    *,
    release_id: str | None,
    deployment_id: str | None,
    activation_id: str | None,
    required: bool,
    airflow_index_sha256: str | None = None,
    max_age_seconds: int = DEFAULT_MAX_RECONCILE_AGE_SECONDS,
) -> tuple[
    dict[str, Any],
    tuple[dict[str, str], ...],
    tuple[dict[str, str], ...],
]:
    """Read status and return fail-visible identity or content blockers."""

    path = root / "status" / "last-reconcile-status.json"
    blockers: list[dict[str, str]] = []
    try:
        snapshot = read_confined_cache_file_with_identity(
            path,
            cache_root=root,
            max_bytes=_MAX_RECONCILE_STATUS_BYTES,
        )
    except AirflowDeploymentIndexError as exc:
        if exc.code == "DPONE_CACHE_ARTIFACT_MISSING":
            missing = (
                (
                    _blocker(
                        "airflow_pack_reconcile_status_missing",
                        path,
                        "strict exact cache has no desired-state reconcile evidence",
                    ),
                )
                if required
                else ()
            )
            return ({}, missing, ()) if required else ({}, (), missing)
        return {}, (_blocker("airflow_pack_reconcile_status_invalid", path, str(exc)),), ()
    raw = snapshot.content
    age_seconds = max(0.0, time.time() - snapshot.modified_at_epoch)
    if max_age_seconds <= 0 or age_seconds > max_age_seconds:
        blockers.append(
            _blocker(
                "airflow_pack_reconcile_status_stale",
                path,
                "desired-state watcher status is older than the configured freshness bound",
            )
        )
    try:
        payload = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return (
            {},
            (
                _blocker(
                    "airflow_pack_reconcile_status_invalid",
                    path,
                    "reconcile status is invalid JSON",
                ),
            ),
            (),
        )
    if not isinstance(payload, dict):
        return (
            {},
            (
                _blocker(
                    "airflow_pack_reconcile_status_invalid",
                    path,
                    "reconcile status must be an object",
                ),
            ),
            (),
        )
    passed = payload.get("passed")
    if not isinstance(passed, bool):
        blockers.append(
            _blocker(
                "airflow_pack_reconcile_status_invalid",
                path,
                "reconcile status must contain a boolean passed field",
            )
        )
    elif passed is False:
        errors = payload.get("errors")
        first = errors[0] if isinstance(errors, list) and errors and isinstance(errors[0], Mapping) else {}
        code = (
            first.get("code") if isinstance(first.get("code"), str) else "DPONE_AIRFLOW_DESIRED_STATE_RECONCILE_FAILED"
        )
        blockers.append(_blocker("airflow_pack_reconcile_failed", path, str(code)))
    elif not _valid_success_payload(payload):
        blockers.append(
            _blocker(
                "airflow_pack_reconcile_status_invalid",
                path,
                "successful reconcile status violates its closed evidence contract",
            )
        )
    elif any(
        payload.get(field) != expected
        for field, expected in {
            "release_id": release_id,
            "deployment_id": deployment_id,
            "activation_id": activation_id,
            "airflow_index_sha256": airflow_index_sha256,
        }.items()
        if expected is not None
    ):
        blockers.append(
            _blocker(
                "airflow_pack_reconcile_identity_mismatch",
                path,
                "last successful reconcile status differs from the active cache identity",
            )
        )
    elif not _checkpoint_matches(root, payload):
        blockers.append(
            _blocker(
                "airflow_pack_reconcile_checkpoint_mismatch",
                root / "status" / "desired-state-checkpoint.json",
                "desired-state checkpoint differs from the latest successful reconcile evidence",
            )
        )
    return dict(payload), tuple(blockers), ()


def _checkpoint_matches(root: Path, status: Mapping[str, Any]) -> bool:
    path = root / "status" / "desired-state-checkpoint.json"
    try:
        raw = read_confined_cache_file(path, cache_root=root, max_bytes=_MAX_RECONCILE_STATUS_BYTES)
        checkpoint = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (AirflowDeploymentIndexError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return False
    fields = (
        "environment",
        "observed_revision",
        "desired_state_sha256",
        "registry_scope_id",
        "source_project",
        "source_ref",
        "release_id",
        "deployment_id",
        "occurrence_id",
        "source_git_sha",
        "airflow_index_sha256",
        "runtime_image_digest",
        "expected_dag_ids",
        "activation_id",
    )
    return isinstance(checkpoint, Mapping) and all(checkpoint.get(field) == status.get(field) for field in fields)


def _valid_success_payload(payload: Mapping[str, Any]) -> bool:
    if set(payload) != _SUCCESS_FIELDS or payload.get("schema") != _SUCCESS_SCHEMA:
        return False
    status = payload.get("status")
    activated = payload.get("activated")
    materialized = payload.get("materialized")
    if status not in {"activated", "recovered", "unchanged"}:
        return False
    if not isinstance(activated, bool) or not isinstance(materialized, bool):
        return False
    if activated != (status == "activated") or materialized != activated:
        return False
    if payload.get("predecessor_status") not in {"bootstrap", "continuous", "recovered", "skipped", "unchanged"}:
        return False
    if (
        not _canonical_text(payload.get("environment"), maximum=64)
        or _ENVIRONMENT.fullmatch(payload["environment"]) is None
    ):
        return False
    if not _canonical_text(payload.get("observed_revision"), maximum=1024):
        return False
    if not _canonical_text(payload.get("source_ref"), maximum=256):
        return False
    project = payload.get("source_project")
    if (
        not isinstance(project, str)
        or not _canonical_text(project, maximum=512)
        or any(_PROJECT_SEGMENT.fullmatch(part) is None for part in project.split("/"))
        or "/" not in project
    ):
        return False
    digest_fields = (
        "desired_state_sha256",
        "registry_scope_id",
        "release_id",
        "deployment_id",
        "airflow_index_sha256",
        "runtime_image_digest",
    )
    if any(
        not isinstance(payload.get(field), str) or _DIGEST.fullmatch(payload[field]) is None for field in digest_fields
    ):
        return False
    if not isinstance(payload.get("source_git_sha"), str) or _GIT_SHA.fullmatch(payload["source_git_sha"]) is None:
        return False
    for field in ("occurrence_id", "activation_id"):
        if not isinstance(payload.get(field), str) or _UUID_V4.fullmatch(payload[field]) is None:
            return False
    previous = payload.get("previous_deployment_id")
    if previous is not None and (not isinstance(previous, str) or _DIGEST.fullmatch(previous) is None):
        return False
    dags = payload.get("expected_dag_ids")
    return bool(
        isinstance(dags, list)
        and 1 <= len(dags) <= _MAX_EXPECTED_DAGS
        and len(dags) == len(set(dags))
        and dags == sorted(dags)
        and all(_canonical_text(dag_id, maximum=250) and _DAG_ID.fullmatch(dag_id) for dag_id in dags)
    )


def _canonical_text(value: object, *, maximum: int) -> bool:
    return bool(
        isinstance(value, str)
        and value
        and value == value.strip()
        and len(value) <= maximum
        and not any(unicodedata.category(char).startswith("C") for char in value)
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("reconcile status contains duplicate fields")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("reconcile status contains a non-finite number")


def _blocker(code: str, path: Path, message: str) -> dict[str, str]:
    return {"code": code, "path": str(path), "message": message}


__all__ = ["DEFAULT_MAX_RECONCILE_AGE_SECONDS", "read_reconcile_status"]
