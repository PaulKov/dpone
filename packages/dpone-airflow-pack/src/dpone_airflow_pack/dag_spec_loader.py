"""Bounded I/O adapter for scheduler-side ``gitops.airflow_dag_spec`` artifacts."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_artifact_contract import read_confined_cache_file
from dpone_airflow_pack.dag_spec_cache_paths import DAG_SPEC_DIRNAME, discover_dag_spec_paths, pinned_dag_specs
from dpone_airflow_pack.dag_spec_contract import (
    DAG_SPEC_KIND,
    DAG_SPEC_SCHEMA_VERSION,
    DEFAULT_MAX_DAG_SPEC_BYTES,
    DagSpecLoadIssue,
    compute_dag_spec_fingerprint,
    redact_parse_error_message,
)
from dpone_airflow_pack.dag_spec_validation import validate_dag_spec_payload
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.pack_storage_consumer import DagSpecCacheMissingError
from dpone_airflow_pack.strict_json import loads_strict_json_object


def _dag_spec_load_error(
    *,
    dag_id: str,
    message: str,
    source: str,
    error_code: str = "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED",
) -> dict[str, str]:
    return {
        "schema": "dpone.error.v1",
        "code": error_code,
        "stage": "airflow_parse",
        "severity": "error",
        "dag_id": dag_id,
        "message": redact_parse_error_message(message),
        "path": redact_parse_error_message(source),
    }


@dataclass(frozen=True)
class LoadedDagSpec:
    path: Path
    payload: Mapping[str, Any]
    dag_id: str


def discover_dag_specs(
    repo_root: Path,
    *,
    domains: Sequence[str] | None = None,
) -> tuple[LoadedDagSpec, ...]:
    specs: list[LoadedDagSpec] = []
    selected_domains = {str(item) for item in domains or ()}
    with pinned_dag_specs(repo_root) as pinned_specs:
        for spec in pinned_specs:
            path = spec.path
            payload, issues = load_dag_spec_file(
                path,
                expected_sha256=spec.expected_sha256,
                expected_bytes=spec.expected_bytes,
                expected_dag_id=spec.expected_dag_id,
                confined_root=spec.confined_root,
            )
            if issues:
                continue
            assert payload is not None
            domain = payload.get("domain")
            if selected_domains and domain and str(domain) not in selected_domains:
                continue
            specs.append(LoadedDagSpec(path=path, payload=payload, dag_id=str(payload["dag_id"])))
    return tuple(specs)


def _dag_spec_paths(repo_root: Path) -> tuple[Path, ...]:
    """Backward-compatible alias for cache/gitops dag-spec path discovery."""

    return discover_dag_spec_paths(repo_root)


def load_dag_spec_file(
    path: Path,
    *,
    expected_sha256: str | None = None,
    expected_bytes: int | None = None,
    expected_dag_id: str | None = None,
    max_bytes: int = DEFAULT_MAX_DAG_SPEC_BYTES,
    confined_root: Path | None = None,
) -> tuple[dict[str, Any] | None, tuple[DagSpecLoadIssue, ...]]:
    """Read one bounded, optional-cache-confined DAG spec and validate it."""

    label = path.as_posix()
    if max_bytes <= 0:
        return _load_failure("DPONE_AIRFLOW_DAG_SPEC_SIZE_LIMIT_INVALID", "max_bytes must be positive", label)
    if expected_bytes is not None and expected_bytes > max_bytes:
        return _load_failure(
            "DPONE_CACHE_ARTIFACT_TOO_LARGE",
            "dag-spec declared size exceeds configured size limit",
            label,
        )
    raw, failure = _read_dag_spec(
        path,
        confined_root=confined_root,
        max_bytes=max_bytes,
        label=label,
    )
    if failure is not None:
        return failure
    assert raw is not None
    identity_issue = _validate_raw_identity(
        raw,
        expected_sha256=expected_sha256,
        expected_bytes=expected_bytes,
        max_bytes=max_bytes,
        label=label,
    )
    if identity_issue is not None:
        return identity_issue
    try:
        payload = loads_strict_json_object(raw.decode("utf-8"))
    except UnicodeDecodeError:
        return _load_failure("DPONE_AIRFLOW_DAG_SPEC_JSON_INVALID", "dag-spec must be UTF-8 JSON", label)
    except (ValueError, RecursionError):
        return _load_failure("DPONE_AIRFLOW_DAG_SPEC_JSON_INVALID", "dag-spec is not strict JSON", label)
    issues = validate_dag_spec_payload(payload, label)
    if not issues and expected_dag_id is not None and payload.get("dag_id") != expected_dag_id:
        return _load_failure(
            "DPONE_AIRFLOW_DAG_SPEC_IDENTITY_MISMATCH",
            "dag-spec dag_id does not match the authorized pack-index key",
            label,
        )
    return (None, issues) if issues else (payload, ())


def _read_dag_spec(
    path: Path,
    *,
    confined_root: Path | None,
    max_bytes: int,
    label: str,
) -> tuple[bytes | None, tuple[None, tuple[DagSpecLoadIssue, ...]] | None]:
    if confined_root is not None:
        try:
            return (
                read_confined_cache_file(
                    path,
                    cache_root=confined_root,
                    max_bytes=max_bytes,
                ),
                None,
            )
        except AirflowDeploymentIndexError as exc:
            return None, _load_failure(exc.code, str(exc), label)
    try:
        with path.open("rb") as handle:
            return handle.read(max_bytes + 1), None
    except FileNotFoundError:
        return None, _load_failure("DPONE_AIRFLOW_DAG_SPEC_NOT_FOUND", "dag-spec file does not exist", label)
    except OSError:
        return None, _load_failure("DPONE_AIRFLOW_DAG_SPEC_READ_FAILED", "dag-spec file could not be read", label)


def _validate_raw_identity(
    raw: bytes,
    *,
    expected_sha256: str | None,
    expected_bytes: int | None,
    max_bytes: int,
    label: str,
) -> tuple[None, tuple[DagSpecLoadIssue, ...]] | None:
    if len(raw) > max_bytes:
        return _load_failure("DPONE_AIRFLOW_DAG_SPEC_TOO_LARGE", "dag-spec exceeds configured size limit", label)
    if expected_bytes is not None and len(raw) != expected_bytes:
        return _load_failure(
            "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
            "dag-spec size does not match deployment index",
            label,
        )
    if expected_sha256 is not None:
        actual_sha256 = "sha256:" + hashlib.sha256(raw).hexdigest()
        if actual_sha256 != expected_sha256:
            return _load_failure(
                "DPONE_CACHE_CHECKSUM_MISMATCH",
                "dag-spec checksum does not match deployment index",
                label,
            )
    return None


def _load_failure(
    code: str,
    message: str,
    path: str,
) -> tuple[None, tuple[DagSpecLoadIssue, ...]]:
    return None, (DagSpecLoadIssue(code, message, path),)


__all__ = [
    "DAG_SPEC_DIRNAME",
    "DAG_SPEC_KIND",
    "DAG_SPEC_SCHEMA_VERSION",
    "DEFAULT_MAX_DAG_SPEC_BYTES",
    "DagSpecCacheMissingError",
    "DagSpecLoadIssue",
    "LoadedDagSpec",
    "compute_dag_spec_fingerprint",
    "discover_dag_specs",
    "load_dag_spec_file",
    "redact_parse_error_message",
]
