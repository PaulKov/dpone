"""Bounded codecs and confined readers for deployment projection inputs."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

import yaml
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.init_fetch_pod_guard import validate_strict_pack_extensions
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    PackIdentityError,
    verify_pack_fingerprint,
)
from dpone_airflow_pack.provider_execution import require_provider_execution
from dpone_airflow_pack.xcom_sidecar import require_strict_xcom_sidecar_image

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.manifest.confined_files import ConfinedFileError
from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)

_ENVIRONMENT_SEGMENT = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")


class ConfinedReader(Protocol):
    def __call__(
        self,
        root: Path,
        relative_path: str,
        *,
        max_bytes: int,
    ) -> bytes: ...


def normalize_environment_segment(environment: object) -> str:
    """Return one bounded lowercase environment segment before path assembly."""

    if not isinstance(environment, str) or not _ENVIRONMENT_SEGMENT.fullmatch(environment):
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_ENVIRONMENT_INVALID",
            "environment must be one bounded lowercase logical name",
        )
    return environment


def capture_confined_source(
    root: Path,
    relative_path: str,
    *,
    path: Path,
    max_bytes: int,
    missing_code: str,
    too_large_code: str,
    changed_code: str,
    unsafe_code: str,
    reader: ConfinedReader,
    missing_message: str | None = None,
    unsafe_message: str = "release input could not be captured as a bounded regular-file snapshot",
) -> bytes:
    """Translate one descriptor-confined read into stable projection errors."""

    if missing_message is None:
        missing_message = (
            "release artifact file is missing" if "ARTIFACT" in missing_code else "required JSON file is missing"
        )
    try:
        return reader(root, relative_path, max_bytes=max_bytes)
    except ConfinedFileError as exc:
        error_code = {
            "file_not_found": missing_code,
            "file_too_large": too_large_code,
            "source_changed": changed_code,
        }.get(exc.code, unsafe_code)
        raise AirflowDeploymentProjectionError(
            error_code,
            missing_message if error_code == missing_code else unsafe_message,
            path=path.as_posix(),
        ) from exc
    except FileNotFoundError as exc:
        raise AirflowDeploymentProjectionError(
            missing_code,
            missing_message,
            path=path.as_posix(),
        ) from exc
    except OSError as exc:
        raise AirflowDeploymentProjectionError(
            unsafe_code,
            unsafe_message,
            path=path.as_posix(),
        ) from exc


def parse_json_object(
    raw: bytes,
    *,
    path: Path,
    invalid_code: str,
    label: str,
) -> dict[str, Any]:
    """Decode one strict UTF-8 JSON object without duplicate keys."""

    def unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result = dict(items)
        if len(result) != len(items):
            raise ValueError("duplicate JSON key")
        return result

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise AirflowDeploymentProjectionError(
            invalid_code,
            f"{label} must be one UTF-8 JSON object without duplicate keys",
            path=path.as_posix(),
        ) from exc
    if not isinstance(payload, dict):
        raise AirflowDeploymentProjectionError(
            invalid_code,
            f"{label} must contain one JSON object",
            path=path.as_posix(),
        )
    return payload


def read_environment_yaml(
    root: Path,
    relative_path: str,
    *,
    code_prefix: str,
    max_bytes: int,
    reader: ConfinedReader,
) -> dict[str, Any]:
    """Read one environment-owned YAML object through the shared confined reader."""

    path = root / relative_path
    raw = capture_confined_source(
        root,
        relative_path,
        path=path,
        max_bytes=max_bytes,
        missing_code=f"{code_prefix}_NOT_FOUND",
        too_large_code=f"{code_prefix}_TOO_LARGE",
        changed_code=f"{code_prefix}_CHANGED",
        unsafe_code=f"{code_prefix}_UNSAFE",
        reader=reader,
        missing_message="required environment YAML file is missing",
        unsafe_message="environment input could not be captured as a bounded regular-file snapshot",
    )
    try:
        payload = yaml.safe_load(raw.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError, RecursionError) as exc:
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_PROJECTION_INVALID",
            "environment input must be one UTF-8 YAML object",
            path=path.as_posix(),
        ) from exc
    if not isinstance(payload, dict):
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_PROJECTION_INVALID",
            "environment input must be one YAML object",
            path=path.as_posix(),
        )
    return payload


def workload_pack_fingerprint(
    content: bytes,
    *,
    payload: Mapping[str, Any] | None,
    path: Path,
    required: bool,
) -> str | None:
    """Read the strict fingerprint or preserve the explicit legacy-v1 omission."""

    if payload is None:
        try:
            payload = parse_json_object(
                content,
                path=path,
                invalid_code="DPONE_RELEASE_ARTIFACT_INVALID",
                label="workload_packs artifact",
            )
        except AirflowDeploymentProjectionError:
            if not required:
                return None
            raise
    fingerprint = payload.get("pack_fingerprint")
    identity = payload.get("pack_identity")
    if not required:
        return str(fingerprint) if is_canonical_sha256_digest(fingerprint) else None
    if (
        not isinstance(identity, Mapping)
        or dict(identity) != {"schema": PACK_IDENTITY_SCHEMA}
        or not is_canonical_sha256_digest(fingerprint)
    ):
        raise AirflowDeploymentProjectionError(
            "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED",
            "workload pack must be rebuilt with canonical whole-pack identity",
            path=path.as_posix(),
        )
    try:
        verified = verify_pack_fingerprint(payload)
    except PackIdentityError as exc:
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_ARTIFACT_INVALID",
            "workload pack fingerprint does not match canonical pack contents",
            path=path.as_posix(),
        ) from exc
    try:
        validate_strict_pack_extensions(payload)
        require_provider_execution(payload)
        require_strict_xcom_sidecar_image(payload)
    except InitFetchProviderError as exc:
        code = (
            exc.code
            if exc.code
            in {
                "DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED",
                "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID",
                "DPONE_INIT_FETCH_RESERVED_COLLISION",
            }
            else "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"
        )
        if code == "DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED":
            code = "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"
        raise AirflowDeploymentProjectionError(
            code,
            "workload pack does not satisfy the closed strict init-fetch contract",
            path=path.as_posix(),
        ) from exc
    return verified


def bytes_descriptor(*, artifact_ref: str, payload: bytes) -> dict[str, Any]:
    if not payload:
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_INCOMPLETE",
            "deployment descriptor is empty",
        )
    return {
        "artifact_ref": artifact_ref,
        "sha256": sha256_bytes(payload),
        "bytes": len(payload),
    }


def json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def digest_dir(digest: str) -> str:
    return digest.replace(":", "-")


def sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"unsupported JSON constant: {value}")


__all__ = [
    "bytes_descriptor",
    "capture_confined_source",
    "digest_dir",
    "json_bytes",
    "normalize_environment_segment",
    "parse_json_object",
    "read_environment_yaml",
    "sha256_bytes",
    "workload_pack_fingerprint",
]
