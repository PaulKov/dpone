"""Wire-version policy for indexed runtime artifact delivery."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError

_INDEX_SCHEMA_V1 = "dpone.airflow-deployment-index.v1"
_INDEX_SCHEMA_V2 = "dpone.airflow-deployment-index.v2"
_INDEX_SCHEMA_V3 = "dpone.airflow-deployment-index.v3"
_EXECUTABLE_INDEX_SCHEMAS = frozenset({_INDEX_SCHEMA_V2, _INDEX_SCHEMA_V3})
_DELIVERY_MODES = frozenset({"local_preview", "init_fetch", "shared_pvc", "embedded_bundle", "csi_volume", "inline"})


def validate_runtime_artifact_delivery(
    payload: Mapping[str, Any],
    *,
    path: Path | None,
) -> Mapping[str, Any]:
    """Return a mode projection allowed by the declared index wire."""

    value = _required_mapping(payload, "runtime_artifact_delivery", path)
    mode = value.get("mode")
    if not isinstance(mode, str) or not mode.strip():
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_MISSING",
            "runtime_artifact_delivery.mode is required",
            path=_path_text(path),
        )
    if mode not in _DELIVERY_MODES:
        raise _field_invalid("runtime_artifact_delivery.mode is unsupported", path)
    schema = payload.get("schema")
    if schema == _INDEX_SCHEMA_V1:
        if mode == "local_preview":
            return value
        if mode == "init_fetch":
            raise AirflowDeploymentIndexError(
                "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED",
                "wire v1 init_fetch must be regenerated as dpone.airflow-deployment-index.v2",
                path=_path_text(path),
            )
        raise _unsupported(path)
    if schema in _EXECUTABLE_INDEX_SCHEMAS:
        if mode != "init_fetch":
            raise _unsupported(path)
        return value
    return value


def _required_mapping(payload: Mapping[str, Any], key: str, path: Path | None) -> Mapping[str, Any]:
    if key not in payload:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_MISSING",
            f"{key} is required",
            path=_path_text(path),
        )
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            f"{key} must be an object",
            path=_path_text(path),
        )
    return value


def _field_invalid(message: str, path: Path | None) -> AirflowDeploymentIndexError:
    return AirflowDeploymentIndexError(
        "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
        message,
        path=_path_text(path),
    )


def _unsupported(path: Path | None) -> AirflowDeploymentIndexError:
    return AirflowDeploymentIndexError(
        "DPONE_RUNTIME_ARTIFACT_DELIVERY_MODE_UNSUPPORTED",
        "runtime_artifact_delivery.mode is unsupported for this index wire",
        path=_path_text(path),
    )


def _path_text(path: Path | None) -> str | None:
    return path.as_posix() if path else None


__all__ = ["validate_runtime_artifact_delivery"]
