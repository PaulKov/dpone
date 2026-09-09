"""Bounded loader-ack diagnostics for an exact Airflow cache snapshot."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_status_files import read_bounded_regular_file
from dpone_airflow_pack.loader_ack import LoaderAcknowledgementError, parse_loader_ack_json

_MAX_ACK_BYTES = 64 * 1024


def attach_loader_ack_status(
    status: Mapping[str, Any],
    *,
    cache_root: Path,
    ack_path: Path,
    ack_root: Path,
) -> dict[str, Any]:
    """Attach one external ACK and fail visibly on unsafe or stale evidence."""

    result = dict(status)
    warnings = list(result.get("warnings") or [])
    blockers = list(result.get("blockers") or [])
    payload: dict[str, Any] = {}
    try:
        _validate_roots(cache_root=cache_root, ack_path=ack_path, ack_root=ack_root)
        raw = read_bounded_regular_file(ack_path, max_bytes=_MAX_ACK_BYTES)
        payload = parse_loader_ack_json(raw).to_jsonable()
        _validate_ack_identity(payload, result)
    except FileNotFoundError:
        warnings.append(_issue("airflow_loader_ack_missing", ack_path, "loader ACK is not available yet"))
    except (OSError, ValueError, LoaderAcknowledgementError) as exc:
        blockers.append(_issue("airflow_loader_ack_invalid", ack_path, str(exc)))
    result["loader_ack"] = payload
    result["warnings"] = warnings
    result["blockers"] = blockers
    result["status"] = "blocked" if blockers else ("warning" if warnings else "success")
    return result


def _validate_roots(*, cache_root: Path, ack_path: Path, ack_root: Path) -> None:
    if ack_path.absolute().parent != ack_root.absolute():
        raise ValueError("loader ACK must stay directly under its configured root")
    cache = cache_root.resolve(strict=True)
    external = ack_root.resolve(strict=True)
    if external == cache or external.is_relative_to(cache) or cache.is_relative_to(external):
        raise ValueError("loader ACK root must be separate from cache root")


def _validate_ack_identity(payload: Mapping[str, Any], status: Mapping[str, Any]) -> None:
    expected_index = status.get("index_sha256")
    if isinstance(expected_index, str) and not expected_index.startswith("sha256:"):
        expected_index = "sha256:" + expected_index
    expected = {
        "release_id": status.get("release_id"),
        "deployment_id": status.get("deployment_id"),
        "activation_id": status.get("activation_id"),
        "airflow_index_sha256": expected_index,
    }
    if (
        payload.get("fatal") is not False
        or payload.get("error_codes")
        or payload.get("skipped_dag_ids")
        or any(payload.get(field) != value for field, value in expected.items())
    ):
        raise ValueError("loader ACK differs from the active cache identity")
    reconcile = status.get("last_reconcile_status")
    expected_dag_ids = reconcile.get("expected_dag_ids") if isinstance(reconcile, Mapping) else None
    if isinstance(expected_dag_ids, list):
        acknowledged = set(payload.get("loaded_dag_ids", ()))
        if acknowledged != set(expected_dag_ids):
            raise ValueError("loader ACK does not cover every DAG expected by the active deployment")


def _issue(code: str, path: Path, message: str) -> dict[str, str]:
    return {"code": code, "path": str(path), "message": message}


__all__ = ["attach_loader_ack_status"]
