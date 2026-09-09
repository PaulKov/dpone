"""Sequence-aware diagnostic evidence for legacy cache synchronization."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from dpone_airflow_pack.airflow_metadata import AirflowVariableStatusPublisher
from dpone_airflow_pack.artifact_uri_redaction import (
    redact_artifact_uri,
    redact_artifact_uris_in_text,
)
from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.cache_authority import read_legacy_cache_authority
from dpone_airflow_pack.cache_generation_files import read_control_json, write_json_durable
from dpone_airflow_pack.cache_layout import LEGACY_PACK_INDEX_LAYOUT, read_cache_layout
from dpone_airflow_pack.cache_permissions import SHARED_CONTROL_MODE
from dpone_airflow_pack.cache_writer_coordination import cache_evidence_lease
from dpone_airflow_pack.dag_spec_contract import redact_parse_error_message
from dpone_airflow_pack.diagnostic_warnings import emit_nonfatal_runtime_warning


class CacheSyncEvidenceOptions(Protocol):
    @property
    def index_uri(self) -> str: ...

    @property
    def cache_dir(self) -> Path: ...

    @property
    def status_path(self) -> Path | None: ...

    @property
    def airflow_variable_key(self) -> str | None: ...


def publish_sync_evidence(
    options: CacheSyncEvidenceOptions,
    *,
    started_at: str,
    attempt_generation: str,
    downloaded: int,
    downloaded_specs: int,
    committed: bool,
    warnings: list[str],
    blockers: list[dict[str, object]],
    cache_bytes: int,
) -> dict[str, Any]:
    """Publish a current-receipt projection without stale concurrent overwrite."""

    status_path = options.status_path or options.cache_dir / "status" / "last-sync-status.json"
    with cache_evidence_lease(options.cache_dir):
        with cache_write_lease(options.cache_dir):
            previous_success = _read_last_success_at(status_path)
            authority = read_legacy_cache_authority(options.cache_dir)
            receipt = authority.receipt
            effective_blockers = list(blockers)
            if receipt is not None and receipt.durable and not authority.is_consistent_durable:
                effective_blockers.append(
                    {
                        "code": "airflow_pack_cache_authority_mismatch",
                        "message": "receipt, current, and current_generation disagree",
                    }
                )
            if (receipt is None or not receipt.durable) and not effective_blockers:
                raise ValueError("airflow_pack_cache_commit_missing: current generation has no durable receipt")
            finished_at = _utc_now()
            evidence = _redact_evidence(
                _sync_evidence(
                    options,
                    receipt=receipt,
                    started_at=started_at,
                    attempt_generation=attempt_generation,
                    downloaded=downloaded,
                    downloaded_specs=downloaded_specs,
                    committed=committed,
                    warnings=warnings,
                    blockers=effective_blockers,
                    cache_bytes=cache_bytes,
                    finished_at=finished_at,
                    last_success_at=finished_at if not effective_blockers else previous_success,
                )
            )
            _write_status_best_effort(status_path, evidence)
        published = _redact_evidence(AirflowVariableStatusPublisher(options.airflow_variable_key).publish(evidence))
        with cache_write_lease(options.cache_dir):
            _write_status_best_effort(status_path, published)
    return published


def write_sync_warning(
    options: CacheSyncEvidenceOptions,
    *,
    reason: str,
    message: str,
) -> dict[str, Any]:
    """Publish one bounded, redacted fail-open diagnostic."""

    status_path = options.status_path or options.cache_dir / "status" / "last-sync-status.json"
    try:
        legacy = options.cache_dir.exists() and read_cache_layout(options.cache_dir) == LEGACY_PACK_INDEX_LAYOUT
    except Exception:  # noqa: BLE001 - reporting cannot mutate an incompatible root.
        legacy = False
    if not legacy:
        return _publish_external_warning(options, reason=reason, message=message, status_path=status_path)
    with cache_evidence_lease(options.cache_dir):
        evidence: dict[str, Any] = {
            "kind": "dpone.airflow_pack_cache_status",
            "schema_version": "1",
            "status": "warning",
            "reason": reason,
            "component": _component(),
            "attempted_at": _utc_now(),
            "finished_at": _utc_now(),
            "last_success_at": _read_last_success_at(status_path),
            "index_uri": redact_artifact_uri(options.index_uri),
            "cache_dir": str(options.cache_dir),
            "pod_name": os.environ.get("DPONE_AIRFLOW_PACK_SYNC_POD_NAME"),
            "pod_namespace": os.environ.get("DPONE_AIRFLOW_PACK_SYNC_NAMESPACE"),
            "warnings": [reason],
            "blockers": [],
            "message": redact_artifact_uris_in_text(redact_parse_error_message(message)),
            "diagnostic_authority": "local_commit_receipt",
        }
        evidence = _redact_evidence(evidence)
        try:
            with cache_write_lease(options.cache_dir):
                current = read_legacy_cache_authority(options.cache_dir).receipt
                if current is not None:
                    evidence["current_generation"] = current.generation
                    evidence["commit_id"] = current.commit_id
                    evidence["commit_sequence"] = current.sequence
                _write_status_best_effort(status_path, evidence)
        except Exception:  # noqa: BLE001 - current may intentionally be blocked/uncertain.
            pass
        published = _redact_evidence(AirflowVariableStatusPublisher(options.airflow_variable_key).publish(evidence))
        try:
            with cache_write_lease(options.cache_dir):
                _write_status_best_effort(status_path, published)
        except Exception:  # noqa: BLE001 - fail-open wrapper cannot raise on diagnostics.
            pass
    return published


def _publish_external_warning(
    options: CacheSyncEvidenceOptions,
    *,
    reason: str,
    message: str,
    status_path: Path,
) -> dict[str, Any]:
    """Publish diagnostics without touching an absent or incompatible cache root."""

    now = _utc_now()
    evidence = _redact_evidence(
        {
            "kind": "dpone.airflow_pack_cache_status",
            "schema_version": "1",
            "status": "warning",
            "reason": reason,
            "component": _component(),
            "attempted_at": now,
            "finished_at": now,
            "last_success_at": _read_last_success_at(status_path),
            "index_uri": redact_artifact_uri(options.index_uri),
            "cache_dir": str(options.cache_dir),
            "pod_name": os.environ.get("DPONE_AIRFLOW_PACK_SYNC_POD_NAME"),
            "pod_namespace": os.environ.get("DPONE_AIRFLOW_PACK_SYNC_NAMESPACE"),
            "warnings": [reason],
            "blockers": [],
            "message": redact_artifact_uris_in_text(redact_parse_error_message(message)),
            "diagnostic_authority": "external_only",
            "status_path": str(status_path),
        }
    )
    published = _redact_evidence(AirflowVariableStatusPublisher(options.airflow_variable_key).publish(evidence))
    if not options.airflow_variable_key or published.get("airflow_variable_published") is not True:
        emit_nonfatal_runtime_warning(
            f"DPONE_AIRFLOW_PACK_EXTERNAL_WARNING_UNPUBLISHED:{reason}",
            stacklevel=2,
        )
    return published


def _sync_evidence(
    options: CacheSyncEvidenceOptions,
    *,
    receipt: Any,
    started_at: str,
    attempt_generation: str,
    downloaded: int,
    downloaded_specs: int,
    committed: bool,
    warnings: list[str],
    blockers: list[dict[str, object]],
    cache_bytes: int,
    finished_at: str,
    last_success_at: str | None,
) -> dict[str, Any]:
    status = "blocked" if blockers else ("warning" if warnings else "success")
    return {
        "kind": "dpone.airflow_pack_cache_status",
        "schema_version": "1",
        "status": status,
        "reason": (
            "sync_blocked_before_commit"
            if receipt is None
            else ("sync_completed" if receipt.generation == attempt_generation else "sync_superseded")
        ),
        "component": _component(),
        "attempted_at": started_at,
        "finished_at": finished_at,
        "last_success_at": last_success_at,
        "index_uri": redact_artifact_uri(options.index_uri),
        "cache_dir": str(options.cache_dir),
        "pod_name": os.environ.get("DPONE_AIRFLOW_PACK_SYNC_POD_NAME"),
        "pod_namespace": os.environ.get("DPONE_AIRFLOW_PACK_SYNC_NAMESPACE"),
        "remote_latest_git_sha": attempt_generation,
        "attempt_generation": attempt_generation,
        "current_generation": receipt.generation if receipt is not None else None,
        "commit_id": receipt.commit_id if receipt is not None else None,
        "commit_sequence": receipt.sequence if receipt is not None else None,
        "index_sha256": receipt.index_sha256 if receipt is not None else None,
        "generation_committed": committed,
        "cache_bytes": cache_bytes,
        "downloaded_pack_count": downloaded,
        "downloaded_dag_spec_count": downloaded_specs,
        "warnings": list(dict.fromkeys(warnings)),
        "blockers": blockers,
        "diagnostic_authority": "local_commit_receipt",
    }


def _write_status_best_effort(path: Path, evidence: dict[str, Any]) -> None:
    try:
        write_json_durable(path, evidence, mode=SHARED_CONTROL_MODE)
    except OSError:
        evidence["warnings"] = [*list(evidence.get("warnings") or []), "cache_status_write_failed"]
        evidence["status"] = "blocked" if evidence.get("blockers") else "warning"


def _read_last_success_at(path: Path) -> str | None:
    try:
        payload = read_control_json(path)
    except (FileNotFoundError, OSError, TypeError, ValueError):
        return None
    value = payload.get("last_success_at")
    if isinstance(value, str) and value:
        return value
    if payload.get("status") in {"success", "warning"}:
        finished_at = payload.get("finished_at")
        if isinstance(finished_at, str) and finished_at:
            return finished_at
    return None


def _component() -> str:
    return os.environ.get("DPONE_AIRFLOW_PACK_SYNC_COMPONENT") or "unspecified"


def _redact_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _redact_evidence_value(value) for key, value in evidence.items()}


def _redact_evidence_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_artifact_uris_in_text(value)
    if isinstance(value, Mapping):
        return _redact_evidence(value)
    if isinstance(value, list):
        return [_redact_evidence_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_evidence_value(item) for item in value)
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")  # noqa: UP017


__all__ = ["publish_sync_evidence", "write_sync_warning"]
