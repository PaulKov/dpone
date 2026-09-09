"""Durable exact-occurrence history required before deployment cache deletion."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    atomic_write_json,
    read_regular_json_object,
    regular_file_identity,
)
from dpone.runtime.deployment_cache_retention_contracts import (
    AirflowLoaderAcknowledgement,
    DeploymentCacheRetentionApplyError,
)
from dpone.runtime.deployment_cache_retention_state_codec import (
    ACTIVATION_HISTORY_SCHEMA,
    DeploymentCacheRetentionStateError,
    build_activation_history,
    canonical_digest,
    canonical_uuid4,
    parse_activation_history,
    require_digest,
)

_LEGACY_SCHEMA = "dpone.deployment-cache-activation-history.v1"
_MAX_ENTRIES = 10_000
_MAX_HISTORY_BYTES = 4 * 1024 * 1024


class DeploymentCacheActivationHistory:
    """Upsert one verified activation occurrence through an fsynced control file."""

    def __init__(
        self,
        cache_root: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._cache_root = cache_root
        self._path = cache_root / ".retention-activation-history.v2.json"
        del clock  # Retained only for constructor compatibility; acknowledgement time is authoritative.

    def upsert(self, ack: AirflowLoaderAcknowledgement) -> str:
        return self.upsert_expected(ack, expected_revision=None)

    def prospective_revision(self, ack: AirflowLoaderAcknowledgement) -> str:
        """Derive the exact next revision without mutating control state."""

        return str(self._prospective_payload(ack)["revision"])

    def upsert_expected(
        self,
        ack: AirflowLoaderAcknowledgement,
        *,
        expected_revision: str | None,
    ) -> str:
        """Persist only the prospective revision bound into an applying receipt."""

        persisted = self._prospective_payload(ack)
        revision = str(persisted["revision"])
        if expected_revision is not None and revision != expected_revision:
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_CONFLICT",
                "activation history prospective revision differs from the applying receipt",
                path=self._path.as_posix(),
                details={"expected_revision": expected_revision, "actual_revision": revision},
            )
        atomic_write_json(self._path, persisted)
        return revision

    def revision_for(self, ack: AirflowLoaderAcknowledgement) -> str | None:
        """Return the current revision only when it already contains this occurrence."""

        activation_id = _canonical_uuid4(ack.activation_id)
        payload = self._read()
        existing = payload["entries"].get(activation_id)
        if existing is None:
            return None
        candidate = _entry(ack, verified_at=str(existing["verified_at"]))
        if _entry_identity(existing) != _entry_identity(candidate):
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_CONFLICT",
                "activation history already binds the occurrence to different deployment evidence",
                path=self._path.as_posix(),
            )
        return str(payload["revision"])

    def _prospective_payload(self, ack: AirflowLoaderAcknowledgement) -> dict[str, Any]:
        activation_id = _canonical_uuid4(ack.activation_id)
        payload = self._read()
        entries = dict(payload["entries"])
        existing = entries.get(activation_id)
        verified_at = str(existing["verified_at"]) if existing is not None else ack.acknowledged_at
        candidate = _entry(ack, verified_at=verified_at)
        if existing is not None and _entry_identity(existing) != _entry_identity(candidate):
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_CONFLICT",
                "activation history already binds the occurrence to different deployment evidence",
                path=self._path.as_posix(),
            )
        if existing is None:
            if len(entries) >= _MAX_ENTRIES:
                raise DeploymentCacheError(
                    "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_FULL",
                    "activation history reached its bounded local capacity",
                    path=self._path.as_posix(),
                )
            entries[activation_id] = candidate
        try:
            persisted = build_activation_history(entries, payload["legacy_v1_diagnostics"])
        except DeploymentCacheRetentionStateError as exc:
            raise _invalid_history("activation history update violates its published contract") from exc
        if (
            len(json.dumps(persisted, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"))
            > _MAX_HISTORY_BYTES
        ):
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_FULL",
                "activation history exceeds its bounded local byte capacity",
                path=self._path.as_posix(),
            )
        return persisted

    def _read(self) -> dict[str, Any]:
        if not self._path.exists():
            return {
                "schema": ACTIVATION_HISTORY_SCHEMA,
                "entries": {},
                "legacy_v1_diagnostics": self._legacy_diagnostics(),
            }
        self._require_bounded_file(self._path, label="deployment cache activation history")
        payload = read_regular_json_object(
            self._path,
            missing_code="DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
            invalid_code="DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
            label="deployment cache activation history",
            root=self._cache_root,
        )
        try:
            return parse_activation_history(payload)
        except DeploymentCacheRetentionStateError as exc:
            raise _invalid_history("deployment cache activation history is invalid", path=self._path) from exc

    def _legacy_diagnostics(self) -> list[dict[str, str]]:
        path = self._cache_root / ".retention-activation-history.v1.json"
        if not path.exists():
            return []
        self._require_bounded_file(path, label="legacy deployment cache activation history")
        payload = read_regular_json_object(
            path,
            missing_code="DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
            invalid_code="DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
            label="legacy deployment cache activation history",
            root=self._cache_root,
        )
        entries = payload.get("entries")
        if payload.get("schema") != _LEGACY_SCHEMA or not isinstance(entries, list) or len(entries) > _MAX_ENTRIES:
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
                "legacy activation history is invalid",
                path=path.as_posix(),
            )
        diagnostics: list[dict[str, str]] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise DeploymentCacheError(
                    "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
                    "legacy activation history entry is invalid",
                    path=path.as_posix(),
                )
            diagnostics.append({"entry_sha256": canonical_digest(entry)})
        return diagnostics

    def _require_bounded_file(self, path: Path, *, label: str) -> None:
        identity = regular_file_identity(
            path,
            root=self._cache_root,
            missing_code="DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
            invalid_code="DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
            label=label,
        )
        if identity.size_bytes > _MAX_HISTORY_BYTES:
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
                f"{label} exceeds the 4 MiB safety limit",
                path=path.as_posix(),
            )


def _entry(ack: AirflowLoaderAcknowledgement, *, verified_at: str) -> dict[str, Any]:
    return {
        "activation_id": _canonical_uuid4(ack.activation_id),
        "release_id": _canonical_digest(ack.release_id),
        "deployment_id": _canonical_digest(ack.deployment_id),
        "airflow_index_sha256": _canonical_digest(ack.airflow_index_sha256),
        "loaded_dag_ids": list(ack.loaded_dag_ids),
        "verified_at": verified_at,
    }


def _entry_identity(value: Mapping[str, object]) -> tuple[object, ...]:
    loaded = value.get("loaded_dag_ids")
    if not isinstance(loaded, list) or not all(isinstance(item, str) and item for item in loaded):
        raise DeploymentCacheError(
            "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
            "activation history DAG inventory is invalid",
        )
    verified_at = value.get("verified_at")
    if not isinstance(verified_at, str) or not verified_at:
        raise DeploymentCacheError(
            "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
            "activation history timestamp is invalid",
        )
    return (
        _canonical_uuid4(value.get("activation_id")),
        _canonical_digest(value.get("release_id")),
        _canonical_digest(value.get("deployment_id")),
        _canonical_digest(value.get("airflow_index_sha256")),
        tuple(loaded),
    )


def _canonical_uuid4(value: object) -> str:
    try:
        return canonical_uuid4(value)
    except DeploymentCacheRetentionStateError as exc:
        raise _invalid_history("activation history requires a canonical UUIDv4 occurrence") from exc


def _canonical_digest(value: object) -> str:
    try:
        return require_digest(value, "activation history identity")
    except DeploymentCacheRetentionStateError as exc:
        raise _invalid_history("activation history requires canonical sha256 identities") from exc


class DeploymentCacheRetentionHistoryBoundary:
    """Map receipt-first history persistence into stable apply errors."""

    def __init__(self, cache_root: Path, history: DeploymentCacheActivationHistory) -> None:
        self._path = cache_root / ".retention-activation-history.v2.json"
        self._history = history

    def prospective_revision(self, ack: AirflowLoaderAcknowledgement) -> str:
        try:
            return self._history.prospective_revision(ack)
        except (DeploymentCacheError, OSError) as exc:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_FAILED",
                "verified activation history revision could not be derived before receipt creation",
                path=self._path.as_posix(),
                details={"state_may_have_changed": False},
            ) from exc

    def persist(
        self,
        ack: AirflowLoaderAcknowledgement,
        *,
        expected_revision: str,
        operation_id: str,
    ) -> str:
        try:
            return self._history.upsert_expected(ack, expected_revision=expected_revision)
        except (DeploymentCacheError, OSError) as exc:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_FAILED",
                "verified activation history could not be committed after receipt creation",
                path=self._path.as_posix(),
                details={
                    "state_may_have_changed": True,
                    "operation_id": operation_id,
                    "activation_history_revision": expected_revision,
                },
            ) from exc

    def ensure(
        self,
        ack: AirflowLoaderAcknowledgement,
        *,
        expected_revision: str,
        operation_id: str,
    ) -> str:
        try:
            current = self._history.revision_for(ack)
            prospective = self._history.prospective_revision(ack) if current is None else current
        except (DeploymentCacheError, OSError) as exc:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_FAILED",
                "verified activation history could not derive replay authority",
                path=self._path.as_posix(),
                details={"state_may_have_changed": True, "operation_id": operation_id},
            ) from exc
        if prospective != expected_revision or current is not None:
            return prospective
        return self.persist(
            ack,
            expected_revision=expected_revision,
            operation_id=operation_id,
        )


def _invalid_history(message: str, *, path: Path | None = None) -> DeploymentCacheError:
    return DeploymentCacheError(
        "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_INVALID",
        message,
        path=path.as_posix() if path is not None else None,
    )


__all__ = ["DeploymentCacheActivationHistory", "DeploymentCacheRetentionHistoryBoundary"]
