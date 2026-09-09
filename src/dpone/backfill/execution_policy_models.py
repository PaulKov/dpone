"""Immutable value objects for governed backfill execution."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.backfill.models import BackfillChunkSpec

_PUBLICATION_FIELDS = frozenset({"mode", "retain_backup", "artifact_scope"})
_PUBLICATION_MODES = frozenset({"direct", "shadow_swap"})
_PUBLICATION_ARTIFACT_SCOPES = frozenset({"stable", "campaign"})


@dataclass(frozen=True, slots=True)
class BackfillStatePolicy:
    """Canonical state-backend portion of one backfill campaign."""

    backend: str
    schema: str
    require_distributed_lock: bool

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "schema": self.schema,
            "require_distributed_lock": self.require_distributed_lock,
        }


@dataclass(frozen=True, slots=True)
class BackfillPublicationPolicy:
    """Canonical target-publication boundary for one backfill campaign."""

    mode: str
    retain_backup: bool
    artifact_scope: str

    def to_jsonable(self) -> dict[str, Any]:
        payload = {
            "mode": self.mode,
            "retain_backup": self.retain_backup,
        }
        if self.artifact_scope != "stable":
            payload["artifact_scope"] = self.artifact_scope
        return payload


def normalize_publication_policy(value: Any) -> BackfillPublicationPolicy:
    """Validate authoring and materialize one immutable publication policy."""

    if not isinstance(value, Mapping):
        raise ValueError("backfill.publication must be an object")
    unsupported = sorted(set(value) - _PUBLICATION_FIELDS)
    if unsupported:
        raise ValueError(f"backfill.publication contains unsupported options: {', '.join(unsupported)}")
    mode = _publication_enum(value.get("mode", "direct"), field="mode", values=_PUBLICATION_MODES)
    retain_backup = value.get("retain_backup", True)
    if not isinstance(retain_backup, bool):
        raise ValueError("backfill.publication.retain_backup must be a boolean")
    if mode == "direct" and "retain_backup" in value:
        raise ValueError("backfill.publication.retain_backup is only valid for mode=shadow_swap")
    artifact_scope = _publication_enum(
        value.get("artifact_scope", "stable"),
        field="artifact_scope",
        values=_PUBLICATION_ARTIFACT_SCOPES,
    )
    if mode == "direct" and "artifact_scope" in value:
        raise ValueError("backfill.publication.artifact_scope is only valid for mode=shadow_swap")
    return BackfillPublicationPolicy(
        mode=mode,
        retain_backup=retain_backup,
        artifact_scope=artifact_scope,
    )


def _publication_enum(value: Any, *, field: str, values: frozenset[str]) -> str:
    if not isinstance(value, str) or value.strip().lower() not in values:
        allowed = ", ".join(sorted(values))
        raise ValueError(f"backfill.publication.{field} must be one of: {allowed}")
    return value.strip().lower()


@dataclass(frozen=True, slots=True)
class BackfillExecutionPolicy:
    """Immutable, connector-neutral execution contract for a campaign.

    Defaults are materialized so equivalent manifests have one digest. The
    advisor is absent because recommendations cannot mutate a planned run.
    """

    inner_mode: str
    parallel_workers: int
    chunk: BackfillChunkSpec | None
    max_chunks: int
    state: BackfillStatePolicy
    publication: BackfillPublicationPolicy
    state_dir: str | None
    retry_policy: str
    backfill_id: str | None
    predicate_dialect: str
    lease_ttl_minutes: int

    @property
    def digest(self) -> str:
        material = json.dumps(
            self.to_jsonable(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    def to_jsonable(self) -> dict[str, Any]:
        chunk = self.chunk
        return {
            "inner_mode": self.inner_mode,
            "parallel_workers": self.parallel_workers,
            "chunk": chunk.to_jsonable() if chunk is not None else None,
            "max_chunks": self.max_chunks,
            "state": self.state.to_jsonable(),
            "publication": self.publication.to_jsonable(),
            "state_dir": self.state_dir,
            "retry_policy": self.retry_policy,
            "backfill_id": self.backfill_id,
            "lease_ttl_minutes": self.lease_ttl_minutes,
        }


__all__ = [
    "BackfillExecutionPolicy",
    "BackfillPublicationPolicy",
    "BackfillStatePolicy",
    "normalize_publication_policy",
]
