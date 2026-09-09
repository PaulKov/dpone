"""Route/schema keyed cache for source export optimizer decisions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.export_optimizer_models import ExportProviderDecision, ExportProviderProbe


@dataclass(frozen=True, slots=True)
class ExportDecisionCacheKey:
    """Stable identity for a benchmarked source export decision."""

    source_identity: str
    query_hash: str
    schema_hash: str
    source_shape_hash: str
    provider_versions: Mapping[str, str]
    dpone_version: str

    def fingerprint(self) -> str:
        payload = {
            "source_identity": self.source_identity,
            "query_hash": self.query_hash,
            "schema_hash": self.schema_hash,
            "source_shape_hash": self.source_shape_hash,
            "provider_versions": dict(sorted(self.provider_versions.items())),
            "dpone_version": self.dpone_version,
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()


class ExportDecisionCache:
    """Tiny JSON-backed cache with an in-memory fast path."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path
        self._items: dict[str, ExportProviderDecision] = {}
        if path and path.exists():
            self._items.update(_read_items(path))

    def get(self, key: ExportDecisionCacheKey) -> ExportProviderDecision | None:
        return self._items.get(key.fingerprint())

    def put(self, key: ExportDecisionCacheKey, decision: ExportProviderDecision) -> None:
        self._items[key.fingerprint()] = decision
        if self._path:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            _write_items(self._path, self._items)


def _read_items(path: Path) -> dict[str, ExportProviderDecision]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        return {}
    return {str(key): _decision_from_evidence(value) for key, value in raw.items() if isinstance(value, Mapping)}


def _write_items(path: Path, items: Mapping[str, ExportProviderDecision]) -> None:
    payload = {key: decision.to_evidence() for key, decision in items.items()}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _decision_from_evidence(payload: Mapping[str, Any]) -> ExportProviderDecision:
    probes = tuple(
        ExportProviderProbe.from_mapping(probe) for probe in payload.get("probes", ()) if isinstance(probe, Mapping)
    )
    return ExportProviderDecision(
        requested_mode=str(payload.get("requested_mode") or "auto"),
        current_default=str(payload.get("current_default") or ""),
        selected_provider=_optional_text(payload.get("selected_provider")),
        recommended_provider=_optional_text(payload.get("recommended_provider")),
        measured_speedup_pct=_optional_float(payload.get("measured_speedup_pct")),
        source_bottleneck=str(payload.get("source_bottleneck") or "export"),
        cache_key=_optional_text(payload.get("cache_key")),
        release_gate=str(payload.get("release_gate") or "green"),
        probes=probes,
        rejected=dict(payload.get("rejected") or {}),
        blockers=tuple(str(item) for item in payload.get("blockers", ()) or ()),
        warnings=tuple(str(item) for item in payload.get("warnings", ()) or ()),
        reasons=tuple(str(item) for item in payload.get("reasons", ()) or ()),
    )


def _optional_text(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


__all__ = ["ExportDecisionCache", "ExportDecisionCacheKey"]
