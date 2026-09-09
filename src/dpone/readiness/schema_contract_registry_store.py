"""Local JSON store for schema contract versions."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dpone.readiness.schema_contract_registry import REGISTRY_SCHEMA


class SchemaContractRegistryStore(Protocol):
    def append(self, version: Mapping[str, Any]) -> dict[str, Any]: ...

    def history(self, *, contract_id: str) -> tuple[dict[str, Any], ...]: ...

    def latest(self, *, contract_id: str) -> dict[str, Any] | None: ...

    def get(self, *, contract_id: str, version: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class LocalJsonSchemaContractRegistryStore:
    path: str | Path

    def append(self, version: Mapping[str, Any]) -> dict[str, Any]:
        payload = self._load()
        versions = [dict(item) for item in payload.get("versions", []) if isinstance(item, dict)]
        existing = _find(versions, version)
        if existing and existing.get("contract_fingerprint") == version.get("contract_fingerprint"):
            return {"status": "duplicate", "contract_version_id": existing.get("contract_version_id"), "blockers": []}
        if existing:
            return {
                "status": "blocked",
                "contract_id": version.get("contract_id"),
                "version": version.get("version"),
                "blockers": ["schema_contract_registry.version_conflict"],
            }
        versions.append(dict(version))
        versions.sort(key=lambda item: (str(item.get("contract_id")), _semver(str(item.get("version", "0.0.0")))))
        self._save({"schema_version": REGISTRY_SCHEMA, "versions": versions})
        return {"status": "recorded", "contract_version_id": version.get("contract_version_id"), "blockers": []}

    def history(self, *, contract_id: str) -> tuple[dict[str, Any], ...]:
        versions = [
            dict(item)
            for item in self._load().get("versions", [])
            if isinstance(item, dict) and item.get("contract_id") == contract_id
        ]
        return tuple(sorted(versions, key=lambda item: _semver(str(item.get("version", "0.0.0")))))

    def latest(self, *, contract_id: str) -> dict[str, Any] | None:
        history = self.history(contract_id=contract_id)
        return history[-1] if history else None

    def get(self, *, contract_id: str, version: str) -> dict[str, Any] | None:
        for item in self.history(contract_id=contract_id):
            if item.get("version") == version:
                return item
        return None

    def _load(self) -> dict[str, Any]:
        raw_path = Path(self.path)
        if not raw_path.exists():
            return {"schema_version": REGISTRY_SCHEMA, "versions": []}
        raw = json.loads(raw_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError(f"{raw_path} must contain an object")
        return raw

    def _save(self, payload: Mapping[str, Any]) -> None:
        raw_path = Path(self.path)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def _find(versions: list[dict[str, Any]], version: Mapping[str, Any]) -> dict[str, Any] | None:
    for item in versions:
        if item.get("contract_id") == version.get("contract_id") and item.get("version") == version.get("version"):
            return item
    return None


def _semver(raw: str) -> tuple[int, int, int]:
    parts = [int(part) for part in raw.split(".")[:3]]
    return tuple((parts + [0, 0, 0])[:3])  # type: ignore[return-value]
