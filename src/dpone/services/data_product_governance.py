"""File-IO facade for data product governance export commands."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductGovernanceFacade:
    """Thin CLI facade; governance decisions live in readiness modules."""

    def plan(
        self,
        *,
        manifest_path: str,
        registry_path: str | None,
        evidence_dir: str | None,
        targets: Sequence[str],
    ) -> dict[str, Any]:
        return (
            _governance()
            .GovernanceExportPlanner()
            .plan(
                manifest=_read_mapping(manifest_path),
                evidence=_evidence(evidence_dir),
                registry_records=_registry_records(registry_path),
                targets=targets,
            )
        )

    def render(self, *, plan_path: str, target: str) -> dict[str, Any]:
        return _governance().GovernancePayloadRenderer().render(plan=_read_mapping(plan_path), provider=target)

    def publish(
        self,
        *,
        payload_path: str,
        provider: str,
        connection_path: str | None,
        execute: bool,
    ) -> dict[str, Any]:
        return (
            _governance()
            .GovernancePublisher()
            .publish(
                payload=_read_mapping(payload_path),
                provider=provider,
                connection=_read_optional(connection_path),
                execute=execute,
            )
        )

    def verify(self, *, receipt_path: str) -> dict[str, Any]:
        return _governance().GovernancePublishVerifier().verify(receipt=_read_mapping(receipt_path))


def _evidence(path: str | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    evidence: dict[str, dict[str, Any]] = {}
    for source in sorted(Path(path).rglob("*")):
        if source.is_file() and source.suffix.lower() in {".json", ".yaml", ".yml"}:
            try:
                payload = _read_mapping(str(source))
            except (OSError, ValueError, json.JSONDecodeError, yaml.YAMLError):
                continue
            evidence[_kind_from_schema(payload) or source.stem.replace("-", "_")] = payload
    return evidence


def _registry_records(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    source = Path(path)
    if source.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
        return _sqlite_records(source)
    payload = _read_mapping(path)
    records = payload.get("records", [])
    return tuple(dict(item) for item in records if isinstance(item, Mapping)) if isinstance(records, list) else ()


def _sqlite_records(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        return ()
    with sqlite3.connect(path) as conn:
        rows = conn.execute("select payload_json from evidence_records order by recorded_at, record_id").fetchall()
    records: list[dict[str, Any]] = []
    for (payload_json,) in rows:
        raw = json.loads(str(payload_json))
        if isinstance(raw, Mapping):
            records.append(dict(raw))
    return tuple(records)


def _kind_from_schema(payload: Mapping[str, Any]) -> str | None:
    schema = str(payload.get("schema_version") or "")
    if schema.startswith("dpone.") and schema.endswith(".v1"):
        return schema.removeprefix("dpone.").removesuffix(".v1")
    return None


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _governance() -> Any:
    return import_module("dpone.readiness.data_product_governance")


__all__ = ["DataProductGovernanceFacade"]
