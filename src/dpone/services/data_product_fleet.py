"""File-IO facade for data product fleet reliability commands."""

from __future__ import annotations

import glob
import json
import sqlite3
from collections.abc import Mapping, Sequence
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductFleetFacade:
    """Thin CLI facade; fleet rules live in provider-neutral readiness modules."""

    def evaluate(
        self,
        *,
        manifest_patterns: Sequence[str],
        registry_path: str | None = None,
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        return (
            _fleet()
            .FleetReliabilityEvaluator()
            .evaluate(
                manifests=_manifests(manifest_patterns),
                registry_records=_registry_records(registry_path),
                observed_at=observed_at,
            )
        )

    def gate(self, *, evaluation_path: str, profile: str) -> dict[str, Any]:
        return _fleet().FleetReliabilityGate().evaluate(evaluation=_read_mapping(evaluation_path), profile=profile)

    def report(self, *, evaluation_path: str) -> dict[str, Any]:
        return _fleet().ReliabilityExportRenderer().report(evaluation=_read_mapping(evaluation_path))

    def export(self, *, evaluation_path: str, target: str) -> dict[str, Any]:
        return _fleet().ReliabilityExportRenderer().render(evaluation=_read_mapping(evaluation_path), target=target)

    def route_dry_run(self, *, payload_path: str, provider: str) -> dict[str, Any]:
        return _fleet().IncidentRouteDryRunEvaluator().evaluate(payload=_read_mapping(payload_path), provider=provider)


def _manifests(patterns: Sequence[str]) -> tuple[dict[str, Any], ...]:
    paths = _expanded_paths(patterns)
    return tuple(_read_mapping(str(path)) for path in paths)


def _expanded_paths(patterns: Sequence[str]) -> tuple[Path, ...]:
    paths: list[Path] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern, recursive=True))
        paths.extend(Path(match) for match in (matches or [pattern]))
    return tuple(dict.fromkeys(path.resolve() for path in paths))


def _registry_records(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    source = Path(path)
    if source.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
        return _sqlite_records(source)
    payload = _read_mapping(path)
    records = payload.get("records")
    if isinstance(records, list):
        return tuple(dict(item) for item in records if isinstance(item, Mapping))
    return (payload,) if payload.get("schema_version") else ()


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


def _read_mapping(path: str) -> dict[str, Any]:
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _fleet() -> Any:
    return import_module("dpone.readiness.data_product_fleet")


__all__ = ["DataProductFleetFacade"]
