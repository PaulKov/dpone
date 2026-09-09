"""File-IO facade for data product SLO and incident commands."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductSloFacade:
    """Thin CLI facade; SLO rules live in readiness modules."""

    def plan(
        self,
        *,
        manifest_path: str,
        contract_gate_path: str | None = None,
        consumer_gate_path: str | None = None,
        consumer_certification_path: str | None = None,
        watch_certificate_path: str | None = None,
        post_apply_certificate_path: str | None = None,
        assertion_gate_path: str | None = None,
    ) -> dict[str, Any]:
        return (
            _slo()
            .DataProductSloPlanner()
            .plan(
                manifest=_read_mapping(manifest_path),
                contract_gate=_read_optional(contract_gate_path),
                consumer_gate=_read_optional(consumer_gate_path),
                consumer_certification=_read_optional(consumer_certification_path),
                watch_certificate=_read_optional(watch_certificate_path),
                post_apply_certificate=_read_optional(post_apply_certificate_path),
                assertion_gate=_read_optional(assertion_gate_path),
            )
        )

    def evaluate(
        self,
        *,
        plan_path: str,
        registry_path: str | None = None,
        runtime_artifact_path: str | None = None,
        target_connection_path: str | None = None,
    ) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        return (
            _slo()
            .DataProductSloEvaluator()
            .evaluate(
                plan=plan,
                registry_records=_registry_records(registry_path),
                runtime_artifacts=_runtime_artifacts(runtime_artifact_path),
                target_evidence=_target_evidence(plan, target_connection_path),
            )
        )

    def gate(self, *, evaluation_path: str, profile: str) -> dict[str, Any]:
        return _slo().DataProductSloGate().evaluate(evaluation=_read_mapping(evaluation_path), profile=profile)

    def incident_report(self, *, evaluation_path: str, slo_gate_path: str) -> dict[str, Any]:
        return (
            _incident()
            .IncidentClassifier()
            .report(
                evaluation=_read_mapping(evaluation_path),
                gate=_read_mapping(slo_gate_path),
            )
        )


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


def _runtime_artifacts(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    payload = _read_mapping(path)
    if isinstance(payload.get("runs"), list):
        return tuple(dict(item) for item in payload["runs"] if isinstance(item, Mapping))
    return (payload,)


def _target_evidence(plan: Mapping[str, Any], path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    connection = _read_mapping(path)
    if str(connection.get("type") or connection.get("sink_type") or "").lower() != "clickhouse":
        return {"warnings": [f"data_product_slo.unsupported_target:{connection.get('type') or 'unknown'}"]}
    return _clickhouse_probe(plan, connection)


def _clickhouse_probe(plan: Mapping[str, Any], connection: Mapping[str, Any]) -> dict[str, Any]:
    try:
        import clickhouse_connect
    except ModuleNotFoundError:
        return {"warnings": ["data_product_slo.clickhouse_probe_unavailable"]}
    target = plan.get("target", {})
    table = target.get("table") if isinstance(target, Mapping) else None
    if not table:
        return {"warnings": ["data_product_slo.target_table_missing"]}
    client = clickhouse_connect.get_client(
        host=str(connection.get("host") or "localhost"),
        port=int(connection.get("port") or 8123),
        username=str(connection.get("username") or connection.get("user") or "default"),
        password=str(connection.get("password") or ""),
        database=str(connection.get("database") or "default"),
    )
    result = client.query(f"SELECT count() AS rows FROM {table}")  # noqa: S608
    first = result.result_rows[0][0] if result.result_rows else 0
    return {"row_count": int(first)}


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str) -> dict[str, Any]:
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _slo() -> Any:
    return import_module("dpone.readiness.data_product_slo")


def _incident() -> Any:
    return import_module("dpone.readiness.data_product_slo_incident")


__all__ = ["DataProductSloFacade"]
