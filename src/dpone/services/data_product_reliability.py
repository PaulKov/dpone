"""File-IO facade for data product reliability evidence commands."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductReliabilityFacade:
    """Thin CLI facade; reliability rules live in provider-neutral readiness modules."""

    def budget_plan(self, *, manifest_path: str, slo_evaluation_path: str | None = None) -> dict[str, Any]:
        return (
            _budget()
            .DataProductErrorBudgetPlanner()
            .plan(manifest=_read_mapping(manifest_path), slo_evaluation=_read_optional(slo_evaluation_path))
        )

    def budget_evaluate(
        self,
        *,
        plan_path: str,
        history_path: str | None = None,
        registry_path: str | None = None,
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        history = _history_items(history_path) + _registry_slo_items(registry_path)
        return (
            _budget()
            .DataProductErrorBudgetEvaluator()
            .evaluate(plan=_read_mapping(plan_path), history=history, observed_at=observed_at)
        )

    def budget_gate(self, *, evaluation_path: str, profile: str) -> dict[str, Any]:
        return (
            _budget().DataProductErrorBudgetGate().evaluate(evaluation=_read_mapping(evaluation_path), profile=profile)
        )

    def incident_open(
        self,
        *,
        slo_evaluation_path: str,
        slo_gate_path: str,
        budget_gate_path: str,
        existing_incident_path: str | None = None,
    ) -> dict[str, Any]:
        return (
            _lifecycle()
            .IncidentLifecycleReducer()
            .open(
                slo_evaluation=_read_mapping(slo_evaluation_path),
                slo_gate=_read_mapping(slo_gate_path),
                budget_gate=_read_mapping(budget_gate_path),
                existing=_read_optional(existing_incident_path),
            )
        )

    def incident_ack(self, *, incident_path: str, actor: str) -> dict[str, Any]:
        return _lifecycle().IncidentLifecycleReducer().ack(incident=_read_mapping(incident_path), actor=actor)

    def incident_resolve(self, *, incident_path: str, evidence_path: str) -> dict[str, Any]:
        return (
            _lifecycle()
            .IncidentLifecycleReducer()
            .resolve(incident=_read_mapping(incident_path), evidence=_read_mapping(evidence_path))
        )

    def route_render(self, *, incident_path: str, provider: str) -> dict[str, Any]:
        return (
            _lifecycle()
            .IncidentRouterPayloadRenderer()
            .render(
                incident=_read_mapping(incident_path),
                provider=provider,
            )
        )

    def closeout_gate(
        self,
        *,
        slo_gate_path: str,
        budget_gate_path: str,
        incident_path: str | None = None,
        watch_certificate_path: str | None = None,
        post_apply_certificate_path: str | None = None,
        policy_gate_path: str | None = None,
        profile: str = "prod_strict",
    ) -> dict[str, Any]:
        return (
            _closeout()
            .ReleaseCloseoutGate()
            .evaluate(
                slo_gate=_read_mapping(slo_gate_path),
                budget_gate=_read_mapping(budget_gate_path),
                incident=_read_optional(incident_path),
                watch_certificate=_read_optional(watch_certificate_path),
                post_apply_certificate=_read_optional(post_apply_certificate_path),
                policy_gate=_read_optional(policy_gate_path),
                profile=profile,
            )
        )


def _history_items(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    payload = _read_mapping(path)
    for key in ("evaluations", "history", "records", "runs"):
        raw = payload.get(key)
        if isinstance(raw, list):
            return tuple(dict(item) for item in raw if isinstance(item, Mapping))
    return (payload,)


def _registry_slo_items(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    source = Path(path)
    records = (
        _sqlite_records(source) if source.suffix.lower() in {".sqlite", ".sqlite3", ".db"} else _history_items(path)
    )
    items: list[dict[str, Any]] = []
    for record in records:
        for ref in record.get("artifact_refs", []):
            if isinstance(ref, Mapping) and ref.get("kind") == "data_product_slo_gate":
                items.append(
                    {
                        "schema_version": "dpone.data_product_slo_evaluation.v1",
                        "status": "blocked" if record.get("status") == "blocked" else "passed",
                        "product_id": _target_key(record),
                        "recorded_at": record.get("recorded_at"),
                        "blockers": list(record.get("blockers", [])),
                        "warnings": list(record.get("warnings", [])),
                    }
                )
    return tuple(items)


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


def _target_key(record: Mapping[str, Any]) -> str | None:
    target = record.get("target")
    if not isinstance(target, Mapping):
        return None
    sink = target.get("sink_type")
    table = target.get("table")
    return ".".join(str(item) for item in (sink, table) if item)


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str) -> dict[str, Any]:
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _budget() -> Any:
    return import_module("dpone.readiness.data_product_error_budget")


def _lifecycle() -> Any:
    return import_module("dpone.readiness.data_product_incident_lifecycle")


def _closeout() -> Any:
    return import_module("dpone.readiness.data_product_release_closeout")


__all__ = ["DataProductReliabilityFacade"]
