"""File-IO facade for data product access enforcement commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductAccessEnforcementFacade:
    """Thin CLI facade; enforcement decisions live in readiness modules."""

    def plan(
        self,
        *,
        manifest_path: str,
        classification_path: str,
        entitlement_plan_path: str,
        privacy_impact_path: str,
        access_gate_path: str | None,
        authority_gate_path: str | None,
        target_connection_path: str,
        environment: str,
    ) -> dict[str, Any]:
        target_connection = _read_mapping(target_connection_path)
        return (
            _core()
            .AccessEnforcementPlanner()
            .plan(
                manifest=_read_mapping(manifest_path),
                classification=_read_mapping(classification_path),
                entitlement_plan=_read_mapping(entitlement_plan_path),
                privacy_impact=_read_mapping(privacy_impact_path),
                access_gate=_read_optional(access_gate_path),
                authority_gate=_read_optional(authority_gate_path),
                target_connection=target_connection,
                dialect=_clickhouse().ClickHouseAccessDialect(),
                environment=environment,
            )
        )

    def apply(
        self,
        *,
        plan_path: str,
        approval_path: str | None,
        execute: bool,
        target_connection_path: str | None,
    ) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        executor = (
            _load_executor()(
                target=plan.get("target", {}),
                target_connection_path=target_connection_path,
            )
            if execute and target_connection_path
            else None
        )
        return (
            _core()
            .AccessEnforcementRunner()
            .apply(
                plan=plan,
                approval=_read_optional(approval_path),
                execute=execute,
                executor=executor,
            )
        )

    def drift_inspect(self, *, plan_path: str, target_connection_path: str) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        target_connection = _read_mapping(target_connection_path)
        actual_state = target_connection.get("access_state")
        return (
            _core()
            .AccessDriftInspector()
            .inspect(
                plan=plan,
                actual_state=dict(actual_state) if isinstance(actual_state, Mapping) else None,
            )
        )

    def certify(self, *, run_path: str, drift_report_path: str, profile: str) -> dict[str, Any]:
        return (
            _core()
            .AccessEnforcementCertifier()
            .certify(
                run=_read_mapping(run_path),
                drift_report=_read_mapping(drift_report_path),
                profile=profile,
            )
        )

    def report(self, *, certificate_path: str) -> dict[str, Any]:
        return _rendering().AccessEnforcementRenderer().report(certificate=_read_mapping(certificate_path))


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


def _core() -> Any:
    return import_module("dpone.readiness.data_product_access_enforcement")


def _load_executor() -> Any:
    return import_module("dpone.services.schema_migration_execution").load_migration_operation_executor


def _clickhouse() -> Any:
    return import_module("dpone.readiness.data_product_access_enforcement_clickhouse")


def _rendering() -> Any:
    return import_module("dpone.readiness.data_product_access_enforcement_rendering")


__all__ = ["DataProductAccessEnforcementFacade"]
