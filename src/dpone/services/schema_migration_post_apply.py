"""File-IO facade for schema migration post-apply verification commands."""

from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.schema_migration_post_apply import (
    PostApplyCertifier,
    PostApplyVerificationPlanner,
    PostApplyVerificationRunner,
    render_post_apply_markdown,
)


class PostApplyVerificationFacade:
    """Application facade for post-apply verification artifacts."""

    def plan(
        self,
        *,
        pack_path: str,
        ledger_path: str,
        manifest_path: str,
        target_connection_path: str,
        environment: str,
        bundle_path: str | None = None,
    ) -> dict[str, Any]:
        connection = _read_mapping(target_connection_path)
        connection["path"] = target_connection_path
        return PostApplyVerificationPlanner().plan(
            pack=_read_mapping(pack_path),
            bundle=_read_mapping(bundle_path) if bundle_path else None,
            ledger=_read_mapping(ledger_path),
            manifest=_read_mapping(manifest_path),
            target_connection=connection,
            environment=environment,
        )

    def run(self, *, plan_path: str, execute: bool = False) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        adapter = _load_adapter(plan) if execute else None
        try:
            return PostApplyVerificationRunner().run(
                plan=plan,
                inspector=adapter,
                canary_executor=adapter,
                data_profiler=adapter,
                execute=execute,
            )
        finally:
            close = getattr(adapter, "close", None)
            if callable(close):
                close()

    def certify(self, *, run_path: str, profile: str = "stage") -> dict[str, Any]:
        return PostApplyCertifier().certify(run=_read_mapping(run_path), profile=profile)

    def report(self, *, certificate_path: str) -> dict[str, Any]:
        certificate = _read_mapping(certificate_path)
        return {**certificate, "markdown": render_post_apply_markdown(certificate)}


def _load_adapter(plan: dict[str, Any]) -> Any:
    connection = _read_mapping(_connection_path(plan))
    sink_type = str(connection.get("type") or connection.get("sink_type") or _target_sink_type(plan)).lower()
    if sink_type != _target_sink_type(plan):
        raise ValueError(f"target connection type {sink_type!r} does not match plan target {_target_sink_type(plan)!r}")
    if sink_type == "clickhouse":
        module = import_module("dpone.runtime.sinks.clickhouse_post_apply")
        return module.ClickHousePostApplyVerifier.from_config(connection)
    raise ValueError(f"post-apply verification is implemented for clickhouse in v1, got {sink_type!r}")


def _connection_path(plan: dict[str, Any]) -> str:
    connection = plan.get("target_connection", {})
    if isinstance(connection, dict) and connection.get("path"):
        return str(connection["path"])
    raise ValueError("post-apply plan does not contain target_connection.path")


def _target_sink_type(plan: dict[str, Any]) -> str:
    target = plan.get("target", {})
    return str(target.get("sink_type", "")) if isinstance(target, dict) else ""


def _read_mapping(path: str | Path) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if str(path).lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


__all__ = ["PostApplyVerificationFacade"]
