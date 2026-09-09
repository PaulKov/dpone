"""File-IO facade for schema contract consumer test kit commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class SchemaConsumerTestKitFacade:
    """Thin file-IO facade; readiness modules own business rules."""

    def plan(
        self,
        *,
        manifest_path: str,
        matrix_path: str,
        compatibility_view_plan_path: str | None = None,
    ) -> dict[str, Any]:
        manifest = _read_mapping(manifest_path)
        return (
            _module()
            .SchemaConsumerTestKitBuilder()
            .build(
                matrix=_read_mapping(matrix_path),
                compatibility_view_plan=_read_optional(compatibility_view_plan_path),
                contract_version=_contracts().SchemaContractVersionBuilder().build(manifest=manifest),
            )
        )

    def render(self, *, kit_path: str, output_format: str) -> dict[str, Any]:
        kit = _read_mapping(kit_path)
        rendered = _module().SchemaConsumerTestKitRenderer().render(kit, output_format)
        return {
            "schema_version": "dpone.schema_contract_consumer_test_kit_render.v1",
            "status": "rendered",
            "test_kit_id": kit.get("test_kit_id"),
            "format": output_format,
            "content": rendered,
        }

    def certify(
        self,
        *,
        kit_path: str,
        result: str,
        result_artifact_path: str | None = None,
    ) -> dict[str, Any]:
        result_payload = _read_optional(result_artifact_path) or {"status": result}
        if "status" not in result_payload:
            result_payload["status"] = result
        return (
            _module()
            .SchemaConsumerCertificationEvaluator()
            .evaluate(
                test_kit=_read_mapping(kit_path),
                result=result_payload,
            )
        )


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if path.lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _module() -> Any:
    return import_module("dpone.readiness.schema_contract_consumer_test_kit")


def _contracts() -> Any:
    return import_module("dpone.readiness.schema_contract_registry")


__all__ = ["SchemaConsumerTestKitFacade"]
