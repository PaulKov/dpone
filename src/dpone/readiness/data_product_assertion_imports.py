"""Local artifact import normalizers for data product assertions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def imported_results(payloads: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    """Normalize dbt/GX/DataHub/OpenMetadata result artifacts."""

    results: list[dict[str, Any]] = []
    for payload in payloads:
        source = _source(payload)
        if not source:
            continue
        status = _status(source, payload)
        results.append(
            {
                "id": f"import:{source}",
                "type": "imported",
                "source": source,
                "status": "passed" if status == "passed" else "failed",
                "severity": "high",
                "details": [] if status == "passed" else [f"data_product_assertions.import_failed:{source}"],
                "metrics": {"source": source, "raw_status": status},
            }
        )
    return tuple(results)


def present_sources(payloads: Sequence[Mapping[str, Any]]) -> set[str]:
    return {source for payload in payloads if (source := _source(payload))}


def _source(payload: Mapping[str, Any]) -> str | None:
    if isinstance(payload.get("metadata"), Mapping) and "dbt_schema_version" in payload["metadata"]:
        return "dbt_run_results"
    if isinstance(payload.get("results"), list):
        if any(isinstance(item, Mapping) and "expectation_config" in item for item in payload["results"]):
            return "great_expectations"
        return "dbt_run_results"
    if isinstance(payload.get("assertions"), list):
        return "datahub_assertions"
    if isinstance(payload.get("tests"), list):
        return "openmetadata_tests"
    source = payload.get("source") or payload.get("kind")
    return str(source) if source else None


def _status(source: str, payload: Mapping[str, Any]) -> str:
    if source == "dbt_run_results":
        rows = payload.get("results", [])
        failed = any(str(item.get("status", "")).lower() not in {"pass", "success"} for item in _items(rows))
        return "failed" if failed else "passed"
    if source == "great_expectations":
        rows = payload.get("results", [])
        failed = any(item.get("success") is False for item in _items(rows))
        return "failed" if failed else "passed"
    if source == "datahub_assertions":
        failed = any(
            str(item.get("status", "")).lower() not in {"success", "passed", "pass"}
            for item in _items(payload.get("assertions", []))
        )
        return "failed" if failed else "passed"
    if source == "openmetadata_tests":
        failed = any(
            str(item.get("status", "")).lower() not in {"success", "passed", "pass"}
            for item in _items(payload.get("tests", []))
        )
        return "failed" if failed else "passed"
    return "passed" if str(payload.get("status", "passed")).lower() in {"passed", "pass", "success"} else "failed"


def _items(raw: object) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in raw if isinstance(item, Mapping)) if isinstance(raw, list) else ()


__all__ = ["imported_results", "present_sources"]
