"""BI-ready benchmark evidence exports."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from tools.oss_benchmark.config import ROOT
from tools.oss_benchmark.payload_utils import get_value, project_name, project_slug

EXPORT_DIR = ROOT / "docs" / "benchmarks" / "data" / "warehouse"
EXPORT_FILES = (
    "projects.csv",
    "metric_groups.csv",
    "quality_gates.csv",
    "claims.csv",
    "runtime_certification.csv",
    "certification_scenarios.csv",
    "contract_checks.csv",
    "run_ledger.csv",
    "release_deltas.csv",
    "debt_ledger.csv",
)


def build_evidence_export_manifest() -> dict[str, Any]:
    """Return the stable export manifest stored in raw evidence."""

    return {
        "schema_version": 1,
        "format": "csv",
        "output_dir": "docs/benchmarks/data/warehouse",
        "files": list(EXPORT_FILES),
    }


def write_evidence_exports(payload: dict[str, Any], *, output_dir: Path = EXPORT_DIR) -> list[Path]:
    """Write BI-friendly CSV files from the final merged payload."""

    output_dir.mkdir(parents=True, exist_ok=True)
    tables = {
        "projects.csv": _project_rows(payload),
        "metric_groups.csv": _metric_group_rows(payload),
        "quality_gates.csv": _quality_gate_rows(payload),
        "claims.csv": _claim_rows(payload),
        "runtime_certification.csv": _certification_rows(payload),
        "certification_scenarios.csv": _certification_scenario_rows(payload),
        "contract_checks.csv": _contract_check_rows(payload),
        "run_ledger.csv": _run_ledger_rows(payload),
        "release_deltas.csv": _release_delta_rows(payload),
        "debt_ledger.csv": _debt_rows(payload),
    }
    written: list[Path] = []
    for name, rows in tables.items():
        path = output_dir / name
        _write_csv(path, rows)
        written.append(path)
    return written


def _project_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for project in payload.get("projects") or []:
        rows.append(
            _base_row(payload)
            | {
                "project_id": project_slug(project),
                "display_name": project_name(project),
                "revision": project.get("revision") or get_value(project, "spec", "commit"),
                "freshness": _project_freshness(project),
                "loc_without_tests": get_value(project, "loc_without_tests", "total_lines"),
                "sloc_without_tests": get_value(project, "loc_without_tests", "total_sloc"),
                "max_sloc": get_value(project, "loc_without_tests", "max_sloc"),
                "avg_clustering": get_value(project, "coupling", "avg_clustering"),
            }
        )
    return rows


def _metric_group_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for project in payload.get("projects") or []:
        for group_name, group in (project.get("metric_groups") or {}).items():
            rows.append(
                _base_row(payload)
                | {
                    "project_id": project_slug(project),
                    "metric_group": group_name,
                    "status": group.get("status"),
                    "last_updated_at": group.get("last_updated_at"),
                    "stale_age_days": group.get("stale_age_days"),
                    "last_error": group.get("last_error"),
                }
            )
    return rows


def _quality_gate_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for check in (payload.get("quality_gates") or {}).get("checks") or []:
        rows.append(
            _base_row(payload)
            | {
                "project_id": check.get("project_id", "dpone"),
                "gate_id": check.get("id"),
                "label": check.get("label"),
                "status": check.get("status"),
                "severity": check.get("severity"),
                "actual": check.get("actual"),
                "expected": check.get("expected"),
            }
        )
    return rows


def _claim_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _base_row(payload)
        | {
            "project_id": claim.get("project_id"),
            "claim_id": claim.get("claim_id"),
            "claim_type": claim.get("claim_type"),
            "status": claim.get("status"),
            "confidence": claim.get("confidence"),
            "freshness": claim.get("freshness"),
            "gate_impact": claim.get("gate_impact"),
        }
        for claim in (payload.get("claims_ledger") or {}).get("claims") or []
    ]


def _certification_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _base_row(payload)
        | {
            "project_id": "dpone",
            "scenario_id": scenario.get("scenario_id"),
            "source": scenario.get("source"),
            "strategy": scenario.get("strategy"),
            "sink": scenario.get("sink"),
            "status": scenario.get("status"),
            "last_error": scenario.get("last_error"),
        }
        for scenario in (payload.get("runtime_certification") or {}).get("scenarios") or []
    ]


def _certification_scenario_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for scenario in (payload.get("runtime_certification_v2") or {}).get("scenarios") or []:
        freshness = scenario.get("freshness") or {}
        rows.append(
            _base_row(payload)
            | {
                "project_id": "dpone",
                "scenario_id": scenario.get("scenario_id"),
                "category": scenario.get("category"),
                "runner": scenario.get("runner"),
                "status": scenario.get("status"),
                "freshness": freshness.get("status"),
                "last_updated_at": freshness.get("last_updated_at"),
                "stale_age_days": freshness.get("stale_age_days"),
                "duration_ms": scenario.get("duration_ms"),
                "input_hash": scenario.get("input_hash"),
                "output_hash": scenario.get("output_hash"),
                "last_error": scenario.get("last_error") or freshness.get("last_error"),
            }
        )
    return rows


def _contract_check_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _base_row(payload)
        | {
            "project_id": "dpone",
            "scenario_id": check.get("scenario_id"),
            "check_id": check.get("check_id"),
            "status": check.get("status"),
            "expected": check.get("expected"),
            "actual": check.get("actual"),
            "message": check.get("message"),
        }
        for check in (payload.get("runtime_certification_v2") or {}).get("contract_checks") or []
    ]


def _run_ledger_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _base_row(payload)
        | {
            "project_id": "dpone",
            "scenario_id": record.get("scenario_id"),
            "category": record.get("category"),
            "runner": record.get("runner"),
            "status": record.get("status"),
            "duration_ms": record.get("duration_ms"),
            "input_hash": record.get("input_hash"),
            "output_hash": record.get("output_hash"),
            "freshness": record.get("freshness"),
            "error_class": record.get("error_class"),
        }
        for record in (payload.get("runtime_certification_v2") or {}).get("run_ledger") or []
    ]


def _release_delta_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for project_id, delta in ((payload.get("release_delta") or {}).get("projects") or {}).items():
        for metric, item in (delta.get("metrics") or {}).items():
            rows.append(
                _base_row(payload)
                | {
                    "project_id": project_id,
                    "metric": metric,
                    "previous": item.get("previous"),
                    "current": item.get("current"),
                    "delta": item.get("delta"),
                    "classification": item.get("classification"),
                }
            )
    return rows


def _debt_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _base_row(payload)
        | {
            "project_id": debt.get("project_id"),
            "metric": debt.get("metric"),
            "value": debt.get("value"),
            "status": debt.get("status"),
            "scope": debt.get("scope"),
        }
        for debt in (payload.get("quality_budgets") or {}).get("debt_ledger") or []
    ]


def _base_row(payload: dict[str, Any]) -> dict[str, Any]:
    release = payload.get("release_context") or {}
    return {
        "schema_version": payload.get("schema_version"),
        "generated_at": payload.get("generated_at"),
        "release_version": release.get("dpone_version"),
        "release_tag": release.get("release_tag"),
        "release_sha": release.get("release_sha"),
    }


def _project_freshness(project: dict[str, Any]) -> str:
    statuses = {str(group.get("status") or "fresh") for group in (project.get("metric_groups") or {}).values()}
    if "unavailable" in statuses:
        return "unavailable"
    if "stale" in statuses:
        return "stale"
    return "fresh"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = _fieldnames(rows)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    return fields or ["schema_version"]
