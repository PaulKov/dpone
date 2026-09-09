"""Exact PostgreSQL to MSSQL route authority for a release candidate."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


codec = _load_sibling("dpone_release_candidate_codec_route_live", "release_candidate_evidence_codec.py")
_BARE_SHA256 = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_SUITE_IDS = (
    "artifact_integrity_faults",
    "backfill_orchestration",
    "boundary_types",
    "explicit_types",
    "lineage_parity",
    "physical_design",
    "postgis_types",
    "schema_evolution",
    "source_identity_authority",
    "strategy_capability",
    "target_behavior",
    "target_database_authority",
    "target_identity_authority",
    "text_key_lifecycle",
    "transaction_governance",
    "wide_performance_soak",
    "wide_strategy",
    "xmin_reconciliation",
)


def validate_manifest(payload: Mapping[str, Any], *, commit_sha: str) -> dict[str, Any]:
    """Validate one semantic-gate manifest and return its release projection."""

    codec.require_exact_keys(
        payload,
        frozenset(
            {
                "schema_version",
                "status",
                "release_ready",
                "route",
                "commit_sha",
                "workflow_run_id",
                "workflow_run_attempt",
                "inventory_sha256",
                "required_suite_ids",
                "suite_count",
                "case_count",
                "vendor_policy_sha256",
                "suites",
                "manifest_sha256",
            }
        ),
        field="postgres_mssql_route_certification",
    )
    required = {
        "schema_version": "dpone.route_live.postgres_mssql.certification.v1",
        "status": "certification_passed",
        "release_ready": True,
        "route": "postgres_mssql",
        "commit_sha": commit_sha,
    }
    if any(type(payload.get(key)) is not type(value) or payload.get(key) != value for key, value in required.items()):
        raise ValueError("route-live manifest identity or status is invalid")
    codec.require_positive_int(
        payload.get("workflow_run_attempt"),
        field="route_live.workflow_run_attempt",
    )
    run_id = codec.require_string(payload.get("workflow_run_id"), field="route_live.workflow_run_id")
    if not run_id.isascii() or not run_id.isdecimal() or run_id.startswith("0"):
        raise ValueError("route-live workflow_run_id must be a positive canonical integer string")
    for field in ("inventory_sha256", "vendor_policy_sha256", "manifest_sha256"):
        _require_bare_sha256(payload.get(field), field=f"route_live.{field}")
    if payload.get("required_suite_ids") != list(REQUIRED_SUITE_IDS):
        raise ValueError("route-live required suite inventory is not frozen")
    suites = _list(payload.get("suites"), "route_live.suites")
    if payload.get("suite_count") != len(suites) or len(suites) != len(REQUIRED_SUITE_IDS):
        raise ValueError("route-live suite count is invalid")
    observed_ids, observed_cases = _validate_suites(suites)
    if observed_ids != list(REQUIRED_SUITE_IDS):
        raise ValueError("route-live suite order or identity is invalid")
    if type(payload.get("case_count")) is not int or payload.get("case_count") != observed_cases:
        raise ValueError("route-live aggregate case count is invalid")
    unsigned = dict(payload)
    manifest_sha256 = unsigned.pop("manifest_sha256")
    if _route_digest(unsigned) != manifest_sha256:
        raise ValueError("route-live manifest semantic digest is invalid")
    return {
        "status": "PASS",
        "suite_count": len(suites),
        "case_count": observed_cases,
        "manifest_sha256": manifest_sha256,
        "workflow_run_id": int(run_id),
        "workflow_run_attempt": payload["workflow_run_attempt"],
    }


def validate_provider_binding(
    payload: Mapping[str, Any],
    *,
    commit_sha: str,
    manifest_raw: bytes,
) -> dict[str, Any]:
    """Bind exact manifest bytes to one immutable provider artifact."""

    codec.require_exact_keys(
        payload,
        frozenset(
            {
                "schema_version",
                "status",
                "artifact_name",
                "artifact_id",
                "artifact_digest",
                "commit_sha",
                "workflow_run_id",
                "workflow_run_attempt",
                "manifest_content_sha256",
                "manifest_sha256",
            }
        ),
        field="postgres_mssql_route_artifact_binding",
    )
    manifest = codec.strict_json_object(manifest_raw, field="postgres_mssql_route_certification")
    required = {
        "schema_version": "dpone.route_live.postgres_mssql.provider_binding.v1",
        "status": "certification_passed",
        "commit_sha": commit_sha,
        "workflow_run_id": int(str(manifest.get("workflow_run_id"))),
        "workflow_run_attempt": manifest.get("workflow_run_attempt"),
        "manifest_content_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "manifest_sha256": manifest.get("manifest_sha256"),
    }
    if any(type(payload.get(key)) is not type(value) or payload.get(key) != value for key, value in required.items()):
        raise ValueError("route-live provider binding does not match exact manifest bytes")
    codec.require_positive_int(payload.get("artifact_id"), field="route_live.artifact_id")
    codec.require_digest(payload.get("artifact_digest"), field="route_live.artifact_digest")
    name = codec.require_string(payload.get("artifact_name"), field="route_live.artifact_name")
    expected_name = (
        f"release-candidate-postgres-mssql-route-{commit_sha}-"
        f"{required['workflow_run_id']}-{required['workflow_run_attempt']}"
    )
    if name != expected_name:
        raise ValueError("route-live provider artifact name is not exact")
    return {
        "status": "PASS",
        "artifact_id": payload["artifact_id"],
        "artifact_digest": payload["artifact_digest"],
        "manifest_sha256": payload["manifest_sha256"],
    }


def _validate_suites(suites: list[Any]) -> tuple[list[str], int]:
    observed_ids: list[str] = []
    observed_cases = 0
    for index, raw_suite in enumerate(suites):
        suite = _mapping(raw_suite, f"route_live.suites[{index}]")
        codec.require_exact_keys(
            suite,
            frozenset(
                {
                    "suite_id",
                    "schema_version",
                    "case_count",
                    "case_set_sha256",
                    "case_contract_sha256",
                    "evidence_file",
                    "evidence_sha256",
                    "junit",
                    "vendors",
                }
            ),
            field=f"route_live.suites[{index}]",
        )
        observed_ids.append(codec.require_string(suite.get("suite_id"), field=f"route_live.suites[{index}].suite_id"))
        case_count = codec.require_positive_int(
            suite.get("case_count"),
            field=f"route_live.suites[{index}].case_count",
        )
        observed_cases += case_count
        for field in ("case_set_sha256", "case_contract_sha256", "evidence_sha256"):
            _require_bare_sha256(suite.get(field), field=f"route_live.suites[{index}].{field}")
        codec.require_string(suite.get("schema_version"), field=f"route_live.suites[{index}].schema_version")
        codec.require_string(suite.get("evidence_file"), field=f"route_live.suites[{index}].evidence_file")
        _validate_junit(
            _mapping(suite.get("junit"), f"route_live.suites[{index}].junit"),
            suite_index=index,
            case_count=case_count,
            case_set_sha256=str(suite["case_set_sha256"]),
        )
        if not isinstance(suite.get("vendors"), Mapping) or not suite["vendors"]:
            raise ValueError("route-live suite vendor evidence is required")
    return observed_ids, observed_cases


def _validate_junit(
    junit: Mapping[str, Any],
    *,
    suite_index: int,
    case_count: int,
    case_set_sha256: str,
) -> None:
    codec.require_exact_keys(
        junit,
        frozenset({"path", "sha256", "case_ids", "tests", "failures", "errors", "skipped"}),
        field=f"route_live.suites[{suite_index}].junit",
    )
    codec.require_string(junit.get("path"), field=f"route_live.suites[{suite_index}].junit.path")
    _require_bare_sha256(junit.get("sha256"), field=f"route_live.suites[{suite_index}].junit.sha256")
    cases = _list(junit.get("case_ids"), f"route_live.suites[{suite_index}].junit.case_ids")
    if any(not isinstance(value, str) or not value for value in cases):
        raise ValueError("route-live JUnit case IDs must be non-empty strings")
    if cases != sorted(cases) or len(cases) != len(set(cases)):
        raise ValueError("route-live JUnit case IDs must be sorted and unique")
    expected = {"tests": case_count, "failures": 0, "errors": 0, "skipped": 0}
    if any(type(junit.get(field)) is not int or junit.get(field) != value for field, value in expected.items()):
        raise ValueError("route-live JUnit totals are not an exact all-pass result")
    if _route_digest(cases) != case_set_sha256:
        raise ValueError("route-live JUnit case inventory digest is invalid")


def _route_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _require_bare_sha256(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _BARE_SHA256.fullmatch(value) is None:
        raise ValueError(f"{field} must be one lowercase SHA-256 digest")
    return value


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    return value


__all__ = ["validate_manifest", "validate_provider_binding"]
