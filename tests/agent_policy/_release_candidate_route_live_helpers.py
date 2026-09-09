"""Small valid route-live authority factory used by release policy tests."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def route_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def valid_route_live_manifest(
    *,
    required_suite_ids: tuple[str, ...],
    commit_sha: str,
) -> dict[str, Any]:
    suites: list[dict[str, Any]] = []
    for suite_id in required_suite_ids:
        case_ids = [f"{suite_id}::certified"]
        suites.append(
            {
                "suite_id": suite_id,
                "schema_version": f"dpone.postgres_mssql.{suite_id}.v1",
                "case_count": 1,
                "case_set_sha256": route_digest(case_ids),
                "case_contract_sha256": "1" * 64,
                "evidence_file": f"{suite_id}.json",
                "evidence_sha256": "2" * 64,
                "junit": {
                    "path": f"{suite_id}.xml",
                    "sha256": "3" * 64,
                    "case_ids": case_ids,
                    "tests": 1,
                    "failures": 0,
                    "errors": 0,
                    "skipped": 0,
                },
                "vendors": {"vendor_identity": "reviewed"},
            }
        )
    payload: dict[str, Any] = {
        "schema_version": "dpone.route_live.postgres_mssql.certification.v1",
        "status": "certification_passed",
        "release_ready": True,
        "route": "postgres_mssql",
        "commit_sha": commit_sha,
        "workflow_run_id": "777",
        "workflow_run_attempt": 2,
        "inventory_sha256": "4" * 64,
        "required_suite_ids": list(required_suite_ids),
        "suite_count": len(suites),
        "case_count": len(suites),
        "vendor_policy_sha256": "5" * 64,
        "suites": suites,
    }
    payload["manifest_sha256"] = route_digest(payload)
    return payload


def valid_route_live_provider_binding(
    manifest: dict[str, Any],
    *,
    manifest_raw: bytes,
    commit_sha: str,
) -> dict[str, Any]:
    run_id = int(str(manifest["workflow_run_id"]))
    run_attempt = int(manifest["workflow_run_attempt"])
    return {
        "schema_version": "dpone.route_live.postgres_mssql.provider_binding.v1",
        "status": "certification_passed",
        "artifact_name": (f"release-candidate-postgres-mssql-route-{commit_sha}-{run_id}-{run_attempt}"),
        "artifact_id": 987,
        "artifact_digest": "sha256:" + "6" * 64,
        "commit_sha": commit_sha,
        "workflow_run_id": run_id,
        "workflow_run_attempt": run_attempt,
        "manifest_content_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "manifest_sha256": manifest["manifest_sha256"],
    }


__all__ = ["valid_route_live_manifest", "valid_route_live_provider_binding"]
