from __future__ import annotations

import copy
import hashlib
import importlib.util
import io
import sys
import warnings
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

from tests.agent_policy._release_candidate_evidence_stress_helpers import (
    valid_stress_benchmark as _valid_stress_benchmark,
)

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "PaulKov/dpone"
COMMIT_SHA = "a" * 40
RELEASE = "v0.75.0"
RUN_ID = 101
RUN_ATTEMPT = 2
CHECK_RUN_ID = 201
CHECK_SUITE_ID = 301
ARTIFACT_ID = 401
JOB_ID = 501


def load_module(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


codec = load_module(
    "dpone_release_candidate_evidence_codec_helpers",
    "tools/agent_policy/release_candidate_evidence_codec.py",
)
policy = load_module(
    "dpone_release_candidate_evidence_policy_helpers",
    "tools/agent_policy/release_candidate_evidence_policy.py",
)


def canonical(payload: dict[str, Any]) -> bytes:
    return codec.canonical_json_bytes(payload)


def digest(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def valid_junit(role: str, *, commit_sha: str = COMMIT_SHA) -> dict[str, Any]:
    expected = policy.JUNIT_CASES[role]
    return {
        "schema_version": 1,
        "status": "PASS",
        "profile": policy.PROFILE,
        "commit_sha": commit_sha,
        "junit": f"{role}.xml",
        "gate": {"min_passed": len(expected), "max_skipped": 0},
        "totals": {
            "tests": len(expected),
            "passed": len(expected),
            "skipped": 0,
            "failures": 0,
            "errors": 0,
        },
        "cases": [
            {
                "node_id": case if "::" in case else f"tests/release/{role}.py::{case}",
                "status": "passed",
            }
            for case in expected
        ],
        "message": f"{role} executed exactly",
    }


def valid_route_execution(route: str) -> dict[str, Any]:
    case_id = policy.ROUTES[route]
    source, remainder = case_id.split("_to_", 1)
    sink, strategy = remainder.split("__", 1)
    return {
        "schema_version": "dpone.route_refresh_execution.v1",
        "route": {
            "source": source,
            "sink": sink,
            "strategy": strategy,
            "pair_id": f"{source}_to_{sink}",
            "case_id": case_id,
            "colon_id": f"{source}:{sink}:{strategy}",
        },
        "profile": copy.deepcopy(policy.ROUTE_PROFILES[route]),
        "dataset": "analytics.orders",
        "runner_id": policy.EXECUTION_RUNNER_ID,
        "route_refresh_plan_json": "route_refresh_plan.json",
        "plan_sha256": "b" * 64,
        "mode": "execute",
        "executed": True,
        "status": "succeeded",
        "passed": True,
        "ready_for_state_promotion": True,
        "summary": {
            "chunks_total": 1,
            "chunks_succeeded": 1,
            "chunks_failed": 0,
            "chunks_skipped": 0,
            "rows_read": policy.ROW_COUNT,
            "rows_written": policy.ROW_COUNT,
        },
        "chunks": [
            {
                "ordinal": 1,
                "idempotency_key": f"{case_id}:analytics.orders:1:1..{policy.ROW_COUNT}",
                "status": "succeeded",
                "passed": True,
                "rows_read": policy.ROW_COUNT,
                "rows_written": policy.ROW_COUNT,
                "artifact_path": "execution/chunks/chunk_1.json",
                "summary": "chunk applied",
                "blockers": [],
                "warnings": [],
                "duration_seconds": 1.0,
                "start": "1",
                "end": str(policy.ROW_COUNT),
                "partition": "id",
                "source_boundary": f"1..{policy.ROW_COUNT}",
                "sink_boundary": f"1..{policy.ROW_COUNT}",
            }
        ],
        "artifacts": [
            {
                "name": "route_refresh_plan",
                "path": "route_refresh_plan.json",
                "required": True,
                "exists": True,
                "sha256": "b" * 64,
                "summary": "artifact attached",
            }
        ],
        "artifact_index": {
            "route_refresh_plan": {
                "name": "route_refresh_plan",
                "path": "route_refresh_plan.json",
                "required": True,
                "exists": True,
                "sha256": "b" * 64,
                "summary": "artifact attached",
            }
        },
        "blockers": [],
        "warnings": [],
        "next_actions": [],
        "output_dir": "execution",
        "json_path": "execution/route_refresh_execution.json",
        "markdown_path": "execution/route_refresh_execution.md",
    }


def valid_route_verification(route: str, *, execution_raw: bytes) -> dict[str, Any]:
    case_id = policy.ROUTES[route]
    source_name, remainder = case_id.split("_to_", 1)
    sink_name, strategy = remainder.split("__", 1)
    side = {
        "row_count": policy.ROW_COUNT,
        "min_boundary": "1",
        "max_boundary": str(policy.ROW_COUNT),
        "typed_hash": "c" * 64,
        "duplicate_keys": 0,
        "null_keys": 0,
        "sample_rows": policy.ROW_COUNT,
        "summary": "exact live snapshot",
    }
    return {
        "schema_version": "dpone.route_refresh_verification.v1",
        "route": {
            "source": source_name,
            "sink": sink_name,
            "strategy": strategy,
            "pair_id": f"{source_name}_to_{sink_name}",
            "case_id": case_id,
            "colon_id": f"{source_name}:{sink_name}:{strategy}",
        },
        "profile": copy.deepcopy(policy.ROUTE_PROFILES[route]),
        "dataset": "analytics.orders",
        "runner_id": policy.VERIFICATION_RUNNER_ID,
        "route_refresh_execution_json": "route_refresh_execution.json",
        "execution_sha256": hashlib.sha256(execution_raw).hexdigest(),
        "status": "verified",
        "passed": True,
        "ready_for_state_promotion": True,
        "summary": {
            "chunks_total": 1,
            "chunks_verified": 1,
            "chunks_failed": 0,
            "source_rows": policy.ROW_COUNT,
            "sink_rows": policy.ROW_COUNT,
        },
        "chunks": [
            {
                "ordinal": 1,
                "idempotency_key": f"{case_id}:analytics.orders:1:1..{policy.ROW_COUNT}",
                "status": "verified",
                "passed": True,
                "source": copy.deepcopy(side),
                "sink": copy.deepcopy(side),
                "summary": "source and sink match",
                "blockers": [],
                "warnings": [],
            }
        ],
        "artifacts": [
            {
                "name": "route_refresh_execution",
                "path": "route_refresh_execution.json",
                "required": True,
                "exists": True,
                "sha256": hashlib.sha256(execution_raw).hexdigest(),
                "summary": "execution receipt verified",
            }
        ],
        "artifact_index": {
            "route_refresh_execution": {
                "name": "route_refresh_execution",
                "path": "route_refresh_execution.json",
                "required": True,
                "exists": True,
                "sha256": hashlib.sha256(execution_raw).hexdigest(),
                "summary": "execution receipt verified",
            }
        },
        "blockers": [],
        "warnings": [],
        "next_actions": [],
        "output_dir": "verification",
        "json_path": "verification/route_refresh_verification.json",
        "markdown_path": "verification/route_refresh_verification.md",
    }


def valid_stress_benchmark() -> dict[str, Any]:
    return _valid_stress_benchmark(
        row_count=policy.ROW_COUNT,
        partition_count=policy.STRESS_PARTITION_COUNT,
        metric_names=policy.STRESS_METRICS,
        partitioning=policy.STRESS_PARTITIONING,
        artifact_type=policy.STRESS_ARTIFACT_TYPE,
    )


def valid_source_payloads(
    *,
    repository: str = REPOSITORY,
    commit_sha: str = COMMIT_SHA,
) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {
        "exact_commit_checks": {
            "schema_version": 1,
            "status": "PASS",
            "decision": "GO",
            "repository": repository,
            "commit_sha": commit_sha,
            "ruleset_id": 1,
            "attempts": 1,
            "contexts": [
                {
                    "blocker_codes": [],
                    "context": "Quality checks (3.12)",
                    "current_observations": [
                        {
                            "context": "Quality checks (3.12)",
                            "source": "check_run",
                            "evidence_id": 1,
                            "state": "success",
                            "integration_id": policy.APP_ID,
                            "commit_sha": commit_sha,
                        }
                    ],
                    "integration_id": policy.APP_ID,
                    "observed_count": 1,
                    "status": "PASS",
                }
            ],
            "blockers": [],
            "policy_sha256": "2" * 64,
            "policy_path": ".agents/policy/github-branch-protection.yml",
        },
        "merge_receipt": {
            "schema_version": 1,
            "status": "PASS",
            "repository": repository,
            "integration_commit_sha": commit_sha,
            "check_run_id": 10,
            "github_app_id": policy.APP_ID,
            "workflow_run_id": 20,
            "workflow_run_attempt": 1,
            "artifact_id": 30,
            "binding_id": "sha256:" + "d" * 64,
            "artifact_digest": "sha256:" + "e" * 64,
            "artifact_size_bytes": 1024,
            "archive_sha256": "sha256:" + "f" * 64,
            "receipt_sha256": "sha256:" + "1" * 64,
        },
        "stress_benchmark": valid_stress_benchmark(),
    }
    for role in policy.JUNIT_CASES:
        payloads[role] = valid_junit(role, commit_sha=commit_sha)
    for route in policy.ROUTES:
        execution_role = f"{route}_execution"
        verification_role = f"{route}_verification"
        execution = valid_route_execution(route)
        execution_raw = canonical(execution)
        payloads[execution_role] = execution
        payloads[verification_role] = valid_route_verification(route, execution_raw=execution_raw)
    assert frozenset(payloads) == frozenset(policy.SOURCE_PATHS)
    return payloads


def write_valid_sources(
    directory: Path,
    *,
    repository: str = REPOSITORY,
    commit_sha: str = COMMIT_SHA,
) -> dict[str, Path]:
    payloads = valid_source_payloads(repository=repository, commit_sha=commit_sha)
    paths: dict[str, Path] = {}
    for role, relative in policy.SOURCE_PATHS.items():
        path = directory / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical(payloads[role]))
        paths[role] = path
    return paths


def zip_bytes(members: list[tuple[str | zipfile.ZipInfo, bytes]]) -> bytes:
    buffer = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content in members:
                archive.writestr(name, content)
    return buffer.getvalue()
