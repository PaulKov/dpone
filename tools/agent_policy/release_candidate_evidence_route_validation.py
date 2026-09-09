"""Pure validators for live route and benchmark release evidence."""

from __future__ import annotations

import importlib.util
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


codec = _load_sibling("dpone_release_candidate_codec_route_validation", "release_candidate_evidence_codec.py")
policy = _load_sibling("dpone_release_candidate_policy_route_validation", "release_candidate_evidence_policy.py")
stress_validation = _load_sibling(
    "dpone_release_candidate_stress_validation_route",
    "release_candidate_evidence_stress_validation.py",
)
route_chunks = _load_sibling(
    "dpone_release_candidate_route_chunks",
    "release_candidate_evidence_route_chunks.py",
)

EXECUTION_KEYS = frozenset(
    {
        "schema_version",
        "route",
        "profile",
        "dataset",
        "runner_id",
        "route_refresh_plan_json",
        "plan_sha256",
        "mode",
        "executed",
        "status",
        "passed",
        "ready_for_state_promotion",
        "summary",
        "chunks",
        "artifacts",
        "artifact_index",
        "blockers",
        "warnings",
        "next_actions",
        "output_dir",
        "json_path",
        "markdown_path",
    }
)
VERIFICATION_KEYS = frozenset(
    {
        "schema_version",
        "route",
        "profile",
        "dataset",
        "runner_id",
        "route_refresh_execution_json",
        "execution_sha256",
        "status",
        "passed",
        "ready_for_state_promotion",
        "summary",
        "chunks",
        "artifacts",
        "artifact_index",
        "blockers",
        "warnings",
        "next_actions",
        "output_dir",
        "json_path",
        "markdown_path",
    }
)
ARTIFACT_KEYS = frozenset({"name", "path", "required", "exists", "sha256", "summary"})
_LOWER_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def validate_route_execution(payload: Mapping[str, Any], *, route: str) -> dict[str, Any]:
    codec.require_exact_keys(payload, EXECUTION_KEYS, field=f"{route}.execution")
    expected_case = policy.ROUTES[route]
    identity = _route_identity(payload.get("route"), route=route, field=f"{route}.execution.route")
    dataset, runner_id = _route_context(payload, field=f"{route}.execution")
    if payload.get("profile") != policy.ROUTE_PROFILES[route] or runner_id != policy.EXECUTION_RUNNER_ID:
        raise ValueError(f"{route} execution profile or runner identity is not the frozen live producer")
    if (
        payload.get("schema_version") != "dpone.route_refresh_execution.v1"
        or identity["case_id"] != expected_case
        or payload.get("mode") != "execute"
        or payload.get("executed") is not True
        or payload.get("status") != "succeeded"
        or payload.get("passed") is not True
        or payload.get("ready_for_state_promotion") is not True
        or payload.get("blockers") != []
    ):
        raise ValueError(f"{route} execution is not a successful state-promotable run")
    summary_field = f"{route}.execution.summary"
    summary = _mapping(payload.get("summary"), summary_field)
    codec.require_exact_keys(
        summary,
        frozenset(
            {
                "chunks_total",
                "chunks_succeeded",
                "chunks_failed",
                "chunks_skipped",
                "rows_read",
                "rows_written",
            }
        ),
        field=summary_field,
    )
    rows_read = codec.require_positive_int(summary.get("rows_read"), field=f"{route}.execution.rows_read")
    rows_written = codec.require_positive_int(summary.get("rows_written"), field=f"{route}.execution.rows_written")
    chunks_total = _counter(summary, "chunks_total", field=f"{route}.execution", positive=True)
    chunks_succeeded = _counter(summary, "chunks_succeeded", field=f"{route}.execution", positive=True)
    chunks_failed = _counter(summary, "chunks_failed", field=f"{route}.execution", positive=False)
    chunks_skipped = _counter(summary, "chunks_skipped", field=f"{route}.execution", positive=False)
    if rows_read != rows_written or chunks_failed != 0 or chunks_skipped != 0:
        raise ValueError(f"{route} execution rows or chunk outcomes are inconsistent")
    chunks = _list(payload.get("chunks"), f"{route}.execution.chunks")
    if not chunks or chunks_total != len(chunks) or chunks_succeeded != len(chunks):
        raise ValueError(f"{route} execution chunk counters are inconsistent")
    chunk_rows_read, chunk_rows_written = route_chunks.validate_execution_chunks(chunks, route=route)
    if chunk_rows_read != rows_read or chunk_rows_written != rows_written:
        raise ValueError(f"{route} execution chunk rows do not reconcile with summary")
    _validate_artifact_closure(
        payload,
        field=f"{route}.execution",
        name="route_refresh_plan",
        path_field="route_refresh_plan_json",
        digest=payload.get("plan_sha256"),
        summary="artifact attached",
    )
    return {
        "status": "PASS",
        "route": expected_case,
        "dataset": dataset,
        "runner_id": runner_id,
        "rows": rows_read,
        "chunks": len(chunks),
    }


def validate_route_verification(
    payload: Mapping[str, Any],
    *,
    route: str,
    execution_raw: bytes,
) -> dict[str, Any]:
    codec.require_exact_keys(payload, VERIFICATION_KEYS, field=f"{route}.verification")
    expected_case = policy.ROUTES[route]
    identity = _route_identity(payload.get("route"), route=route, field=f"{route}.verification.route")
    dataset, runner_id = _route_context(payload, field=f"{route}.verification")
    if payload.get("profile") != policy.ROUTE_PROFILES[route] or runner_id != policy.VERIFICATION_RUNNER_ID:
        raise ValueError(f"{route} verification profile or runner identity is not the frozen live producer")
    if (
        payload.get("schema_version") != "dpone.route_refresh_verification.v1"
        or identity["case_id"] != expected_case
        or payload.get("status") != "verified"
        or payload.get("passed") is not True
        or payload.get("ready_for_state_promotion") is not True
        or payload.get("blockers") != []
    ):
        raise ValueError(f"{route} verification is not state-promotable")
    expected_execution_digest = codec.sha256_bytes(execution_raw).removeprefix("sha256:")
    if payload.get("execution_sha256") != expected_execution_digest:
        raise ValueError(f"{route} verification does not bind execution bytes")
    execution = codec.strict_json_object(execution_raw, field=f"{route}.execution")
    codec.require_exact_keys(execution, EXECUTION_KEYS, field=f"{route}.execution")
    execution_dataset, execution_runner_id = _route_context(
        execution,
        field=f"{route}.execution",
    )
    if dataset != execution_dataset or execution_runner_id != policy.EXECUTION_RUNNER_ID:
        raise ValueError(f"{route} verification dataset or bound execution runner is invalid")
    summary_field = f"{route}.verification.summary"
    summary = _mapping(payload.get("summary"), summary_field)
    codec.require_exact_keys(
        summary,
        frozenset({"chunks_total", "chunks_verified", "chunks_failed", "source_rows", "sink_rows"}),
        field=summary_field,
    )
    source_rows = codec.require_positive_int(summary.get("source_rows"), field=f"{route}.source_rows")
    sink_rows = codec.require_positive_int(summary.get("sink_rows"), field=f"{route}.sink_rows")
    chunks_total = _counter(summary, "chunks_total", field=f"{route}.verification", positive=True)
    chunks_verified = _counter(summary, "chunks_verified", field=f"{route}.verification", positive=True)
    chunks_failed = _counter(summary, "chunks_failed", field=f"{route}.verification", positive=False)
    if source_rows != sink_rows or chunks_failed != 0:
        raise ValueError(f"{route} verification summary is inconsistent")
    chunks = _list(payload.get("chunks"), f"{route}.verification.chunks")
    if not chunks or chunks_total != len(chunks) or chunks_verified != len(chunks):
        raise ValueError(f"{route} verification chunk counters are inconsistent")
    execution_chunks = _list(execution.get("chunks"), f"{route}.execution.chunks")
    chunk_source_rows, chunk_sink_rows = route_chunks.validate_verification_chunks(
        chunks,
        execution_chunks=execution_chunks,
        route=route,
    )
    if chunk_source_rows != source_rows or chunk_sink_rows != sink_rows:
        raise ValueError(f"{route} verification chunk rows do not reconcile with summary")
    _validate_artifact_closure(
        payload,
        field=f"{route}.verification",
        name="route_refresh_execution",
        path_field="route_refresh_execution_json",
        digest=expected_execution_digest,
        summary="execution receipt verified",
    )
    return {
        "status": "PASS",
        "route": expected_case,
        "dataset": dataset,
        "runner_id": runner_id,
        "rows": source_rows,
        "chunks": len(chunks),
    }


validate_stress = stress_validation.validate_stress


def _route_identity(value: Any, *, route: str, field: str) -> Mapping[str, Any]:
    identity = _mapping(value, field)
    codec.require_exact_keys(
        identity, frozenset({"source", "sink", "strategy", "pair_id", "case_id", "colon_id"}), field=field
    )
    expected_case = policy.ROUTES[route]
    source, remainder = expected_case.split("_to_", 1)
    sink, strategy = remainder.split("__", 1)
    expected = {
        "source": source,
        "sink": sink,
        "strategy": strategy,
        "pair_id": f"{source}_to_{sink}",
        "case_id": expected_case,
        "colon_id": f"{source}:{sink}:{strategy}",
    }
    if identity != expected:
        raise ValueError(f"{field} is not the frozen route identity")
    return identity


def _route_context(payload: Mapping[str, Any], *, field: str) -> tuple[str, str]:
    dataset = codec.require_string(payload.get("dataset"), field=f"{field}.dataset")
    runner_id = codec.require_string(payload.get("runner_id"), field=f"{field}.runner_id")
    return dataset, runner_id


def _validate_artifact_closure(
    payload: Mapping[str, Any],
    *,
    field: str,
    name: str,
    path_field: str,
    digest: Any,
    summary: str,
) -> None:
    path = codec.require_string(payload.get(path_field), field=f"{field}.{path_field}")
    if not isinstance(digest, str) or _LOWER_SHA256.fullmatch(digest) is None:
        raise ValueError(f"{field} artifact digest must be 64 lowercase hexadecimal characters")
    artifacts = _list(payload.get("artifacts"), f"{field}.artifacts")
    if len(artifacts) != 1:
        raise ValueError(f"{field} must contain exactly one authority artifact")
    artifact = _mapping(artifacts[0], f"{field}.artifacts[0]")
    codec.require_exact_keys(artifact, ARTIFACT_KEYS, field=f"{field}.artifacts[0]")
    expected = {
        "name": name,
        "path": path,
        "required": True,
        "exists": True,
        "sha256": digest,
        "summary": summary,
    }
    if artifact != expected:
        raise ValueError(f"{field} authority artifact is invalid")
    artifact_index = _mapping(payload.get("artifact_index"), f"{field}.artifact_index")
    if artifact_index != {name: expected}:
        raise ValueError(f"{field} artifact index does not bind the authority artifact")


def _counter(summary: Mapping[str, Any], name: str, *, field: str, positive: bool) -> int:
    validator = codec.require_positive_int if positive else codec.require_nonnegative_int
    return validator(summary.get(name), field=f"{field}.{name}")


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    return value


__all__ = ["validate_route_execution", "validate_route_verification", "validate_stress"]
