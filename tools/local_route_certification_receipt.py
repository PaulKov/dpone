#!/usr/bin/env python3
"""Exact local-Docker provenance receipts for route certification tools."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

from local_route_certification_validation import (  # noqa: E402
    artifact,
    assert_secret_free,
    canonical_sha256,
    integer_field,
    json_mapping,
    payload_passed,
    required_text,
    route_slug,
    verify_artifact,
    verify_result_payload,
    verify_runtime_decisions,
    verify_transport_details,
)
from mssql_dbt_wide_evidence import (  # noqa: E402
    assert_certification_payload_secret_free,
    copy_dbt_wide_evidence_bundle,
    load_json_object,
    require_exact_dbt_wide_evidence,
)

_SCHEMA_VERSION = "dpone.local_route_certification_receipt.v1"
_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1] / "docs/schemas/dpone.local-route-certification-receipt.v1.schema.json"
)
_PRODUCTION_REASON = "local Docker evidence is not production deployment certification"
_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "created_at",
        "release_id",
        "git_head_sha",
        "source_snapshot_sha256",
        "worktree_dirty",
        "environment_class",
        "route",
        "source_relation",
        "source_generation_sha256",
        "mssql_connection_sha256",
        "clickhouse_connection_sha256",
        "target_relation",
        "target_schema_sha256",
        "transport",
        "transport_details",
        "binary_format",
        "requested_acceleration_mode",
        "runtime_decisions",
        "result",
        "upstream_evidence",
        "artifact_cleanup",
        "passed",
        "evidence_status",
        "blockers",
        "production_certification",
        "production_certification_reason",
        "receipt_sha256",
    }
)


@dataclass(frozen=True, slots=True)
class LocalSourceSnapshot:
    """Git and source-tree identity captured before a live certification run."""

    git_head_sha: str
    source_snapshot_sha256: str
    worktree_dirty: bool


@dataclass(frozen=True, slots=True)
class LocalRouteIdentity:
    """Closed source, sink, and strategy identity for one route receipt."""

    source: str
    sink: str
    strategy: str

    @property
    def case_id(self) -> str:
        return f"{route_slug(self.source)}_to_{route_slug(self.sink)}__{route_slug(self.strategy)}"

    def to_dict(self) -> dict[str, str]:
        return {
            "case_id": self.case_id,
            "source": route_slug(self.source),
            "sink": route_slug(self.sink),
            "strategy": route_slug(self.strategy),
        }


@dataclass(frozen=True, slots=True)
class WrittenLocalRouteReceipt:
    """Path and canonical digest of one written certification receipt."""

    path: Path
    sha256: str


@dataclass(frozen=True, slots=True)
class LocalRouteExpectedSubject:
    """Trusted subject supplied independently when a receipt is verified."""

    release_id: str
    route: LocalRouteIdentity
    source_relation: str
    target_relation: str
    transport: str
    binary_format: str | None
    requested_acceleration_mode: str | None
    expected_rows: int
    expected_column_count: int
    mssql_connection_sha256: str
    clickhouse_connection_sha256: str
    target_schema_sha256: str
    s3_endpoint_sha256: str | None = None
    s3_bucket: str | None = None
    s3_named_collection: str | None = None


def capture_local_source_snapshot(repo_root: Path | None = None) -> LocalSourceSnapshot:
    """Capture HEAD plus tracked/untracked source bytes without mutating git state."""

    root = (repo_root or Path.cwd()).resolve()
    head = _git(root, "rev-parse", "HEAD").strip()
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    tracked_diff = subprocess.run(
        ("git", "diff", "--binary", "HEAD", "--", "src", "packages", "tests", "tools", "docs"),
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    untracked = _git(
        root,
        "ls-files",
        "--others",
        "--exclude-standard",
        "--",
        "src",
        "packages",
        "tests",
        "tools",
        "docs",
    ).splitlines()
    hasher = hashlib.sha256()
    hasher.update(head.encode())
    hasher.update(tracked_diff)
    for relative in sorted(untracked):
        path = root / relative
        if not path.is_file() or "/target/" in f"/{relative}" or relative.endswith(".user.yml"):
            continue
        hasher.update(relative.encode())
        hasher.update(b"\0")
        hasher.update(path.read_bytes())
        hasher.update(b"\0")
    return LocalSourceSnapshot(
        git_head_sha=head,
        source_snapshot_sha256="sha256:" + hasher.hexdigest(),
        worktree_dirty=bool(status.strip()),
    )


def write_local_route_certification_receipt(
    *,
    output_dir: Path,
    source_snapshot: LocalSourceSnapshot,
    final_source_snapshot: LocalSourceSnapshot,
    result_path: Path,
    release_id: str,
    route: LocalRouteIdentity,
    source_relation: str,
    target_relation: str,
    transport: str,
    transport_details: Mapping[str, Any],
    binary_format: str | None,
    requested_acceleration_mode: str | None,
    runtime_decisions: Sequence[Mapping[str, Any]],
    mssql_connection_sha256: str,
    clickhouse_connection_sha256: str,
    target_schema_sha256: str | None,
    upstream_evidence_path: Path | None,
    cleanup_verified: bool,
    remaining_artifacts: Sequence[str],
    passed: bool,
    created_at: str | None = None,
) -> WrittenLocalRouteReceipt:
    """Write a canonical receipt that binds one live result to exact source bytes."""

    output_dir.mkdir(parents=True, exist_ok=True)
    retained_result = output_dir / result_path.name
    if retained_result.resolve() != result_path.resolve():
        with retained_result.open("xb") as stream:
            stream.write(result_path.read_bytes())
    result = artifact(retained_result)
    if passed and not payload_passed(result_path):
        raise ValueError("local_route_certification_result_not_passed")
    result_payload = verify_result_payload(retained_result, transport=transport) if passed else None
    if upstream_evidence_path is not None:
        raw_upstream = _upstream_payload(upstream_evidence_path)
        if raw_upstream is None:
            raise ValueError("upstream_evidence_invalid")
        expected_rows = integer_field(result_payload, "rows") if result_payload is not None else 10_000
        expected_target_columns = integer_field(result_payload, "column_count") if result_payload is not None else 202
        require_exact_dbt_wide_evidence(
            upstream_evidence_path,
            release_id=release_id,
            git_head_sha=source_snapshot.git_head_sha,
            source_snapshot_sha256=source_snapshot.source_snapshot_sha256,
            output_relation=source_relation,
            connection_sha256=mssql_connection_sha256,
            expected_rows=expected_rows,
            expected_source_columns=expected_target_columns - 1,
            expected_target_columns=expected_target_columns,
        )
        if result_payload is not None and (
            result_payload.get("typed_hash_source") != raw_upstream.get("output_data_sha256")
            or result_payload.get("source_count") != raw_upstream.get("output_data_rows")
            or result_payload.get("column_count") != raw_upstream.get("target_column_count")
        ):
            raise ValueError("local_route_certification_upstream_generation_mismatch")
    blockers = ["source_snapshot.worktree_dirty"] if source_snapshot.worktree_dirty else []
    if final_source_snapshot != source_snapshot:
        blockers.append("source_snapshot.changed_during_run")
    if not cleanup_verified or remaining_artifacts:
        blockers.append("artifact_cleanup.incomplete")
    evidence_status = "PASS" if passed and not blockers else "UNVERIFIED"
    upstream = _copy_upstream_evidence(output_dir, upstream_evidence_path)
    upstream_payload = _upstream_payload(upstream)
    decisions = [json_mapping(item) for item in runtime_decisions]
    assert_secret_free(decisions)
    payload: dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "created_at": created_at or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "release_id": required_text(release_id, "release_id"),
        "git_head_sha": source_snapshot.git_head_sha,
        "source_snapshot_sha256": source_snapshot.source_snapshot_sha256,
        "worktree_dirty": source_snapshot.worktree_dirty,
        "environment_class": "LOCAL_DOCKER",
        "route": route.to_dict(),
        "source_relation": required_text(source_relation, "source_relation"),
        "source_generation_sha256": (
            upstream_payload.get("relation_generation_sha256") if upstream_payload is not None else None
        ),
        "mssql_connection_sha256": mssql_connection_sha256,
        "clickhouse_connection_sha256": clickhouse_connection_sha256,
        "target_relation": required_text(target_relation, "target_relation"),
        "target_schema_sha256": target_schema_sha256,
        "transport": required_text(transport, "transport"),
        "transport_details": json_mapping(transport_details),
        "binary_format": binary_format,
        "requested_acceleration_mode": requested_acceleration_mode,
        "runtime_decisions": decisions,
        "result": result,
        "upstream_evidence": artifact(upstream) if upstream is not None else None,
        "artifact_cleanup": {
            "verified": bool(cleanup_verified),
            "remaining_artifacts": sorted({required_text(item, "remaining_artifact") for item in remaining_artifacts}),
        },
        "passed": bool(passed),
        "evidence_status": evidence_status,
        "blockers": blockers,
        "production_certification": "UNVERIFIED",
        "production_certification_reason": _PRODUCTION_REASON,
    }
    receipt_sha256 = canonical_sha256(payload)
    payload["receipt_sha256"] = receipt_sha256
    path = output_dir / "local_route_certification_receipt.json"
    with path.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
    return WrittenLocalRouteReceipt(path=path, sha256=receipt_sha256)


def require_exact_upstream_evidence(
    path: Path,
    *,
    source_snapshot: LocalSourceSnapshot,
    output_relation: str,
    connection_sha256: str,
    release_id: str,
    expected_rows: int,
    expected_source_columns: int,
    expected_target_columns: int,
    forbidden_secret_values: tuple[str, ...] = (),
) -> Mapping[str, Any]:
    """Fail closed unless an upstream producer proves this exact clean relation."""

    return require_exact_dbt_wide_evidence(
        path,
        release_id=release_id,
        git_head_sha=source_snapshot.git_head_sha,
        source_snapshot_sha256=source_snapshot.source_snapshot_sha256,
        output_relation=output_relation,
        connection_sha256=connection_sha256,
        expected_rows=expected_rows,
        expected_source_columns=expected_source_columns,
        expected_target_columns=expected_target_columns,
        forbidden_secret_values=forbidden_secret_values,
    )


def verify_local_route_certification_receipt(
    path: Path,
    *,
    source_snapshot: LocalSourceSnapshot,
    expected: LocalRouteExpectedSubject,
    forbidden_secret_values: tuple[str, ...] = (),
) -> Mapping[str, Any]:
    """Authenticate a clean local receipt and every file it references."""

    if path.is_symlink() or not path.is_file():
        raise ValueError("local_route_certification_receipt_path_invalid")
    value = load_json_object(path)
    assert_certification_payload_secret_free(value, forbidden_secret_values=forbidden_secret_values)
    if not isinstance(value, Mapping) or frozenset(value) != _RECEIPT_FIELDS:
        raise ValueError("local_route_certification_receipt_shape_invalid")
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    if tuple(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value)):
        raise ValueError("local_route_certification_receipt_schema_invalid")
    if value.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError("local_route_certification_receipt_schema_invalid")
    canonical = dict(value)
    claimed_sha256 = canonical.pop("receipt_sha256", None)
    if claimed_sha256 != canonical_sha256(canonical):
        raise ValueError("local_route_certification_receipt_digest_mismatch")
    if value.get("git_head_sha") != source_snapshot.git_head_sha:
        raise ValueError("local_route_certification_receipt_git_head_mismatch")
    if value.get("source_snapshot_sha256") != source_snapshot.source_snapshot_sha256:
        raise ValueError("local_route_certification_receipt_source_snapshot_mismatch")
    if value.get("worktree_dirty") is not False or source_snapshot.worktree_dirty:
        raise ValueError("local_route_certification_receipt_worktree_dirty")
    if value.get("passed") is not True or value.get("evidence_status") != "PASS" or value.get("blockers") != []:
        raise ValueError("local_route_certification_receipt_not_passed")
    if value.get("artifact_cleanup") != {"verified": True, "remaining_artifacts": []}:
        raise ValueError("local_route_certification_receipt_cleanup_invalid")
    if value.get("environment_class") != "LOCAL_DOCKER" or value.get("production_certification") != "UNVERIFIED":
        raise ValueError("local_route_certification_receipt_environment_invalid")
    expected_fields = {
        "release_id": required_text(expected.release_id, "release_id"),
        "route": expected.route.to_dict(),
        "source_relation": required_text(expected.source_relation, "source_relation"),
        "target_relation": required_text(expected.target_relation, "target_relation"),
        "transport": required_text(expected.transport, "transport"),
        "binary_format": expected.binary_format,
        "requested_acceleration_mode": expected.requested_acceleration_mode,
        "mssql_connection_sha256": expected.mssql_connection_sha256,
        "clickhouse_connection_sha256": expected.clickhouse_connection_sha256,
        "target_schema_sha256": expected.target_schema_sha256,
    }
    for field, expected_value in expected_fields.items():
        if value.get(field) != expected_value:
            raise ValueError(f"local_route_certification_receipt_{field}_mismatch")
    result_path = verify_artifact(path.parent, value.get("result"))
    result_payload = verify_result_payload(result_path, transport=str(value.get("transport") or ""))
    assert_certification_payload_secret_free(result_payload, forbidden_secret_values=forbidden_secret_values)
    if (
        result_payload.get("rows") != expected.expected_rows
        or result_payload.get("column_count") != expected.expected_column_count
    ):
        raise ValueError("local_route_certification_release_shape_mismatch")
    verify_transport_details(value, result_payload)
    if value.get("transport") == "parquet_s3_pull":
        details = value.get("transport_details")
        if not isinstance(details, Mapping) or (
            details.get("s3_endpoint_sha256") != expected.s3_endpoint_sha256
            or details.get("bucket") != expected.s3_bucket
            or details.get("named_collection") != expected.s3_named_collection
        ):
            raise ValueError("local_route_certification_s3_authority_mismatch")
    upstream = value.get("upstream_evidence")
    if upstream is not None:
        upstream_path = verify_artifact(path.parent, upstream)
        require_exact_upstream_evidence(
            upstream_path,
            source_snapshot=source_snapshot,
            output_relation=str(value.get("source_relation") or ""),
            connection_sha256=str(value.get("mssql_connection_sha256") or ""),
            release_id=str(value.get("release_id") or ""),
            expected_rows=expected.expected_rows,
            expected_source_columns=expected.expected_column_count - 1,
            expected_target_columns=expected.expected_column_count,
            forbidden_secret_values=forbidden_secret_values,
        )
        upstream_payload = _upstream_payload(upstream_path)
        if upstream_payload is None:
            raise ValueError("local_route_certification_upstream_invalid")
        if value.get("source_generation_sha256") != upstream_payload.get("relation_generation_sha256"):
            raise ValueError("local_route_certification_source_generation_mismatch")
        if result_payload.get("typed_hash_source") != upstream_payload.get("output_data_sha256"):
            raise ValueError("local_route_certification_source_data_mismatch")
        if result_payload.get("source_count") != upstream_payload.get("output_data_rows"):
            raise ValueError("local_route_certification_source_count_mismatch")
        if result_payload.get("column_count") != upstream_payload.get("target_column_count"):
            raise ValueError("local_route_certification_source_column_count_mismatch")
    elif value.get("source_generation_sha256") is not None:
        raise ValueError("local_route_certification_unbound_source_identity")
    verify_runtime_decisions(value, result_payload)
    return value


def _copy_upstream_evidence(output_dir: Path, path: Path | None) -> Path | None:
    if path is None:
        return None
    if not path.is_file():
        raise ValueError(f"certification_artifact_missing:{path.name}")
    return copy_dbt_wide_evidence_bundle(output_dir, path)


def _upstream_payload(path: Path | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    value = load_json_object(path)
    if not isinstance(value, Mapping):
        raise ValueError("upstream_evidence_invalid")
    return value


def _git(root: Path, *args: str) -> str:
    return subprocess.run(("git", *args), cwd=root, check=True, capture_output=True, text=True).stdout
