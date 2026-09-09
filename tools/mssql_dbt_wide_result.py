"""Immutable result and evidence persistence for the wide dbt live proof."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any, Protocol

from mssql_dbt_wide_evidence import (
    canonical_sha256,
    mssql_connection_sha256,
    require_exact_dbt_wide_evidence,
)

_SCHEMA_VERSION = "dpone.dbt_sqlserver.wide_materialization.v1"


class DbtWideFailureConfig(Protocol):
    @property
    def release_id(self) -> str: ...

    @property
    def source_schema(self) -> str: ...

    @property
    def source_table(self) -> str: ...

    @property
    def target_schema(self) -> str: ...

    @property
    def target_table(self) -> str: ...

    @property
    def project_dir(self) -> Path: ...

    @property
    def mssql_params(self) -> Mapping[str, Any]: ...


class DbtWideSourceSnapshot(Protocol):
    @property
    def git_head_sha(self) -> str: ...

    @property
    def source_snapshot_sha256(self) -> str: ...

    @property
    def worktree_dirty(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class DbtWideMaterializationResult:
    """Exact materialization and calculation evidence for the wide dbt model."""

    created_at: str
    release_id: str
    git_head_sha: str
    source_snapshot_sha256: str
    worktree_dirty: bool
    project_sha256: str
    mssql_connection_sha256: str
    source_relation: str
    output_relation: str
    model_unique_id: str
    materialization: str
    source_count: int
    target_count: int
    distinct_key_count: int
    source_column_count: int
    target_column_count: int
    schema_mismatch_count: int
    canonical_source_schema_sha256: str | None
    canonical_source_mismatch_count: int
    source_schema_sha256: str | None
    passthrough_schema_sha256: str | None
    source_data_sha256: str | None
    passthrough_data_sha256: str | None
    output_data_sha256: str | None
    output_data_rows: int
    relation_generation_sha256: str | None
    calculated_column_type: str
    calculated_column_nullable: bool
    calculated_mismatch_count: int
    run_results_path: str | None
    run_results_sha256: str | None
    manifest_path: str | None
    manifest_sha256: str | None
    passed: bool
    error: str | None = None
    schema_version: str = _SCHEMA_VERSION
    environment_class: str = "LOCAL_DOCKER"
    production_certification: str = "UNVERIFIED"
    production_certification_reason: str = "local Docker evidence is not production deployment certification"


def write_evidence(
    output_dir: Path,
    result: DbtWideMaterializationResult,
    *,
    forbidden_secret_values: tuple[str, ...] = (),
) -> Path:
    """Create one self-hashed artifact and verify every claimed PASS."""

    blockers = ["source_snapshot.worktree_dirty"] if result.worktree_dirty else []
    evidence_status = "PASS" if result.passed and not blockers else "UNVERIFIED"
    payload = {**asdict(result), "evidence_status": evidence_status, "blockers": blockers}
    payload["evidence_sha256"] = canonical_sha256(payload)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "mssql_dbt_wide_materialization.json"
    descriptor, pending_name = tempfile.mkstemp(prefix=".mssql-dbt-wide-pending-", suffix=".json", dir=output_dir)
    pending = Path(pending_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
        if evidence_status == "PASS":
            require_exact_dbt_wide_evidence(
                pending,
                release_id=result.release_id,
                git_head_sha=result.git_head_sha,
                source_snapshot_sha256=result.source_snapshot_sha256,
                output_relation=result.output_relation,
                connection_sha256=result.mssql_connection_sha256,
                expected_rows=10_000,
                expected_source_columns=201,
                expected_target_columns=202,
                forbidden_secret_values=forbidden_secret_values,
            )
        os.link(pending, path)
    finally:
        pending.unlink(missing_ok=True)
    return path


def failure_result(
    config: DbtWideFailureConfig,
    snapshot: DbtWideSourceSnapshot,
    error: str,
    *,
    project_sha256: str,
) -> DbtWideMaterializationResult:
    """Return the closed non-passing result for a failed dbt invocation."""

    return DbtWideMaterializationResult(
        created_at=now(),
        release_id=config.release_id,
        git_head_sha=snapshot.git_head_sha,
        source_snapshot_sha256=snapshot.source_snapshot_sha256,
        worktree_dirty=snapshot.worktree_dirty,
        project_sha256=project_sha256,
        mssql_connection_sha256=mssql_connection_sha256(config.mssql_params),
        source_relation=f"{config.source_schema}.{config.source_table}",
        output_relation=f"{config.target_schema}.{config.target_table}",
        model_unique_id="model.dpone_mssql_clickhouse_wide.wide_dbt_result",
        materialization="table",
        source_count=0,
        target_count=0,
        distinct_key_count=0,
        source_column_count=0,
        target_column_count=0,
        schema_mismatch_count=0,
        canonical_source_schema_sha256=None,
        canonical_source_mismatch_count=0,
        source_schema_sha256=None,
        passthrough_schema_sha256=None,
        source_data_sha256=None,
        passthrough_data_sha256=None,
        output_data_sha256=None,
        output_data_rows=0,
        relation_generation_sha256=None,
        calculated_column_type="",
        calculated_column_nullable=False,
        calculated_mismatch_count=0,
        run_results_path=None,
        run_results_sha256=None,
        manifest_path=None,
        manifest_sha256=None,
        passed=False,
        error=error,
    )


def retain_artifact(source: Path, output_dir: Path, name: str) -> Path:
    """Copy one dbt artifact exactly once into the evidence directory."""

    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / name
    with target.open("xb") as stream:
        stream.write(source.read_bytes())
    return target


def file_sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "DbtWideMaterializationResult",
    "failure_result",
    "file_sha256",
    "now",
    "retain_artifact",
    "write_evidence",
]
