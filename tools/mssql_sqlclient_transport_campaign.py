#!/usr/bin/env python3
"""Close the exact synthetic SqlClient transport campaign without private coordinates."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.services.mssql_native_evidence_privacy import scan_mssql_native_shareable_artifacts

_SCHEMA_VERSION = "dpone.mssql-sqlclient.transport-campaign.v2"
_EVIDENCE_VERSION = "dpone.mssql-sqlclient.transport-certification.v2"
_SHA = re.compile(r"[0-9a-f]{40}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_TREE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_EVIDENCE_FIELDS = {
    "schema_version",
    "status",
    "scenario",
    "source_commit_sha",
    "source_tree_oid",
    "runner_image_sha256",
    "fixture_id",
    "row_count",
    "receipt_count",
    "import_parallelism",
    "layout_version",
    "elapsed_seconds",
    "stage_seconds",
    "verification_seconds",
    "delivery_phases",
    "writer_phases",
    "business_rows_read_back",
    "bcp_process_count",
    "synthetic_only",
    "package_version",
    "artifact_sha256",
    "writer_identity_sha256",
    "runtime_identity_sha256",
    "protocol",
    "recovery_classification",
    "recovery_observation",
}
_RUNNER_FIELDS = {
    "schema_version",
    "status",
    "source_commit_sha",
    "source_tree_oid",
    "runner_image_sha256",
    "runner_platform",
    "source_mode",
    "worktree_dirty",
    "execution_count",
    "executions",
    "evidence_artifacts",
}
_EXECUTION_FIELDS = {
    "scenario",
    "fixture_id",
    "row_count",
    "layout_version",
    "import_parallelism",
    "max_rows",
    "max_bytes",
    "max_pending",
    "max_staging_tables",
    "encoding_parallelism",
    "retained_work_capacity",
    "container_memory_limit_bytes",
    "container_memory_swap_limit_bytes",
    "container_oom_kill_disabled",
    "container_sampled_cache_adjusted_memory_bytes",
    "container_image_sha256",
}
_REQUIRED_EXECUTIONS = {
    ("success", "narrow-sqlclient-v1", 10_000, 1, 1),
    ("success", "wide100-sqlclient-v1", 10_000, 1, 1),
    ("success", "narrow-sqlclient-v1", 10_000, 2, 2),
    ("success", "wide100-sqlclient-v1", 10_000, 2, 2),
    ("success", "narrow-sqlclient-v1", 1_000_000, 2, 2),
    ("success", "wide100-sqlclient-v1", 1_000_000, 2, 2),
    ("force_kill_recovery", "wide100-sqlclient-v1", 10_000, 2, 2),
}


@dataclass(frozen=True, order=True, slots=True)
class CampaignCell:
    """One required, independently named certification observation."""

    scenario: str
    fixture_id: str
    row_count: int
    layout_version: int
    import_parallelism: int


REQUIRED_CELLS = (
    CampaignCell("force_kill_recovery", "wide100-sqlclient-v1", 10_000, 2, 2),
    CampaignCell("success", "narrow-sqlclient-v1", 10_000, 1, 1),
    CampaignCell("success", "narrow-sqlclient-v1", 10_000, 2, 2),
    CampaignCell("success", "narrow-sqlclient-v1", 1_000_000, 2, 2),
    CampaignCell("success", "wide100-sqlclient-v1", 10_000, 1, 1),
    CampaignCell("success", "wide100-sqlclient-v1", 10_000, 2, 2),
    CampaignCell("success", "wide100-sqlclient-v1", 1_000_000, 2, 2),
)


def close_campaign(
    evidence_dir: Path,
    *,
    source_commit_sha: str,
    runner_receipt: Path,
    package_version: str,
    output: Path,
    secret_needles: tuple[str, ...] = (),
) -> Path:
    """Verify the complete exact-source matrix and atomically write its index."""
    if _SHA.fullmatch(source_commit_sha) is None or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", package_version) is None:
        raise ValueError("sqlclient_campaign.invalid_source_commit")
    runner = _load_runner_receipt(runner_receipt, source_commit_sha)
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    root = evidence_dir.resolve(strict=True)
    if not root.is_dir() or evidence_dir.is_symlink():
        raise ValueError("sqlclient_campaign.unsafe_evidence_directory")
    evidence_paths = tuple(sorted(root.glob("*.json")))
    artifacts = runner["evidence_artifacts"]
    if set(artifacts) != {path.name for path in evidence_paths} or any(
        artifacts[path.name] != hashlib.sha256(path.read_bytes()).hexdigest() for path in evidence_paths
    ):
        raise ValueError("sqlclient_campaign.runner_evidence_mismatch")
    scan_mssql_native_shareable_artifacts(evidence_paths, secret_needles=secret_needles)
    observed: dict[CampaignCell, dict[str, Any]] = {}
    paths: dict[CampaignCell, Path] = {}
    for path in evidence_paths:
        value = _load_evidence(path, runner, package_version)
        cell = _cell(value)
        if cell in observed:
            raise ValueError("sqlclient_campaign.duplicate_cell")
        observed[cell], paths[cell] = value, path
    required = set(REQUIRED_CELLS)
    if set(observed) != required:
        raise ValueError("sqlclient_campaign.cell_closure_mismatch")
    identity_fields = ("artifact_sha256", "writer_identity_sha256", "runtime_identity_sha256")
    if any(len({value[field] for value in observed.values()}) != 1 for field in identity_fields):
        raise ValueError("sqlclient_campaign.mixed_runtime_identity")
    entries = []
    for cell in REQUIRED_CELLS:
        path = paths[cell]
        entries.append(
            {
                "cell": asdict(cell),
                "artifact": path.name,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "status": "PASS",
        "source_commit_sha": source_commit_sha,
        "source_tree_oid": runner["source_tree_oid"],
        "runner_image_sha256": runner["runner_image_sha256"],
        "runner_platform": runner["runner_platform"],
        "runner_receipt_sha256": hashlib.sha256(runner_receipt.read_bytes()).hexdigest(),
        "package_version": package_version,
        "cell_count": len(entries),
        "cells": entries,
        "privacy_scan_status": "PASS",
        "synthetic_only": True,
    }
    _atomic_create(output, _canonical(payload))
    scan_mssql_native_shareable_artifacts((output,), secret_needles=secret_needles)
    return output


def _load_runner_receipt(path: Path, source_commit_sha: str) -> dict[str, Any]:
    value = _load_json(path, "sqlclient_campaign.invalid_runner_receipt")
    if set(value) != _RUNNER_FIELDS or (
        value["schema_version"] != "dpone.mssql-sqlclient.certification-runner.v3"
        or value["status"] != "PASS"
        or value["source_commit_sha"] != source_commit_sha
        or _TREE.fullmatch(str(value["source_tree_oid"])) is None
        or _DIGEST.fullmatch(str(value["runner_image_sha256"])) is None
        or value["runner_platform"] != "linux/amd64"
        or value["source_mode"] != "exact_git_archive"
        or value["worktree_dirty"] is not False
        or type(value["execution_count"]) is not int
        or value["execution_count"] != len(_REQUIRED_EXECUTIONS)
        or not _valid_executions(value["executions"], value["runner_image_sha256"])
        or not _valid_artifacts(value["evidence_artifacts"])
    ):
        raise ValueError("sqlclient_campaign.invalid_runner_receipt")
    return value


def _valid_executions(value: object, image_digest: object) -> bool:
    if not isinstance(value, list) or len(value) != len(_REQUIRED_EXECUTIONS):
        return False
    observed = set()
    for item in value:
        if (
            not isinstance(item, dict)
            or set(item) != _EXECUTION_FIELDS
            or item["container_image_sha256"] != image_digest
            or any(
                type(item[field]) is not int
                for field in (
                    "row_count",
                    "layout_version",
                    "import_parallelism",
                    "max_rows",
                    "max_bytes",
                    "max_pending",
                    "max_staging_tables",
                    "encoding_parallelism",
                    "retained_work_capacity",
                    "container_memory_limit_bytes",
                    "container_memory_swap_limit_bytes",
                    "container_sampled_cache_adjusted_memory_bytes",
                )
            )
            or type(item["container_oom_kill_disabled"]) is not bool
            or not _valid_execution_resources(item)
        ):
            return False
        observed.add(
            (
                item["scenario"],
                item["fixture_id"],
                item["row_count"],
                item["layout_version"],
                item["import_parallelism"],
            )
        )
    return observed == _REQUIRED_EXECUTIONS


def _valid_execution_resources(item: dict[str, Any]) -> bool:
    expected_rows = (
        5_000
        if item["scenario"] == "force_kill_recovery"
        else 8_192
        if item["fixture_id"] == "wide100-sqlclient-v1"
        else 65_536
    )
    expected_capacity = max(2, item["import_parallelism"]) + 1
    return (
        item["max_rows"] == expected_rows
        and item["max_bytes"] == 48 << 20
        and item["max_pending"] == 1
        and item["max_staging_tables"] == 128
        and item["encoding_parallelism"] == 2
        and item["retained_work_capacity"] == expected_capacity
        and item["container_memory_limit_bytes"] == 2 << 30
        and item["container_memory_swap_limit_bytes"] == 2 << 30
        and item["container_oom_kill_disabled"] is False
        and 0 < item["container_sampled_cache_adjusted_memory_bytes"] <= item["container_memory_limit_bytes"]
    )


def _valid_artifacts(value: object) -> bool:
    return (
        isinstance(value, dict)
        and all(type(name) is str and Path(name).name == name for name in value)
        and all(_DIGEST.fullmatch(str(digest)) is not None for digest in value.values())
    )


def _load_evidence(path: Path, runner: dict[str, Any], package_version: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1_000_000:
        raise ValueError("sqlclient_campaign.unsafe_evidence_file")
    try:
        value = _load_json(path, "sqlclient_campaign.invalid_evidence")
    except ValueError:
        raise
    if set(value) != _EVIDENCE_FIELDS or value.get("schema_version") != _EVIDENCE_VERSION:
        raise ValueError("sqlclient_campaign.invalid_evidence")
    for field in ("row_count", "receipt_count", "import_parallelism", "layout_version"):
        if type(value.get(field)) is not int:
            raise ValueError("sqlclient_campaign.invalid_integer")
    if (
        value.get("status") != "PASS"
        or value.get("source_commit_sha") != runner["source_commit_sha"]
        or value.get("source_tree_oid") != runner["source_tree_oid"]
        or value.get("runner_image_sha256") != runner["runner_image_sha256"]
        or value.get("package_version") != package_version
    ):
        raise ValueError("sqlclient_campaign.untrusted_evidence")
    for field in ("artifact_sha256", "writer_identity_sha256", "runtime_identity_sha256"):
        if _DIGEST.fullmatch(str(value.get(field, ""))) is None:
            raise ValueError("sqlclient_campaign.invalid_identity")
    if (
        value.get("synthetic_only") is not True
        or value.get("business_rows_read_back") != 0
        or value.get("bcp_process_count") != 0
        or not _finite_nonnegative(value.get("elapsed_seconds"))
    ):
        raise ValueError("sqlclient_campaign.invalid_privacy_or_readback")
    expected_protocol = (
        "dpone.mssql-sqlclient.ipc.v1" if value.get("layout_version") == 1 else "dpone.mssql-sqlclient.ipc.v2"
    )
    if value.get("protocol") != expected_protocol:
        raise ValueError("sqlclient_campaign.invalid_protocol")
    scenario = value.get("scenario")
    if scenario == "success":
        if value.get("recovery_classification") != "not_required" or value.get("recovery_observation") is not None:
            raise ValueError("sqlclient_campaign.invalid_success")
        if value.get("row_count", 0) <= 0 or value.get("receipt_count", 0) <= 0:
            raise ValueError("sqlclient_campaign.invalid_success")
        if not all(_finite_nonnegative(value.get(field)) for field in ("stage_seconds", "verification_seconds")):
            raise ValueError("sqlclient_campaign.incomplete_success_timing")
        _validate_success_phases(value)
    elif scenario == "force_kill_recovery":
        expected = {
            "initial_terminal": "UNKNOWN",
            "initial_writer_outcome": "lost_ack",
            "bulk_copy_active": True,
            "transaction_active": True,
            "session_applock_held": True,
            "exact_stage_lock_held": True,
            "competing_writer_active": True,
            "competing_writer_ignored": True,
            "barrier_settled": True,
            "repeated_digest_stable": True,
            "final_terminal": "RETIRED",
            "stage_absent": True,
            "custody_clear": True,
            "source_reopened": False,
            "writer_relaunched": False,
        }
        if (
            value.get("recovery_classification") != "partial_retired"
            or value.get("recovery_observation") != expected
            or value.get("receipt_count") != 0
            or value.get("delivery_phases") != {}
            or value.get("writer_phases") != {}
        ):
            raise ValueError("sqlclient_campaign.invalid_recovery")
    else:
        raise ValueError("sqlclient_campaign.invalid_scenario")
    return value


def _validate_success_phases(value: dict[str, Any]) -> None:
    phases = value.get("delivery_phases")
    if not isinstance(phases, dict) or set(phases) != {"encode", "import_verify"}:
        raise ValueError("sqlclient_campaign.invalid_delivery_phases")
    fields = {
        "effective_parallelism",
        "failed_operations",
        "operations",
        "peak_workers",
        "wall_seconds",
        "worker_active_seconds",
    }
    for phase in phases.values():
        if not isinstance(phase, dict) or set(phase) != fields:
            raise ValueError("sqlclient_campaign.invalid_delivery_phases")
        if phase["failed_operations"] != 0 or not all(_finite_nonnegative(phase[field]) for field in fields):
            raise ValueError("sqlclient_campaign.invalid_delivery_phases")
        if phase["operations"] <= 0 or phase["peak_workers"] <= 0:
            raise ValueError("sqlclient_campaign.invalid_delivery_phases")
    writer = value.get("writer_phases")
    if not isinstance(writer, dict) or set(writer) != {"launch_seconds", "write_seconds", "dispose_seconds"}:
        raise ValueError("sqlclient_campaign.invalid_writer_phases")
    if not all(_finite_nonnegative(item) for item in writer.values()):
        raise ValueError("sqlclient_campaign.invalid_writer_phases")


def _finite_nonnegative(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value)) and float(value) >= 0


def _load_json(path: Path, code: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1_000_000:
        raise ValueError(code)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(code) from error
    if not isinstance(value, dict):
        raise ValueError(code)
    return value


def _cell(value: dict[str, Any]) -> CampaignCell:
    try:
        if any(type(value[field]) is not int for field in ("row_count", "layout_version", "import_parallelism")):
            raise TypeError
        return CampaignCell(
            scenario=str(value["scenario"]),
            fixture_id=str(value["fixture_id"]),
            row_count=value["row_count"],
            layout_version=value["layout_version"],
            import_parallelism=value["import_parallelism"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("sqlclient_campaign.invalid_cell") from error


def _canonical(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _atomic_create(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--source-commit-sha", required=True)
    parser.add_argument("--runner-receipt", type=Path, required=True)
    parser.add_argument("--package-version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--secret-env", action="append", default=[])
    args = parser.parse_args()
    needles = tuple(os.environ[name] for name in args.secret_env if os.environ.get(name))
    close_campaign(
        args.evidence_dir,
        source_commit_sha=args.source_commit_sha,
        runner_receipt=args.runner_receipt,
        package_version=args.package_version,
        output=args.output,
        secret_needles=needles,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
