#!/usr/bin/env python3
"""Aggregate the exact three-slot local MSSQL/dbt -> ClickHouse release proof."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

from local_route_certification_receipt import (  # noqa: E402
    LocalRouteExpectedSubject,
    LocalRouteIdentity,
    capture_local_source_snapshot,
    verify_local_route_certification_receipt,
)
from mssql_clickhouse_wide_release_authority import load_authority  # noqa: E402
from mssql_dbt_wide_evidence import (  # noqa: E402
    assert_certification_payload_secret_free,
    canonical_sha256,
    load_json_object,
    require_exact_dbt_wide_evidence,
)

_SCHEMA_VERSION = "dpone.mssql-clickhouse-wide-release-campaign.v1"
_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1] / "docs/schemas/dpone.mssql-clickhouse-wide-release-campaign.v1.schema.json"
)
_SLOTS = ("bcp_native_required", "bcp_native_off", "parquet_s3_pull")
_EXPECTED_ROWS = 10_000
_EXPECTED_SOURCE_COLUMNS = 201
_EXPECTED_TARGET_COLUMNS = 202


def build_campaign(
    *,
    release_id: str,
    source_relation: str,
    dbt_evidence: Path,
    authority: Path,
    receipts: Mapping[str, Path],
    targets: Mapping[str, str],
    output_dir: Path,
    mssql_connection_sha256: str,
    clickhouse_connection_sha256: str,
    target_schema_sha256_by_slot: Mapping[str, str],
    s3_endpoint_sha256: str,
    s3_bucket: str,
    s3_named_collection: str,
    forbidden_secret_values: tuple[str, ...],
) -> Path:
    """Verify trusted subjects, copy their exact closure, and write one index."""

    if (
        tuple(sorted(receipts)) != tuple(sorted(_SLOTS))
        or tuple(sorted(targets)) != tuple(sorted(_SLOTS))
        or tuple(sorted(target_schema_sha256_by_slot)) != tuple(sorted(_SLOTS))
    ):
        raise ValueError("wide_release_campaign_slot_closure_invalid")
    _require_forbidden_secret_values(forbidden_secret_values)
    snapshot = capture_local_source_snapshot()
    if snapshot.worktree_dirty:
        raise ValueError("wide_release_campaign_worktree_dirty")
    upstream = require_exact_dbt_wide_evidence(
        dbt_evidence,
        release_id=release_id,
        git_head_sha=snapshot.git_head_sha,
        source_snapshot_sha256=snapshot.source_snapshot_sha256,
        output_relation=source_relation,
        connection_sha256=mssql_connection_sha256,
        expected_rows=_EXPECTED_ROWS,
        expected_source_columns=_EXPECTED_SOURCE_COLUMNS,
        expected_target_columns=_EXPECTED_TARGET_COLUMNS,
        forbidden_secret_values=forbidden_secret_values,
    )
    authority_value = load_authority(
        authority,
        release_id=release_id,
        source_relation=source_relation,
    )
    assert_certification_payload_secret_free(
        authority_value,
        forbidden_secret_values=forbidden_secret_values,
    )
    authority_expected = {
        "mssql_connection_sha256": mssql_connection_sha256,
        "clickhouse_connection_sha256": clickhouse_connection_sha256,
        "target_schema_sha256_by_slot": dict(target_schema_sha256_by_slot),
        "s3_endpoint_sha256": s3_endpoint_sha256,
        "s3_bucket": s3_bucket,
        "s3_named_collection": s3_named_collection,
    }
    if any(authority_value.get(field) != expected for field, expected in authority_expected.items()):
        raise ValueError("wide_release_campaign_authority_mismatch")

    verified_receipts: dict[str, Mapping[str, Any]] = {}
    for slot in _SLOTS:
        expected = _expected_subject(
            slot=slot,
            release_id=release_id,
            source_relation=source_relation,
            target_relation=targets[slot],
            mssql_connection_sha256=mssql_connection_sha256,
            clickhouse_connection_sha256=clickhouse_connection_sha256,
            target_schema_sha256=target_schema_sha256_by_slot[slot],
            s3_endpoint_sha256=s3_endpoint_sha256,
            s3_bucket=s3_bucket,
            s3_named_collection=s3_named_collection,
        )
        value = verify_local_route_certification_receipt(
            receipts[slot],
            source_snapshot=snapshot,
            expected=expected,
            forbidden_secret_values=forbidden_secret_values,
        )
        if value.get("source_generation_sha256") != upstream.get("relation_generation_sha256"):
            raise ValueError("wide_release_campaign_mixed_source_generation")
        verified_receipts[slot] = value
    if capture_local_source_snapshot() != snapshot:
        raise ValueError("wide_release_campaign_source_snapshot_changed")

    if output_dir.exists():
        raise FileExistsError(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        authority_artifact = _copy_authority(authority, staging_dir / "authority")
        dbt_artifact = _copy_dbt_closure(dbt_evidence, staging_dir / "dbt")
        slot_payloads: list[dict[str, Any]] = []
        for slot in _SLOTS:
            value = verified_receipts[slot]
            copied = _copy_receipt_closure(receipts[slot], staging_dir / "slots" / slot)
            slot_payloads.append(
                {
                    "slot": slot,
                    "receipt": _artifact(staging_dir, copied),
                    "receipt_sha256": value["receipt_sha256"],
                    "target_relation": targets[slot],
                }
            )
        payload: dict[str, Any] = {
            "schema_version": _SCHEMA_VERSION,
            "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "release_id": release_id,
            "git_head_sha": snapshot.git_head_sha,
            "source_snapshot_sha256": snapshot.source_snapshot_sha256,
            "worktree_dirty": False,
            "environment_class": "LOCAL_DOCKER",
            "source_relation": source_relation,
            "source_generation_sha256": upstream["relation_generation_sha256"],
            "expected_rows": _EXPECTED_ROWS,
            "expected_source_columns": _EXPECTED_SOURCE_COLUMNS,
            "expected_target_columns": _EXPECTED_TARGET_COLUMNS,
            "mssql_connection_sha256": mssql_connection_sha256,
            "clickhouse_connection_sha256": clickhouse_connection_sha256,
            "authority": _artifact(staging_dir, authority_artifact),
            "dbt_evidence": _artifact(staging_dir, dbt_artifact),
            "slots": slot_payloads,
            "local_behavior_passed": True,
            "local_evidence_status": "PASS",
            "production_certification": "UNVERIFIED",
            "production_certification_reason": "local Docker evidence is not production deployment certification",
        }
        payload["campaign_sha256"] = canonical_sha256(payload)
        staged_path = staging_dir / "mssql_clickhouse_wide_release_campaign.json"
        staged_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        verify_campaign(
            staged_path,
            release_id=release_id,
            source_relation=source_relation,
            targets=targets,
            mssql_connection_sha256=mssql_connection_sha256,
            clickhouse_connection_sha256=clickhouse_connection_sha256,
            target_schema_sha256_by_slot=target_schema_sha256_by_slot,
            s3_endpoint_sha256=s3_endpoint_sha256,
            s3_bucket=s3_bucket,
            s3_named_collection=s3_named_collection,
            forbidden_secret_values=forbidden_secret_values,
        )
        if output_dir.exists():
            raise FileExistsError(output_dir)
        staging_dir.rename(output_dir)
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
    return output_dir / "mssql_clickhouse_wide_release_campaign.json"


def _expected_subject(
    *,
    slot: str,
    release_id: str,
    source_relation: str,
    target_relation: str,
    mssql_connection_sha256: str,
    clickhouse_connection_sha256: str,
    target_schema_sha256: str,
    s3_endpoint_sha256: str,
    s3_bucket: str,
    s3_named_collection: str,
) -> LocalRouteExpectedSubject:
    if slot == "bcp_native_required":
        transport, binary_format, acceleration = "typed_binary_bcp_native", "native", "required"
    elif slot == "bcp_native_off":
        transport, binary_format, acceleration = "typed_binary_bcp_native", "native", "off"
    elif slot == "parquet_s3_pull":
        transport, binary_format, acceleration = "parquet_s3_pull", "parquet", None
    else:
        raise ValueError("wide_release_campaign_slot_unknown")
    return LocalRouteExpectedSubject(
        release_id=release_id,
        route=LocalRouteIdentity(source="mssql", sink="clickhouse", strategy="full_refresh"),
        source_relation=source_relation,
        target_relation=target_relation,
        transport=transport,
        binary_format=binary_format,
        requested_acceleration_mode=acceleration,
        expected_rows=_EXPECTED_ROWS,
        expected_column_count=_EXPECTED_TARGET_COLUMNS,
        mssql_connection_sha256=mssql_connection_sha256,
        clickhouse_connection_sha256=clickhouse_connection_sha256,
        target_schema_sha256=target_schema_sha256,
        s3_endpoint_sha256=s3_endpoint_sha256 if slot == "parquet_s3_pull" else None,
        s3_bucket=s3_bucket if slot == "parquet_s3_pull" else None,
        s3_named_collection=s3_named_collection if slot == "parquet_s3_pull" else None,
    )


def verify_campaign(
    path: Path,
    *,
    release_id: str,
    source_relation: str,
    targets: Mapping[str, str],
    mssql_connection_sha256: str,
    clickhouse_connection_sha256: str,
    target_schema_sha256_by_slot: Mapping[str, str],
    s3_endpoint_sha256: str,
    s3_bucket: str,
    s3_named_collection: str,
    forbidden_secret_values: tuple[str, ...],
) -> Mapping[str, Any]:
    """Deep-verify the immutable campaign and every retained evidence file."""

    if path.is_symlink() or not path.is_file():
        raise ValueError("wide_release_campaign_path_invalid")
    _require_forbidden_secret_values(forbidden_secret_values)
    value = load_json_object(path)
    assert_certification_payload_secret_free(
        value,
        forbidden_secret_values=forbidden_secret_values,
    )
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    if tuple(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value)):
        raise ValueError("wide_release_campaign_schema_invalid")
    canonical = dict(value)
    claimed = canonical.pop("campaign_sha256", None)
    if claimed != canonical_sha256(canonical):
        raise ValueError("wide_release_campaign_digest_mismatch")
    snapshot = capture_local_source_snapshot()
    expected_top = {
        "schema_version": _SCHEMA_VERSION,
        "release_id": release_id,
        "git_head_sha": snapshot.git_head_sha,
        "source_snapshot_sha256": snapshot.source_snapshot_sha256,
        "worktree_dirty": False,
        "environment_class": "LOCAL_DOCKER",
        "source_relation": source_relation,
        "expected_rows": _EXPECTED_ROWS,
        "expected_source_columns": _EXPECTED_SOURCE_COLUMNS,
        "expected_target_columns": _EXPECTED_TARGET_COLUMNS,
        "mssql_connection_sha256": mssql_connection_sha256,
        "clickhouse_connection_sha256": clickhouse_connection_sha256,
        "local_behavior_passed": True,
        "local_evidence_status": "PASS",
        "production_certification": "UNVERIFIED",
    }
    if any(value.get(field) != expected for field, expected in expected_top.items()):
        raise ValueError("wide_release_campaign_subject_mismatch")
    dbt_path = _verify_campaign_artifact(path.parent, value.get("dbt_evidence"))
    authority_path = _verify_campaign_artifact(path.parent, value.get("authority"))
    authority = load_authority(authority_path, release_id=release_id, source_relation=source_relation)
    assert_certification_payload_secret_free(
        authority,
        forbidden_secret_values=forbidden_secret_values,
    )
    authority_expected = {
        "mssql_connection_sha256": mssql_connection_sha256,
        "clickhouse_connection_sha256": clickhouse_connection_sha256,
        "target_schema_sha256_by_slot": dict(target_schema_sha256_by_slot),
        "s3_endpoint_sha256": s3_endpoint_sha256,
        "s3_bucket": s3_bucket,
        "s3_named_collection": s3_named_collection,
    }
    if any(authority.get(field) != expected for field, expected in authority_expected.items()):
        raise ValueError("wide_release_campaign_authority_mismatch")
    upstream = require_exact_dbt_wide_evidence(
        dbt_path,
        release_id=release_id,
        git_head_sha=snapshot.git_head_sha,
        source_snapshot_sha256=snapshot.source_snapshot_sha256,
        output_relation=source_relation,
        connection_sha256=mssql_connection_sha256,
        expected_rows=_EXPECTED_ROWS,
        expected_source_columns=_EXPECTED_SOURCE_COLUMNS,
        expected_target_columns=_EXPECTED_TARGET_COLUMNS,
        forbidden_secret_values=forbidden_secret_values,
    )
    if value.get("source_generation_sha256") != upstream.get("relation_generation_sha256"):
        raise ValueError("wide_release_campaign_source_generation_mismatch")
    slots = value.get("slots")
    if not isinstance(slots, list) or [item.get("slot") for item in slots if isinstance(item, Mapping)] != list(_SLOTS):
        raise ValueError("wide_release_campaign_slot_closure_invalid")
    for item in slots:
        if not isinstance(item, Mapping):
            raise ValueError("wide_release_campaign_slot_invalid")
        slot = str(item.get("slot") or "")
        receipt_path = _verify_campaign_artifact(path.parent, item.get("receipt"))
        expected = _expected_subject(
            slot=slot,
            release_id=release_id,
            source_relation=source_relation,
            target_relation=targets[slot],
            mssql_connection_sha256=mssql_connection_sha256,
            clickhouse_connection_sha256=clickhouse_connection_sha256,
            target_schema_sha256=target_schema_sha256_by_slot[slot],
            s3_endpoint_sha256=s3_endpoint_sha256,
            s3_bucket=s3_bucket,
            s3_named_collection=s3_named_collection,
        )
        receipt = verify_local_route_certification_receipt(
            receipt_path,
            source_snapshot=snapshot,
            expected=expected,
            forbidden_secret_values=forbidden_secret_values,
        )
        if (
            item.get("receipt_sha256") != receipt.get("receipt_sha256")
            or item.get("target_relation") != targets[slot]
            or receipt.get("source_generation_sha256") != upstream.get("relation_generation_sha256")
        ):
            raise ValueError("wide_release_campaign_slot_identity_mismatch")
    if capture_local_source_snapshot() != snapshot:
        raise ValueError("wide_release_campaign_source_snapshot_changed")
    return value


def _verify_campaign_artifact(root: Path, value: object) -> Path:
    if not isinstance(value, Mapping) or frozenset(value) != {"path", "sha256"}:
        raise ValueError("wide_release_campaign_artifact_invalid")
    relative = Path(str(value.get("path") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("wide_release_campaign_artifact_path_invalid")
    candidate = root / relative
    resolved_root = root.resolve()
    resolved_candidate = candidate.resolve()
    if candidate.is_symlink() or not resolved_candidate.is_relative_to(resolved_root):
        raise ValueError("wide_release_campaign_artifact_path_invalid")
    if not candidate.is_file() or _artifact(root, candidate) != dict(value):
        raise ValueError("wide_release_campaign_artifact_digest_mismatch")
    return candidate


def _copy_dbt_closure(source: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    value = load_json_object(source)
    target = destination / source.name
    target.write_bytes(source.read_bytes())
    for prefix in ("run_results", "manifest"):
        name = str(value[f"{prefix}_path"])
        (destination / name).write_bytes((source.parent / name).read_bytes())
    return target


def _copy_authority(source: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / source.name
    target.write_bytes(source.read_bytes())
    return target


def _copy_receipt_closure(source: Path, destination: Path) -> Path:
    value = load_json_object(source)
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / source.name
    target.write_bytes(source.read_bytes())
    for key in ("result", "upstream_evidence"):
        artifact = value.get(key)
        if isinstance(artifact, Mapping):
            name = str(artifact["path"])
            (destination / name).write_bytes((source.parent / name).read_bytes())
    upstream = value.get("upstream_evidence")
    if isinstance(upstream, Mapping):
        upstream_value = load_json_object(source.parent / str(upstream["path"]))
        for prefix in ("run_results", "manifest"):
            name = str(upstream_value[f"{prefix}_path"])
            (destination / name).write_bytes((source.parent / name).read_bytes())
    return target


def _artifact(root: Path, path: Path) -> dict[str, str]:
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _require_forbidden_secret_values(values: tuple[str, ...]) -> None:
    if len(values) < 4 or any(len(value) < 4 for value in values):
        raise ValueError("wide_release_campaign_secret_context_required")


def _campaign_secrets_from_environment() -> tuple[str, ...]:
    names = (
        "DPONE_IT_MSSQL_PASSWORD",
        "DPONE_IT_CH_PASSWORD",
        "DPONE_IT_S3_ACCESS_KEY",
        "DPONE_IT_S3_SECRET_KEY",
    )
    values = tuple(os.environ.get(name, "") for name in names)
    _require_forbidden_secret_values(values)
    return values


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--source-relation", required=True)
    parser.add_argument("--dbt-evidence", type=Path, required=True)
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--native-required-receipt", type=Path)
    parser.add_argument("--native-required-target", required=True)
    parser.add_argument("--native-off-receipt", type=Path)
    parser.add_argument("--native-off-target", required=True)
    parser.add_argument("--parquet-receipt", type=Path)
    parser.add_argument("--parquet-target", required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--verify-existing", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    authority = load_authority(
        args.authority,
        release_id=args.release_id,
        source_relation=args.source_relation,
    )
    targets = {
        "bcp_native_required": args.native_required_target,
        "bcp_native_off": args.native_off_target,
        "parquet_s3_pull": args.parquet_target,
    }
    target_schemas = dict(authority["target_schema_sha256_by_slot"])
    common = {
        "release_id": args.release_id,
        "source_relation": args.source_relation,
        "targets": targets,
        "mssql_connection_sha256": str(authority["mssql_connection_sha256"]),
        "clickhouse_connection_sha256": str(authority["clickhouse_connection_sha256"]),
        "target_schema_sha256_by_slot": target_schemas,
        "s3_endpoint_sha256": str(authority["s3_endpoint_sha256"]),
        "s3_bucket": str(authority["s3_bucket"]),
        "s3_named_collection": str(authority["s3_named_collection"]),
        "forbidden_secret_values": _campaign_secrets_from_environment(),
    }
    if args.verify_existing is not None:
        verify_campaign(args.verify_existing, **common)
        print(args.verify_existing)
        return 0
    if None in (args.native_required_receipt, args.native_off_receipt, args.parquet_receipt, args.output_dir):
        raise ValueError("wide_release_campaign_build_arguments_required")
    assert isinstance(args.native_required_receipt, Path)
    assert isinstance(args.native_off_receipt, Path)
    assert isinstance(args.parquet_receipt, Path)
    assert isinstance(args.output_dir, Path)
    path = build_campaign(
        dbt_evidence=args.dbt_evidence,
        authority=args.authority,
        receipts={
            "bcp_native_required": args.native_required_receipt,
            "bcp_native_off": args.native_off_receipt,
            "parquet_s3_pull": args.parquet_receipt,
        },
        output_dir=args.output_dir,
        **common,
    )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
