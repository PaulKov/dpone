"""Canonical artifact and transport validation for local route receipts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from mssql_dbt_wide_evidence import load_json_object


def required_text(value: str, field: str) -> str:
    """Normalize one required public coordinate or fail with its field name."""

    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field}_required")
    return normalized


def route_slug(value: str) -> str:
    """Return the canonical identifier used in a route receipt case ID."""

    return required_text(value, "route_coordinate").lower().replace("-", "_").replace(" ", "_")


def artifact(path: Path) -> dict[str, str]:
    """Describe one JSON artifact by closed filename, schema, and digest."""

    if not path.is_file():
        raise ValueError(f"certification_artifact_missing:{path.name}")
    payload = load_json_object(path)
    if not isinstance(payload, Mapping):
        raise ValueError(f"certification_artifact_invalid:{path.name}")
    schema = str(payload.get("schema_version") or payload.get("schema") or "")
    return {
        "path": path.name,
        "schema_version": schema,
        "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def prepare_create_once_directory(path: Path) -> Path:
    """Create an empty run directory and reject every replay with retained bytes."""

    if path.exists():
        raise ValueError("local_route_certification_output_already_exists")
    path.mkdir(parents=True)
    return path


def verify_artifact(root: Path, value: object) -> Path:
    """Authenticate a referenced JSON artifact below the receipt directory."""

    if not isinstance(value, Mapping) or frozenset(value) != {"path", "schema_version", "sha256"}:
        raise ValueError("local_route_certification_artifact_invalid")
    name = str(value.get("path") or "")
    if not name or Path(name).name != name:
        raise ValueError("local_route_certification_artifact_path_invalid")
    path = root / name
    if path.is_symlink() or path.resolve().parent != root.resolve():
        raise ValueError("local_route_certification_artifact_path_invalid")
    if artifact(path) != dict(value):
        raise ValueError("local_route_certification_artifact_digest_mismatch")
    assert_secret_free(load_json_object(path))
    return path


def verify_result_payload(path: Path, *, transport: str) -> Mapping[str, Any]:
    """Validate the exact result projection for one supported local transport."""

    value = load_json_object(path)
    if not isinstance(value, Mapping):
        raise ValueError("local_route_certification_result_invalid")
    if transport == "typed_binary_bcp_native":
        required = {
            "schema_version",
            "rows",
            "column_count",
            "source_count",
            "target_count",
            "duplicate_count",
            "typed_hash_passed",
            "typed_hash_source",
            "typed_hash_target",
            "elapsed_seconds",
            "prepare_seconds",
            "export_seconds",
            "load_seconds",
            "artifact_bytes",
            "passed",
            "failed_phase",
            "error",
            "rows_per_second",
        }
        if (
            frozenset(value) != required
            or value.get("schema_version") != "dpone.mssql_clickhouse.wide_type_certification.v1"
        ):
            raise ValueError("local_route_certification_bcp_result_shape_invalid")
        if value.get("typed_hash_passed") is not True:
            raise ValueError("local_route_certification_bcp_hash_failed")
    elif transport == "parquet_s3_pull":
        required = {
            "schema_version",
            "rows",
            "column_count",
            "source_count",
            "target_count",
            "duplicate_count",
            "typed_hash_source",
            "typed_hash_target",
            "parquet_chunks",
            "parquet_bytes",
            "object_prefix",
            "cleanup_verified",
            "elapsed_seconds",
            "passed",
            "failed_phase",
            "error",
        }
        if (
            frozenset(value) != required
            or value.get("schema_version") != "dpone.mssql_clickhouse.parquet_s3_type_certification.v1"
        ):
            raise ValueError("local_route_certification_parquet_result_shape_invalid")
        if value.get("cleanup_verified") is not True or int(value.get("parquet_chunks") or 0) <= 0:
            raise ValueError("local_route_certification_parquet_result_invalid")
    else:
        raise ValueError("local_route_certification_transport_invalid")
    if (
        value.get("passed") is not True
        or value.get("failed_phase") is not None
        or value.get("error") is not None
        or value.get("source_count") != value.get("target_count")
        or value.get("source_count") != value.get("rows")
        or value.get("duplicate_count") != 0
        or not _digest(value.get("typed_hash_source"))
        or not _digest(value.get("typed_hash_target"))
        or value.get("typed_hash_source") != value.get("typed_hash_target")
    ):
        raise ValueError("local_route_certification_result_not_exact")
    return value


def assert_secret_free(value: object, *, key: str | None = None) -> None:
    """Reject retained JSON containing secret-bearing keys at any depth."""

    if key is not None and _secret_key(key):
        raise ValueError("local_route_certification_secret_material_detected")
    if isinstance(value, Mapping):
        for item_key, item in value.items():
            assert_secret_free(item, key=str(item_key))
    elif isinstance(value, list | tuple):
        for item in value:
            assert_secret_free(item)


def verify_runtime_decisions(value: Mapping[str, Any], result: Mapping[str, Any]) -> None:
    """Prove the requested native/Python decision was actually selected."""

    decisions = value.get("runtime_decisions")
    if not isinstance(decisions, list):
        raise ValueError("local_route_certification_runtime_decisions_invalid")
    if value.get("transport") == "parquet_s3_pull":
        if decisions or value.get("requested_acceleration_mode") is not None:
            raise ValueError("local_route_certification_parquet_acceleration_invalid")
        return
    if value.get("binary_format") != "native":
        return
    acceleration = [
        item
        for item in decisions
        if isinstance(item, Mapping) and item.get("decision_id") == "native_transfer.acceleration"
    ]
    mode = value.get("requested_acceleration_mode")
    transport_details = value.get("transport_details")
    loaded_slices = transport_details.get("loaded_slices") if isinstance(transport_details, Mapping) else None
    expected_decisions = len(loaded_slices) if isinstance(loaded_slices, list) else 0
    if len(acceleration) != expected_decisions or mode not in {"required", "off", "auto"}:
        raise ValueError("local_route_certification_acceleration_missing")
    if any(item != acceleration[0] for item in acceleration[1:]):
        raise ValueError("local_route_certification_acceleration_partition_mismatch")
    for item in acceleration:
        if frozenset(item) != {
            "schema_version",
            "decision_id",
            "phase",
            "component",
            "category",
            "requested",
            "selected",
            "fallback_allowed",
            "fallback_reason",
            "release_gate",
            "warnings",
            "blockers",
            "route_id",
            "provider",
            "provider_version",
            "details",
        } or (
            item.get("schema_version") != "dpone.runtime.decision_audit.v1"
            or item.get("phase") != "load"
            or item.get("component") != "native_wire_transcoder"
            or item.get("category") != "backend_selection"
            or item.get("route_id") is not None
            or item.get("requested") != mode
            or item.get("blockers")
        ):
            raise ValueError("local_route_certification_acceleration_mismatch")
        if item.get("provider") != "dpone-native-accel":
            raise ValueError("local_route_certification_acceleration_provider_mismatch")
        details = item.get("details")
        expected_type_count = result.get("column_count")
        if (
            not isinstance(details, Mapping)
            or frozenset(details)
            != {
                "available",
                "backend_id",
                "certified",
                "schema_hash",
                "source_format",
                "source_type_count",
                "target_format",
                "type_layout_hash",
            }
            or (
                details.get("source_format") != "mssql-bcp-native"
                or details.get("target_format") != "Native"
                or details.get("source_type_count") != expected_type_count
                or not _digest(details.get("schema_hash"))
                or not _digest(details.get("type_layout_hash"))
            )
        ):
            raise ValueError("local_route_certification_acceleration_details_invalid")
        if mode == "required" and (
            item.get("selected") != "native_accelerated"
            or item.get("release_gate") != "green"
            or item.get("provider_version") != value.get("release_id")
            or details.get("available") is not True
            or details.get("certified") is not True
            or details.get("backend_id") != "mssql_bcp_native_to_clickhouse_native"
            or item.get("fallback_allowed") is not False
            or item.get("fallback_reason") is not None
            or item.get("warnings") != []
        ):
            raise ValueError("local_route_certification_native_required_not_observed")
        if mode == "off" and (
            item.get("selected") != "python_reference"
            or item.get("fallback_reason") != "native_acceleration_disabled"
            or item.get("release_gate") != "warning"
            or "native_acceleration_disabled" not in (item.get("warnings") or [])
            or item.get("provider_version") is not None
            or details.get("available") is not False
            or details.get("certified") is not False
            or details.get("backend_id") is not None
            or item.get("fallback_allowed") is not False
        ):
            raise ValueError("local_route_certification_python_reference_not_observed")


def verify_transport_details(value: Mapping[str, Any], result: Mapping[str, Any]) -> None:
    """Validate transport-specific runtime coordinates and complete hashing."""

    details = value.get("transport_details")
    if not isinstance(details, Mapping) or details.get("hashed_rows") != result.get("rows"):
        raise ValueError("local_route_certification_transport_details_invalid")
    if value.get("transport") == "typed_binary_bcp_native":
        if frozenset(details) != {
            "hashed_rows",
            "target_rows_per_partition",
            "export_workers",
            "load_workers",
            "bcp_file_format",
            "loaded_slices",
        }:
            raise ValueError("local_route_certification_bcp_details_shape_invalid")
        loaded_slices = details.get("loaded_slices")
        if (
            details.get("bcp_file_format") != "native"
            or min(
                int(details.get("target_rows_per_partition") or 0),
                int(details.get("export_workers") or 0),
                int(details.get("load_workers") or 0),
            )
            <= 0
            or not isinstance(loaded_slices, list)
            or not loaded_slices
        ):
            raise ValueError("local_route_certification_bcp_details_invalid")
        seen: set[tuple[int, int]] = set()
        exported_rows = loaded_rows = 0
        for item in loaded_slices:
            if not isinstance(item, Mapping) or frozenset(item) != {
                "partition_index",
                "slice_index",
                "rows_exported",
                "rows_loaded",
            }:
                raise ValueError("local_route_certification_bcp_slice_invalid")
            partition_index = item.get("partition_index")
            slice_index = item.get("slice_index")
            rows_exported = item.get("rows_exported")
            rows_loaded = item.get("rows_loaded")
            if (
                not isinstance(partition_index, int)
                or isinstance(partition_index, bool)
                or partition_index < 0
                or not isinstance(slice_index, int)
                or isinstance(slice_index, bool)
                or slice_index < 0
                or not isinstance(rows_exported, int)
                or isinstance(rows_exported, bool)
                or rows_exported <= 0
                or not isinstance(rows_loaded, int)
                or isinstance(rows_loaded, bool)
                or rows_loaded != rows_exported
                or (partition_index, slice_index) in seen
            ):
                raise ValueError("local_route_certification_bcp_slice_invalid")
            seen.add((partition_index, slice_index))
            exported_rows += rows_exported
            loaded_rows += rows_loaded
        if exported_rows != result.get("rows") or loaded_rows != result.get("rows"):
            raise ValueError("local_route_certification_bcp_slice_closure_mismatch")
    elif value.get("transport") == "parquet_s3_pull":
        if frozenset(details) != {
            "hashed_rows",
            "s3_endpoint_sha256",
            "bucket",
            "object_prefix",
            "named_collection",
            "parquet_chunks",
            "parquet_bytes",
        }:
            raise ValueError("local_route_certification_parquet_details_shape_invalid")
        if (
            details.get("object_prefix") != result.get("object_prefix")
            or details.get("parquet_chunks") != result.get("parquet_chunks")
            or details.get("parquet_bytes") != result.get("parquet_bytes")
            or not str(details.get("s3_endpoint_sha256") or "").startswith("sha256:")
        ):
            raise ValueError("local_route_certification_parquet_details_invalid")


def payload_passed(path: Path) -> bool:
    """Return whether a supported producer explicitly reports PASS."""

    payload = load_json_object(path)
    return isinstance(payload, Mapping) and (
        payload.get("passed") is True or payload.get("status") == "PASS" or payload.get("evidence_status") == "PASS"
    )


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    """Hash one JSON projection with the repository canonical encoding."""

    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def json_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return a deterministic JSON-compatible mapping projection."""

    return json.loads(json.dumps(dict(value), ensure_ascii=False, sort_keys=True, default=str))


def integer_field(value: Mapping[str, Any] | None, field: str) -> int:
    """Return one required positive integer evidence field."""

    raw = value.get(field) if value is not None else None
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
        raise ValueError(f"local_route_certification_{field}_invalid")
    return raw


def _secret_key(value: str) -> bool:
    normalized = value.strip().lower().replace("-", "_")
    return any(
        token in normalized
        for token in ("password", "secret", "access_key", "private_key", "authorization", "credential", "token")
    )


def _digest(value: object) -> bool:
    text = str(value or "")
    return (
        len(text) == 71
        and text.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in text[7:])
    )
