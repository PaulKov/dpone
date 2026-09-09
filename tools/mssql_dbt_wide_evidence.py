"""Closed verifier and identity helpers for wide dbt SQL Server evidence."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from mssql_dbt_wide_schema import canonical_wide_source_schema_sha256

_SCHEMA = Path(__file__).resolve().parents[1] / "docs/schemas/dpone.dbt-sqlserver-wide-materialization.v1.schema.json"
_SCHEMA_VERSION = "dpone.dbt_sqlserver.wide_materialization.v1"


def mssql_connection_sha256(params: Mapping[str, Any]) -> str:
    """Bind the non-secret SQL Server endpoint used by dbt and reconciliation."""

    payload = {
        "database": _text(params.get("database")),
        "driver": _text(params.get("driver")),
        "encrypt": _text(params.get("encrypt", "yes")).lower(),
        "host": _text(params.get("host")).lower(),
        "port": int(params.get("port") or 1433),
        "schema": "dpone.mssql-local-connection-identity.v1",
        "trust_server_certificate": _text(params.get("trust_server_certificate", "yes")).lower(),
        "username": _text(params.get("username")),
    }
    return canonical_sha256(payload)


def clickhouse_connection_sha256(params: Mapping[str, Any]) -> str:
    """Bind the non-secret ClickHouse native and HTTP endpoint identity."""

    return canonical_sha256(
        {
            "database": _text(params.get("database")),
            "host": _text(params.get("host")).lower(),
            "http_host": _text(params.get("http_host") or params.get("host")).lower(),
            "http_port": int(params.get("http_port") or 8123),
            "port": int(params.get("port") or 9000),
            "schema": "dpone.clickhouse-local-connection-identity.v1",
            "secure": bool(params.get("secure", False)),
            "username": _text(params.get("username") or params.get("user")),
        }
    )


def object_storage_authority_sha256(
    *,
    endpoint_url: str,
    region_name: str,
    bucket: str,
    named_collection: str,
) -> str:
    """Bind the non-secret S3 endpoint and ClickHouse read authority coordinates."""

    return canonical_sha256(
        {
            "bucket": _text(bucket),
            "endpoint_url": _text(endpoint_url),
            "named_collection": _text(named_collection),
            "region_name": _text(region_name),
            "schema": "dpone.local-s3-endpoint-authority.v1",
        }
    )


def relation_generation_sha256(payload: Mapping[str, Any]) -> str:
    """Derive the immutable relation state joined to every downstream route receipt."""

    return canonical_sha256(
        {
            "mssql_connection_sha256": payload.get("mssql_connection_sha256"),
            "output_data_rows": payload.get("output_data_rows"),
            "output_data_sha256": payload.get("output_data_sha256"),
            "output_relation": payload.get("output_relation"),
            "passthrough_schema_sha256": payload.get("passthrough_schema_sha256"),
            "schema": "dpone.dbt-sqlserver-relation-generation.v1",
        }
    )


def require_exact_dbt_wide_evidence(
    path: Path,
    *,
    release_id: str,
    git_head_sha: str,
    source_snapshot_sha256: str,
    output_relation: str,
    connection_sha256: str,
    expected_rows: int,
    expected_source_columns: int,
    expected_target_columns: int,
    forbidden_secret_values: tuple[str, ...] = (),
) -> Mapping[str, Any]:
    """Validate the closed schema, self digest, retained artifacts, and exact metrics."""

    if path.is_symlink() or not path.is_file():
        raise ValueError("upstream_evidence_path_invalid")
    value = load_json_object(path)
    if not isinstance(value, Mapping):
        raise ValueError("upstream_evidence_invalid")
    _assert_secret_free(value, forbidden_secret_values=forbidden_secret_values)
    schema = json.loads(_SCHEMA.read_text(encoding="utf-8"))
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value),
        key=lambda item: tuple(str(part) for part in item.absolute_path),
    )
    if errors:
        raise ValueError("upstream_evidence_shape_invalid")
    canonical = dict(value)
    claimed_sha256 = canonical.pop("evidence_sha256")
    if claimed_sha256 != canonical_sha256(canonical):
        raise ValueError("upstream_evidence_digest_mismatch")
    expected = {
        "schema_version": _SCHEMA_VERSION,
        "release_id": release_id,
        "git_head_sha": git_head_sha,
        "source_snapshot_sha256": source_snapshot_sha256,
        "worktree_dirty": False,
        "output_relation": output_relation,
        "mssql_connection_sha256": connection_sha256,
        "passed": True,
        "evidence_status": "PASS",
        "blockers": [],
        "environment_class": "LOCAL_DOCKER",
        "production_certification": "UNVERIFIED",
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            raise ValueError(f"upstream_evidence_{field}_mismatch")
    if not (value.get("source_count") == value.get("target_count") == value.get("output_data_rows") == expected_rows):
        raise ValueError("upstream_evidence_row_count_mismatch")
    if value.get("distinct_key_count") != value.get("target_count"):
        raise ValueError("upstream_evidence_key_set_mismatch")
    if (
        value.get("source_column_count") != expected_source_columns
        or value.get("target_column_count") != expected_target_columns
        or expected_source_columns + 1 != expected_target_columns
    ):
        raise ValueError("upstream_evidence_column_count_mismatch")
    if (
        value.get("schema_mismatch_count") != 0
        or value.get("canonical_source_mismatch_count") != 0
        or value.get("canonical_source_schema_sha256") != canonical_wide_source_schema_sha256(expected_source_columns)
        or value.get("source_schema_sha256") != value.get("passthrough_schema_sha256")
    ):
        raise ValueError("upstream_evidence_schema_reconciliation_failed")
    if value.get("source_data_sha256") != value.get("passthrough_data_sha256"):
        raise ValueError("upstream_evidence_data_reconciliation_failed")
    if (
        value.get("calculated_column_type") != "decimal(38,8)"
        or value.get("calculated_column_nullable") is not True
        or value.get("calculated_mismatch_count") != 0
    ):
        raise ValueError("upstream_evidence_calculation_failed")
    if value.get("relation_generation_sha256") != relation_generation_sha256(value):
        raise ValueError("upstream_evidence_relation_generation_mismatch")
    for prefix in ("run_results", "manifest"):
        _verify_artifact(
            path.parent,
            value,
            prefix,
            forbidden_secret_values=forbidden_secret_values,
        )
    return value


def copy_dbt_wide_evidence_bundle(output_dir: Path, path: Path) -> Path:
    """Copy upstream evidence and its retained dbt artifacts into a route bundle."""

    if path.is_symlink() or not path.is_file():
        raise ValueError("upstream_evidence_path_invalid")
    value = load_json_object(path)
    if not isinstance(value, Mapping):
        raise ValueError("upstream_evidence_invalid")
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"upstream_{path.name}"
    with target.open("xb") as stream:
        stream.write(path.read_bytes())
    for prefix in ("run_results", "manifest"):
        name = str(value.get(f"{prefix}_path") or "")
        if not name or Path(name).name != name:
            raise ValueError("upstream_evidence_artifact_path_invalid")
        source_artifact = path.parent / name
        destination = output_dir / name
        with destination.open("xb") as stream:
            stream.write(source_artifact.read_bytes())
    return target


def assert_dbt_wide_artifact_secret_free(
    path: Path,
    *,
    forbidden_secret_values: tuple[str, ...] = (),
) -> None:
    """Reject a retained dbt JSON artifact before it enters public evidence."""

    _assert_secret_free(
        load_json_object(path),
        forbidden_secret_values=forbidden_secret_values,
    )


def assert_certification_payload_secret_free(
    value: object,
    *,
    forbidden_secret_values: tuple[str, ...] = (),
) -> None:
    """Reject credential-shaped fields and the caller's in-memory secret values."""

    _assert_secret_free(value, forbidden_secret_values=forbidden_secret_values)


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def load_json_object(path: Path) -> Mapping[str, Any]:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("certification_json_duplicate_key")
            value[key] = item
        return value

    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    if not isinstance(value, Mapping):
        raise ValueError("certification_json_object_required")
    return value


def _verify_artifact(
    root: Path,
    value: Mapping[str, Any],
    prefix: str,
    *,
    forbidden_secret_values: tuple[str, ...],
) -> None:
    name = str(value.get(f"{prefix}_path") or "")
    if not name or Path(name).name != name:
        raise ValueError("upstream_evidence_artifact_path_invalid")
    path = root / name
    if path.is_symlink() or path.resolve().parent != root.resolve():
        raise ValueError("upstream_evidence_artifact_path_invalid")
    if not path.is_file():
        raise ValueError("upstream_evidence_artifact_missing")
    actual = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != value.get(f"{prefix}_sha256"):
        raise ValueError("upstream_evidence_artifact_digest_mismatch")
    _assert_secret_free(
        load_json_object(path),
        forbidden_secret_values=forbidden_secret_values,
    )


def _assert_secret_free(
    value: object,
    *,
    key: str | None = None,
    forbidden_secret_values: tuple[str, ...] = (),
) -> None:
    if key is not None and not isinstance(value, Mapping | list | tuple):
        normalized = key.strip().lower().replace("-", "_")
        if any(
            token in normalized
            for token in ("password", "secret", "access_key", "private_key", "authorization", "credential", "token")
        ):
            raise ValueError("upstream_evidence_secret_material_detected")
    if isinstance(value, str):
        normalized = value.casefold()
        if (
            any(secret and len(secret) >= 4 and secret.casefold() in normalized for secret in forbidden_secret_values)
            or _SECRET_ASSIGNMENT.search(value)
            or _CREDENTIAL_URI.search(value)
            or _SENSITIVE_LITERAL.search(value)
        ):
            raise ValueError("upstream_evidence_secret_material_detected")
    if isinstance(value, Mapping):
        for item_key, item in value.items():
            _assert_secret_free(
                item,
                key=str(item_key),
                forbidden_secret_values=forbidden_secret_values,
            )
    elif isinstance(value, list | tuple):
        for item in value:
            _assert_secret_free(item, forbidden_secret_values=forbidden_secret_values)


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(?:password|pwd|secret|token|access[_-]?key|authorization|credential)\s*[:=]\s*[^\s,;]+"
)
_CREDENTIAL_URI = re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^/@\s:]+:[^/@\s]+@")
_SENSITIVE_LITERAL = re.compile(r"(?i)\b(?:plain[-_ ]?secret|actual[-_ ]?password|bearer[-_ ]?token)\b")


def _text(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("mssql_connection_identity_field_required")
    return normalized


__all__ = [
    "assert_dbt_wide_artifact_secret_free",
    "canonical_sha256",
    "clickhouse_connection_sha256",
    "copy_dbt_wide_evidence_bundle",
    "mssql_connection_sha256",
    "object_storage_authority_sha256",
    "load_json_object",
    "relation_generation_sha256",
    "require_exact_dbt_wide_evidence",
]
