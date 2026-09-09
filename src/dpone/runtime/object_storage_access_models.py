"""Typed object storage access request and evidence models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.runtime.object_storage_retention import ObjectStorageBudgetResult, ObjectStorageRetentionPolicy
from dpone.storage import ObjectStorageUri

SCHEMA_VERSION = "dpone.object_storage.access_preflight.v1"
ALLOWED_CONNECTION_TYPES = {"airflow", "env", "params", "vault"}
ALLOWED_READ_MODES = {"named_collection", "connection", "presigned_url"}
SECRET_KEYS = {"password", "secret", "token", "key", "access_key", "secret_key", "session_token"}


@dataclass(frozen=True, slots=True)
class ObjectStorageConnectionRef:
    connection_type: str
    connection_id: str
    vault_mount_point: str | None = None
    vault_path: str | None = None

    def __post_init__(self) -> None:
        connection_type = self.connection_type.strip().lower()
        if connection_type not in ALLOWED_CONNECTION_TYPES:
            raise ValueError(f"Unsupported object storage connection_type: {self.connection_type}")
        if not self.connection_id.strip():
            raise ValueError("object_storage.runtime_access.connection_id is required")
        object.__setattr__(self, "connection_type", connection_type)

    def to_evidence(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "connection_type": self.connection_type,
            "connection_id": redact_connection_id(self.connection_id),
        }
        if self.vault_mount_point:
            payload["vault_mount_point"] = self.vault_mount_point
        if self.vault_path:
            payload["vault_path"] = self.vault_path
        return payload


@dataclass(frozen=True, slots=True)
class ObjectStorageRuntimeAccess:
    connection: ObjectStorageConnectionRef
    required_permissions: tuple[str, ...] = ("put_object", "get_object", "list_prefix", "delete_prefix")

    @property
    def connection_id(self) -> str:
        return self.connection.connection_id

    def requires(self, permission: str) -> bool:
        return permission in set(self.required_permissions)

    def to_evidence(self) -> dict[str, object]:
        return {
            **self.connection.to_evidence(),
            "required_permissions": list(self.required_permissions),
        }


@dataclass(frozen=True, slots=True)
class ObjectStorageReadContract:
    mode: str
    named_collection: str | None = None
    connection: ObjectStorageConnectionRef | None = None
    required_permissions: tuple[str, ...] = ("get_object", "list_prefix")
    allow_anonymous_read: bool = False
    presigned_url_ttl_seconds: int | None = None

    def __post_init__(self) -> None:
        mode = self.mode.strip().lower()
        if mode not in ALLOWED_READ_MODES:
            raise ValueError(f"Unsupported clickhouse_read_access.mode: {self.mode}")
        if mode == "named_collection" and not (self.named_collection or "").strip():
            raise ValueError("object_storage.clickhouse_read_access.named_collection is required")
        if mode == "connection" and self.connection is None:
            raise ValueError("object_storage.clickhouse_read_access.connection_id is required for connection mode")
        if mode == "presigned_url" and self.connection is None and not self.allow_anonymous_read:
            raise ValueError("presigned_url mode requires connection_id unless allow_anonymous_read=true")
        object.__setattr__(self, "mode", mode)

    def to_evidence(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "mode": self.mode,
            "required_permissions": list(self.required_permissions),
            "allow_anonymous_read": self.allow_anonymous_read,
        }
        if self.named_collection:
            payload["named_collection"] = self.named_collection
        if self.connection:
            payload["connection"] = self.connection.to_evidence()
        if self.presigned_url_ttl_seconds is not None:
            payload["presigned_url_ttl_seconds"] = self.presigned_url_ttl_seconds
        return payload


@dataclass(frozen=True, slots=True)
class ObjectStorageAccessRequest:
    uri_prefix: str
    runtime_access: ObjectStorageRuntimeAccess
    clickhouse_read_access: ObjectStorageReadContract
    require_runtime_write: bool = True
    require_clickhouse_read: bool = True
    require_cluster_read: bool = False
    sentinel_format: str = "parquet"
    fail_on_region_mismatch: str = "warn"
    use_cluster_function: str = "auto"
    cluster: str | None = None
    retention_policy: ObjectStorageRetentionPolicy = ObjectStorageRetentionPolicy()

    def __post_init__(self) -> None:
        if not self.uri_prefix.strip():
            raise ValueError("object_storage.uri_prefix is required")
        if self.fail_on_region_mismatch not in {"warn", "error"}:
            raise ValueError("object_storage.preflight.fail_on_region_mismatch must be warn or error")

    @classmethod
    def from_options(
        cls,
        object_storage: dict[str, Any],
        *,
        columnar_pull: dict[str, Any] | None = None,
    ) -> ObjectStorageAccessRequest:
        preflight = mapping(object_storage.get("preflight"))
        return cls(
            uri_prefix=str(object_storage.get("uri_prefix") or ""),
            runtime_access=runtime_access_from_options(mapping(object_storage.get("runtime_access"))),
            clickhouse_read_access=read_contract_from_options(mapping(object_storage.get("clickhouse_read_access"))),
            require_runtime_write=bool_option(preflight.get("require_runtime_write"), default=True),
            require_clickhouse_read=bool_option(preflight.get("require_clickhouse_read"), default=True),
            require_cluster_read=bool_option(preflight.get("require_cluster_read"), default=False),
            sentinel_format=str(preflight.get("sentinel_format") or "parquet"),
            fail_on_region_mismatch=str(preflight.get("fail_on_region_mismatch") or "warn"),
            use_cluster_function=str(mapping(columnar_pull).get("use_cluster_function") or "auto"),
            cluster=text(mapping(columnar_pull).get("cluster")),
            retention_policy=ObjectStorageRetentionPolicy.from_options(mapping(object_storage.get("retention"))),
        )

    def resolved_prefix(self, run_id: str) -> ObjectStorageUri:
        return ObjectStorageUri.parse(self.uri_prefix.format(run_id=run_id)).prefix()


@dataclass(frozen=True, slots=True)
class ObjectStorageAccessEvidence:
    passed: bool
    runtime_access: ObjectStorageRuntimeAccess
    clickhouse_read_access: ObjectStorageReadContract
    uri_prefix: str
    sentinel_uri: str | None
    clickhouse_probe_sql: str | None
    checks: tuple[str, ...]
    warnings: tuple[str, ...]
    blockers: tuple[str, ...]
    schema_version: str = SCHEMA_VERSION
    retention_policy: ObjectStorageRetentionPolicy | None = None
    budget_result: ObjectStorageBudgetResult | None = None

    def to_dict(self) -> dict[str, object]:
        return redact(
            {
                "schema_version": self.schema_version,
                "passed": self.passed,
                "uri_prefix": self.uri_prefix,
                "sentinel_uri": self.sentinel_uri,
                "clickhouse_probe_sql": self.clickhouse_probe_sql,
                "runtime_access": self.runtime_access.to_evidence(),
                "clickhouse_read_access": self.clickhouse_read_access.to_evidence(),
                "checks": list(self.checks),
                "warnings": list(self.warnings),
                "blockers": list(self.blockers),
                "retention_policy": self.retention_policy.to_evidence() if self.retention_policy else None,
                "budget_guard": self.budget_result.to_dict() if self.budget_result else None,
            }
        )


def runtime_access_from_options(payload: dict[str, Any]) -> ObjectStorageRuntimeAccess:
    connection = connection_ref_from_options(payload)
    if connection is None:
        raise ValueError("object_storage.runtime_access.connection_id is required")
    return ObjectStorageRuntimeAccess(
        connection=connection,
        required_permissions=tuple_option(
            payload.get("required_permissions"),
            ("put_object", "get_object", "list_prefix", "delete_prefix"),
        ),
    )


def read_contract_from_options(payload: dict[str, Any]) -> ObjectStorageReadContract:
    return ObjectStorageReadContract(
        mode=str(payload.get("mode") or "named_collection"),
        named_collection=text(payload.get("named_collection")),
        connection=connection_ref_from_options(payload, required=False),
        required_permissions=tuple_option(payload.get("required_permissions"), ("get_object", "list_prefix")),
        allow_anonymous_read=bool_option(payload.get("allow_anonymous_read"), default=False),
        presigned_url_ttl_seconds=int_or_none(payload.get("presigned_url_ttl_seconds")),
    )


def connection_ref_from_options(payload: dict[str, Any], *, required: bool = True) -> ObjectStorageConnectionRef | None:
    # Accept connection_id (canonical) or connection_ref (connection_ref pilot alias).
    connection_id = text(payload.get("connection_id")) or text(payload.get("connection_ref"))
    if not connection_id and not required:
        return None
    return ObjectStorageConnectionRef(
        connection_type=str(payload.get("connection_type") or "airflow"),
        connection_id=connection_id or "",
        vault_mount_point=text(payload.get("vault_mount_point") or payload.get("mount_point")),
        vault_path=text(payload.get("vault_path") or payload.get("path")),
    )


def mapping(value: object | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def tuple_option(value: object, default: tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    return default


def bool_option(value: object | None, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def int_or_none(value: object | None) -> int | None:
    if value is None or value == "":
        return None
    return int(str(value))


def redact_connection_id(value: str) -> str:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return "<redacted>" if looks_secret(value) else value
    return json.dumps(redact(parsed), sort_keys=True, ensure_ascii=False)


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: ("<redacted>" if is_secret_key(key) else redact(item)) for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def is_secret_key(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in SECRET_KEYS)


def looks_secret(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in ("password=", "secret=", "token=", "access_key="))


def text(value: object | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
