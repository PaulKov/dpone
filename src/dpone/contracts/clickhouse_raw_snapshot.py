"""Canonical, bounded ClickHouse raw snapshot identities for durable recovery.

Documents are closed JSON preimages: they carry sanitized identities, fixed
settings, and counts, never SQL text, relation coordinates, credentials, or
row values. Rehydration validates every field before it can become authority.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Literal

from dpone.contracts.clickhouse_raw_snapshot_bounds import (
    preflight_raw_snapshot_shape,
    validate_raw_snapshot_document,
)
from dpone.contracts.strict_json import canonical_json_bytes

PROFILE_KIND = "dpone.clickhouse-raw-snapshot-profile.v1"
EOF_KIND = "dpone.clickhouse-raw-query-snapshot.v1"
BINDING_KIND = "dpone.clickhouse-raw-query-binding.v1"
BINDING_PREFIX = "clickhouse.raw-query.v1:"
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_MARKER = re.compile(r"clickhouse\.raw-query\.v([0-9]+):(.+)\Z")
_FIXED_READ_SETTINGS: dict[str, str | int] = {
    "final": 0,
    "use_query_cache": 0,
    "apply_mutations_on_fly": 0,
    "apply_patch_parts": 1,
    "apply_deleted_mask": 1,
    "max_parallel_replicas": 1,
    "skip_unavailable_shards": 0,
    "read_overflow_mode": "throw",
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
}
_BOUNDED_LIMIT_NAMES = frozenset(
    {
        "max_rows_to_read",
        "max_bytes_to_read",
        "max_result_rows",
        "max_result_bytes",
        "max_execution_time",
    }
)
_PROFILE_FIELDS = frozenset(
    {
        "kind",
        "engine_signature",
        "relation_uuid",
        "database_engine",
        "ordered_schema",
        "partition_key_sha256",
        "sorting_key_sha256",
        "primary_key_sha256",
        "window",
        "read_settings",
        "server_revision",
        "endpoint_authority_sha256",
        "replica_scope",
        "replica_identity_sha256",
        "principal_authority_sha256",
        "row_policy_sha256",
        "policy_filtered",
        "query_shape_sha256",
        "projection_sha256",
        "typed_parameters_sha256",
    }
)
_EOF_FIELDS = frozenset(
    {
        "kind",
        "vendor_query_id_sha256",
        "rows",
        "touched_part_coverage_sha256",
        "before_physical_profile_sha256",
        "after_physical_profile_sha256",
        "endpoint_authority_agreement",
    }
)
_EXTENSION_FIELDS = frozenset(
    {
        "kind",
        "mode",
        "source_query_binding",
        "profile",
        "source_snapshot_profile_sha256",
        "source_eof",
        "source_eof_descriptor_sha256",
    }
)


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _is_digest(value: object) -> bool:
    return type(value) is str and _DIGEST.fullmatch(value) is not None


def _text(value: object) -> bool:
    return type(value) is str and bool(value)


@dataclass(frozen=True)
class ClickHouseRawSnapshotProfileV1:
    """Frozen pre-query authority, including access and query-shape digests."""

    engine_signature: str
    relation_uuid: str
    database_engine: str
    ordered_schema: tuple[tuple[str, str, str], ...]
    partition_key_sha256: str
    sorting_key_sha256: str
    primary_key_sha256: str
    window: tuple[str, str, str] | None
    read_settings: tuple[tuple[str, str | int], ...]
    server_revision: str
    endpoint_authority_sha256: str
    replica_scope: Literal["single_server", "connected_replica"]
    replica_identity_sha256: str | None
    principal_authority_sha256: str
    row_policy_sha256: str
    policy_filtered: bool
    query_shape_sha256: str
    projection_sha256: str
    typed_parameters_sha256: str

    def __post_init__(self) -> None:
        """Reject malformed producer values before a digest can be published."""
        preflight_raw_snapshot_shape(
            (
                self.engine_signature,
                self.relation_uuid,
                self.database_engine,
                self.ordered_schema,
                self.window,
                self.read_settings,
                self.server_revision,
            )
        )
        text_fields = ("engine_signature", "relation_uuid", "database_engine", "server_revision")
        digest_fields = (
            "partition_key_sha256",
            "sorting_key_sha256",
            "primary_key_sha256",
            "endpoint_authority_sha256",
            "principal_authority_sha256",
            "row_policy_sha256",
            "query_shape_sha256",
            "projection_sha256",
            "typed_parameters_sha256",
        )
        schema_ok = type(self.ordered_schema) is tuple and all(
            type(item) is tuple and len(item) == 3 and all(type(part) is str for part in item)
            for item in self.ordered_schema
        )
        settings_ok = type(self.read_settings) is tuple and all(
            type(item) is tuple and len(item) == 2 and _text(item[0]) and (type(item[1]) is str or type(item[1]) is int)
            for item in self.read_settings
        )
        if settings_ok:
            names = [item[0] for item in self.read_settings]
            settings_ok = len(names) == len(set(names))
        if settings_ok:
            values = dict(self.read_settings)
            settings_ok = all(
                name in values and type(values[name]) is type(expected) and values[name] == expected
                for name, expected in _FIXED_READ_SETTINGS.items()
            ) and all(
                name in _FIXED_READ_SETTINGS or (name in _BOUNDED_LIMIT_NAMES and type(value) is int and value > 0)
                for name, value in self.read_settings
            )
        window_ok = self.window is None or (
            type(self.window) is tuple and len(self.window) == 3 and all(_text(part) for part in self.window)
        )
        if (
            not all(_text(getattr(self, name)) for name in text_fields)
            or not all(_is_digest(getattr(self, name)) for name in digest_fields)
            or not schema_ok
            or not settings_ok
            or not window_ok
            or self.replica_scope not in {"single_server", "connected_replica"}
            or (self.replica_identity_sha256 is not None and not _is_digest(self.replica_identity_sha256))
            or (self.replica_scope == "connected_replica" and self.replica_identity_sha256 is None)
            or type(self.policy_filtered) is not bool
        ):
            raise ValueError("mssql_native.source_snapshot_profile_invalid")
        validate_raw_snapshot_document(self.document())

    def document(self) -> dict[str, object]:
        """Return the stable JSON preimage; tuples become ordered JSON arrays."""
        return {
            "kind": PROFILE_KIND,
            "engine_signature": self.engine_signature,
            "relation_uuid": self.relation_uuid,
            "database_engine": self.database_engine,
            "ordered_schema": [list(item) for item in self.ordered_schema],
            "partition_key_sha256": self.partition_key_sha256,
            "sorting_key_sha256": self.sorting_key_sha256,
            "primary_key_sha256": self.primary_key_sha256,
            "window": list(self.window) if self.window is not None else None,
            "read_settings": [list(item) for item in self.read_settings],
            "server_revision": self.server_revision,
            "endpoint_authority_sha256": self.endpoint_authority_sha256,
            "replica_scope": self.replica_scope,
            "replica_identity_sha256": self.replica_identity_sha256,
            "principal_authority_sha256": self.principal_authority_sha256,
            "row_policy_sha256": self.row_policy_sha256,
            "policy_filtered": self.policy_filtered,
            "query_shape_sha256": self.query_shape_sha256,
            "projection_sha256": self.projection_sha256,
            "typed_parameters_sha256": self.typed_parameters_sha256,
        }

    @classmethod
    def from_document(cls, value: object) -> ClickHouseRawSnapshotProfileV1:
        """Decode one exact profile document without aliasing or coercion."""
        if not isinstance(value, dict) or set(value) != _PROFILE_FIELDS or value.get("kind") != PROFILE_KIND:
            raise ValueError("mssql_native.source_snapshot_profile_invalid")
        validate_raw_snapshot_document(value)
        schema, settings, window = value["ordered_schema"], value["read_settings"], value["window"]
        if (
            type(schema) is not list
            or type(settings) is not list
            or any(type(item) is not list for item in schema + settings)
            or (window is not None and type(window) is not list)
        ):
            raise ValueError("mssql_native.source_snapshot_profile_invalid")
        try:
            return cls(
                **{
                    name: value[name]
                    for name in _PROFILE_FIELDS - {"kind", "ordered_schema", "read_settings", "window"}
                },
                ordered_schema=tuple(tuple(item) for item in schema),
                read_settings=tuple(tuple(item) for item in settings),
                window=tuple(window) if window is not None else None,
            )
        except (TypeError, ValueError) as error:
            raise ValueError("mssql_native.source_snapshot_profile_invalid") from error

    @property
    def sha256(self) -> str:
        """Canonical SHA-256 over the complete versioned profile preimage."""
        return _digest(self.document())


@dataclass(frozen=True)
class ClickHouseRawSourceEofV1:
    """Bounded EOF facts sealed alongside the stage-complete row count."""

    vendor_query_id_sha256: str
    rows: int
    touched_part_coverage_sha256: str
    before_physical_profile_sha256: str
    after_physical_profile_sha256: str
    endpoint_authority_agreement: bool

    def __post_init__(self) -> None:
        """Validate producer facts before they can become recovery authority."""
        if (
            not all(
                _is_digest(getattr(self, name))
                for name in (
                    "vendor_query_id_sha256",
                    "touched_part_coverage_sha256",
                    "before_physical_profile_sha256",
                    "after_physical_profile_sha256",
                )
            )
            or type(self.rows) is not int
            or self.rows < 0
            or type(self.endpoint_authority_agreement) is not bool
        ):
            raise ValueError("mssql_native.source_snapshot_eof_invalid")

    def document(self) -> dict[str, object]:
        """Return the bounded, versioned EOF JSON preimage."""
        return {
            "kind": EOF_KIND,
            "vendor_query_id_sha256": self.vendor_query_id_sha256,
            "rows": self.rows,
            "touched_part_coverage_sha256": self.touched_part_coverage_sha256,
            "before_physical_profile_sha256": self.before_physical_profile_sha256,
            "after_physical_profile_sha256": self.after_physical_profile_sha256,
            "endpoint_authority_agreement": self.endpoint_authority_agreement,
        }

    @classmethod
    def from_document(cls, value: object) -> ClickHouseRawSourceEofV1:
        """Reject missing, extra, or type-coerced EOF fields."""
        if not isinstance(value, dict) or set(value) != _EOF_FIELDS or value.get("kind") != EOF_KIND:
            raise ValueError("mssql_native.source_snapshot_eof_invalid")
        try:
            return cls(**{name: value[name] for name in _EOF_FIELDS - {"kind"}})
        except (TypeError, ValueError) as error:
            raise ValueError("mssql_native.source_snapshot_eof_invalid") from error

    @property
    def sha256(self) -> str:
        """Canonical SHA-256 over the complete versioned EOF preimage."""
        return _digest(self.document())


def raw_source_query_binding(legacy_source_binding_sha256: str, profile: ClickHouseRawSnapshotProfileV1) -> str:
    """Separate raw identity from unchanged legacy source-query bindings."""
    if not _is_digest(legacy_source_binding_sha256) or not isinstance(profile, ClickHouseRawSnapshotProfileV1):
        raise ValueError("mssql_native.source_snapshot_binding_invalid")
    return BINDING_PREFIX + _digest(
        {
            "kind": BINDING_KIND,
            "legacy_source_binding_sha256": legacy_source_binding_sha256,
            "source_snapshot_profile_sha256": profile.sha256,
        }
    )


def raw_source_query_binding_version(value: str) -> int | None:
    """Identify legacy bindings or reject unknown/malformed raw markers."""
    if _is_digest(value):
        return None
    if type(value) is not str:
        raise ValueError("mssql_native.source_snapshot_binding_invalid")
    match = _MARKER.fullmatch(value)
    if match is None or match.group(1) != "1" or not _is_digest(match.group(2)):
        raise ValueError("mssql_native.source_snapshot_binding_invalid")
    return 1


def raw_snapshot_extension(
    binding: str,
    profile: ClickHouseRawSnapshotProfileV1,
    eof: ClickHouseRawSourceEofV1,
) -> dict[str, object]:
    """Build the closed stage-complete extension from a valid raw binding."""
    if raw_source_query_binding_version(binding) != 1 or not isinstance(eof, ClickHouseRawSourceEofV1):
        raise ValueError("mssql_native.source_snapshot_extension_invalid")
    extension: dict[str, object] = {
        "kind": EOF_KIND,
        "mode": "exact_raw_rows",
        "source_query_binding": binding,
        "profile": profile.document(),
        "source_snapshot_profile_sha256": profile.sha256,
        "source_eof": eof.document(),
        "source_eof_descriptor_sha256": eof.sha256,
    }
    validate_raw_snapshot_document(extension)
    return extension


def restore_raw_snapshot_extension(
    value: object,
    *,
    binding: str,
    completed_rows: int,
) -> tuple[ClickHouseRawSnapshotProfileV1, ClickHouseRawSourceEofV1]:
    """Verify one sealed extension before source-free recovery can act."""
    if (
        not isinstance(value, dict)
        or set(value) != _EXTENSION_FIELDS
        or value.get("kind") != EOF_KIND
        or value.get("mode") != "exact_raw_rows"
        or value.get("source_query_binding") != binding
        or raw_source_query_binding_version(binding) != 1
        or type(completed_rows) is not int
        or completed_rows < 0
    ):
        raise ValueError("mssql_native.source_snapshot_extension_invalid")
    validate_raw_snapshot_document(value)
    profile = ClickHouseRawSnapshotProfileV1.from_document(value["profile"])
    eof = ClickHouseRawSourceEofV1.from_document(value["source_eof"])
    if (
        value["source_snapshot_profile_sha256"] != profile.sha256
        or value["source_eof_descriptor_sha256"] != eof.sha256
        or eof.rows != completed_rows
    ):
        raise ValueError("mssql_native.source_snapshot_extension_invalid")
    return profile, eof
