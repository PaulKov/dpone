"""Pure capability decision for PostgreSQL to SQL Server correctness R1."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.strict_json import canonical_json_bytes

CAPABILITY_ID = "postgres_mssql_target_uow_v2"
IMPLEMENTATION_ABSENT = "absent"
CERTIFICATION_UNVERIFIED = "unverified"
ACTIVATION_BLOCKED = "blocked"
_ALLOWED_KEYS = frozenset({"int2", "int4", "int8", "uuid"})
_MAX_DESCENDANTS = 100_000
_IMPLEMENTATION_STATUSES = frozenset({"absent", "experimental", "implemented"})
_CERTIFICATION_STATUSES = frozenset({"unverified", "local_pass", "vendor_pass", "expired"})
_ACTIVATION_STATUSES = frozenset({"blocked", "explicit_opt_in", "default"})
_CONNECTION_REF = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,126}[A-Za-z0-9])?$")
_KEY_TYPE_ALIASES = {
    "smallint": "int2",
    "int2": "int2",
    "integer": "int4",
    "int": "int4",
    "int4": "int4",
    "bigint": "int8",
    "int8": "int8",
    "uuid": "uuid",
}


class PostgresMssqlProfileError(ValueError):
    """A profile document is internally invalid and cannot be evaluated."""


class SourceMode(StrEnum):
    """Closed R1 PostgreSQL source semantics."""

    BATCH_FULL_REFRESH = "batch_full_refresh"
    XMIN_CURRENT_STATE = "xmin_current_state"


@dataclass(frozen=True, slots=True)
class PostgresMssqlCorrectnessRequirements:
    """User-semantic minimums resolved before business-source I/O."""

    source_mode: SourceMode
    business_key_types: tuple[str, ...]
    target_effect: str = "effectively_once"
    transaction_atomicity: str = "route_visible_events"
    schema_policy: str = "stable_fail_closed"
    maximum_ack_boundary: str = "durable_target_database_authority"

    def __post_init__(self) -> None:
        if not isinstance(self.source_mode, SourceMode):
            raise PostgresMssqlProfileError("source mode is unsupported")
        if len(self.business_key_types) != 1 or not self.business_key_types[0]:
            raise PostgresMssqlProfileError("business key must contain exactly one declared type")


@dataclass(frozen=True, slots=True)
class PostgresMssqlCorrectnessRouteRequest:
    """Exact semantic route coordinates used by an environment selector."""

    source_connection_ref: str
    sink_connection_ref: str
    source_mode: SourceMode
    target_schema: str
    target_table: str
    business_key_types: tuple[str, ...]

    def __post_init__(self) -> None:
        if _CONNECTION_REF.fullmatch(self.source_connection_ref) is None:
            raise PostgresMssqlProfileError("source_connection_ref must be a canonical logical reference")
        if _CONNECTION_REF.fullmatch(self.sink_connection_ref) is None:
            raise PostgresMssqlProfileError("sink_connection_ref must be a canonical logical reference")
        if not isinstance(self.source_mode, SourceMode):
            raise PostgresMssqlProfileError("source mode is unsupported")
        _require_canonical_text(self.target_schema, "target_schema")
        _require_canonical_text(self.target_table, "target_table")
        if len(self.business_key_types) != 1 or not self.business_key_types[0]:
            raise PostgresMssqlProfileError("business key must contain exactly one declared type")

    def requirements(self) -> PostgresMssqlCorrectnessRequirements:
        """Project selector coordinates to the public semantic minimums."""

        return PostgresMssqlCorrectnessRequirements(
            source_mode=self.source_mode,
            business_key_types=self.business_key_types,
        )


@dataclass(frozen=True, slots=True)
class PostgresMssqlCorrectnessProfile:
    """Environment-owned candidate for one exact capability tuple."""

    profile_id: str
    source_connection_ref: str
    sink_connection_ref: str
    allowed_source_modes: tuple[SourceMode, ...]
    source_major: int
    target_major: int
    topology: str
    object_profile: str
    key_types: tuple[str, ...]
    receipt_contract: str
    hash_policy: str
    writer_fence: str
    session_count: int
    transaction_scope: str
    delayed_durability_disabled: bool
    max_descendant_proof_receipts: int
    target_binding_uuid: str
    target_contract_revision: int
    quality_policy: str
    certification_ref: str
    implementation_status: str
    certification_status: str
    activation_status: str

    def __post_init__(self) -> None:
        if not self.profile_id or self.profile_id != self.profile_id.strip():
            raise PostgresMssqlProfileError("profile_id must be canonical non-empty text")
        if _CONNECTION_REF.fullmatch(self.source_connection_ref) is None:
            raise PostgresMssqlProfileError("source_connection_ref must be a canonical logical reference")
        if _CONNECTION_REF.fullmatch(self.sink_connection_ref) is None:
            raise PostgresMssqlProfileError("sink_connection_ref must be a canonical logical reference")
        if (
            not self.allowed_source_modes
            or len(self.allowed_source_modes) != len(set(self.allowed_source_modes))
            or any(not isinstance(mode, SourceMode) for mode in self.allowed_source_modes)
        ):
            raise PostgresMssqlProfileError("allowed source modes must be a unique closed tuple")
        if not self.key_types or len(self.key_types) != len(set(self.key_types)):
            raise PostgresMssqlProfileError("key types must be a unique non-empty tuple")
        if not 1 <= self.max_descendant_proof_receipts <= _MAX_DESCENDANTS:
            raise PostgresMssqlProfileError("descendant proof bound must be between 1 and 100000")
        if _canonical_uuid(self.target_binding_uuid) != self.target_binding_uuid:
            raise PostgresMssqlProfileError("target_binding_uuid must be one canonical UUID")
        if isinstance(self.target_contract_revision, bool) or self.target_contract_revision < 1:
            raise PostgresMssqlProfileError("target_contract_revision must be positive")
        _require_canonical_text(self.quality_policy, "quality_policy")
        _require_canonical_text(self.certification_ref, "certification_ref")
        if isinstance(self.session_count, bool) or self.session_count < 1:
            raise PostgresMssqlProfileError("session count must be positive")
        if self.implementation_status not in _IMPLEMENTATION_STATUSES:
            raise PostgresMssqlProfileError("implementation status is unsupported")
        if self.certification_status not in _CERTIFICATION_STATUSES:
            raise PostgresMssqlProfileError("certification status is unsupported")
        if self.activation_status not in _ACTIVATION_STATUSES:
            raise PostgresMssqlProfileError("activation status is unsupported")


@dataclass(frozen=True, slots=True)
class PostgresMssqlCorrectnessProfileDecision:
    """Explainable immutable semantic-to-environment resolution result."""

    profile_id: str
    source_mode: SourceMode
    accepted: bool
    blockers: tuple[str, ...]
    implementation_status: str
    certification_status: str
    activation_status: str
    canonical_sha256: str
    capability_id: str = CAPABILITY_ID


@dataclass(frozen=True, slots=True)
class PostgresMssqlCorrectnessActivation:
    """Internal immutable binding carried from trusted evidence to runtime."""

    profile: PostgresMssqlCorrectnessProfile
    decision: PostgresMssqlCorrectnessProfileDecision

    def __post_init__(self) -> None:
        exact = (
            self.decision.profile_id == self.profile.profile_id
            and self.decision.source_mode in self.profile.allowed_source_modes
            and self.decision.implementation_status == self.profile.implementation_status
            and self.decision.certification_status == self.profile.certification_status
            and self.decision.activation_status == self.profile.activation_status
            and self.decision.accepted
            and not self.decision.blockers
            and self.profile.implementation_status == "implemented"
            and self.profile.certification_status == "vendor_pass"
            and self.profile.activation_status in {"explicit_opt_in", "default"}
        )
        if not exact:
            raise PostgresMssqlProfileError("runtime activation requires one exact activatable profile decision")


def decide_postgres_mssql_correctness_profile(
    requirements: PostgresMssqlCorrectnessRequirements,
    profile: PostgresMssqlCorrectnessProfile,
) -> PostgresMssqlCorrectnessProfileDecision:
    """Evaluate every R1 dimension explicitly and fail closed on unknowns."""

    blockers: list[str] = []
    if requirements.business_key_types[0] not in _ALLOWED_KEYS or requirements.business_key_types[0] not in set(
        profile.key_types
    ):
        blockers.append("DPONE_POSTGRES_MSSQL_UNSUPPORTED_KEY")
    if requirements.source_mode not in profile.allowed_source_modes:
        blockers.append("DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED")
    if profile.topology != "standalone_same_database":
        blockers.append("DPONE_POSTGRES_MSSQL_SAME_DATABASE_REQUIRED")
    if profile.receipt_contract != "mssql_effect_receipt_v2":
        blockers.append("DPONE_POSTGRES_MSSQL_RECEIPT_V2_REQUIRED")
    if not _matches_exact_r1_dimensions(profile, requirements):
        blockers.append("DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED")
    stable = tuple(dict.fromkeys(blockers))
    payload = {
        "activation_status": profile.activation_status,
        "allowed_source_modes": [mode.value for mode in profile.allowed_source_modes],
        "blockers": list(stable),
        "business_key_types": list(requirements.business_key_types),
        "capability_id": CAPABILITY_ID,
        "certification_status": profile.certification_status,
        "certification_ref": profile.certification_ref,
        "delayed_durability_disabled": profile.delayed_durability_disabled,
        "hash_policy": profile.hash_policy,
        "implementation_status": profile.implementation_status,
        "key_types": list(profile.key_types),
        "max_descendant_proof_receipts": profile.max_descendant_proof_receipts,
        "object_profile": profile.object_profile,
        "profile_id": profile.profile_id,
        "quality_policy": profile.quality_policy,
        "receipt_contract": profile.receipt_contract,
        "session_count": profile.session_count,
        "source_major": profile.source_major,
        "source_connection_ref": profile.source_connection_ref,
        "source_mode": requirements.source_mode.value,
        "target_major": profile.target_major,
        "sink_connection_ref": profile.sink_connection_ref,
        "target_binding_uuid": profile.target_binding_uuid,
        "target_contract_revision": profile.target_contract_revision,
        "topology": profile.topology,
        "transaction_scope": profile.transaction_scope,
        "writer_fence": profile.writer_fence,
    }
    digest = "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    return PostgresMssqlCorrectnessProfileDecision(
        profile_id=profile.profile_id,
        source_mode=requirements.source_mode,
        accepted=not stable,
        blockers=stable,
        implementation_status=profile.implementation_status,
        certification_status=profile.certification_status,
        activation_status=profile.activation_status,
        canonical_sha256=digest,
    )


def build_postgres_mssql_correctness_route_request(
    *,
    source_type: str,
    sink_type: str,
    strategy: str,
    load_config: Any,
    raw_config: Mapping[str, Any],
) -> PostgresMssqlCorrectnessRouteRequest | None:
    """Build exact R1 selector coordinates without importing runtime types."""

    if source_type != "postgres" or sink_type != "mssql":
        return None
    source_mode = _source_mode(strategy, getattr(load_config, "options", {}))
    if source_mode is None:
        return None
    source_ref = _coordinate(getattr(load_config, "source_conn_id", None))
    sink_ref = _coordinate(getattr(load_config, "target_conn_id", None))
    target_schema = _coordinate(getattr(load_config, "target_schema", None))
    target_table = _coordinate(getattr(load_config, "target_table", None))
    if source_ref is None or sink_ref is None or target_schema is None or target_table is None:
        return None
    return PostgresMssqlCorrectnessRouteRequest(
        source_connection_ref=source_ref,
        sink_connection_ref=sink_ref,
        source_mode=source_mode,
        target_schema=target_schema,
        target_table=target_table,
        business_key_types=(_business_key_type(load_config, raw_config),),
    )


def _source_mode(strategy: str, options: object) -> SourceMode | None:
    raw = options if isinstance(options, Mapping) else {}
    if str(raw.get("incremental_strategy") or "").strip().casefold() == "xmin":
        return SourceMode.XMIN_CURRENT_STATE
    if strategy == "full_refresh":
        return SourceMode.BATCH_FULL_REFRESH
    return None


def _coordinate(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _business_key_type(load_config: Any, raw_config: Mapping[str, Any]) -> str:
    keys = _string_items(getattr(load_config, "unique_key", None))
    if len(keys) != 1:
        return "unsupported"
    source = raw_config.get("source")
    source_options = source.get("options") if isinstance(source, Mapping) else None
    for name, data_type in _declared_columns(source_options):
        if name == keys[0]:
            normalized = " ".join(data_type.strip().casefold().split())
            return _KEY_TYPE_ALIASES.get(normalized, normalized or "unknown")
    return "unknown"


def _declared_columns(value: object) -> tuple[tuple[str, str], ...]:
    options = value if isinstance(value, Mapping) else {}
    columns = options.get("columns")
    if isinstance(columns, Mapping):
        return tuple((str(name), str(data_type)) for name, data_type in columns.items())
    if isinstance(columns, Sequence) and not isinstance(columns, str | bytes):
        return tuple(
            (str(item.get("name") or ""), str(item.get("type", item.get("dtype", "")) or ""))
            for item in columns
            if isinstance(item, Mapping)
        )
    return ()


def _string_items(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return tuple(str(item) for item in value)
    return ()


def _canonical_uuid(value: str) -> str:
    try:
        return str(UUID(value))
    except (AttributeError, TypeError, ValueError):
        return ""


def _require_canonical_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise PostgresMssqlProfileError(f"{field} must be canonical non-empty text")


def _matches_exact_r1_dimensions(
    profile: PostgresMssqlCorrectnessProfile,
    requirements: PostgresMssqlCorrectnessRequirements,
) -> bool:
    return (
        profile.source_major == 16
        and profile.target_major == 2022
        and profile.object_profile == "ordinary_disk_rowstore"
        and profile.hash_policy == "postgres_mssql_row_hash_v1"
        and profile.writer_fence == "mssql_target_head_v2"
        and profile.session_count == 1
        and profile.transaction_scope == "local_database"
        and profile.delayed_durability_disabled is True
        and requirements.target_effect == "effectively_once"
        and requirements.transaction_atomicity == "route_visible_events"
        and requirements.schema_policy == "stable_fail_closed"
        and requirements.maximum_ack_boundary == "durable_target_database_authority"
    )


__all__ = [
    "ACTIVATION_BLOCKED",
    "CAPABILITY_ID",
    "CERTIFICATION_UNVERIFIED",
    "IMPLEMENTATION_ABSENT",
    "PostgresMssqlCorrectnessProfile",
    "PostgresMssqlCorrectnessActivation",
    "PostgresMssqlCorrectnessProfileDecision",
    "PostgresMssqlCorrectnessRouteRequest",
    "PostgresMssqlCorrectnessRequirements",
    "PostgresMssqlProfileError",
    "SourceMode",
    "build_postgres_mssql_correctness_route_request",
    "decide_postgres_mssql_correctness_profile",
]
