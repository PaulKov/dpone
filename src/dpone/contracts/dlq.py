"""Pure public contracts for the connector-neutral dpone dead-letter queue."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest

DlqPiiPolicy = Literal["reference_only", "masked", "preserve"]
DlqReplayStatus = Literal["pending", "acknowledged"]

DLQ_RECORD_SCHEMA = "dpone.dlq.v1"
DLQ_INDEX_SCHEMA = "dpone.dlq-index.v1"

_PII_POLICIES = frozenset({"reference_only", "masked", "preserve"})
_REASON_MESSAGES = {
    "schema.required_null": "A required value is null.",
    "schema.type_mismatch": "Value does not match the declared logical type.",
    "source.decode_failed": "The source record could not be decoded.",
    "transform.failed": "The transform could not process the record.",
    "quality.rule_failed": "The record failed a data-quality rule.",
    "sink.row_rejected": "The sink rejected the record.",
    "cdc.poison": "The CDC event cannot be applied safely.",
    "unknown.unclassified": "The record was rejected for an unclassified reason.",
}
_REASON_CATEGORIES = {
    "schema.required_null": "schema_validation",
    "schema.type_mismatch": "schema_validation",
    "source.decode_failed": "source_decode",
    "transform.failed": "transform",
    "quality.rule_failed": "quality",
    "sink.row_rejected": "sink_rejection",
    "cdc.poison": "cdc_poison",
    "unknown.unclassified": "unknown",
}
DLQ_DIAGNOSTIC_FIELDS = frozenset(
    {"actual_type", "column", "expected_type", "rule_id", "source_offset", "source_partition"}
)


@dataclass(frozen=True, slots=True)
class DlqPolicy:
    """Validated storage, retention, and PII policy."""

    directory: Path = Path(".dpone/dlq")
    enabled: bool = True
    retention_days: int = 30
    pii_policy: DlqPiiPolicy = "reference_only"
    max_record_bytes: int = 262_144
    max_diagnostic_bytes: int = 16_384
    max_records_per_run: int = 100_000
    max_index_bytes: int = 67_108_864

    @classmethod
    def from_config(cls, raw: Mapping[str, object] | None, *, environment: str | None = None) -> DlqPolicy:
        config = dict(raw or {})
        directory = str(config.get("directory", ".dpone/dlq")).strip()
        if not directory:
            raise ValueError("dlq.directory must not be empty")
        policy = cls(
            directory=Path(directory),
            enabled=_boolean(config.get("enabled", True), "enabled"),
            retention_days=_integer(config.get("retention_days", 30), "retention_days"),
            pii_policy=_pii_policy(config.get("pii_policy", "reference_only")),
            max_record_bytes=_integer(config.get("max_record_bytes", 262_144), "max_record_bytes"),
            max_diagnostic_bytes=_integer(config.get("max_diagnostic_bytes", 16_384), "max_diagnostic_bytes"),
            max_records_per_run=_integer(config.get("max_records_per_run", 100_000), "max_records_per_run"),
            max_index_bytes=_integer(config.get("max_index_bytes", 67_108_864), "max_index_bytes"),
        )
        policy.validate(environment=environment)
        return policy

    def validate(self, *, environment: str | None = None) -> None:
        if not 1 <= self.retention_days <= 3650:
            raise ValueError("dlq.retention_days must be between 1 and 3650")
        if not 4096 <= self.max_record_bytes <= 1_048_576:
            raise ValueError("dlq.max_record_bytes must be between 4096 and 1048576")
        if not 1024 <= self.max_diagnostic_bytes <= 65_536:
            raise ValueError("dlq.max_diagnostic_bytes must be between 1024 and 65536")
        if self.max_diagnostic_bytes > self.max_record_bytes:
            raise ValueError("dlq.max_diagnostic_bytes cannot exceed max_record_bytes")
        if not 1 <= self.max_records_per_run <= 1_000_000:
            raise ValueError("dlq.max_records_per_run must be between 1 and 1000000")
        if not 1_048_576 <= self.max_index_bytes <= 536_870_912:
            raise ValueError("dlq.max_index_bytes must be between 1048576 and 536870912")
        normalized_environment = str(environment or "").lower()
        if self.pii_policy == "preserve" and (
            normalized_environment == "production" or normalized_environment.startswith("prod")
        ):
            raise ValueError("dlq.pii_policy=preserve is forbidden in production")


@dataclass(frozen=True, slots=True)
class DlqReason:
    category: str
    code: str
    stage: str
    message: str
    column: str | None = None

    @classmethod
    def registered(cls, code: str, *, stage: str, column: str | None = None) -> DlqReason:
        normalized = code if code in _REASON_MESSAGES else "unknown.unclassified"
        return cls(
            category=_REASON_CATEGORIES[normalized],
            code=normalized,
            stage=stage,
            message=_REASON_MESSAGES[normalized],
            column=column,
        )

    @classmethod
    def schema_type_mismatch(cls, *, column: str) -> DlqReason:
        return cls.registered("schema.type_mismatch", stage="contract_enforcement", column=column)

    @classmethod
    def schema_required_null(cls, *, column: str) -> DlqReason:
        return cls.registered("schema.required_null", stage="contract_enforcement", column=column)

    def to_dict(self) -> dict[str, object]:
        return {key: value for key, value in asdict(self).items() if value is not None}

    def validate(self) -> None:
        if self.code not in _REASON_MESSAGES:
            raise ValueError("dlq reason code is not registered")
        if self.category != _REASON_CATEGORIES[self.code]:
            raise ValueError("dlq reason category does not match code")
        if self.message != _REASON_MESSAGES[self.code]:
            raise ValueError("dlq reason message does not match registered safe text")
        if not self.stage or len(self.stage) > 128:
            raise ValueError("dlq reason stage is invalid")


@dataclass(frozen=True, slots=True)
class DlqPayload:
    policy: DlqPiiPolicy
    value: object | None

    def to_dict(self) -> dict[str, object | None]:
        return {"policy": self.policy, "value": self.value}


@dataclass(frozen=True, slots=True)
class DlqReplayState:
    status: DlqReplayStatus = "pending"
    replayable: bool = True

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DlqWriteReceipt:
    record_id: str
    reason_code: str


@dataclass(frozen=True, slots=True)
class DlqRecordLocator:
    record_id: str
    record_ref: str
    sha256: str


@dataclass(frozen=True, slots=True)
class DlqRecord:
    record_id: str
    run_id: str
    load_id: str
    record_ref: str
    reason: DlqReason
    diagnostics: Mapping[str, object]
    payload: DlqPayload
    replay: DlqReplayState
    created_at: str
    expires_at: str
    sha256: str
    schema: str = DLQ_RECORD_SCHEMA

    @classmethod
    def create(
        cls,
        *,
        record_id: str,
        run_id: str,
        load_id: str,
        record_ref: str,
        reason: DlqReason,
        diagnostics: Mapping[str, object],
        payload: DlqPayload,
        retention_days: int,
        created_at: datetime | None = None,
    ) -> DlqRecord:
        created = created_at or datetime.now(UTC)
        draft = cls(
            record_id=record_id,
            run_id=run_id,
            load_id=load_id,
            record_ref=record_ref,
            reason=reason,
            diagnostics=dict(diagnostics),
            payload=payload,
            replay=DlqReplayState(),
            created_at=created.isoformat(),
            expires_at=(created + timedelta(days=retention_days)).isoformat(),
            sha256="",
        )
        return replace(draft, sha256=draft.fingerprint())

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DlqRecord:
        reason_raw = _mapping(payload.get("reason"), "reason")
        body_raw = _mapping(payload.get("payload"), "payload")
        replay_raw = _mapping(payload.get("replay"), "replay")
        policy = _pii_policy(body_raw.get("policy", "reference_only"))
        status = str(replay_raw.get("status", "pending"))
        if status not in {"pending", "acknowledged"}:
            raise ValueError("dlq replay status is invalid")
        return cls(
            schema=str(payload.get("schema", "")),
            record_id=str(payload.get("record_id", "")),
            run_id=str(payload.get("run_id", "")),
            load_id=str(payload.get("load_id", "")),
            record_ref=str(payload.get("record_ref", "")),
            reason=DlqReason(
                category=str(reason_raw.get("category", "")),
                code=str(reason_raw.get("code", "")),
                stage=str(reason_raw.get("stage", "")),
                message=str(reason_raw.get("message", "")),
                column=str(reason_raw["column"]) if reason_raw.get("column") is not None else None,
            ),
            diagnostics=dict(_mapping(payload.get("diagnostics", {}), "diagnostics")),
            payload=DlqPayload(policy=policy, value=body_raw.get("value")),
            replay=DlqReplayState(
                status=cast(DlqReplayStatus, status), replayable=bool(replay_raw.get("replayable", True))
            ),
            created_at=str(payload.get("created_at", "")),
            expires_at=str(payload.get("expires_at", "")),
            sha256=str(payload.get("sha256", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "record_id": self.record_id,
            "run_id": self.run_id,
            "load_id": self.load_id,
            "record_ref": self.record_ref,
            "reason": self.reason.to_dict(),
            "diagnostics": dict(self.diagnostics),
            "payload": self.payload.to_dict(),
            "replay": self.replay.to_dict(),
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "sha256": self.sha256,
        }

    def fingerprint(self) -> str:
        return canonical_fingerprint(self.to_dict(), exclude_top_level=frozenset({"sha256"}))

    def validate(self) -> None:
        if self.schema != DLQ_RECORD_SCHEMA:
            raise ValueError("unsupported dlq record schema")
        if (
            not self.record_ref.startswith("dpone://")
            or len(self.record_ref) > 2048
            or any(ord(char) < 32 for char in self.record_ref)
        ):
            raise ValueError("dlq record_ref is invalid")
        self.reason.validate()
        if set(self.diagnostics) - DLQ_DIAGNOSTIC_FIELDS:
            raise ValueError("dlq diagnostics contain non-safe fields")
        if self.payload.policy == "reference_only" and self.payload.value is not None:
            raise ValueError("reference_only dlq payload must be null")
        if self.payload.policy == "masked" and not _is_masked(self.payload.value):
            raise ValueError("masked dlq payload contains an unmasked scalar")
        created = _aware_datetime(self.created_at, "created_at")
        expires = _aware_datetime(self.expires_at, "expires_at")
        if expires <= created:
            raise ValueError("dlq expires_at must be after created_at")


def project_payload(row: Mapping[str, object], policy: DlqPiiPolicy) -> DlqPayload:
    if policy == "reference_only":
        return DlqPayload(policy=policy, value=None)
    if policy == "masked":
        return DlqPayload(policy=policy, value=_mask(row))
    return DlqPayload(policy=policy, value=dict(row))


def _mask(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _mask(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, list | tuple):
        return [_mask(item) for item in value]
    return "[REDACTED]"


def _is_masked(value: object) -> bool:
    if isinstance(value, Mapping):
        return all(_is_masked(item) for item in value.values())
    if isinstance(value, list | tuple):
        return all(_is_masked(item) for item in value)
    return value == "[REDACTED]"


def _aware_datetime(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"dlq {field} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"dlq {field} must be offset-aware")
    return parsed


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"dlq.{field} must be an integer")
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        raise ValueError(f"dlq.{field} must be an integer")
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"dlq.{field} must be an integer") from exc


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"dlq.{field} must be a boolean")
    return value


def _pii_policy(value: object) -> DlqPiiPolicy:
    normalized = str(value)
    if normalized not in _PII_POLICIES:
        raise ValueError("dlq.pii_policy must be reference_only, masked, or preserve")
    return cast(DlqPiiPolicy, normalized)


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"dlq.{field} must be an object")
    return cast(Mapping[str, object], value)


__all__ = [
    "DLQ_INDEX_SCHEMA",
    "DLQ_RECORD_SCHEMA",
    "DLQ_DIAGNOSTIC_FIELDS",
    "DlqPayload",
    "DlqPiiPolicy",
    "DlqPolicy",
    "DlqReason",
    "DlqRecord",
    "DlqRecordLocator",
    "DlqReplayState",
    "DlqWriteReceipt",
    "canonical_fingerprint",
    "is_canonical_sha256_digest",
    "project_payload",
]
