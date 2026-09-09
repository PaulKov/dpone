"""Complete baseline-adoption authority for semantic refresh V2."""

from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
from typing import Any, ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_evidence_common import require_utc_interval, require_utc_timestamp, require_uuid

BASELINE_ADOPTION_RECEIPT_SCHEMA = "dpone.semantic-refresh-baseline-adoption-receipt.v1"
_DIGEST_FIELD = "baseline_adoption_receipt_sha256"
_STATUS = "COMPLETE"
_CLICKHOUSE_INTERNAL_CONFORMANCE = "PROVEN"
_CROSS_ENGINE_EQUIVALENCE = "NOT_CLAIMED"
_TEXT_FIELDS = (
    "model_unique_id",
    "source_relation_id",
    "mssql_relation_id",
    "clickhouse_relation_id",
    "mssql_connection_authority_id",
    "mssql_target_authority_id",
    "clickhouse_cluster_authority_id",
    "clickhouse_target_authority_id",
)
_GENERATION_FIELDS = ("source_generation", "mssql_generation", "clickhouse_generation")
_DIGEST_FIELDS = (
    "release_id",
    "deployment_id",
    "source_snapshot_sha256",
    "source_schema_sha256",
    "mssql_schema_sha256",
    "clickhouse_schema_sha256",
    "source_key_sha256",
    "mssql_key_sha256",
    "clickhouse_key_sha256",
    "source_physical_sha256",
    "mssql_physical_sha256",
    "clickhouse_physical_sha256",
    "source_coverage_sha256",
    "mssql_coverage_sha256",
    "clickhouse_coverage_sha256",
    "source_assurance_sha256",
    "mssql_assurance_sha256",
    "clickhouse_assurance_sha256",
    "utc_assurance_sha256",
    "writer_assurance_sha256",
    "ddl_assurance_sha256",
    "certified_codec_mapping_sha256",
    "historical_clickhouse_internal_multiset_evidence_sha256",
)


class BaselineAssuranceKind(str, Enum):  # noqa: UP042
    """Closed origin of a complete historical baseline."""

    CERTIFIED_INITIAL_LOAD = "certified_initial_load"
    ADOPTED_COMPLETE_RELATION_CONFORMANT = "adopted_complete_relation_conformant"


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineAdoptionReceipt(SemanticRefreshDocumentCodec):
    """Complete adopted source/MSSQL/ClickHouse generation and assurance closure."""

    model_unique_id: str
    baseline_kind: BaselineAssuranceKind
    release_id: str
    deployment_id: str
    source_snapshot_sha256: str
    source_relation_id: str
    source_generation: int
    mssql_relation_id: str
    mssql_generation: int
    clickhouse_relation_id: str
    clickhouse_generation: int
    clickhouse_target_uuid: str
    mssql_connection_authority_id: str
    mssql_target_authority_id: str
    clickhouse_cluster_authority_id: str
    clickhouse_target_authority_id: str
    source_schema_sha256: str
    mssql_schema_sha256: str
    clickhouse_schema_sha256: str
    source_key_sha256: str
    mssql_key_sha256: str
    clickhouse_key_sha256: str
    source_physical_sha256: str
    mssql_physical_sha256: str
    clickhouse_physical_sha256: str
    coverage_start: str
    coverage_end: str
    source_coverage_sha256: str
    mssql_coverage_sha256: str
    clickhouse_coverage_sha256: str
    source_assurance_sha256: str
    mssql_assurance_sha256: str
    clickhouse_assurance_sha256: str
    utc_assurance_sha256: str
    writer_assurance_sha256: str
    ddl_assurance_sha256: str
    certified_codec_mapping_sha256: str
    historical_clickhouse_internal_multiset_evidence_sha256: str
    adopted_at: str
    baseline_adoption_receipt_sha256: str
    status: str = _STATUS
    historical_clickhouse_internal_multiset_conformance: str = _CLICKHOUSE_INTERNAL_CONFORMANCE
    historical_cross_engine_payload_value_equivalence: str = _CROSS_ENGINE_EQUIVALENCE
    schema: str = BASELINE_ADOPTION_RECEIPT_SCHEMA

    schema_id: ClassVar[str] = BASELINE_ADOPTION_RECEIPT_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if self.status != _STATUS:
            raise SemanticRefreshContractError("baseline adoption receipt must be COMPLETE")
        if not isinstance(self.baseline_kind, BaselineAssuranceKind):
            raise SemanticRefreshContractError("baseline_kind is unsupported")
        if self.historical_clickhouse_internal_multiset_conformance != _CLICKHOUSE_INTERNAL_CONFORMANCE:
            raise SemanticRefreshContractError("historical ClickHouse internal multiset conformance must be PROVEN")
        if self.historical_cross_engine_payload_value_equivalence != _CROSS_ENGINE_EQUIVALENCE:
            raise SemanticRefreshContractError("historical cross-engine payload value equivalence is not claimed")
        for field_name in _TEXT_FIELDS:
            require_text(getattr(self, field_name), field_name)
        for field_name in _GENERATION_FIELDS:
            require_positive_int(getattr(self, field_name), field_name)
        require_uuid(self.clickhouse_target_uuid, "clickhouse_target_uuid")
        for field_name in _DIGEST_FIELDS:
            require_digest(getattr(self, field_name), field_name)
        require_utc_interval(self.coverage_start, self.coverage_end, "coverage_start", "coverage_end")
        require_utc_timestamp(self.adopted_at, "adopted_at")
        validate_digest(self._unsigned(), self.baseline_adoption_receipt_sha256, self.digest_field)

    @classmethod
    def build(cls, **values: Any) -> SemanticRefreshBaselineAdoptionReceipt:
        """Build a complete receipt; partial or unknown coordinates are rejected."""

        expected = _payload_fields()
        if set(values) != expected:
            missing, unknown = expected - set(values), set(values) - expected
            raise SemanticRefreshContractError(
                f"baseline_adoption_receipt fields are invalid; missing={sorted(missing)}, unknown={sorted(unknown)}"
            )
        unsigned = {
            "historical_clickhouse_internal_multiset_conformance": _CLICKHOUSE_INTERNAL_CONFORMANCE,
            "historical_cross_engine_payload_value_equivalence": _CROSS_ENGINE_EQUIVALENCE,
            "schema": cls.schema_id,
            "status": _STATUS,
            **values,
        }
        return cls(**values, baseline_adoption_receipt_sha256=semantic_refresh_sha256(unsigned))

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshBaselineAdoptionReceipt:
        """Parse a closed receipt and recompute its content digest."""

        required = frozenset(
            {
                "schema",
                "status",
                "historical_clickhouse_internal_multiset_conformance",
                "historical_cross_engine_payload_value_equivalence",
                _DIGEST_FIELD,
                *_payload_fields(),
            }
        )
        raw = require_closed_mapping(value, "baseline_adoption_receipt", required=required)
        values: dict[str, object] = {}
        values["baseline_kind"] = require_enum(raw.get("baseline_kind"), "baseline_kind", BaselineAssuranceKind)
        for field_name in _TEXT_FIELDS:
            values[field_name] = require_text(raw.get(field_name), field_name)
        for field_name in _GENERATION_FIELDS:
            values[field_name] = require_positive_int(raw.get(field_name), field_name)
        for field_name in _DIGEST_FIELDS:
            values[field_name] = require_digest(raw.get(field_name), field_name)
        values["coverage_start"] = require_utc_timestamp(raw.get("coverage_start"), "coverage_start")
        values["coverage_end"] = require_utc_timestamp(raw.get("coverage_end"), "coverage_end")
        values["adopted_at"] = require_utc_timestamp(raw.get("adopted_at"), "adopted_at")
        values["clickhouse_target_uuid"] = require_uuid(raw.get("clickhouse_target_uuid"), "clickhouse_target_uuid")
        return cls(
            **values,  # type: ignore[arg-type]
            baseline_adoption_receipt_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            status=require_text(raw.get("status"), "status"),
            historical_clickhouse_internal_multiset_conformance=require_text(
                raw.get("historical_clickhouse_internal_multiset_conformance"),
                "historical_clickhouse_internal_multiset_conformance",
            ),
            historical_cross_engine_payload_value_equivalence=require_text(
                raw.get("historical_cross_engine_payload_value_equivalence"),
                "historical_cross_engine_payload_value_equivalence",
            ),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        result = {item.name: getattr(self, item.name) for item in fields(self) if item.name != self.digest_field}
        result["baseline_kind"] = self.baseline_kind.value
        return result

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.baseline_adoption_receipt_sha256}


def _payload_fields() -> set[str]:
    return {
        *_TEXT_FIELDS,
        *_GENERATION_FIELDS,
        *_DIGEST_FIELDS,
        "baseline_kind",
        "clickhouse_target_uuid",
        "coverage_start",
        "coverage_end",
        "adopted_at",
    }


def semantic_refresh_target_predecessor_generation_id(
    receipt: SemanticRefreshBaselineAdoptionReceipt,
) -> str:
    """Derive target predecessor identity from the protected baseline and exact ClickHouse generation."""

    if not isinstance(receipt, SemanticRefreshBaselineAdoptionReceipt):
        raise SemanticRefreshContractError("receipt must be a semantic-refresh baseline receipt")
    return semantic_refresh_sha256(
        {
            "baseline_adoption_receipt_sha256": receipt.baseline_adoption_receipt_sha256,
            "clickhouse_generation": receipt.clickhouse_generation,
            "clickhouse_relation_id": receipt.clickhouse_relation_id,
            "clickhouse_target_authority_id": receipt.clickhouse_target_authority_id,
            "clickhouse_target_uuid": receipt.clickhouse_target_uuid,
        }
    )


__all__ = [
    "BASELINE_ADOPTION_RECEIPT_SCHEMA",
    "BaselineAssuranceKind",
    "SemanticRefreshBaselineAdoptionReceipt",
    "semantic_refresh_target_predecessor_generation_id",
]
