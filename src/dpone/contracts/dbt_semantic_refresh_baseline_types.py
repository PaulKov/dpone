"""Typed plan and observation authority for semantic-refresh baseline issuance."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.semantic_refresh_baseline_receipt import BaselineAssuranceKind
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_evidence_common import require_utc_interval, require_utc_timestamp, require_uuid
from dpone.contracts.semantic_refresh_route_certification import SemanticRefreshRouteLiveCertificationReceipt
from dpone.contracts.semantic_refresh_runtime_assurance import SemanticRefreshRuntimeAssuranceReceipt

BASELINE_ISSUANCE_PLAN_SCHEMA = "dpone.dbt-semantic-refresh-baseline-issuance-plan.v1"
BASELINE_EVIDENCE_BUNDLE_SCHEMA = "dpone.dbt-semantic-refresh-baseline-evidence-bundle.v1"


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineIssuanceSubject:
    """Protected release, deployment, proof, physical target, and coverage subject."""

    baseline_kind: BaselineAssuranceKind
    model_unique_id: str
    release_id: str
    deployment_id: str
    environment: str
    source_relation_id: str
    mssql_relation_id: str
    mssql_connection_authority_id: str
    mssql_target_authority_id: str
    clickhouse_relation_id: str
    clickhouse_cluster_authority_id: str
    clickhouse_target_authority_id: str
    coverage_start: str
    coverage_end: str
    model_definition_proof_sha256: str
    effective_key_template_sha256: str
    writable_schema_sha256: str
    sqlserver_lifecycle_policy_sha256: str
    certification_coordinate_sha256: str
    route_certification_receipt_sha256: str
    certified_codec_mapping_sha256: str
    mssql_control_database: str
    mssql_control_schema: str
    mssql_image_schema: str
    scope_image_namespace_policy_sha256: str
    event_time_column: str
    utc_assurance_required: bool
    issuance_policy_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.baseline_kind, BaselineAssuranceKind):
            raise SemanticRefreshContractError("baseline_kind is unsupported")
        for field in (
            "model_unique_id",
            "environment",
            "source_relation_id",
            "mssql_relation_id",
            "mssql_connection_authority_id",
            "mssql_target_authority_id",
            "clickhouse_relation_id",
            "clickhouse_cluster_authority_id",
            "clickhouse_target_authority_id",
            "mssql_control_database",
            "mssql_control_schema",
            "mssql_image_schema",
            "event_time_column",
        ):
            require_text(getattr(self, field), field)
        for field in (
            "release_id",
            "deployment_id",
            "model_definition_proof_sha256",
            "effective_key_template_sha256",
            "writable_schema_sha256",
            "sqlserver_lifecycle_policy_sha256",
            "certification_coordinate_sha256",
            "route_certification_receipt_sha256",
            "certified_codec_mapping_sha256",
            "scope_image_namespace_policy_sha256",
            "issuance_policy_sha256",
        ):
            require_digest(getattr(self, field), field)
        require_utc_interval(self.coverage_start, self.coverage_end, "coverage_start", "coverage_end")
        if not isinstance(self.utc_assurance_required, bool):
            raise SemanticRefreshContractError("utc_assurance_required must be boolean")
        mssql_parts = self.mssql_relation_id.split(".")
        if len(mssql_parts) != 3 or any(not part for part in mssql_parts):
            raise SemanticRefreshContractError("mssql_relation_id must contain exact database.schema.table")
        database, schema, table = mssql_parts
        relation = f"{schema}.{table}"
        clickhouse_database, clickhouse_separator, clickhouse_table = self.clickhouse_relation_id.partition(".")
        if not clickhouse_separator or not clickhouse_database or not clickhouse_table:
            raise SemanticRefreshContractError("clickhouse_relation_id must contain database and table")
        expected_mssql = f"mssql://{self.mssql_connection_authority_id}/{database}/{relation}"
        expected_clickhouse = (
            f"clickhouse://{self.clickhouse_cluster_authority_id}/{clickhouse_database}/{clickhouse_table}"
        )
        if self.mssql_target_authority_id != expected_mssql:
            raise SemanticRefreshContractError("MSSQL target authority differs from the physical baseline relation")
        if self.clickhouse_target_authority_id != expected_clickhouse:
            raise SemanticRefreshContractError(
                "ClickHouse target authority differs from the physical baseline relation"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            field: self.baseline_kind.value if field == "baseline_kind" else getattr(self, field)
            for field in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineIssuancePlan:
    """Immutable plan whose evidence is observed only during apply."""

    subject: SemanticRefreshBaselineIssuanceSubject
    planned_at: str
    baseline_issuance_plan_sha256: str
    schema: str = BASELINE_ISSUANCE_PLAN_SCHEMA

    def __post_init__(self) -> None:
        if not isinstance(self.subject, SemanticRefreshBaselineIssuanceSubject):
            raise SemanticRefreshContractError("baseline issuance subject must be typed")
        require_utc_timestamp(self.planned_at, "planned_at")
        require_digest(self.baseline_issuance_plan_sha256, "baseline_issuance_plan_sha256")
        if (
            self.schema != BASELINE_ISSUANCE_PLAN_SCHEMA
            or self.baseline_issuance_plan_sha256 != semantic_refresh_sha256(self._unsigned())
        ):
            raise SemanticRefreshContractError("baseline issuance plan is malformed or digest-invalid")

    @classmethod
    def build(
        cls,
        subject: SemanticRefreshBaselineIssuanceSubject,
        *,
        planned_at: str,
    ) -> SemanticRefreshBaselineIssuancePlan:
        unsigned = {
            "planned_at": planned_at,
            "schema": BASELINE_ISSUANCE_PLAN_SCHEMA,
            "subject": subject.to_dict(),
        }
        return cls(subject, planned_at, semantic_refresh_sha256(unsigned))

    def _unsigned(self) -> dict[str, object]:
        return {"planned_at": self.planned_at, "schema": self.schema, "subject": self.subject.to_dict()}

    def to_dict(self) -> dict[str, object]:
        return {**self._unsigned(), "baseline_issuance_plan_sha256": self.baseline_issuance_plan_sha256}


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineRelationObservation:
    """One provider-observed immutable relation generation and evidence closure."""

    relation_id: str
    generation: int
    schema_sha256: str
    key_sha256: str
    physical_sha256: str
    coverage_start: str
    coverage_end: str
    coverage_sha256: str
    assurance_sha256: str
    observation_receipt_sha256: str

    def __post_init__(self) -> None:
        require_text(self.relation_id, "relation_id")
        require_positive_int(self.generation, "generation")
        for field in (
            "schema_sha256",
            "key_sha256",
            "physical_sha256",
            "coverage_sha256",
            "assurance_sha256",
        ):
            require_digest(getattr(self, field), field)
        require_utc_interval(self.coverage_start, self.coverage_end, "coverage_start", "coverage_end")
        require_digest(self.observation_receipt_sha256, "observation_receipt_sha256")
        if self.observation_receipt_sha256 != semantic_refresh_sha256(self._unsigned()):
            raise SemanticRefreshContractError("baseline relation observation digest differs")

    @classmethod
    def build(cls, **values: object) -> SemanticRefreshBaselineRelationObservation:
        return cls(**values, observation_receipt_sha256=semantic_refresh_sha256(values))  # type: ignore[arg-type]

    def _unsigned(self) -> dict[str, object]:
        return {
            field: getattr(self, field) for field in self.__dataclass_fields__ if field != "observation_receipt_sha256"
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._unsigned(), "observation_receipt_sha256": self.observation_receipt_sha256}


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineSourceObservation:
    """Source snapshot plus its typed relation observation."""

    relation: SemanticRefreshBaselineRelationObservation
    source_snapshot_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.relation, SemanticRefreshBaselineRelationObservation):
            raise SemanticRefreshContractError("source relation observation must be typed")
        require_digest(self.source_snapshot_sha256, "source_snapshot_sha256")

    def to_dict(self) -> dict[str, object]:
        return {"relation": self.relation.to_dict(), "source_snapshot_sha256": self.source_snapshot_sha256}


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineMssqlObservation:
    """MSSQL generation observed under exact protected connection and target authority."""

    relation: SemanticRefreshBaselineRelationObservation
    mssql_connection_authority_id: str
    mssql_target_authority_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.relation, SemanticRefreshBaselineRelationObservation):
            raise SemanticRefreshContractError("MSSQL relation observation must be typed")
        require_text(self.mssql_connection_authority_id, "mssql_connection_authority_id")
        require_text(self.mssql_target_authority_id, "mssql_target_authority_id")

    def to_dict(self) -> dict[str, object]:
        return {
            "mssql_connection_authority_id": self.mssql_connection_authority_id,
            "mssql_target_authority_id": self.mssql_target_authority_id,
            "relation": self.relation.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineClickHouseObservation:
    """ClickHouse generation, physical UUID, and internal multiset proof."""

    relation: SemanticRefreshBaselineRelationObservation
    clickhouse_cluster_authority_id: str
    clickhouse_target_authority_id: str
    clickhouse_target_uuid: str
    historical_internal_multiset_evidence_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.relation, SemanticRefreshBaselineRelationObservation):
            raise SemanticRefreshContractError("ClickHouse relation observation must be typed")
        require_text(self.clickhouse_cluster_authority_id, "clickhouse_cluster_authority_id")
        require_text(self.clickhouse_target_authority_id, "clickhouse_target_authority_id")
        require_uuid(self.clickhouse_target_uuid, "clickhouse_target_uuid")
        require_digest(
            self.historical_internal_multiset_evidence_sha256,
            "historical_internal_multiset_evidence_sha256",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "clickhouse_target_authority_id": self.clickhouse_target_authority_id,
            "clickhouse_target_uuid": self.clickhouse_target_uuid,
            "historical_internal_multiset_evidence_sha256": self.historical_internal_multiset_evidence_sha256,
            "relation": self.relation.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineEvidenceBundle:
    """Typed observations and signed assurances obtained by the protected provider."""

    baseline_issuance_plan_sha256: str
    source: SemanticRefreshBaselineSourceObservation
    mssql: SemanticRefreshBaselineMssqlObservation
    clickhouse: SemanticRefreshBaselineClickHouseObservation
    route_certification: SemanticRefreshRouteLiveCertificationReceipt
    runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...]
    historical_utc_assurance_sha256: str
    observed_at: str
    baseline_evidence_bundle_sha256: str
    schema: str = BASELINE_EVIDENCE_BUNDLE_SCHEMA

    def __post_init__(self) -> None:
        require_digest(self.baseline_issuance_plan_sha256, "baseline_issuance_plan_sha256")
        if not isinstance(self.source, SemanticRefreshBaselineSourceObservation):
            raise SemanticRefreshContractError("source baseline observation must be typed")
        if not isinstance(self.mssql, SemanticRefreshBaselineMssqlObservation):
            raise SemanticRefreshContractError("MSSQL baseline observation must be typed")
        if not isinstance(self.clickhouse, SemanticRefreshBaselineClickHouseObservation):
            raise SemanticRefreshContractError("ClickHouse baseline observation must be typed")
        if not isinstance(self.route_certification, SemanticRefreshRouteLiveCertificationReceipt):
            raise SemanticRefreshContractError("route certification must be a typed receipt")
        if any(not isinstance(item, SemanticRefreshRuntimeAssuranceReceipt) for item in self.runtime_assurances):
            raise SemanticRefreshContractError("runtime assurance closure must contain typed receipts")
        kinds = tuple(item.subject.assurance_kind for item in self.runtime_assurances)
        if len(kinds) != len(set(kinds)):
            raise SemanticRefreshContractError("runtime assurance closure contains duplicate kinds")
        require_digest(self.historical_utc_assurance_sha256, "historical_utc_assurance_sha256")
        require_utc_timestamp(self.observed_at, "observed_at")
        require_digest(self.baseline_evidence_bundle_sha256, "baseline_evidence_bundle_sha256")
        if self.schema != BASELINE_EVIDENCE_BUNDLE_SCHEMA or self.baseline_evidence_bundle_sha256 != (
            semantic_refresh_sha256(self._unsigned())
        ):
            raise SemanticRefreshContractError("baseline evidence bundle is malformed or digest-invalid")

    @classmethod
    def build(
        cls,
        *,
        baseline_issuance_plan_sha256: str,
        source: SemanticRefreshBaselineSourceObservation,
        mssql: SemanticRefreshBaselineMssqlObservation,
        clickhouse: SemanticRefreshBaselineClickHouseObservation,
        route_certification: SemanticRefreshRouteLiveCertificationReceipt,
        runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...],
        historical_utc_assurance_sha256: str,
        observed_at: str,
    ) -> SemanticRefreshBaselineEvidenceBundle:
        canonical = tuple(sorted(runtime_assurances, key=lambda item: item.subject.assurance_kind.value))
        unsigned = _evidence_payload(
            baseline_issuance_plan_sha256=baseline_issuance_plan_sha256,
            source=source,
            mssql=mssql,
            clickhouse=clickhouse,
            route_certification=route_certification,
            runtime_assurances=canonical,
            historical_utc_assurance_sha256=historical_utc_assurance_sha256,
            observed_at=observed_at,
        )
        return cls(
            baseline_issuance_plan_sha256,
            source,
            mssql,
            clickhouse,
            route_certification,
            canonical,
            historical_utc_assurance_sha256,
            observed_at,
            semantic_refresh_sha256(unsigned),
        )

    def _unsigned(self) -> dict[str, object]:
        return _evidence_payload(
            baseline_issuance_plan_sha256=self.baseline_issuance_plan_sha256,
            source=self.source,
            mssql=self.mssql,
            clickhouse=self.clickhouse,
            route_certification=self.route_certification,
            runtime_assurances=self.runtime_assurances,
            historical_utc_assurance_sha256=self.historical_utc_assurance_sha256,
            observed_at=self.observed_at,
        )


def _evidence_payload(
    *,
    baseline_issuance_plan_sha256: str,
    source: SemanticRefreshBaselineSourceObservation,
    mssql: SemanticRefreshBaselineMssqlObservation,
    clickhouse: SemanticRefreshBaselineClickHouseObservation,
    route_certification: SemanticRefreshRouteLiveCertificationReceipt,
    runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...],
    historical_utc_assurance_sha256: str,
    observed_at: str,
) -> dict[str, object]:
    return {
        "baseline_issuance_plan_sha256": baseline_issuance_plan_sha256,
        "clickhouse": clickhouse.to_dict(),
        "historical_utc_assurance_sha256": historical_utc_assurance_sha256,
        "mssql": mssql.to_dict(),
        "observed_at": observed_at,
        "route_certification": route_certification.to_dict(),
        "runtime_assurances": [item.to_dict() for item in runtime_assurances],
        "schema": BASELINE_EVIDENCE_BUNDLE_SCHEMA,
        "source": source.to_dict(),
    }


__all__ = [
    "BASELINE_EVIDENCE_BUNDLE_SCHEMA",
    "BASELINE_ISSUANCE_PLAN_SCHEMA",
    "SemanticRefreshBaselineClickHouseObservation",
    "SemanticRefreshBaselineEvidenceBundle",
    "SemanticRefreshBaselineIssuancePlan",
    "SemanticRefreshBaselineIssuanceSubject",
    "SemanticRefreshBaselineMssqlObservation",
    "SemanticRefreshBaselineRelationObservation",
    "SemanticRefreshBaselineSourceObservation",
]
