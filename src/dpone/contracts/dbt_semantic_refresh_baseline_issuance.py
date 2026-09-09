"""Fail-closed plan/apply boundary for complete semantic-refresh baselines."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Protocol

from dpone.contracts.dbt_semantic_refresh_activation import SemanticRefreshProtectedAssuranceVerifierPort
from dpone.contracts.dbt_semantic_refresh_baseline_types import (
    SemanticRefreshBaselineEvidenceBundle,
    SemanticRefreshBaselineIssuancePlan,
    SemanticRefreshBaselineIssuanceSubject,
)
from dpone.contracts.semantic_refresh_baseline_receipt import SemanticRefreshBaselineAdoptionReceipt
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError
from dpone.contracts.semantic_refresh_evidence_common import parse_utc_timestamp
from dpone.contracts.semantic_refresh_runtime_assurance import (
    RuntimeAssuranceKind,
    RuntimeAssuranceSubjectType,
    SemanticRefreshRuntimeAssuranceReceipt,
    SemanticRefreshRuntimeAssuranceSubject,
)


class SemanticRefreshBaselineEvidenceProviderPort(Protocol):
    """Observe all engines and resolve assurances without caller evidence inputs."""

    def observe(self, plan: SemanticRefreshBaselineIssuancePlan) -> SemanticRefreshBaselineEvidenceBundle: ...


class SemanticRefreshBaselineAuthorityVerifierPort(Protocol):
    """Authenticate promoted plan authority and the complete observed evidence bundle."""

    def verify_plan(self, plan: SemanticRefreshBaselineIssuancePlan) -> bool: ...

    def verify_evidence(
        self,
        plan: SemanticRefreshBaselineIssuancePlan,
        evidence: SemanticRefreshBaselineEvidenceBundle,
        *,
        as_of: datetime,
    ) -> bool: ...


class SemanticRefreshBaselineReceiptStorePort(Protocol):
    """Persist one canonical complete baseline create-only with exact replay."""

    def persist_exact(
        self,
        receipt: SemanticRefreshBaselineAdoptionReceipt,
    ) -> SemanticRefreshBaselineAdoptionReceipt: ...


class SemanticRefreshBaselineIssuanceError(RuntimeError):
    """Stable fail-closed operator error from baseline plan/apply."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class SemanticRefreshBaselineIssuanceService:
    """Plan and apply baseline adoption from protected current observations only."""

    def __init__(
        self,
        *,
        evidence_provider: SemanticRefreshBaselineEvidenceProviderPort,
        authority_verifier: SemanticRefreshBaselineAuthorityVerifierPort,
        assurance_verifier: SemanticRefreshProtectedAssuranceVerifierPort,
        receipt_store: SemanticRefreshBaselineReceiptStorePort,
        clock: Callable[[], datetime],
    ) -> None:
        self._evidence_provider = evidence_provider
        self._authority_verifier = authority_verifier
        self._assurance_verifier = assurance_verifier
        self._receipt_store = receipt_store
        self._clock = clock

    def plan(self, subject: SemanticRefreshBaselineIssuanceSubject) -> SemanticRefreshBaselineIssuancePlan:
        """Create a non-mutating plan only for authenticated promoted authority."""

        plan = SemanticRefreshBaselineIssuancePlan.build(subject, planned_at=_utc_text(self._clock()))
        if not self._plan_authorized(plan):
            raise SemanticRefreshBaselineIssuanceError(
                "DPONE_DBT_V2_BASELINE_AUTHORITY_INVALID",
                "baseline issuance plan is not backed by exact promoted release/deployment authority",
            )
        return plan

    def apply(self, plan: SemanticRefreshBaselineIssuancePlan) -> SemanticRefreshBaselineAdoptionReceipt:
        """Observe, verify, build, and create-once persist one complete receipt."""

        now = self._clock()
        if not isinstance(plan, SemanticRefreshBaselineIssuancePlan) or not self._plan_authorized(plan):
            raise SemanticRefreshBaselineIssuanceError(
                "DPONE_DBT_V2_BASELINE_AUTHORITY_INVALID",
                "baseline issuance plan is not current protected authority",
            )
        try:
            evidence = self._evidence_provider.observe(plan)
            evidence_authorized = self._authority_verifier.verify_evidence(plan, evidence, as_of=now)
        except Exception as exc:
            raise _evidence_error("baseline evidence observation is unavailable") from exc
        if not isinstance(evidence, SemanticRefreshBaselineEvidenceBundle) or evidence_authorized is not True:
            raise _evidence_error("baseline evidence is not protected and exact")
        try:
            receipt = self._receipt(plan, evidence, now)
        except SemanticRefreshBaselineIssuanceError:
            raise
        except Exception as exc:
            raise _evidence_error("baseline evidence is malformed or incomplete") from exc
        try:
            persisted = self._receipt_store.persist_exact(receipt)
        except Exception as exc:
            raise SemanticRefreshBaselineIssuanceError(
                "DPONE_DBT_V2_BASELINE_STORE_CONFLICT",
                "complete baseline receipt could not be persisted create-only",
            ) from exc
        if persisted != receipt:
            raise SemanticRefreshBaselineIssuanceError(
                "DPONE_DBT_V2_BASELINE_STORE_CONFLICT",
                "baseline store acknowledgement differs from the canonical receipt",
            )
        return receipt

    def _plan_authorized(self, plan: SemanticRefreshBaselineIssuancePlan) -> bool:
        try:
            return self._authority_verifier.verify_plan(plan) is True
        except Exception:
            return False

    def _receipt(
        self,
        plan: SemanticRefreshBaselineIssuancePlan,
        evidence: SemanticRefreshBaselineEvidenceBundle,
        now: datetime,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        subject = plan.subject
        _validate_observations(plan, evidence, now)
        writer, ddl, utc = _validate_assurances(subject, evidence, now, self._assurance_verifier)
        source, mssql, clickhouse = evidence.source.relation, evidence.mssql.relation, evidence.clickhouse.relation
        return SemanticRefreshBaselineAdoptionReceipt.build(
            model_unique_id=subject.model_unique_id,
            baseline_kind=subject.baseline_kind,
            release_id=subject.release_id,
            deployment_id=subject.deployment_id,
            source_snapshot_sha256=evidence.source.source_snapshot_sha256,
            source_relation_id=source.relation_id,
            source_generation=source.generation,
            mssql_relation_id=mssql.relation_id,
            mssql_generation=mssql.generation,
            clickhouse_relation_id=clickhouse.relation_id,
            clickhouse_generation=clickhouse.generation,
            clickhouse_target_uuid=evidence.clickhouse.clickhouse_target_uuid,
            mssql_connection_authority_id=evidence.mssql.mssql_connection_authority_id,
            mssql_target_authority_id=evidence.mssql.mssql_target_authority_id,
            clickhouse_cluster_authority_id=evidence.clickhouse.clickhouse_cluster_authority_id,
            clickhouse_target_authority_id=evidence.clickhouse.clickhouse_target_authority_id,
            source_schema_sha256=source.schema_sha256,
            mssql_schema_sha256=mssql.schema_sha256,
            clickhouse_schema_sha256=clickhouse.schema_sha256,
            source_key_sha256=source.key_sha256,
            mssql_key_sha256=mssql.key_sha256,
            clickhouse_key_sha256=clickhouse.key_sha256,
            source_physical_sha256=source.physical_sha256,
            mssql_physical_sha256=mssql.physical_sha256,
            clickhouse_physical_sha256=clickhouse.physical_sha256,
            coverage_start=subject.coverage_start,
            coverage_end=subject.coverage_end,
            source_coverage_sha256=source.coverage_sha256,
            mssql_coverage_sha256=mssql.coverage_sha256,
            clickhouse_coverage_sha256=clickhouse.coverage_sha256,
            source_assurance_sha256=source.assurance_sha256,
            mssql_assurance_sha256=mssql.assurance_sha256,
            clickhouse_assurance_sha256=clickhouse.assurance_sha256,
            utc_assurance_sha256=(
                utc.runtime_assurance_receipt_sha256 if utc else evidence.historical_utc_assurance_sha256
            ),
            writer_assurance_sha256=writer.runtime_assurance_receipt_sha256,
            ddl_assurance_sha256=ddl.runtime_assurance_receipt_sha256,
            certified_codec_mapping_sha256=subject.certified_codec_mapping_sha256,
            historical_clickhouse_internal_multiset_evidence_sha256=(
                evidence.clickhouse.historical_internal_multiset_evidence_sha256
            ),
            adopted_at=plan.planned_at,
        )


def _validate_observations(
    plan: SemanticRefreshBaselineIssuancePlan,
    evidence: SemanticRefreshBaselineEvidenceBundle,
    now: datetime,
) -> None:
    subject = plan.subject
    source, mssql, clickhouse = evidence.source.relation, evidence.mssql.relation, evidence.clickhouse.relation
    expected_interval = (subject.coverage_start, subject.coverage_end)
    if (
        evidence.baseline_issuance_plan_sha256 != plan.baseline_issuance_plan_sha256
        or parse_utc_timestamp(evidence.observed_at, "observed_at") > now
        or source.relation_id != subject.source_relation_id
        or mssql.relation_id != subject.mssql_relation_id
        or clickhouse.relation_id != subject.clickhouse_relation_id
        or evidence.mssql.mssql_connection_authority_id != subject.mssql_connection_authority_id
        or evidence.mssql.mssql_target_authority_id != subject.mssql_target_authority_id
        or evidence.clickhouse.clickhouse_cluster_authority_id != subject.clickhouse_cluster_authority_id
        or evidence.clickhouse.clickhouse_target_authority_id != subject.clickhouse_target_authority_id
        or any((item.coverage_start, item.coverage_end) != expected_interval for item in (source, mssql, clickhouse))
    ):
        raise _evidence_error("baseline observations differ from the exact plan subject")


def _validate_assurances(
    subject: SemanticRefreshBaselineIssuanceSubject,
    evidence: SemanticRefreshBaselineEvidenceBundle,
    now: datetime,
    verifier: SemanticRefreshProtectedAssuranceVerifierPort,
) -> tuple[
    SemanticRefreshRuntimeAssuranceReceipt,
    SemanticRefreshRuntimeAssuranceReceipt,
    SemanticRefreshRuntimeAssuranceReceipt | None,
]:
    route = evidence.route_certification
    if (
        route.route_certification_receipt_sha256 != subject.route_certification_receipt_sha256
        or route.coordinates.environment != subject.environment
        or route.coordinates.load_strategy != "semantic_refresh_v2"
        or verifier.verify_route(route, expected_coordinate_sha256=subject.certification_coordinate_sha256) is not True
        or not route.authorizes(now, trusted_receipt_sha256=route.route_certification_receipt_sha256)
    ):
        raise _evidence_error("baseline route certification is not current protected authority")
    expected = _expected_assurance_subjects(subject)
    observed = {item.subject.assurance_kind: item for item in evidence.runtime_assurances}
    if set(observed) != set(expected):
        raise _evidence_error("baseline runtime assurance closure is incomplete")
    for kind, expected_subject in expected.items():
        receipt = observed[kind]
        if verifier.verify_runtime(receipt) is not True or not receipt.authorizes(
            now,
            trusted_digest=receipt.runtime_assurance_receipt_sha256,
            expected_subject=expected_subject,
        ):
            raise _evidence_error("baseline runtime assurance is not current protected authority")
    utc = observed.get(RuntimeAssuranceKind.UTC_SEMANTICS)
    if utc is not None and evidence.historical_utc_assurance_sha256 != utc.runtime_assurance_receipt_sha256:
        raise _evidence_error("historical UTC evidence differs from the required current column assurance")
    return observed[RuntimeAssuranceKind.WRITER_EXCLUSIVITY], observed[RuntimeAssuranceKind.DDL_FREEZE], utc


def _expected_assurance_subjects(
    subject: SemanticRefreshBaselineIssuanceSubject,
) -> dict[RuntimeAssuranceKind, SemanticRefreshRuntimeAssuranceSubject]:
    common = {
        "release_id": subject.release_id,
        "deployment_id": subject.deployment_id,
        "environment": subject.environment,
        "database": subject.mssql_relation_id.partition(".")[0],
        "model_unique_id": subject.model_unique_id,
        "mssql_target_authority_id": subject.mssql_target_authority_id,
        "model_definition_proof_sha256": subject.model_definition_proof_sha256,
        "effective_key_template_sha256": subject.effective_key_template_sha256,
        "writable_schema_sha256": subject.writable_schema_sha256,
        "sqlserver_lifecycle_policy_sha256": subject.sqlserver_lifecycle_policy_sha256,
        "route_certification_receipt_sha256": subject.route_certification_receipt_sha256,
        "mssql_control_database": subject.mssql_control_database,
        "mssql_control_schema": subject.mssql_control_schema,
        "mssql_image_schema": subject.mssql_image_schema,
        "scope_image_namespace_policy_sha256": subject.scope_image_namespace_policy_sha256,
    }
    result = {
        kind: SemanticRefreshRuntimeAssuranceSubject(
            assurance_kind=kind,
            subject_type=RuntimeAssuranceSubjectType.TARGET,
            **common,
        )
        for kind in (RuntimeAssuranceKind.WRITER_EXCLUSIVITY, RuntimeAssuranceKind.DDL_FREEZE)
    }
    if subject.utc_assurance_required:
        result[RuntimeAssuranceKind.UTC_SEMANTICS] = SemanticRefreshRuntimeAssuranceSubject(
            assurance_kind=RuntimeAssuranceKind.UTC_SEMANTICS,
            subject_type=RuntimeAssuranceSubjectType.COLUMN,
            column_name=subject.event_time_column,
            **common,
        )
    return result


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SemanticRefreshContractError("baseline issuance clock must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")  # noqa: UP017


def _evidence_error(message: str) -> SemanticRefreshBaselineIssuanceError:
    return SemanticRefreshBaselineIssuanceError("DPONE_DBT_V2_BASELINE_EVIDENCE_UNVERIFIED", message)


__all__ = [
    "SemanticRefreshBaselineAuthorityVerifierPort",
    "SemanticRefreshBaselineEvidenceBundle",
    "SemanticRefreshBaselineEvidenceProviderPort",
    "SemanticRefreshBaselineIssuanceError",
    "SemanticRefreshBaselineIssuanceService",
    "SemanticRefreshBaselineReceiptStorePort",
]
