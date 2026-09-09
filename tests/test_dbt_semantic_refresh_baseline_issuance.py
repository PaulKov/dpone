"""Plan/apply tests for protected cross-engine baseline issuance."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from dpone.contracts.dbt_semantic_refresh_baseline_issuance import (
    SemanticRefreshBaselineEvidenceBundle,
    SemanticRefreshBaselineIssuanceError,
    SemanticRefreshBaselineIssuanceService,
)
from dpone.contracts.dbt_semantic_refresh_baseline_types import (
    SemanticRefreshBaselineClickHouseObservation,
    SemanticRefreshBaselineIssuancePlan,
    SemanticRefreshBaselineIssuanceSubject,
    SemanticRefreshBaselineMssqlObservation,
    SemanticRefreshBaselineRelationObservation,
    SemanticRefreshBaselineSourceObservation,
)
from dpone.contracts.semantic_refresh_baseline_receipt import (
    BaselineAssuranceKind,
    SemanticRefreshBaselineAdoptionReceipt,
)
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError
from dpone.contracts.semantic_refresh_route_certification import (
    LiveCertificationStatus,
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_route_coordinates import SemanticRefreshRouteCapabilityCoordinates
from dpone.contracts.semantic_refresh_runtime_assurance import (
    RuntimeAssuranceKind,
    RuntimeAssuranceSubjectType,
    SemanticRefreshRuntimeAssuranceReceipt,
    SemanticRefreshRuntimeAssuranceSubject,
)

NOW = datetime(2026, 8, 9, tzinfo=timezone.utc)  # noqa: UP017


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _subject(*, utc_assurance_required: bool = True) -> SemanticRefreshBaselineIssuanceSubject:
    return SemanticRefreshBaselineIssuanceSubject(
        baseline_kind=BaselineAssuranceKind.ADOPTED_COMPLETE_RELATION_CONFORMANT,
        model_unique_id="model.analytics.events",
        release_id=_digest("1"),
        deployment_id=_digest("2"),
        environment="prod",
        source_relation_id="source.analytics.events",
        mssql_relation_id="DWH.mart.events",
        mssql_connection_authority_id="mssql-prod",
        mssql_target_authority_id="mssql://mssql-prod/DWH/mart.events",
        clickhouse_relation_id="analytics.mart.events",
        clickhouse_cluster_authority_id="clickhouse-prod",
        clickhouse_target_authority_id="clickhouse://clickhouse-prod/analytics/mart.events",
        coverage_start="2026-08-01T00:00:00Z",
        coverage_end="2026-08-08T00:00:00Z",
        model_definition_proof_sha256=_digest("3"),
        effective_key_template_sha256=_digest("4"),
        writable_schema_sha256=_digest("5"),
        sqlserver_lifecycle_policy_sha256=_digest("6"),
        certification_coordinate_sha256=_digest("7"),
        route_certification_receipt_sha256=_route_receipt().route_certification_receipt_sha256,
        certified_codec_mapping_sha256=_digest("8"),
        mssql_control_database="DWH",
        mssql_control_schema="dpone_control",
        mssql_image_schema="dpone_images",
        scope_image_namespace_policy_sha256=_digest("9"),
        event_time_column="occurred_at",
        utc_assurance_required=utc_assurance_required,
        issuance_policy_sha256=_digest("a"),
    )


def _route_receipt() -> SemanticRefreshRouteLiveCertificationReceipt:
    return SemanticRefreshRouteLiveCertificationReceipt.build(
        coordinates=SemanticRefreshRouteCapabilityCoordinates(
            source_connector="mssql",
            source_connector_version="16.0.1000.6",
            sink_connector="clickhouse",
            sink_connector_version="25.7",
            load_strategy="semantic_refresh_v2",
            environment="prod",
            runtime_image_digest=_digest("b"),
            toolchain_sha256=_digest("c"),
            capability_policy_sha256=_digest("d"),
            source_capability_sha256=_digest("e"),
            sink_capability_sha256=_digest("f"),
            route_policy_sha256=_digest("0"),
        ),
        status=LiveCertificationStatus.PASS,
        tested_at="2026-08-01T00:00:00Z",
        effective_from="2026-08-01T00:00:00Z",
        expires_at="2026-09-01T00:00:00Z",
        live_environment_sha256=_digest("1"),
        live_evidence_sha256=_digest("2"),
        failure_matrix_sha256=_digest("3"),
        certification_policy_sha256=_digest("4"),
        issuer_authority="dpone-release-certifier",
        issuer_attestation_sha256=_digest("5"),
        issuer_signature_sha256=_digest("6"),
    )


def _assurance_subject(
    subject: SemanticRefreshBaselineIssuanceSubject,
    kind: RuntimeAssuranceKind,
) -> SemanticRefreshRuntimeAssuranceSubject:
    return SemanticRefreshRuntimeAssuranceSubject(
        assurance_kind=kind,
        subject_type=(
            RuntimeAssuranceSubjectType.COLUMN
            if kind is RuntimeAssuranceKind.UTC_SEMANTICS
            else RuntimeAssuranceSubjectType.TARGET
        ),
        release_id=subject.release_id,
        deployment_id=subject.deployment_id,
        environment=subject.environment,
        database="DWH",
        model_unique_id=subject.model_unique_id,
        mssql_target_authority_id=subject.mssql_target_authority_id,
        model_definition_proof_sha256=subject.model_definition_proof_sha256,
        effective_key_template_sha256=subject.effective_key_template_sha256,
        writable_schema_sha256=subject.writable_schema_sha256,
        sqlserver_lifecycle_policy_sha256=subject.sqlserver_lifecycle_policy_sha256,
        route_certification_receipt_sha256=subject.route_certification_receipt_sha256,
        mssql_control_database=subject.mssql_control_database,
        mssql_control_schema=subject.mssql_control_schema,
        mssql_image_schema=subject.mssql_image_schema,
        scope_image_namespace_policy_sha256=subject.scope_image_namespace_policy_sha256,
        column_name=subject.event_time_column if kind is RuntimeAssuranceKind.UTC_SEMANTICS else None,
    )


def _assurance(
    subject: SemanticRefreshBaselineIssuanceSubject,
    kind: RuntimeAssuranceKind,
) -> SemanticRefreshRuntimeAssuranceReceipt:
    values: dict[str, object] = {
        "subject": _assurance_subject(subject, kind),
        "producer_version": "1.0.0",
        "transformation_version": "1.0.0",
        "acl_policy_version": "1.0.0",
        "runtime_assurance_policy_sha256": _digest("7"),
        "approver_authority": "dpone-control-plane",
        "approver_attestation_sha256": _digest("8"),
        "approver_signature_sha256": _digest("9"),
        "effective_from": "2026-08-01T00:00:00Z",
        "expires_at": "2026-09-01T00:00:00Z",
        "evidence_sha256": _digest("a"),
    }
    if kind is RuntimeAssuranceKind.WRITER_EXCLUSIVITY:
        values.update(
            engine_acl_proof_sha256=_digest("b"),
            platform_allowlist_sha256=_digest("c"),
            external_job_inventory_sha256=_digest("d"),
            organizational_control_sha256=_digest("e"),
        )
    elif kind is RuntimeAssuranceKind.DDL_FREEZE:
        values["ddl_epoch"] = 7
    return SemanticRefreshRuntimeAssuranceReceipt.build(**values)  # type: ignore[arg-type]


def _relation(relation_id: str, generation: int, character: str) -> SemanticRefreshBaselineRelationObservation:
    return SemanticRefreshBaselineRelationObservation.build(
        relation_id=relation_id,
        generation=generation,
        schema_sha256=_digest(character),
        key_sha256=_digest(character),
        physical_sha256=_digest(character),
        coverage_start="2026-08-01T00:00:00Z",
        coverage_end="2026-08-08T00:00:00Z",
        coverage_sha256=_digest(character),
        assurance_sha256=_digest(character),
    )


def _evidence(
    plan: SemanticRefreshBaselineIssuancePlan,
    *,
    mssql_relation_id: str = "DWH.mart.events",
) -> SemanticRefreshBaselineEvidenceBundle:
    subject = plan.subject
    assurances = [
        _assurance(subject, RuntimeAssuranceKind.WRITER_EXCLUSIVITY),
        _assurance(subject, RuntimeAssuranceKind.DDL_FREEZE),
    ]
    if subject.utc_assurance_required:
        assurances.append(_assurance(subject, RuntimeAssuranceKind.UTC_SEMANTICS))
    historical_utc = assurances[-1].runtime_assurance_receipt_sha256 if subject.utc_assurance_required else _digest("f")
    return SemanticRefreshBaselineEvidenceBundle.build(
        baseline_issuance_plan_sha256=plan.baseline_issuance_plan_sha256,
        source=SemanticRefreshBaselineSourceObservation(
            _relation("source.analytics.events", 3, "1"),
            _digest("2"),
        ),
        mssql=SemanticRefreshBaselineMssqlObservation(
            _relation(mssql_relation_id, 4, "3"),
            "mssql-prod",
            "mssql://mssql-prod/DWH/mart.events",
        ),
        clickhouse=SemanticRefreshBaselineClickHouseObservation(
            _relation("analytics.mart.events", 8, "4"),
            "clickhouse-prod",
            "clickhouse://clickhouse-prod/analytics/mart.events",
            "0198f11c-6956-74f2-984b-4cfcb1653b87",
            _digest("5"),
        ),
        route_certification=_route_receipt(),
        runtime_assurances=tuple(assurances),
        historical_utc_assurance_sha256=historical_utc,
        observed_at="2026-08-09T00:00:00Z",
    )


class _Provider:
    def __init__(self, build: Callable[[SemanticRefreshBaselineIssuancePlan], SemanticRefreshBaselineEvidenceBundle]):
        self._build = build
        self.calls = 0

    def observe(self, plan: SemanticRefreshBaselineIssuancePlan) -> SemanticRefreshBaselineEvidenceBundle:
        self.calls += 1
        return self._build(plan)


class _AuthorityVerifier:
    def __init__(self, *, plan: bool = True, evidence: bool = True) -> None:
        self.plan_authorized = plan
        self.evidence_authorized = evidence

    def verify_plan(self, _plan: SemanticRefreshBaselineIssuancePlan) -> bool:
        return self.plan_authorized

    def verify_evidence(self, *_args: object, **_kwargs: object) -> bool:
        return self.evidence_authorized


class _AssuranceVerifier:
    def verify_route(self, *_args: object, **_kwargs: object) -> bool:
        return True

    def verify_runtime(self, *_args: object, **_kwargs: object) -> bool:
        return True


class _Store:
    def __init__(self) -> None:
        self.receipts: list[SemanticRefreshBaselineAdoptionReceipt] = []

    def persist_exact(
        self,
        receipt: SemanticRefreshBaselineAdoptionReceipt,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        self.receipts.append(receipt)
        return receipt


def _service(
    provider: _Provider,
    *,
    authority: _AuthorityVerifier | None = None,
    store: _Store | None = None,
    clock: Callable[[], datetime] = lambda: NOW,
) -> tuple[SemanticRefreshBaselineIssuanceService, _Store]:
    receipt_store = store or _Store()
    return (
        SemanticRefreshBaselineIssuanceService(
            evidence_provider=provider,
            authority_verifier=authority or _AuthorityVerifier(),
            assurance_verifier=_AssuranceVerifier(),
            receipt_store=receipt_store,
            clock=clock,
        ),
        receipt_store,
    )


def test_apply_observes_and_persists_one_exact_complete_baseline() -> None:
    provider = _Provider(_evidence)
    service, store = _service(provider)

    plan = service.plan(_subject())
    receipt = service.apply(plan)

    assert receipt.status == "COMPLETE"
    assert receipt.source_generation == 3
    assert receipt.mssql_generation == 4
    assert receipt.clickhouse_generation == 8
    assert (
        receipt.utc_assurance_sha256
        == _assurance(_subject(), RuntimeAssuranceKind.UTC_SEMANTICS).runtime_assurance_receipt_sha256
    )
    assert store.receipts == [receipt]
    assert provider.calls == 1


def test_plan_rejects_unprotected_authority_before_observation() -> None:
    provider = _Provider(_evidence)
    service, store = _service(provider, authority=_AuthorityVerifier(plan=False))

    with pytest.raises(SemanticRefreshBaselineIssuanceError) as raised:
        service.plan(_subject())

    assert raised.value.code == "DPONE_DBT_V2_BASELINE_AUTHORITY_INVALID"
    assert provider.calls == 0
    assert store.receipts == []


def test_subject_rejects_noncanonical_mssql_physical_identity() -> None:
    with pytest.raises(SemanticRefreshContractError, match="database.schema.table"):
        replace(_subject(), mssql_relation_id="DWH.mart.events.extra")


def test_apply_rejects_cross_wired_observation_before_persistence() -> None:
    provider = _Provider(lambda plan: _evidence(plan, mssql_relation_id="DWH.mart.other"))
    service, store = _service(provider)
    plan = service.plan(_subject())

    with pytest.raises(SemanticRefreshBaselineIssuanceError) as raised:
        service.apply(plan)

    assert raised.value.code == "DPONE_DBT_V2_BASELINE_EVIDENCE_UNVERIFIED"
    assert store.receipts == []


def test_apply_rejects_untrusted_evidence_before_persistence() -> None:
    provider = _Provider(_evidence)
    service, store = _service(provider, authority=_AuthorityVerifier(evidence=False))
    plan = service.plan(_subject())

    with pytest.raises(SemanticRefreshBaselineIssuanceError) as raised:
        service.apply(plan)

    assert raised.value.code == "DPONE_DBT_V2_BASELINE_EVIDENCE_UNVERIFIED"
    assert store.receipts == []


def test_apply_translates_unavailable_observation_and_never_calls_store() -> None:
    def unavailable(_plan: SemanticRefreshBaselineIssuancePlan) -> SemanticRefreshBaselineEvidenceBundle:
        raise OSError("observer unavailable")

    provider = _Provider(unavailable)
    service, store = _service(provider)
    plan = service.plan(_subject())

    with pytest.raises(SemanticRefreshBaselineIssuanceError) as raised:
        service.apply(plan)

    assert raised.value.code == "DPONE_DBT_V2_BASELINE_EVIDENCE_UNVERIFIED"
    assert store.receipts == []


def test_date_baseline_uses_verified_historical_calendar_assurance_without_runtime_utc_receipt() -> None:
    provider = _Provider(_evidence)
    service, _store = _service(provider)
    plan = service.plan(_subject(utc_assurance_required=False))

    receipt = service.apply(plan)

    assert receipt.utc_assurance_sha256 == _digest("f")


def test_apply_reuses_plan_bound_adoption_identity_after_store_ack_loss() -> None:
    class _CreateOnceCrashStore(_Store):
        def __init__(self) -> None:
            super().__init__()
            self.persisted: SemanticRefreshBaselineAdoptionReceipt | None = None
            self.crash_once = True

        def persist_exact(
            self,
            receipt: SemanticRefreshBaselineAdoptionReceipt,
        ) -> SemanticRefreshBaselineAdoptionReceipt:
            if self.persisted is None:
                self.persisted = receipt
            elif self.persisted != receipt:
                raise RuntimeError("create-only conflict")
            if self.crash_once:
                self.crash_once = False
                raise OSError("ack lost after durable create")
            return self.persisted

    instants = iter(
        (
            NOW,
            datetime(2026, 8, 9, 1, tzinfo=timezone.utc),  # noqa: UP017
            datetime(2026, 8, 9, 2, tzinfo=timezone.utc),  # noqa: UP017
        )
    )
    store = _CreateOnceCrashStore()
    service, _ = _service(_Provider(_evidence), store=store, clock=lambda: next(instants))
    plan = service.plan(_subject())

    with pytest.raises(SemanticRefreshBaselineIssuanceError) as raised:
        service.apply(plan)
    assert raised.value.code == "DPONE_DBT_V2_BASELINE_STORE_CONFLICT"

    replayed = service.apply(plan)

    assert replayed == store.persisted
    assert replayed.adopted_at == plan.planned_at


def test_duplicate_apply_services_produce_one_exact_receipt_from_the_same_plan() -> None:
    class _CreateOnlyStore(_Store):
        def __init__(self) -> None:
            super().__init__()
            self.persisted: SemanticRefreshBaselineAdoptionReceipt | None = None

        def persist_exact(
            self,
            receipt: SemanticRefreshBaselineAdoptionReceipt,
        ) -> SemanticRefreshBaselineAdoptionReceipt:
            if self.persisted is None:
                self.persisted = receipt
            elif self.persisted != receipt:
                raise RuntimeError("create-only conflict")
            return self.persisted

    store = _CreateOnlyStore()
    planning, _ = _service(_Provider(_evidence), store=store)
    plan = planning.plan(_subject())
    first, _ = _service(
        _Provider(_evidence),
        store=store,
        clock=lambda: datetime(2026, 8, 9, 1, tzinfo=timezone.utc),  # noqa: UP017
    )
    second, _ = _service(
        _Provider(_evidence),
        store=store,
        clock=lambda: datetime(2026, 8, 9, 2, tzinfo=timezone.utc),  # noqa: UP017
    )

    first_receipt = first.apply(plan)
    second_receipt = second.apply(plan)

    assert first_receipt == second_receipt == store.persisted
