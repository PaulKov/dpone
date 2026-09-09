from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace

import pytest

from dpone.adapters.semantic_refresh_clickhouse_authority import (
    StaticSemanticRefreshClickHousePublicationAuthority,
)
from dpone.adapters.semantic_refresh_clickhouse_callbacks import (
    CallbackSemanticRefreshClickHouseGateway,
    CallbackSemanticRefreshPublicationState,
)
from dpone.ports.semantic_refresh_clickhouse_authority import ClickHousePublicationAuthority
from dpone.ports.semantic_refresh_clickhouse_prepared import (
    DurableClickHousePreparedPublication,
)
from dpone.runtime.semantic_refresh_clickhouse_authority_state import (
    AuthorityBoundSemanticRefreshPublicationState,
)
from dpone.runtime.semantic_refresh_clickhouse_conformance import ClickHousePrepareSqlBuilder
from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHouseHeadPublicationPlan,
    ClickHousePreparePlan,
    GroupedMultisetConformance,
    ShadowEquation,
    semantic_refresh_fingerprint,
)
from dpone.runtime.semantic_refresh_clickhouse_plan_factory import (
    ClickHouseHeadPublicationPlanFactory,
)
from dpone.runtime.semantic_refresh_clickhouse_prepared_codec import (
    load_prepared_publication,
    prepared_publication_documents,
)
from dpone.runtime.semantic_refresh_clickhouse_service import (
    ClickHouseCommittedIncompleteError,
    ClickHouseConformanceError,
    ClickHousePublicationAuthorityError,
    ClickHousePublicationService,
    ClickHouseUuidAmbiguityError,
)


def _digest(char: str) -> str:
    return "sha256:" + char * 64


_CLUSTER = "dpone-semref-primary"


def _plan() -> ClickHousePreparePlan:
    return ClickHousePreparePlan(
        operation_id=_digest("0"),
        operation_plan_sha256=_digest("a"),
        workflow_plan_sha256=_digest("b"),
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("c"),
        attempt_binding_sha256=_digest("d"),
        fence_epoch=7,
        artifact_manifest_key="semantic-refresh/orders/manifest.json",
        artifact_manifest_version="generation-17",
        artifact_manifest_sha256=_digest("e"),
        target_resource_id="clickhouse-target://mart/orders",
        target_authority_id=f"clickhouse://{_CLUSTER}/mart/orders",
        clickhouse_cluster_authority_id=_CLUSTER,
        database="mart",
        target_table="orders",
        scope_id="2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
        scope_start="2026-08-08T00:00:00Z",
        scope_end="2026-08-09T00:00:00Z",
        scope_revision=9,
        event_time_column="occurred_at",
        staging_table="orders__dpone_stage__000000000000",
        shadow_table="orders__dpone_shadow__000000000000",
        expected_target_uuid="00000000-0000-0000-0000-000000000001",
        expected_schema_sha256=_digest("f"),
        expected_physical_sha256=_digest("1"),
        business_columns=("event_id", "occurred_at", "amount"),
        effective_key_columns=("event_id", "occurred_at"),
        max_staging_rows=100,
        max_target_scope_rows=1_000,
        max_staging_bytes=2_000,
        max_shadow_bytes=8_000,
        max_retained_backup_bytes=10_000,
        max_total_transient_bytes=20_000,
        shadow_equation=ShadowEquation(
            retained_target_rule="TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY",
            append_rule="APPEND_ALL_STAGING_ROWS",
        ),
        conformance=GroupedMultisetConformance(
            mode="BIDIRECTIONAL_GROUPED_MULTISET",
        ),
    )


def _prepare_evidence(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "target_uuid": "00000000-0000-0000-0000-000000000001",
        "staging_uuid": "00000000-0000-0000-0000-000000000002",
        "shadow_uuid": "00000000-0000-0000-0000-000000000003",
        "staging_rows": 3,
        "target_scope_rows": 7,
        "shadow_rows": 10,
        "desired_rows": 10,
        "staging_null_key_rows": 0,
        "staging_duplicate_key_groups": 0,
        "target_null_key_rows": 0,
        "target_duplicate_key_groups": 0,
        "forward_difference_groups": 0,
        "reverse_difference_groups": 0,
        "schema_sha256": _digest("f"),
        "physical_sha256": _digest("1"),
        "staging_bytes": 200,
        "shadow_bytes": 500,
        "retained_backup_bytes": 300,
        "total_transient_bytes": 1_000,
        "guard_operation_id": _digest("0"),
        "guard_attempt_binding_sha256": _digest("d"),
        "guard_fence_epoch": 7,
        "database_engine": "Atomic",
        "table_engine": "MergeTree",
        "shard_count": 1,
        "replica_count": 1,
    }
    payload.update(overrides)
    return payload


class _Harness:
    def __init__(self, *, evidence: Mapping[str, object] | None = None) -> None:
        self.evidence = dict(evidence or _prepare_evidence())
        self.uuid_map = {
            "target_uuid": "00000000-0000-0000-0000-000000000001",
            "shadow_uuid": "00000000-0000-0000-0000-000000000003",
        }
        self.exchange_calls = 0
        self.prepare_calls = 0
        self.revalidate_calls = 0
        self.state_calls: list[Mapping[str, object]] = []
        self.transition_calls: list[Mapping[str, object]] = []
        self.raise_after_exchange = False
        self.raise_on_uuid_inspection = False
        self.raise_after_exchange_inspection_once = False
        self.cleanup_calls = 0
        self.raise_cleanup_once = False
        self.post_exchange_physical_sha256: str | None = None
        self.events: list[str] = []

    def prepare(
        self,
        _plan: Mapping[str, object],
        _sql: Mapping[str, str],
    ) -> Mapping[str, object]:
        self.prepare_calls += 1
        return self.evidence

    def inspect_uuid(self, _request: Mapping[str, object]) -> Mapping[str, object]:
        if self.raise_on_uuid_inspection:
            raise TimeoutError("UUID inventory unavailable")
        if self.raise_after_exchange_inspection_once and self.exchange_calls:
            self.raise_after_exchange_inspection_once = False
            raise TimeoutError("post-exchange UUID inventory unavailable")
        return self.uuid_map

    def revalidate(
        self,
        _plan: Mapping[str, object],
        _sql: Mapping[str, str],
    ) -> Mapping[str, object]:
        self.revalidate_calls += 1
        return self.evidence

    def inspect_target_authority(self, _plan: Mapping[str, object]) -> Mapping[str, object]:
        return {
            "target_uuid": self.uuid_map["target_uuid"],
            "schema_sha256": self.evidence["schema_sha256"],
            "physical_sha256": self.post_exchange_physical_sha256 or self.evidence["physical_sha256"],
            "guard_operation_id": self.evidence["guard_operation_id"],
            "guard_attempt_binding_sha256": self.evidence["guard_attempt_binding_sha256"],
            "guard_fence_epoch": self.evidence["guard_fence_epoch"],
            "database_engine": self.evidence["database_engine"],
            "table_engine": self.evidence["table_engine"],
            "shard_count": self.evidence["shard_count"],
            "replica_count": self.evidence["replica_count"],
        }

    def exchange(self, _request: Mapping[str, object]) -> None:
        self.exchange_calls += 1
        self.uuid_map = {
            "target_uuid": "00000000-0000-0000-0000-000000000003",
            "shadow_uuid": "00000000-0000-0000-0000-000000000001",
        }
        if self.raise_after_exchange:
            raise TimeoutError("exchange acknowledgement lost")

    def cleanup(self, _request: Mapping[str, object]) -> None:
        self.cleanup_calls += 1
        self.events.append("cleanup")
        if self.raise_cleanup_once:
            self.raise_cleanup_once = False
            raise TimeoutError("cleanup acknowledgement lost")

    def publish(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self.events.append("publish_complete")
        self.state_calls.append(request)
        return {
            **request,
            "committed": True,
            "atomic": True,
            "journal_state": "COMPLETE",
        }

    def transition(self, request: Mapping[str, object]) -> Mapping[str, object]:
        self.transition_calls.append(request)
        return {
            **request,
            "committed": True,
            "atomic": True,
            "journal_state": request["next_journal_state"],
        }


def _service(harness: _Harness) -> ClickHousePublicationService:
    gateway = CallbackSemanticRefreshClickHouseGateway(
        prepare_callback=harness.prepare,
        revalidate_callback=harness.revalidate,
        inspect_uuid_callback=harness.inspect_uuid,
        exchange_callback=harness.exchange,
        inspect_target_authority_callback=harness.inspect_target_authority,
        cleanup_callback=harness.cleanup,
    )
    state = AuthorityBoundSemanticRefreshPublicationState(
        authority=_authority(),
        delegate=CallbackSemanticRefreshPublicationState(
            persist_prepared_callback=harness.transition,
            mark_committing_callback=harness.transition,
            publish_callback=harness.publish,
        ),
    )
    return ClickHousePublicationService(gateway=gateway, state=state, authority=_authority())


def _heads() -> ClickHouseHeadPublicationPlan:
    target_generation_id = semantic_refresh_fingerprint(
        {
            "schema": "dpone.semantic-refresh-target-generation.v1",
            "target_authority_id": f"clickhouse://{_CLUSTER}/mart/orders",
            "operation_id": _digest("0"),
            "operation_plan_sha256": _digest("a"),
            "target_generation": 4,
            "target_uuid": "00000000-0000-0000-0000-000000000003",
        }
    )
    return ClickHouseHeadPublicationPlan(
        scope_id="2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
        expected_target_generation=3,
        target_generation=4,
        target_generation_id=target_generation_id,
        expected_scope_revision=8,
        scope_revision=9,
        expected_checkpoint_sha256=_digest("8"),
        checkpoint_sha256=semantic_refresh_fingerprint(
            {
                "schema": "dpone.semantic-refresh-checkpoint.v1",
                "operation_id": _digest("0"),
                "scope_id": "2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
                "scope_revision": 9,
                "target_generation_id": target_generation_id,
                "target_uuid": "00000000-0000-0000-0000-000000000003",
                "scope_end": "2026-08-09T00:00:00Z",
            }
        ),
    )


def _authority() -> StaticSemanticRefreshClickHousePublicationAuthority:
    plan = _plan()
    heads = _heads()
    return StaticSemanticRefreshClickHousePublicationAuthority(
        records=(
            ClickHousePublicationAuthority(
                authority_sha256=_digest("7"),
                workflow_execution_id=plan.workflow_execution_id,
                operation_id=plan.operation_id,
                operation_plan_sha256=plan.operation_plan_sha256,
                workflow_plan_sha256=plan.workflow_plan_sha256,
                workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
                attempt_binding_sha256=plan.attempt_binding_sha256,
                fencing_epoch=plan.fence_epoch,
                owner_id="owner-1",
                guard_resource_id=plan.target_resource_id,
                guard_status="HELD",
                target_resource_id="clickhouse-target://mart/orders",
                target_authority_id=f"clickhouse://{_CLUSTER}/mart/orders",
                clickhouse_cluster_authority_id=_CLUSTER,
                database=plan.database,
                target_table=plan.target_table,
                scope_id=heads.scope_id,
                scope_start=plan.scope_start,
                scope_end=plan.scope_end,
                scope_revision=heads.scope_revision,
                target_predecessor_generation_id=_digest("6"),
                scope_predecessor_operation_id=_digest("4"),
                predecessor_target_generation=heads.expected_target_generation,
                predecessor_target_uuid=plan.expected_target_uuid,
                predecessor_target_operation_id=_digest("5"),
                predecessor_scope_revision=heads.expected_scope_revision,
                predecessor_checkpoint_sha256=heads.expected_checkpoint_sha256,
                predecessor_checkpoint_operation_id=_digest("4"),
                predecessor_checkpoint_version=8,
                expected_target_uuid=plan.expected_target_uuid,
                expected_schema_sha256=plan.expected_schema_sha256,
                expected_physical_sha256=plan.expected_physical_sha256,
                business_columns=plan.business_columns,
                effective_key_columns=plan.effective_key_columns,
                event_time_column=plan.event_time_column,
                effective_key_mapping_sha256=_digest("2"),
                route_certification_receipt_sha256=_digest("3"),
                artifact_prefix="semantic-refresh/orders",
                artifact_provider="s3",
                encryption_policy_sha256=_digest("4"),
                retention_policy_sha256=_digest("5"),
                max_artifact_bytes=100_000,
                max_staging_rows=plan.max_staging_rows,
                max_target_scope_rows=plan.max_target_scope_rows,
                max_staging_bytes=plan.max_staging_bytes,
                max_shadow_bytes=plan.max_shadow_bytes,
                max_retained_backup_bytes=plan.max_retained_backup_bytes,
                max_total_transient_bytes=plan.max_total_transient_bytes,
            ),
        )
    )


def test_prepare_requires_exact_bidirectional_grouped_multiset_conformance() -> None:
    prepared = _service(_Harness()).prepare(_plan())

    assert prepared.status == "PREPARED"
    assert prepared.forward_difference_groups == 0
    assert prepared.reverse_difference_groups == 0
    assert prepared.shadow_equation == {
        "retained_target_rule": "TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY",
        "append_rule": "APPEND_ALL_STAGING_ROWS",
    }


def test_prepare_fails_closed_on_one_reverse_difference_group() -> None:
    harness = _Harness(evidence=_prepare_evidence(reverse_difference_groups=1))

    with pytest.raises(ClickHouseConformanceError, match="grouped-multiset"):
        _service(harness).prepare(_plan())

    assert harness.exchange_calls == 0
    assert harness.state_calls == []


def test_prepare_fails_closed_when_shadow_and_desired_counts_differ() -> None:
    harness = _Harness(evidence=_prepare_evidence(shadow_rows=9, desired_rows=10))

    with pytest.raises(ClickHouseConformanceError, match="row count"):
        _service(harness).prepare(_plan())

    assert harness.exchange_calls == 0


def test_prepare_fails_closed_on_atomic_topology_drift() -> None:
    harness = _Harness(evidence=_prepare_evidence(database_engine="Ordinary"))

    with pytest.raises(ClickHouseConformanceError, match="topology"):
        _service(harness).prepare(_plan())


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("target_scope_rows", 1_001, "target scope row"),
        ("staging_bytes", 2_001, "staging byte"),
        ("shadow_bytes", 8_001, "shadow byte"),
        ("retained_backup_bytes", 10_001, "retained backup byte"),
        ("total_transient_bytes", 20_001, "total transient byte"),
    ],
)
def test_prepare_enforces_each_protected_clickhouse_resource_axis(
    field_name: str,
    value: int,
    message: str,
) -> None:
    evidence = _prepare_evidence(**{field_name: value})
    if field_name != "total_transient_bytes":
        evidence["total_transient_bytes"] = sum(
            int(evidence[name]) for name in ("staging_bytes", "shadow_bytes", "retained_backup_bytes")
        )
    harness = _Harness(evidence=evidence)

    with pytest.raises(ClickHouseConformanceError, match=message):
        _service(harness).prepare(_plan())

    assert harness.state_calls == []


def test_sql_plan_encodes_anti_join_append_and_all_column_multiset() -> None:
    sql = ClickHousePrepareSqlBuilder().build(_plan())

    assert "LEFT ANTI JOIN" in sql.populate_shadow_retained
    assert "`event_id`" in sql.populate_shadow_retained
    assert "`occurred_at`" in sql.populate_shadow_retained
    assert "INSERT INTO `mart`.`orders__dpone_shadow__000000000000`" in sql.append_staging
    assert "`amount`" in sql.forward_multiset_difference
    assert "GROUP BY `event_id`, `occurred_at`, `amount`" in sql.forward_multiset_difference
    assert "EXCEPT DISTINCT" in sql.forward_multiset_difference
    assert "EXCEPT DISTINCT" in sql.reverse_multiset_difference
    assert sql.forward_multiset_difference != sql.reverse_multiset_difference


def test_dotted_protected_table_is_one_safely_quoted_clickhouse_identifier() -> None:
    plan = replace(
        _plan(),
        target_table="mart.orders",
        target_authority_id=f"clickhouse://{_CLUSTER}/mart/mart.orders",
        staging_table="mart.orders__dpone_stage__000000000000",
        shadow_table="mart.orders__dpone_shadow__000000000000",
    )

    sql = ClickHousePrepareSqlBuilder().build(plan)

    assert "`mart`.`mart.orders`" in sql.populate_shadow_retained
    assert "`mart`.`mart.orders__dpone_shadow__000000000000`" in sql.populate_shadow_retained


def test_relation_identifier_rejects_sql_control_characters() -> None:
    with pytest.raises(ValueError, match="relation identifier"):
        replace(
            _plan(),
            target_table="orders`; DROP TABLE mart.orders",
            target_authority_id=(f"clickhouse://{_CLUSTER}/mart/orders`; DROP TABLE mart.orders"),
        )


def test_exchange_ack_loss_reconciles_uuid_map_then_publishes_all_heads() -> None:
    harness = _Harness()
    harness.raise_after_exchange = True
    service = _service(harness)
    prepared = service.prepare(_plan())

    receipt = service.commit(_plan(), prepared=prepared, heads=_heads())

    assert receipt.status == "COMPLETE"
    assert receipt.exchange_outcome == "TARGET_COMMITTED"
    assert harness.exchange_calls == 1
    assert harness.revalidate_calls == 1
    committing = next(call for call in harness.transition_calls if call["next_journal_state"] == "COMMITTING")
    assert committing["expected_target_uuid"] == _plan().expected_target_uuid
    assert committing["expected_target_generation"] == 3
    assert committing["expected_scope_revision"] == 8
    assert committing["expected_checkpoint_sha256"] == _digest("8")
    assert len(harness.state_calls) == 1
    assert harness.state_calls[0]["target_generation"] == 4
    assert harness.state_calls[0]["target_generation_id"] == _heads().target_generation_id
    assert harness.state_calls[0]["scope_revision"] == 9
    assert harness.state_calls[0]["checkpoint_sha256"] == _heads().checkpoint_sha256
    assert harness.state_calls[0]["expected_target_generation"] == 3
    assert harness.state_calls[0]["expected_scope_revision"] == 8
    assert harness.state_calls[0]["expected_checkpoint_sha256"] == _digest("8")
    assert harness.state_calls[0]["fence_epoch"] == 7
    assert harness.state_calls[0]["terminal_receipt_sha256"] == receipt.terminal_receipt_sha256
    assert harness.cleanup_calls == 1
    assert harness.events == ["publish_complete", "cleanup"]
    assert receipt.cleanup_status == "COMPLETE"
    assert receipt.retained_generation_status == "RETAINED_FOR_POLICY"


def test_cleanup_failure_keeps_complete_and_reports_independent_maintenance_outcome() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    first = _durable_service(gateway, state)
    prepared = first.prepare(_plan())
    gateway.raise_cleanup_once = True

    receipt = first.commit(_plan(), prepared=prepared, heads=_heads())

    assert receipt.status == "COMPLETE"
    assert receipt.cleanup_status == "FAILED"
    assert receipt.retained_generation_status == "RETAINED_FOR_POLICY"
    assert state.journal_state == "COMPLETE"
    assert gateway.exchange_calls == 1
    assert gateway.cleanup_calls == 1


def test_post_exchange_physical_drift_blocks_complete_after_target_commit() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    service = _durable_service(gateway, state)
    prepared = service.prepare(_plan())
    gateway.post_exchange_physical_sha256 = _digest("9")

    with pytest.raises(ClickHouseCommittedIncompleteError, match="physical authority"):
        service.commit(_plan(), prepared=prepared, heads=_heads())

    assert gateway.exchange_calls == 1
    assert state.journal_state == "COMMITTED_INCOMPLETE"
    assert gateway.cleanup_calls == 0


def test_commit_retry_reconciles_already_cleaned_shadow_without_exchange() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    service = _durable_service(gateway, state)
    prepared = service.prepare(_plan())
    state.journal_state = "COMPLETE"
    gateway.uuid_map = {
        "target_uuid": prepared.shadow_uuid,
        "shadow_uuid": None,
    }

    receipt = _durable_service(gateway, state).commit(_plan(), prepared=prepared, heads=_heads())

    assert receipt.status == "COMPLETE"
    assert gateway.exchange_calls == 0
    assert gateway.cleanup_calls == 1


def test_commit_retry_reconciles_already_exchanged_without_transition_or_exchange() -> None:
    harness = _Harness()
    service = _service(harness)
    prepared = service.prepare(_plan())
    harness.uuid_map = {
        "target_uuid": "00000000-0000-0000-0000-000000000003",
        "shadow_uuid": "00000000-0000-0000-0000-000000000001",
    }

    receipt = service.commit(_plan(), prepared=prepared, heads=_heads())

    assert receipt.status == "COMPLETE"
    assert harness.exchange_calls == 0
    assert harness.revalidate_calls == 0
    assert len(harness.transition_calls) == 2  # PREPARED + durable TARGET_COMMITTED; no COMMITTING replay
    assert harness.transition_calls[-1]["next_journal_state"] == "TARGET_COMMITTED"
    assert len(harness.state_calls) == 1
    assert harness.state_calls[0]["terminal_receipt_sha256"] == receipt.terminal_receipt_sha256


def test_commit_revalidates_conformance_after_parallel_prepare_wait() -> None:
    harness = _Harness()
    service = _service(harness)
    prepared = service.prepare(_plan())
    harness.evidence = _prepare_evidence(reverse_difference_groups=1)

    with pytest.raises(ClickHouseConformanceError, match="grouped-multiset"):
        service.commit(_plan(), prepared=prepared, heads=_heads())

    assert harness.revalidate_calls == 1
    assert harness.exchange_calls == 0
    assert harness.state_calls == []


def test_commit_rejects_another_conformant_dataset_after_prepare() -> None:
    harness = _Harness()
    service = _service(harness)
    prepared = service.prepare(_plan())
    harness.evidence = _prepare_evidence(staging_rows=4, shadow_rows=11, desired_rows=11)

    with pytest.raises(ClickHouseConformanceError, match="evidence drifted"):
        service.commit(_plan(), prepared=prepared, heads=_heads())

    assert harness.revalidate_calls == 1
    assert harness.exchange_calls == 0
    assert harness.state_calls == []


def test_ambiguous_uuid_map_never_publishes_state_or_success() -> None:
    harness = _Harness()
    service = _service(harness)
    prepared = service.prepare(_plan())
    harness.uuid_map = {
        "target_uuid": "00000000-0000-0000-0000-000000000099",
        "shadow_uuid": "00000000-0000-0000-0000-000000000098",
    }

    with pytest.raises(ClickHouseUuidAmbiguityError):
        service.commit(_plan(), prepared=prepared, heads=_heads())

    assert harness.exchange_calls == 0
    assert harness.state_calls == []


def test_unavailable_uuid_authority_is_commit_unknown_not_raw_success() -> None:
    harness = _Harness()
    service = _service(harness)
    prepared = service.prepare(_plan())
    harness.raise_on_uuid_inspection = True

    with pytest.raises(ClickHouseUuidAmbiguityError, match="invalid or unavailable"):
        service.commit(_plan(), prepared=prepared, heads=_heads())

    assert harness.exchange_calls == 0
    assert harness.state_calls == []


def test_invalid_atomic_head_ack_is_committed_incomplete_not_success() -> None:
    harness = _Harness()

    def invalid_ack(request: Mapping[str, object]) -> Mapping[str, object]:
        return {**request, "committed": True, "atomic": False, "journal_state": "COMPLETE"}

    service = ClickHousePublicationService(
        gateway=CallbackSemanticRefreshClickHouseGateway(
            prepare_callback=harness.prepare,
            revalidate_callback=harness.revalidate,
            inspect_uuid_callback=harness.inspect_uuid,
            exchange_callback=harness.exchange,
            inspect_target_authority_callback=harness.inspect_target_authority,
        ),
        state=AuthorityBoundSemanticRefreshPublicationState(
            authority=_authority(),
            delegate=CallbackSemanticRefreshPublicationState(
                persist_prepared_callback=harness.transition,
                mark_committing_callback=harness.transition,
                publish_callback=invalid_ack,
            ),
        ),
        authority=_authority(),
    )
    prepared = service.prepare(_plan())

    with pytest.raises(ClickHouseCommittedIncompleteError) as exc:
        service.commit(_plan(), prepared=prepared, heads=_heads())

    assert exc.value.exchange_receipt.status == "TARGET_COMMITTED"


class _DurablePostExchangeState:
    def __init__(self) -> None:
        self.journal_state = "PREPARING"
        self.reconciliation_states: list[str] = []
        self.fail_target_ack_once = False
        self.fail_terminal_once = False

    def transition(self, request: Mapping[str, object]) -> Mapping[str, object]:
        next_state = str(request["next_journal_state"])
        if self.journal_state != next_state:
            if self.journal_state != request["expected_journal_state"]:
                raise RuntimeError("durable state transition predecessor differs")
            self.journal_state = next_state
        return self._ack(request)

    def record_target_committed(self, request: Mapping[str, object]) -> Mapping[str, object]:
        if self.journal_state in {"COMMITTING", "COMMIT_UNKNOWN"}:
            self.journal_state = "TARGET_COMMITTED"
        if self.fail_target_ack_once:
            self.fail_target_ack_once = False
            raise TimeoutError("TARGET_COMMITTED acknowledgement lost")
        return self._ack(request)

    def record_committed_incomplete(self, request: Mapping[str, object]) -> Mapping[str, object]:
        if self.journal_state == "TARGET_COMMITTED":
            self.journal_state = "COMMITTED_INCOMPLETE"
        return self._ack(request)

    def record_commit_unknown(self, request: Mapping[str, object]) -> Mapping[str, object]:
        if self.journal_state == "COMMITTING":
            self.journal_state = "COMMIT_UNKNOWN"
        return self._ack(request)

    def reconcile_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        if self.journal_state == "COMMITTING":
            self.journal_state = "COMMIT_UNKNOWN"
            self.reconciliation_states.append(self.journal_state)
        if self.journal_state == "COMMIT_UNKNOWN":
            self.journal_state = "PREPARED"
            self.reconciliation_states.append(self.journal_state)
        return self._ack(request)

    def publish(self, request: Mapping[str, object]) -> Mapping[str, object]:
        if self.fail_terminal_once:
            self.fail_terminal_once = False
            raise TimeoutError("terminal publication failed")
        self.journal_state = "COMPLETE"
        return self._ack(request)

    def _ack(self, request: Mapping[str, object]) -> Mapping[str, object]:
        return {
            **request,
            "committed": True,
            "atomic": True,
            "journal_state": self.journal_state,
        }


def _durable_service(
    gateway: _Harness,
    state: _DurablePostExchangeState,
) -> ClickHousePublicationService:
    return ClickHousePublicationService(
        gateway=CallbackSemanticRefreshClickHouseGateway(
            prepare_callback=gateway.prepare,
            revalidate_callback=gateway.revalidate,
            inspect_uuid_callback=gateway.inspect_uuid,
            exchange_callback=gateway.exchange,
            inspect_target_authority_callback=gateway.inspect_target_authority,
            cleanup_callback=gateway.cleanup,
        ),
        state=AuthorityBoundSemanticRefreshPublicationState(
            authority=_authority(),
            delegate=CallbackSemanticRefreshPublicationState(
                persist_prepared_callback=state.transition,
                mark_committing_callback=state.transition,
                publish_callback=state.publish,
                record_target_committed_callback=state.record_target_committed,
                record_committed_incomplete_callback=state.record_committed_incomplete,
                record_commit_unknown_callback=state.record_commit_unknown,
                reconcile_prepared_callback=state.reconcile_prepared,
            ),
        ),
        authority=_authority(),
    )


def test_target_commit_ack_loss_recovers_in_new_service_without_exchange() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    first = _durable_service(gateway, state)
    prepared = first.prepare(_plan())
    state.fail_target_ack_once = True

    with pytest.raises(ClickHouseCommittedIncompleteError, match="TARGET_COMMITTED"):
        first.commit(_plan(), prepared=prepared, heads=_heads())

    assert state.journal_state == "TARGET_COMMITTED"
    assert gateway.exchange_calls == 1
    receipt = _durable_service(gateway, state).commit(_plan(), prepared=prepared, heads=_heads())
    assert receipt.status == "COMPLETE"
    assert state.journal_state == "COMPLETE"
    assert gateway.exchange_calls == 1


def test_terminal_failure_persists_incomplete_and_new_service_recovers() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    first = _durable_service(gateway, state)
    prepared = first.prepare(_plan())
    state.fail_terminal_once = True

    with pytest.raises(ClickHouseCommittedIncompleteError):
        first.commit(_plan(), prepared=prepared, heads=_heads())

    assert state.journal_state == "COMMITTED_INCOMPLETE"
    receipt = _durable_service(gateway, state).commit(_plan(), prepared=prepared, heads=_heads())
    assert receipt.status == "COMPLETE"
    assert state.journal_state == "COMPLETE"
    assert gateway.exchange_calls == 1


def test_unavailable_post_exchange_uuid_evidence_persists_commit_unknown() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    service = _durable_service(gateway, state)
    prepared = service.prepare(_plan())
    gateway.raise_after_exchange_inspection_once = True

    with pytest.raises(ClickHouseUuidAmbiguityError, match="invalid or unavailable"):
        service.commit(_plan(), prepared=prepared, heads=_heads())

    assert gateway.exchange_calls == 1
    assert state.journal_state == "COMMIT_UNKNOWN"


def test_commit_unknown_reconciles_predecessor_then_retries_exchange() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    first = _durable_service(gateway, state)
    prepared = first.prepare(_plan())
    state.journal_state = "COMMIT_UNKNOWN"

    receipt = _durable_service(gateway, state).commit(_plan(), prepared=prepared, heads=_heads())

    assert receipt.status == "COMPLETE"
    assert gateway.exchange_calls == 1
    assert state.journal_state == "COMPLETE"


def test_committing_crash_before_exchange_reconciles_then_retries_once() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    first = _durable_service(gateway, state)
    prepared = first.prepare(_plan())
    state.journal_state = "COMMITTING"

    receipt = _durable_service(gateway, state).commit(_plan(), prepared=prepared, heads=_heads())

    assert receipt.status == "COMPLETE"
    assert gateway.exchange_calls == 1
    assert state.reconciliation_states == ["COMMIT_UNKNOWN", "PREPARED"]
    assert state.journal_state == "COMPLETE"


def test_commit_unknown_reconciles_successor_without_second_exchange() -> None:
    gateway = _Harness()
    state = _DurablePostExchangeState()
    service = _durable_service(gateway, state)
    prepared = service.prepare(_plan())
    state.journal_state = "COMMIT_UNKNOWN"
    gateway.uuid_map = {
        "target_uuid": "00000000-0000-0000-0000-000000000003",
        "shadow_uuid": "00000000-0000-0000-0000-000000000001",
    }

    receipt = _durable_service(gateway, state).commit(_plan(), prepared=prepared, heads=_heads())

    assert receipt.status == "COMPLETE"
    assert gateway.exchange_calls == 0
    assert state.journal_state == "COMPLETE"


def test_transport_only_columns_cannot_enter_business_conformance() -> None:
    plan = _plan()

    with pytest.raises(ValueError, match="transport-only"):
        ClickHousePreparePlan(
            **{
                **plan.to_mapping(),
                "business_columns": ("event_id", "__dpone_seal_chunk_sha256"),
                "shadow_equation": plan.shadow_equation,
                "conformance": plan.conformance,
            }
        )


def test_prepare_rejects_wrong_target_before_gateway_side_effect() -> None:
    harness = _Harness()

    with pytest.raises(ClickHousePublicationAuthorityError, match="target authority"):
        _service(harness).prepare(
            replace(
                _plan(),
                target_table="other_orders",
                target_authority_id=f"clickhouse://{_CLUSTER}/mart/other_orders",
            )
        )

    assert harness.exchange_calls == 0
    assert harness.state_calls == []


@pytest.mark.parametrize(
    ("field_name", "wrong_name"),
    [
        ("staging_table", "unrelated_safe_table"),
        ("shadow_table", "another_safe_table"),
    ],
)
def test_prepare_rejects_noncanonical_mutation_table_before_gateway_side_effect(
    field_name: str,
    wrong_name: str,
) -> None:
    harness = _Harness()

    with pytest.raises(ClickHousePublicationAuthorityError, match="target authority"):
        _service(harness).prepare(replace(_plan(), **{field_name: wrong_name}))

    assert harness.prepare_calls == 0
    assert harness.transition_calls == []


def test_commit_rejects_wrong_scope_before_exchange() -> None:
    harness = _Harness()
    service = _service(harness)
    prepared = service.prepare(_plan())

    with pytest.raises(ClickHousePublicationAuthorityError, match="scope/head authority"):
        service.commit(
            _plan(),
            prepared=prepared,
            heads=replace(_heads(), scope_id="2026-08-09T00:00:00Z/2026-08-10T00:00:00Z"),
        )

    assert harness.exchange_calls == 0


@pytest.mark.parametrize(
    ("field_name", "wrong_value"),
    [
        ("target_generation_id", _digest("f")),
        ("checkpoint_sha256", _digest("f")),
    ],
)
def test_commit_rejects_caller_selected_successor_before_exchange(
    field_name: str,
    wrong_value: str,
) -> None:
    harness = _Harness()
    service = _service(harness)
    prepared = service.prepare(_plan())

    with pytest.raises(ClickHousePublicationAuthorityError, match="scope/head authority"):
        service.commit(
            _plan(),
            prepared=prepared,
            heads=replace(_heads(), **{field_name: wrong_value}),
        )

    assert harness.exchange_calls == 0
    assert harness.state_calls == []


def test_head_factory_derives_exact_successor_from_protected_shadow_uuid() -> None:
    prepared = _service(_Harness()).prepare(_plan())
    authority = _authority().records[0]

    assert ClickHouseHeadPublicationPlanFactory.build(authority, prepared) == _heads()


def test_durable_prepared_documents_round_trip_between_distinct_tasks() -> None:
    plan = _plan()
    prepared = _service(_Harness()).prepare(plan)
    documents = prepared_publication_documents(plan, prepared)

    loaded = load_prepared_publication(
        DurableClickHousePreparedPublication(
            workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
            operation_id=plan.operation_id,
            **documents,
        )
    )

    assert loaded.plan == plan
    assert loaded.receipt == prepared


def test_durable_prepared_loader_rejects_plan_json_under_old_digest() -> None:
    plan = _plan()
    prepared = _service(_Harness()).prepare(plan)
    documents = prepared_publication_documents(plan, prepared)
    tampered = json.loads(str(documents["prepare_plan_json"]))
    tampered["shadow_table"] = "orders__dpone_shadow_swapped"
    documents["prepare_plan_json"] = json.dumps(
        tampered,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )

    with pytest.raises(ValueError, match="identity differs"):
        load_prepared_publication(
            DurableClickHousePreparedPublication(
                workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
                operation_id=plan.operation_id,
                **documents,
            )
        )
