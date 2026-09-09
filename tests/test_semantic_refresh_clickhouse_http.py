"""Concrete HTTP ClickHouse gateway and durable-state composition tests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from dpone.adapters import semantic_refresh_clickhouse_http_queries as queries
from dpone.adapters.semantic_refresh_artifact_memory import (
    InMemoryCreateOnlyArtifactStore,
    StaticCreateOnlyArtifactStoreResolver,
    StaticSemanticRefreshArtifactAuthority,
    StaticSemanticRefreshSealAuthorization,
)
from dpone.adapters.semantic_refresh_artifact_reader import (
    AuthorityBoundVersionPinnedSealedArtifactReader,
    SealedArtifactReadError,
    VersionPinnedSealedArtifactReader,
)
from dpone.adapters.semantic_refresh_clickhouse_authority import (
    MssqlProtectedSemanticRefreshClickHouseAuthority,
    StaticSemanticRefreshClickHousePublicationAuthority,
)
from dpone.adapters.semantic_refresh_clickhouse_callbacks import (
    CallbackSemanticRefreshPublicationState,
)
from dpone.adapters.semantic_refresh_clickhouse_cleanup import (
    ClickHouseFailedPrecommitScratchCleaner,
)
from dpone.adapters.semantic_refresh_clickhouse_connection import (
    ClickHouseConnectionAuthorityVerifier,
)
from dpone.adapters.semantic_refresh_clickhouse_http import (
    ClickHouseHttpGatewayError,
    ClickHouseHttpSemanticRefreshGateway,
)
from dpone.adapters.semantic_refresh_mssql_publication import (
    MssqlSemanticRefreshPublicationState,
    SemanticRefreshMssqlPublicationError,
)
from dpone.adapters.semantic_refresh_mssql_publication_authority import (
    PUBLICATION_AUTHORITY_FIELDS,
    MssqlPublicationCanonicalAuthority,
)
from dpone.adapters.semantic_refresh_mssql_publication_heads import (
    MssqlPublicationHeadConflict,
    MssqlPublicationHeadStore,
)
from dpone.adapters.semantic_refresh_mssql_publication_prepared import (
    MssqlPreparedPublicationConflict,
    MssqlPreparedPublicationStore,
)
from dpone.adapters.semantic_refresh_mssql_publication_validation import (
    validate_publication_request,
)
from dpone.contracts.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationReceipt,
)
from dpone.ports.semantic_refresh_artifact_seal import (
    ArtifactChunk,
    ArtifactSealPlan,
    SealedArtifactReceipt,
)
from dpone.ports.semantic_refresh_artifact_store import (
    artifact_store_binding,
    operation_artifact_prefix,
)
from dpone.ports.semantic_refresh_clickhouse_authority import (
    ClickHousePublicationAuthority,
    clickhouse_authority_rows_sha256,
    clickhouse_physical_authority_rows,
)
from dpone.ports.semantic_refresh_clickhouse_connection import (
    ClickHouseClusterConnectionAuthority,
    clickhouse_cluster_topology_sha256,
)
from dpone.ports.semantic_refresh_clickhouse_prepared import (
    DurableClickHousePreparedPublication,
)
from dpone.ports.semantic_refresh_mssql import (
    MssqlPrerequisiteAuthorityClaim,
    MssqlRuntimeAssuranceClaim,
)
from dpone.ports.semantic_refresh_mssql_after_image import (
    MssqlCommittedAfterImageSnapshot,
)
from dpone.ports.semantic_refresh_mssql_authority import (
    MssqlCanonicalAuthorityRecord,
    MssqlProtectedArtifactAuthority,
    MssqlProtectedOperationAuthority,
    MssqlProtectedResourcePolicy,
    MssqlProtectedWritableColumn,
)
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlFailedScratchCleanupAuthority,
    MssqlScratchCleanupRelation,
)
from dpone.ports.semantic_refresh_seal_policy import (
    semantic_refresh_artifact_authority_sha256,
)
from dpone.runtime.semantic_refresh_clickhouse_authority_state import (
    AuthorityBoundSemanticRefreshPublicationState,
)
from dpone.runtime.semantic_refresh_clickhouse_conformance import (
    ClickHouseConformanceError,
    ClickHousePrepareEvidence,
    ClickHousePrepareSqlBuilder,
    assert_prepare_evidence,
)
from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHousePreparePlan,
    GroupedMultisetConformance,
    ShadowEquation,
    semantic_refresh_fingerprint,
)
from dpone.runtime.semantic_refresh_clickhouse_plan_factory import (
    ClickHouseHeadPublicationPlanFactory,
    ClickHousePlanFactoryError,
    ProtectedClickHousePreparePlanFactory,
)
from dpone.runtime.semantic_refresh_clickhouse_prepared import (
    AuthenticatedClickHousePreparedPublicationLoader,
    ClickHousePreparedPublicationLoadError,
)
from dpone.runtime.semantic_refresh_clickhouse_prepared_codec import (
    prepared_publication_documents,
)
from dpone.runtime.semantic_refresh_clickhouse_service import (
    ClickHousePublicationService,
)
from dpone.runtime.semantic_refresh_parquet_codec import (
    DponeParquetV1Codec,
    SemanticRefreshParquetField,
)
from dpone.services.semantic_refresh_artifact import SealedArtifactService
from dpone.services.semantic_refresh_artifact_mssql import (
    MssqlCommittedAfterImageArtifactPublisher,
    MssqlCommittedAfterImageSealError,
)


def _digest(char: str) -> str:
    return "sha256:" + char * 64


_CLUSTER = "dpone-semref-primary"
_ENDPOINT = "https://clickhouse.example:8443/"


def _seal_authorization(*, after_image_row_count: int = 2) -> SemanticRefreshSealAuthorizationReceipt:
    return SemanticRefreshSealAuthorizationReceipt.build(
        operation_id=_digest("0"),
        operation_plan_sha256=_digest("a"),
        model_unique_id="model.orders",
        workflow_id=_digest("1"),
        workflow_plan_sha256=_digest("b"),
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("c"),
        attempt_binding_sha256=_digest("d"),
        fencing_epoch=3,
        journal_version=11,
        event_time_source_type="datetime2(6)",
        effective_key_template_sha256=_digest("5"),
        effective_key_mapping_sha256=_digest("7"),
        ordered_writable_schema_sha256=_digest("4"),
        serializer_sha256=_digest("6"),
        parquet_schema_mapping_sha256=_digest("5"),
        clickhouse_input_mapping_sha256=_digest("4"),
        codec_mapping_certification_sha256=_digest("3"),
        before_image_relation_id="analytics.dpone_images.orders_before",
        before_image_sha256=_digest("4"),
        before_image_row_count=1,
        after_image_relation_id="analytics.dpone_images.orders_after",
        after_image_sha256=_digest("5"),
        after_image_row_count=after_image_row_count,
        model_build_receipt_sha256=_digest("5"),
        baseline_adoption_receipt_sha256=_digest("6"),
        route_certification_receipt_sha256=_digest("8"),
        writer_exclusivity_assurance_receipt_sha256=_digest("1"),
        utc_semantics_assurance_receipt_sha256=_digest("2"),
        ddl_freeze_assurance_receipt_sha256=_digest("3"),
        artifact_authority_sha256=_digest("4"),
        seal_policy_sha256=_digest("5"),
        created_at="2026-08-08T12:35:00Z",
        issuer_authority="mssql-protected-control/test",
        issuer_attestation_sha256=_digest("6"),
        issuer_signature_sha256=_digest("7"),
    )


_ARTIFACT_PLAN = ArtifactSealPlan(
    seal_authorization=_seal_authorization(),
    artifact_prefix=f"operations/{_digest('0')}",
    provider="s3",
    encryption_policy_sha256=_digest("9"),
    retention_policy_sha256=_digest("f"),
    encryption_scope="test",
    retention_until="2026-09-08T00:00:00Z",
    chunks=(ArtifactChunk(1, b"PAR1rowsPAR1", 2),),
)
_ARTIFACT_STORE = InMemoryCreateOnlyArtifactStore()
_ARTIFACT_RECEIPT = SealedArtifactService(
    store=_ARTIFACT_STORE,
    authority=StaticSemanticRefreshArtifactAuthority(_ARTIFACT_PLAN.authority_inventory()),
).seal(_ARTIFACT_PLAN)
_ARTIFACT_READER = VersionPinnedSealedArtifactReader(_ARTIFACT_STORE)


_SCHEMA_ROWS = (
    ("event_id", "Int64", 1, "", "", ""),
    ("occurred_at", "DateTime64(6, 'UTC')", 2, "", "", ""),
    ("amount", "Decimal(18, 2)", 3, "", "", ""),
)
_PHYSICAL_OBSERVATION = (
    "MergeTree",
    "",
    "event_id, occurred_at",
    "event_id, occurred_at",
    "",
    "default",
    "MergeTree ORDER BY (event_id, occurred_at) SETTINGS index_granularity = 8192",
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
)
_PHYSICAL_ROWS = clickhouse_physical_authority_rows(
    database_engine="Atomic",
    shard_count=1,
    replica_count=1,
    table_observation=_PHYSICAL_OBSERVATION,
)
_STAGING_TABLE = "orders__dpone_stage__000000000000"
_SHADOW_TABLE = "orders__dpone_shadow__000000000000"


def _rows_digest(rows: tuple[tuple[object, ...], ...]) -> str:
    raw = json.dumps(rows, allow_nan=False, ensure_ascii=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _plan() -> ClickHousePreparePlan:
    return ClickHousePreparePlan(
        operation_id=_digest("0"),
        operation_plan_sha256=_digest("a"),
        workflow_plan_sha256=_digest("b"),
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("c"),
        attempt_binding_sha256=_digest("d"),
        fence_epoch=3,
        artifact_manifest_key=_ARTIFACT_RECEIPT.manifest.key,
        artifact_manifest_version=_ARTIFACT_RECEIPT.manifest.version,
        artifact_manifest_sha256=_ARTIFACT_RECEIPT.sealed_manifest.artifact_manifest_sha256,
        target_resource_id="clickhouse-target://analytics/orders",
        target_authority_id=f"clickhouse://{_CLUSTER}/analytics/orders",
        clickhouse_cluster_authority_id=_CLUSTER,
        database="analytics",
        target_table="orders",
        scope_id="2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
        scope_start="2026-08-08T00:00:00Z",
        scope_end="2026-08-09T00:00:00Z",
        scope_revision=1,
        event_time_column="occurred_at",
        staging_table=_STAGING_TABLE,
        shadow_table=_SHADOW_TABLE,
        expected_target_uuid="00000000-0000-0000-0000-000000000001",
        expected_schema_sha256=_rows_digest(_SCHEMA_ROWS),
        expected_physical_sha256=_rows_digest(_PHYSICAL_ROWS),
        business_columns=("event_id", "occurred_at", "amount"),
        effective_key_columns=("event_id", "occurred_at"),
        max_staging_rows=10,
        max_target_scope_rows=10,
        max_staging_bytes=1_000,
        max_shadow_bytes=2_000,
        max_retained_backup_bytes=1_000,
        max_total_transient_bytes=4_000,
        shadow_equation=ShadowEquation(
            retained_target_rule="TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY",
            append_rule="APPEND_ALL_STAGING_ROWS",
        ),
        conformance=GroupedMultisetConformance(mode="BIDIRECTIONAL_GROUPED_MULTISET"),
        artifact_chunk_count=1,
        artifact_total_rows=2,
    )


class _HttpClient:
    def __init__(self, *, endpoint_authority_id: str = _ENDPOINT) -> None:
        self.statements: list[str] = []
        self.endpoint_authority_id = endpoint_authority_id
        self.topology_rows: list[tuple[object, ...]] = [("clickhouse-01", 9000, 1, 1)]
        self.schema_rows = list(_SCHEMA_ROWS)
        self.physical_rows = [_PHYSICAL_OBSERVATION]
        self.shard_count = 1
        self.replica_count = 1
        self.target_scope_rows = 0
        self.target_bytes = 300
        self.staging_bytes = 200
        self.shadow_bytes = 400
        self.prior_retained_backup_bytes = 0
        self.relations = {"orders"}
        self.comments: dict[str, str] = {}
        self.target_uuid = "00000000-0000-0000-0000-000000000001"
        self.staging_uuid = "00000000-0000-0000-0000-000000000002"
        self.shadow_uuid = "00000000-0000-0000-0000-000000000003"

    def execute(self, statement: str) -> list[tuple[object, ...]]:
        self.statements.append(statement)
        if not statement.lstrip().startswith("SELECT"):
            if statement.startswith("EXCHANGE TABLES"):
                self.target_uuid, self.shadow_uuid = self.shadow_uuid, self.target_uuid
                target_comment = self.comments.pop("orders", None)
                shadow_comment = self.comments.pop(_SHADOW_TABLE, None)
                if shadow_comment is not None:
                    self.comments["orders"] = shadow_comment
                if target_comment is not None:
                    self.comments[_SHADOW_TABLE] = target_comment
            for relation in (_STAGING_TABLE, _SHADOW_TABLE):
                if statement.startswith(f"CREATE TABLE `analytics`.`{relation}`"):
                    self.relations.add(relation)
                    if " COMMENT " in statement:
                        self.comments[relation] = f"dpone-semantic-refresh:{_digest('a')}"
                elif statement.startswith(f"DROP TABLE `analytics`.`{relation}`"):
                    self.relations.discard(relation)
                    self.comments.pop(relation, None)
                elif statement.startswith(f"ALTER TABLE `analytics`.`{relation}` MODIFY COMMENT"):
                    self.comments[relation] = f"dpone-semantic-refresh:{_digest('a')}"
            return []
        if "FROM system.columns" in statement:
            return self.schema_rows
        if "FROM system.clusters" in statement:
            return self.topology_rows
        if "engine, partition_key" in statement:
            return self.physical_rows
        if "uniqExact(hostName())" in statement:
            return [(self.shard_count,)]
        if "FROM system.replicas" in statement:
            return [(self.replica_count,)]
        if "toString(uuid)" in statement:
            if _STAGING_TABLE in statement:
                if _STAGING_TABLE not in self.relations:
                    return []
                return [(self.staging_uuid,)]
            if _SHADOW_TABLE in statement:
                if _SHADOW_TABLE not in self.relations:
                    return []
                return [(self.shadow_uuid,)]
            return [(self.target_uuid,)]
        if "system.databases" in statement:
            return [("Atomic",)]
        if "SELECT engine FROM system.tables" in statement:
            return [("MergeTree",)]
        if "SELECT comment FROM system.tables" in statement:
            relation = (
                _STAGING_TABLE
                if _STAGING_TABLE in statement
                else (_SHADOW_TABLE if _SHADOW_TABLE in statement else "orders")
            )
            return [(self.comments[relation],)] if relation in self.comments else []
        if "sum(total_bytes)" in statement:
            if "startsWith(name" in statement:
                return [(self.prior_retained_backup_bytes,)]
            if f"name = '{_STAGING_TABLE}'" in statement:
                return [(self.staging_bytes if _STAGING_TABLE in self.relations else 0,)]
            if f"name = '{_SHADOW_TABLE}'" in statement:
                return [(self.shadow_bytes if _SHADOW_TABLE in self.relations else 0,)]
            return [(self.target_bytes,)]
        if "HAVING count() > 1" in statement or " IS NULL" in statement:
            return [(0,)]
        if "EXCEPT DISTINCT" in statement:
            return [(0,)]
        if "LEFT ANTI JOIN" in statement:
            return [(4,)]
        if "`occurred_at` >=" in statement:
            return [(self.target_scope_rows,)]
        if _SHADOW_TABLE in statement:
            return [(0 if len(self.statements) < 3 else 4,)]
        if _STAGING_TABLE in statement:
            return [(2,)]
        return [(3,)]

    def execute_operation(self, statement: str, *, query_id: str) -> list[tuple[object, ...]]:
        assert query_id.startswith("dpone-semref-")
        return self.execute(statement)

    def insert_parquet(self, statement: str, content: bytes) -> list[tuple[object, ...]]:
        self.statements.append(statement)
        assert content == b"PAR1rowsPAR1"
        return []

    def insert_parquet_operation(
        self,
        statement: str,
        content: bytes,
        *,
        query_id: str,
    ) -> list[tuple[object, ...]]:
        assert query_id.startswith("dpone-semref-")
        return self.insert_parquet(statement, content)


class _CreateAcknowledgementLostHttpClient(_HttpClient):
    def __init__(self) -> None:
        super().__init__()
        self._fail_staging_create_once = True

    def execute(self, statement: str) -> list[tuple[object, ...]]:
        result = super().execute(statement)
        if self._fail_staging_create_once and statement.startswith(f"CREATE TABLE `analytics`.`{_STAGING_TABLE}`"):
            self._fail_staging_create_once = False
            raise TimeoutError("CREATE acknowledgement lost")
        return result


class _DropAcknowledgementLostHttpClient(_HttpClient):
    def __init__(self, relation: str) -> None:
        super().__init__()
        self._relation = relation
        self._fail_once = True

    def execute(self, statement: str) -> list[tuple[object, ...]]:
        result = super().execute(statement)
        if self._fail_once and statement.startswith(f"DROP TABLE `analytics`.`{self._relation}`"):
            self._fail_once = False
            raise TimeoutError("DROP acknowledgement lost")
        return result


def _gateway(
    client: Any,
    *,
    authority: StaticSemanticRefreshClickHousePublicationAuthority | None = None,
) -> ClickHouseHttpSemanticRefreshGateway:
    return ClickHouseHttpSemanticRefreshGateway(
        client=client,
        authority=authority or _publication_authority(),
        artifact_reader=_ARTIFACT_READER,
        seal_authorization=StaticSemanticRefreshSealAuthorization((_ARTIFACT_PLAN.seal_authorization,)),
        connection_authority=_connection_authority(),
    )


def _connection_authority() -> ClickHouseClusterConnectionAuthority:
    topology = (("clickhouse-01", 9000, 1, 1),)
    return ClickHouseClusterConnectionAuthority(
        clickhouse_cluster_authority_id=_CLUSTER,
        endpoint_authority_id=_ENDPOINT,
        cluster_name="semantic_refresh_production",
        host_names=("clickhouse-01",),
        topology_sha256=clickhouse_cluster_topology_sha256(topology),
    )


def _publication_authority() -> StaticSemanticRefreshClickHousePublicationAuthority:
    plan = _plan()
    return StaticSemanticRefreshClickHousePublicationAuthority(
        records=(
            ClickHousePublicationAuthority(
                authority_sha256=_digest("2"),
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
                target_resource_id="clickhouse-target://analytics/orders",
                target_authority_id=f"clickhouse://{_CLUSTER}/analytics/orders",
                clickhouse_cluster_authority_id=_CLUSTER,
                database=plan.database,
                target_table=plan.target_table,
                scope_id="2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
                scope_start=plan.scope_start,
                scope_end=plan.scope_end,
                scope_revision=1,
                target_predecessor_generation_id=_digest("3"),
                scope_predecessor_operation_id=None,
                predecessor_target_generation=7,
                predecessor_target_uuid=plan.expected_target_uuid,
                predecessor_target_operation_id=_digest("e"),
                predecessor_scope_revision=None,
                predecessor_checkpoint_sha256=None,
                predecessor_checkpoint_operation_id=None,
                predecessor_checkpoint_version=None,
                expected_target_uuid=plan.expected_target_uuid,
                expected_schema_sha256=plan.expected_schema_sha256,
                expected_physical_sha256=plan.expected_physical_sha256,
                business_columns=plan.business_columns,
                effective_key_columns=plan.effective_key_columns,
                event_time_column=plan.event_time_column,
                effective_key_mapping_sha256=_ARTIFACT_PLAN.effective_key_mapping_sha256,
                route_certification_receipt_sha256=_ARTIFACT_PLAN.route_certification_receipt_sha256,
                artifact_prefix=_ARTIFACT_PLAN.artifact_prefix,
                artifact_provider=_ARTIFACT_PLAN.provider,
                encryption_policy_sha256=_ARTIFACT_PLAN.encryption_policy_sha256,
                retention_policy_sha256=_ARTIFACT_PLAN.retention_policy_sha256,
                max_artifact_bytes=1_000,
                max_staging_rows=plan.max_staging_rows,
                max_target_scope_rows=plan.max_target_scope_rows,
                max_staging_bytes=plan.max_staging_bytes,
                max_shadow_bytes=plan.max_shadow_bytes,
                max_retained_backup_bytes=plan.max_retained_backup_bytes,
                max_total_transient_bytes=plan.max_total_transient_bytes,
            ),
        )
    )


def test_http_gateway_executes_prepare_observations_and_exchange() -> None:
    client = _HttpClient()
    gateway = _gateway(client)
    plan = _plan()
    sql = ClickHousePrepareSqlBuilder().build(plan).to_mapping()

    evidence = gateway.prepare(plan.to_mapping(), sql)

    assert evidence == {
        "target_uuid": plan.expected_target_uuid,
        "staging_uuid": "00000000-0000-0000-0000-000000000002",
        "shadow_uuid": "00000000-0000-0000-0000-000000000003",
        "staging_rows": 2,
        "target_scope_rows": 0,
        "shadow_rows": 4,
        "desired_rows": 4,
        "staging_null_key_rows": 0,
        "staging_duplicate_key_groups": 0,
        "target_null_key_rows": 0,
        "target_duplicate_key_groups": 0,
        "forward_difference_groups": 0,
        "reverse_difference_groups": 0,
        "schema_sha256": _rows_digest(_SCHEMA_ROWS),
        "physical_sha256": _rows_digest(_PHYSICAL_ROWS),
        "staging_bytes": 200,
        "shadow_bytes": 400,
        "retained_backup_bytes": 300,
        "total_transient_bytes": 900,
        "guard_operation_id": _digest("0"),
        "guard_attempt_binding_sha256": _digest("d"),
        "guard_fence_epoch": 3,
        "database_engine": "Atomic",
        "table_engine": "MergeTree",
        "shard_count": 1,
        "replica_count": 1,
    }
    assert sql["populate_shadow_retained"] in client.statements
    assert sql["append_staging"] in client.statements

    gateway.exchange(
        {
            "workflow_execution_id": plan.workflow_execution_id,
            "operation_id": _digest("0"),
            "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
            "attempt_binding_sha256": _digest("d"),
            "fence_epoch": 3,
            "clickhouse_cluster_authority_id": _CLUSTER,
            "database": "analytics",
            "target_table": "orders",
            "shadow_table": _SHADOW_TABLE,
            "expected_target_uuid": plan.expected_target_uuid,
            "expected_shadow_uuid": "00000000-0000-0000-0000-000000000003",
            "prepare_receipt_sha256": _digest("9"),
        }
    )
    assert client.statements[-1] == f"EXCHANGE TABLES `analytics`.`orders` AND `analytics`.`{_SHADOW_TABLE}`"


@pytest.mark.parametrize("field_name", ["staging_table", "shadow_table"])
def test_http_gateway_rejects_noncanonical_mutation_table_before_command(field_name: str) -> None:
    client = _HttpClient()
    gateway = _gateway(client)
    plan = replace(_plan(), **{field_name: "unrelated_safe_table"})

    with pytest.raises(ClickHouseHttpGatewayError, match="authority differs"):
        gateway.prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())

    assert client.statements == []


def test_http_gateway_rejects_target_scope_budget_before_temporary_mutation() -> None:
    client = _HttpClient()
    client.target_scope_rows = 11

    with pytest.raises(ClickHouseHttpGatewayError, match="target scope row budget"):
        _gateway(client).prepare(_plan().to_mapping(), ClickHousePrepareSqlBuilder().build(_plan()).to_mapping())

    assert not any(statement.startswith(("CREATE ", "DROP ", "INSERT ")) for statement in client.statements)


def test_http_gateway_rejects_artifact_rows_before_temporary_mutation() -> None:
    client = _HttpClient()
    plan = replace(_plan(), max_staging_rows=1)
    authority = StaticSemanticRefreshClickHousePublicationAuthority(
        records=(replace(_publication_authority().records[0], max_staging_rows=1),)
    )

    with pytest.raises(ClickHouseHttpGatewayError, match="artifact row count exceeds staging row budget"):
        _gateway(client, authority=authority).prepare(
            plan.to_mapping(),
            ClickHousePrepareSqlBuilder().build(plan).to_mapping(),
        )

    assert not any(statement.startswith(("CREATE ", "DROP ", "INSERT ")) for statement in client.statements)


def test_http_gateway_rejects_protected_peak_before_temporary_mutation() -> None:
    client = _HttpClient()
    plan = replace(_plan(), max_total_transient_bytes=3_000)
    authority = StaticSemanticRefreshClickHousePublicationAuthority(
        records=(replace(_publication_authority().records[0], max_total_transient_bytes=3_000),)
    )

    with pytest.raises(ClickHouseHttpGatewayError, match="protected staging/shadow peak"):
        _gateway(client, authority=authority).prepare(
            plan.to_mapping(),
            ClickHousePrepareSqlBuilder().build(plan).to_mapping(),
        )

    assert not any(statement.startswith(("CREATE ", "DROP ", "INSERT ")) for statement in client.statements)


def test_http_gateway_rejects_known_target_larger_than_shadow_before_mutation() -> None:
    client = _HttpClient()
    client.target_bytes = _plan().max_shadow_bytes + 1

    with pytest.raises(ClickHouseHttpGatewayError, match="target bytes exceed shadow byte budget"):
        _gateway(client).prepare(_plan().to_mapping(), ClickHousePrepareSqlBuilder().build(_plan()).to_mapping())

    assert not any(statement.startswith(("CREATE ", "DROP ", "INSERT ")) for statement in client.statements)


def test_http_gateway_stops_after_chunk_exceeds_staging_bytes() -> None:
    client = _HttpClient()
    client.staging_bytes = _plan().max_staging_bytes + 1
    plan = _plan()
    sql = ClickHousePrepareSqlBuilder().build(plan).to_mapping()

    with pytest.raises(ClickHouseHttpGatewayError, match="staging byte budget exceeded after sealed artifact chunk"):
        _gateway(client).prepare(plan.to_mapping(), sql)

    assert any(statement.startswith("INSERT INTO") for statement in client.statements)
    assert not any(
        _SHADOW_TABLE in statement and statement.startswith("CREATE TABLE") for statement in client.statements
    )


def test_http_gateway_stops_before_append_when_retained_shadow_exceeds_bytes() -> None:
    client = _HttpClient()
    client.shadow_bytes = _plan().max_shadow_bytes + 1
    plan = _plan()
    sql = ClickHousePrepareSqlBuilder().build(plan).to_mapping()

    with pytest.raises(ClickHouseHttpGatewayError, match="shadow byte budget exceeded after retained-target"):
        _gateway(client).prepare(plan.to_mapping(), sql)

    assert sql["populate_shadow_retained"] in client.statements
    assert sql["append_staging"] not in client.statements


def test_http_gateway_rejects_wrong_protected_endpoint_before_temporary_mutation() -> None:
    client = _HttpClient(endpoint_authority_id="https://disaster-recovery.example:8443/")

    with pytest.raises(ClickHouseHttpGatewayError, match="endpoint authority"):
        _gateway(client).prepare(_plan().to_mapping(), ClickHousePrepareSqlBuilder().build(_plan()).to_mapping())

    assert not any(statement.startswith(("CREATE ", "DROP ", "INSERT ")) for statement in client.statements)


def test_http_gateway_rejects_multi_node_topology_before_temporary_mutation() -> None:
    client = _HttpClient()
    client.topology_rows.append(("clickhouse-02", 9000, 2, 1))

    with pytest.raises(ClickHouseHttpGatewayError, match="topology authority"):
        _gateway(client).prepare(_plan().to_mapping(), ClickHousePrepareSqlBuilder().build(_plan()).to_mapping())

    assert not any(statement.startswith(("CREATE ", "DROP ", "INSERT ")) for statement in client.statements)


def test_http_gateway_never_quarantines_or_drops_unowned_existing_temporary_relation() -> None:
    client = _HttpClient()
    client.relations.add(_STAGING_TABLE)
    client.comments[_STAGING_TABLE] = "another-owner"

    with pytest.raises(ClickHouseHttpGatewayError, match="not owned"):
        _gateway(client).prepare(_plan().to_mapping(), ClickHousePrepareSqlBuilder().build(_plan()).to_mapping())

    assert not any(statement.startswith(("DROP ", "RENAME ")) for statement in client.statements)


def test_http_gateway_rebuilds_only_exact_owned_stale_relation_without_quarantine() -> None:
    client = _HttpClient()
    client.relations.add(_STAGING_TABLE)
    client.comments[_STAGING_TABLE] = f"dpone-semantic-refresh:{_digest('a')}"

    _gateway(client).prepare(_plan().to_mapping(), ClickHousePrepareSqlBuilder().build(_plan()).to_mapping())

    assert f"DROP TABLE `analytics`.`{_STAGING_TABLE}`" in client.statements
    assert not any(statement.startswith("RENAME TABLE") for statement in client.statements)


def test_http_gateway_recovers_create_acknowledgement_loss_from_atomic_owner_comment() -> None:
    client = _CreateAcknowledgementLostHttpClient()
    gateway = _gateway(client)
    plan = _plan()
    sql = ClickHousePrepareSqlBuilder().build(plan).to_mapping()

    with pytest.raises(ClickHouseHttpGatewayError, match="command outcome"):
        gateway.prepare(plan.to_mapping(), sql)

    assert _STAGING_TABLE in client.relations
    assert client.comments[_STAGING_TABLE] == f"dpone-semantic-refresh:{plan.operation_plan_sha256}"

    gateway.prepare(plan.to_mapping(), sql)

    assert f"DROP TABLE `analytics`.`{_STAGING_TABLE}`" in client.statements
    assert not any("MODIFY COMMENT" in statement for statement in client.statements)


def test_http_gateway_counts_prior_retained_backups_before_next_operation() -> None:
    client = _HttpClient()
    client.prior_retained_backup_bytes = _plan().max_retained_backup_bytes

    with pytest.raises(ClickHouseHttpGatewayError, match="retained backup byte budget"):
        _gateway(client).prepare(_plan().to_mapping(), ClickHousePrepareSqlBuilder().build(_plan()).to_mapping())

    assert not any(statement.startswith(("CREATE ", "DROP ", "INSERT ")) for statement in client.statements)


def test_prior_scratch_inventory_query_counts_staging_and_shadow_prefixes() -> None:
    client = _HttpClient()
    client.prior_retained_backup_bytes = _plan().max_retained_backup_bytes

    with pytest.raises(ClickHouseHttpGatewayError, match="retained backup byte budget"):
        _gateway(client).prepare(_plan().to_mapping(), ClickHousePrepareSqlBuilder().build(_plan()).to_mapping())

    query = next(statement for statement in client.statements if "startsWith(name" in statement)
    assert "orders__dpone_shadow__" in query
    assert "orders__dpone_stage__" in query
    assert _SHADOW_TABLE in query
    assert _STAGING_TABLE in query


def test_prepare_plan_factory_consumes_only_protected_sealed_authority() -> None:
    plan = ProtectedClickHousePreparePlanFactory(
        authority=_publication_authority(),
        seal_authorization=StaticSemanticRefreshSealAuthorization((_seal_authorization(),)),
    ).build(
        workflow_execution_binding_sha256=_digest("c"),
        operation_id=_digest("0"),
        sealed_artifact=_ARTIFACT_RECEIPT,
    )

    assert plan.artifact_manifest_version == _ARTIFACT_RECEIPT.manifest.version
    assert plan.staging_table == "orders__dpone_stage__000000000000"
    assert plan.shadow_table == "orders__dpone_shadow__000000000000"
    assert plan.target_authority_id == f"clickhouse://{_CLUSTER}/analytics/orders"


def test_prepare_plan_factory_rejects_tampered_version_pinned_receipt() -> None:
    factory = ProtectedClickHousePreparePlanFactory(
        authority=_publication_authority(),
        seal_authorization=StaticSemanticRefreshSealAuthorization((_seal_authorization(),)),
    )
    tampered = replace(
        _ARTIFACT_RECEIPT,
        manifest=replace(_ARTIFACT_RECEIPT.manifest, sha256=_digest("f")),
    )

    with pytest.raises(ClickHousePlanFactoryError, match="receipt is invalid"):
        factory.build(
            workflow_execution_binding_sha256=_digest("c"),
            operation_id=_digest("0"),
            sealed_artifact=tampered,
        )


def _empty_artifact() -> tuple[
    InMemoryCreateOnlyArtifactStore,
    SemanticRefreshSealAuthorizationReceipt,
    SealedArtifactReceipt,
]:
    seal = _seal_authorization(after_image_row_count=0)
    plan = ArtifactSealPlan(
        seal_authorization=seal,
        artifact_prefix=_ARTIFACT_PLAN.artifact_prefix,
        provider="s3",
        encryption_policy_sha256=_ARTIFACT_PLAN.encryption_policy_sha256,
        retention_policy_sha256=_ARTIFACT_PLAN.retention_policy_sha256,
        encryption_scope="test",
        retention_until="2026-09-08T00:00:00Z",
        chunks=(),
    )
    store = InMemoryCreateOnlyArtifactStore()
    receipt = SealedArtifactService(
        store=store,
        authority=StaticSemanticRefreshArtifactAuthority(plan.authority_inventory()),
    ).seal(plan)
    return store, seal, receipt


def _empty_scope_service(
    client: _HttpClient,
    state_calls: list[dict[str, object]],
) -> tuple[ClickHousePublicationService, ClickHousePreparePlan]:
    store, seal, receipt = _empty_artifact()
    authority = _publication_authority()
    plan = ProtectedClickHousePreparePlanFactory(
        authority=authority,
        seal_authorization=StaticSemanticRefreshSealAuthorization((seal,)),
    ).build(
        workflow_execution_binding_sha256=_digest("c"),
        operation_id=_digest("0"),
        sealed_artifact=receipt,
    )

    def acknowledge(request: Any) -> dict[str, object]:
        value = dict(request)
        state_calls.append(value)
        next_state = value.get("next_journal_state", "COMPLETE")
        return {**value, "committed": True, "atomic": True, "journal_state": next_state}

    state = AuthorityBoundSemanticRefreshPublicationState(
        authority=authority,
        delegate=CallbackSemanticRefreshPublicationState(
            persist_prepared_callback=acknowledge,
            mark_committing_callback=acknowledge,
            publish_callback=acknowledge,
            publish_empty_callback=acknowledge,
        ),
    )
    gateway = ClickHouseHttpSemanticRefreshGateway(
        client=client,
        authority=authority,
        artifact_reader=VersionPinnedSealedArtifactReader(store),
        seal_authorization=StaticSemanticRefreshSealAuthorization((seal,)),
        connection_authority=_connection_authority(),
    )
    return ClickHousePublicationService(gateway=gateway, state=state, authority=authority), plan


def test_empty_scope_completes_without_temporary_tables_or_exchange() -> None:
    client = _HttpClient()
    state_calls: list[dict[str, object]] = []
    service, plan = _empty_scope_service(client, state_calls)

    prepared = service.prepare(plan)
    heads = ClickHouseHeadPublicationPlanFactory.build(_publication_authority().records[0], prepared)
    receipt = service.commit(plan, prepared=prepared, heads=heads)

    assert prepared.publication_mode == "EMPTY_SCOPE"
    assert receipt.target_mutation_outcome == "NOT_REQUIRED_EMPTY_SCOPE"
    assert receipt.value_conversion_outcome == "NOT_APPLICABLE_NO_DATA"
    assert receipt.target_generation == heads.expected_target_generation
    assert state_calls[-1]["expected_journal_state"] == "PREPARED"
    assert state_calls[-1]["target_generation_id"] == _digest("3")
    assert not any(
        statement.startswith(("CREATE ", "INSERT ", "RENAME ", "EXCHANGE ")) for statement in client.statements
    )


def test_empty_manifest_rejects_nonempty_target_scope_without_mutation() -> None:
    client = _HttpClient()
    client.target_scope_rows = 1
    state_calls: list[dict[str, object]] = []
    service, plan = _empty_scope_service(client, state_calls)

    with pytest.raises(ClickHouseConformanceError, match="diverges"):
        service.prepare(plan)

    assert state_calls == []
    assert not any(
        statement.startswith(("CREATE ", "INSERT ", "RENAME ", "EXCHANGE ")) for statement in client.statements
    )


def test_http_gateway_observes_schema_physical_and_topology_drift() -> None:
    client = _HttpClient()
    gateway = _gateway(client)
    before = gateway.inspect_relation_authority(database="analytics", table="orders")
    client.schema_rows.append(("new_column", "String", 4, "", "", ""))
    client.physical_rows = [
        (
            "MergeTree",
            "toYYYYMM(occurred_at)",
            "event_id",
            "event_id",
            "",
            "default",
            "MergeTree PARTITION BY toYYYYMM(occurred_at) ORDER BY event_id SETTINGS index_granularity = 8192",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        )
    ]

    after = gateway.inspect_relation_authority(database="analytics", table="orders")

    assert after["schema_sha256"] != before["schema_sha256"]
    assert after["physical_sha256"] != before["physical_sha256"]
    assert (after["shard_count"], after["replica_count"]) == (1, 1)


def test_physical_query_observes_incoming_materialized_view_target() -> None:
    statement = queries.physical_query("analytics", "orders")

    assert "mv.engine = 'MaterializedView'" in statement
    assert "extract(mv.create_table_query" in statement
    assert "= 'analytics.orders'" in statement
    assert "length(dependencies_table)" not in statement


def test_relation_authority_rejects_incoming_materialized_view_writer() -> None:
    client = _HttpClient()
    observation = list(_PHYSICAL_OBSERVATION)
    observation[12] = 1
    client.physical_rows = [tuple(observation)]

    with pytest.raises(
        ClickHouseHttpGatewayError,
        match="DPONE_REFRESH_CLICKHOUSE_MATERIALIZED_WRITER_UNSUPPORTED",
    ):
        _gateway(client).inspect_relation_authority(database="analytics", table="orders")


@pytest.mark.parametrize(
    ("field_index", "feature"),
    [
        (7, "ttl"),
        (8, "codec"),
        (9, "projection"),
        (10, "index declaration"),
        (11, "constraint"),
        (13, "skipping index"),
        (14, "pending mutation"),
    ],
)
def test_prepare_rejects_every_forbidden_physical_feature(field_index: int, feature: str) -> None:
    client = _HttpClient()
    observation = list(_PHYSICAL_OBSERVATION)
    observation[field_index] = 1
    client.physical_rows = [tuple(observation)]
    plan = _plan()
    raw = _gateway(client).prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())

    with pytest.raises(ClickHouseConformanceError, match="physical digest"):
        assert_prepare_evidence(plan, ClickHousePrepareEvidence.from_mapping(dict(raw)))


def test_http_gateway_checks_expected_uuid_map_immediately_before_exchange() -> None:
    client = _HttpClient()
    client.relations.add(_SHADOW_TABLE)
    gateway = _gateway(client)

    with pytest.raises(ClickHouseHttpGatewayError, match="UUID guard differs"):
        gateway.exchange(
            {
                "workflow_execution_id": _plan().workflow_execution_id,
                "operation_id": _digest("0"),
                "workflow_execution_binding_sha256": _plan().workflow_execution_binding_sha256,
                "attempt_binding_sha256": _digest("d"),
                "fence_epoch": 3,
                "clickhouse_cluster_authority_id": _CLUSTER,
                "database": "analytics",
                "target_table": "orders",
                "shadow_table": _SHADOW_TABLE,
                "expected_target_uuid": "00000000-0000-0000-0000-000000000009",
                "expected_shadow_uuid": "00000000-0000-0000-0000-000000000003",
            }
        )

    assert not any(statement.startswith("EXCHANGE TABLES") for statement in client.statements)


def test_http_gateway_cleans_exact_completed_attempt_relations_idempotently() -> None:
    client = _HttpClient()
    gateway = _gateway(client)
    plan = _plan()
    prepared = gateway.prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())
    gateway.exchange(
        {
            "workflow_execution_id": plan.workflow_execution_id,
            "operation_id": plan.operation_id,
            "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
            "attempt_binding_sha256": plan.attempt_binding_sha256,
            "fence_epoch": plan.fence_epoch,
            "clickhouse_cluster_authority_id": plan.clickhouse_cluster_authority_id,
            "database": plan.database,
            "target_table": plan.target_table,
            "shadow_table": plan.shadow_table,
            "expected_target_uuid": prepared["target_uuid"],
            "expected_shadow_uuid": prepared["shadow_uuid"],
        }
    )
    request = _cleanup_request(plan, prepared)

    gateway.cleanup_retained(request)
    gateway.cleanup_retained(request)

    assert _STAGING_TABLE not in client.relations
    assert _SHADOW_TABLE in client.relations
    assert gateway.inspect_uuid_map(
        {
            "workflow_execution_id": plan.workflow_execution_id,
            "operation_id": plan.operation_id,
            "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
            "attempt_binding_sha256": plan.attempt_binding_sha256,
            "fence_epoch": plan.fence_epoch,
            "clickhouse_cluster_authority_id": plan.clickhouse_cluster_authority_id,
            "database": plan.database,
            "target_table": plan.target_table,
            "shadow_table": plan.shadow_table,
        }
    ) == {"target_uuid": prepared["shadow_uuid"], "shadow_uuid": prepared["target_uuid"]}
    assert sum(statement.startswith("DROP TABLE") for statement in client.statements) == 1


def test_http_gateway_reconciles_cleanup_drop_acknowledgement_loss() -> None:
    lost_relation = _STAGING_TABLE
    client = _DropAcknowledgementLostHttpClient(lost_relation)
    gateway = _gateway(client)
    plan = _plan()
    prepared = gateway.prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())
    gateway.exchange(
        {
            "workflow_execution_id": plan.workflow_execution_id,
            "operation_id": plan.operation_id,
            "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
            "attempt_binding_sha256": plan.attempt_binding_sha256,
            "fence_epoch": plan.fence_epoch,
            "clickhouse_cluster_authority_id": plan.clickhouse_cluster_authority_id,
            "database": plan.database,
            "target_table": plan.target_table,
            "shadow_table": plan.shadow_table,
            "expected_target_uuid": prepared["target_uuid"],
            "expected_shadow_uuid": prepared["shadow_uuid"],
        }
    )

    gateway.cleanup_retained(_cleanup_request(plan, prepared))

    assert _STAGING_TABLE not in client.relations
    assert _SHADOW_TABLE in client.relations


def test_http_gateway_rejects_wrong_retained_uuid_before_any_cleanup_drop() -> None:
    client = _HttpClient()
    gateway = _gateway(client)
    plan = _plan()
    prepared = gateway.prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())
    gateway.exchange(
        {
            "workflow_execution_id": plan.workflow_execution_id,
            "operation_id": plan.operation_id,
            "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
            "attempt_binding_sha256": plan.attempt_binding_sha256,
            "fence_epoch": plan.fence_epoch,
            "clickhouse_cluster_authority_id": plan.clickhouse_cluster_authority_id,
            "database": plan.database,
            "target_table": plan.target_table,
            "shadow_table": plan.shadow_table,
            "expected_target_uuid": prepared["target_uuid"],
            "expected_shadow_uuid": prepared["shadow_uuid"],
        }
    )
    client.shadow_uuid = "00000000-0000-0000-0000-000000000099"
    drops_before = sum(statement.startswith("DROP TABLE") for statement in client.statements)

    with pytest.raises(ClickHouseHttpGatewayError, match="retained target UUID"):
        gateway.cleanup_retained(_cleanup_request(plan, prepared))

    assert sum(statement.startswith("DROP TABLE") for statement in client.statements) == drops_before


def _failed_scratch_authority(
    prepared: Mapping[str, object],
    *,
    staging_uuid: str | None = None,
    shadow_uuid: str | None = None,
) -> MssqlFailedScratchCleanupAuthority:
    plan = _plan()
    return MssqlFailedScratchCleanupAuthority(
        workflow_execution_id=plan.workflow_execution_id,
        workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
        operation_id=plan.operation_id,
        operation_plan_sha256=plan.operation_plan_sha256,
        attempt_binding_sha256=plan.attempt_binding_sha256,
        fencing_epoch=plan.fence_epoch,
        protected_target_authority_id=plan.target_authority_id,
        protected_target_database=plan.database,
        protected_target_table=plan.target_table,
        protected_target_uuid=plan.expected_target_uuid,
        relations=(
            MssqlScratchCleanupRelation(
                relation_role="SHADOW",
                database_name=plan.database,
                table_name=plan.shadow_table,
                observed_uuid=shadow_uuid or str(prepared["shadow_uuid"]),
            ),
            MssqlScratchCleanupRelation(
                relation_role="STAGING",
                database_name=plan.database,
                table_name=plan.staging_table,
                observed_uuid=staging_uuid or str(prepared["staging_uuid"]),
            ),
        ),
    )


def _failed_scratch_cleaner(client: _HttpClient) -> ClickHouseFailedPrecommitScratchCleaner:
    return ClickHouseFailedPrecommitScratchCleaner(
        client=client,
        connection=ClickHouseConnectionAuthorityVerifier(
            client=client,
            authority=_connection_authority(),
        ),
    )


def test_failed_precommit_cleaner_drops_only_durable_exact_scratch_closure() -> None:
    client = _HttpClient()
    plan = _plan()
    prepared = _gateway(client).prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())

    receipt = _failed_scratch_cleaner(client).cleanup(_failed_scratch_authority(prepared))

    assert client.target_uuid == plan.expected_target_uuid
    assert _STAGING_TABLE not in client.relations
    assert _SHADOW_TABLE not in client.relations
    assert sum(statement.startswith("DROP TABLE") for statement in client.statements) == 2
    assert tuple(item.kind for item in receipt.relations) == ("shadow", "staging")
    assert tuple(item.observed_uuid for item in receipt.relations) == (None, None)
    assert receipt.to_mapping()["scratch_absence_evidence_sha256"] == receipt.scratch_absence_evidence_sha256
    assert receipt.scratch_absence_evidence_sha256.startswith("sha256:")


@pytest.mark.parametrize("lost_relation", [_STAGING_TABLE, _SHADOW_TABLE])
def test_failed_precommit_cleaner_reconciles_drop_acknowledgement_loss(lost_relation: str) -> None:
    client = _DropAcknowledgementLostHttpClient(lost_relation)
    plan = _plan()
    prepared = _gateway(client).prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())

    receipt = _failed_scratch_cleaner(client).cleanup(_failed_scratch_authority(prepared))

    assert _STAGING_TABLE not in client.relations
    assert _SHADOW_TABLE not in client.relations
    assert all(item.observed_uuid is None for item in receipt.relations)


@pytest.mark.parametrize(
    ("relation", "message"),
    [
        ("staging", "scratch UUID differs"),
        ("shadow", "scratch UUID differs"),
    ],
)
def test_failed_precommit_cleaner_validates_full_closure_before_any_drop(
    relation: str,
    message: str,
) -> None:
    client = _HttpClient()
    plan = _plan()
    prepared = _gateway(client).prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())
    overrides = {f"{relation}_uuid": "00000000-0000-0000-0000-000000000099"}
    drops_before = sum(statement.startswith("DROP TABLE") for statement in client.statements)

    with pytest.raises(ClickHouseHttpGatewayError, match=message):
        _failed_scratch_cleaner(client).cleanup(_failed_scratch_authority(prepared, **overrides))

    assert sum(statement.startswith("DROP TABLE") for statement in client.statements) == drops_before
    assert _STAGING_TABLE in client.relations
    assert _SHADOW_TABLE in client.relations


def test_failed_precommit_cleaner_rejects_wrong_owner_before_any_drop() -> None:
    client = _HttpClient()
    plan = _plan()
    prepared = _gateway(client).prepare(plan.to_mapping(), ClickHousePrepareSqlBuilder().build(plan).to_mapping())
    client.comments[_STAGING_TABLE] = "dpone-semantic-refresh:sha256:" + "f" * 64
    drops_before = sum(statement.startswith("DROP TABLE") for statement in client.statements)

    with pytest.raises(ClickHouseHttpGatewayError, match="scratch owner differs"):
        _failed_scratch_cleaner(client).cleanup(_failed_scratch_authority(prepared))

    assert sum(statement.startswith("DROP TABLE") for statement in client.statements) == drops_before
    assert _STAGING_TABLE in client.relations
    assert _SHADOW_TABLE in client.relations


def _cleanup_request(plan: ClickHousePreparePlan, prepared: Mapping[str, object]) -> dict[str, object]:
    return {
        "workflow_execution_id": plan.workflow_execution_id,
        "operation_id": plan.operation_id,
        "operation_plan_sha256": plan.operation_plan_sha256,
        "workflow_execution_binding_sha256": plan.workflow_execution_binding_sha256,
        "attempt_binding_sha256": plan.attempt_binding_sha256,
        "fence_epoch": plan.fence_epoch,
        "clickhouse_cluster_authority_id": plan.clickhouse_cluster_authority_id,
        "database": plan.database,
        "target_table": plan.target_table,
        "staging_table": plan.staging_table,
        "shadow_table": plan.shadow_table,
        "expected_target_uuid": prepared["shadow_uuid"],
        "expected_staging_uuid": prepared["staging_uuid"],
        "expected_retained_uuid": prepared["target_uuid"],
        "terminal_receipt_sha256": _digest("f"),
    }


class _UnavailableHttpClient:
    endpoint_authority_id = _ENDPOINT

    def execute(self, _statement: str) -> list[tuple[object, ...]]:
        raise TimeoutError("response deadline exceeded")

    def insert_parquet(self, _statement: str, _content: bytes) -> list[tuple[object, ...]]:
        raise TimeoutError("response deadline exceeded")


class _MalformedHttpClient:
    endpoint_authority_id = _ENDPOINT

    def execute(self, _statement: str) -> list[tuple[object, ...]]:
        return []

    def insert_parquet(self, _statement: str, _content: bytes) -> list[tuple[object, ...]]:
        return []


@pytest.mark.parametrize("client", [_UnavailableHttpClient(), _MalformedHttpClient()])
def test_http_gateway_fails_closed_on_unavailable_or_malformed_response(client: object) -> None:
    gateway = _gateway(client)

    with pytest.raises(ClickHouseHttpGatewayError, match="unavailable|invalid"):
        gateway.inspect_uuid_map(
            {
                "workflow_execution_id": _plan().workflow_execution_id,
                "operation_id": _digest("0"),
                "workflow_execution_binding_sha256": _plan().workflow_execution_binding_sha256,
                "attempt_binding_sha256": _digest("d"),
                "fence_epoch": 3,
                "clickhouse_cluster_authority_id": _CLUSTER,
                "database": "analytics",
                "target_table": "orders",
                "shadow_table": _SHADOW_TABLE,
            }
        )


def test_authenticated_prepared_loader_rebuilds_exact_cross_task_input() -> None:
    state_calls: list[dict[str, object]] = []
    service, plan = _empty_scope_service(_HttpClient(), state_calls)
    prepared = service.prepare(plan)
    documents = prepared_publication_documents(plan, prepared)
    durable_value = DurableClickHousePreparedPublication(
        workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
        operation_id=plan.operation_id,
        **documents,
    )

    class _DurablePrepared:
        def load_prepared(
            self,
            workflow_execution_binding_sha256: str,
            operation_id: str,
        ) -> DurableClickHousePreparedPublication:
            assert workflow_execution_binding_sha256 == plan.workflow_execution_binding_sha256
            assert operation_id == plan.operation_id
            return durable_value

    loader = AuthenticatedClickHousePreparedPublicationLoader(
        durable=_DurablePrepared(),
        authority=_publication_authority(),
        seal_authorization=StaticSemanticRefreshSealAuthorization((_seal_authorization(after_image_row_count=0),)),
    )
    loaded = loader.load(
        workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
        operation_id=plan.operation_id,
    )

    assert loaded.plan == plan
    assert loaded.receipt == prepared


def test_authenticated_prepared_loader_rejects_resigned_noncanonical_mutation_table() -> None:
    state_calls: list[dict[str, object]] = []
    service, plan = _empty_scope_service(_HttpClient(), state_calls)
    prepared = service.prepare(plan)
    swapped_plan = replace(plan, shadow_table="unrelated_safe_table")
    receipt_values = {
        field_name: getattr(prepared, field_name)
        for field_name in prepared.__dataclass_fields__
        if field_name != "receipt_sha256"
    }
    receipt_values["prepare_plan_sha256"] = swapped_plan.sha256
    swapped_receipt = replace(
        prepared,
        prepare_plan_sha256=swapped_plan.sha256,
        receipt_sha256=semantic_refresh_fingerprint(receipt_values),
    )
    documents = prepared_publication_documents(swapped_plan, swapped_receipt)
    durable_value = DurableClickHousePreparedPublication(
        workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
        operation_id=plan.operation_id,
        **documents,
    )

    class _DurablePrepared:
        def load_prepared(self, *_identity: str) -> DurableClickHousePreparedPublication:
            return durable_value

    loader = AuthenticatedClickHousePreparedPublicationLoader(
        durable=_DurablePrepared(),
        authority=_publication_authority(),
        seal_authorization=StaticSemanticRefreshSealAuthorization((_seal_authorization(after_image_row_count=0),)),
    )

    with pytest.raises(ClickHousePreparedPublicationLoadError, match="unavailable"):
        loader.load(
            workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
            operation_id=plan.operation_id,
        )


class _ProtectedOperations:
    def load_operation(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedOperationAuthority:
        return MssqlProtectedOperationAuthority(
            workflow_execution_id="scheduled__2026-08-08",
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            canonical_authority_sha256=_digest("2"),
            workflow_plan_sha256=_digest("b"),
            operation_id=operation_id,
            operation_plan_sha256=_digest("a"),
            attempt_binding_sha256=_digest("d"),
            fencing_epoch=3,
            owner_id="owner-1",
            guard_resource_id="clickhouse-target://analytics/orders",
            guard_status="HELD",
            journal_status="ADMITTED",
            strategy_authority_sha256=_digest("7"),
            strategy_authority_json="{}",
            model_unique_id="model.orders",
            target_resource_id="clickhouse-target://analytics/orders",
            target_authority_id=f"clickhouse://{_CLUSTER}/analytics/orders",
            mssql_connection_authority_id="mssql-connection-primary",
            mssql_target_authority_id="mssql-target-orders",
            clickhouse_cluster_authority_id=_CLUSTER,
            publication_database="analytics",
            publication_target_table="orders",
            publication_scope_id="2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
            scope_family_id=_digest("8"),
            scope_start="2026-08-08T00:00:00Z",
            scope_end="2026-08-09T00:00:00Z",
            scope_revision=1,
            mutation_closure_sha256=_digest("9"),
            target_predecessor_generation_id=_digest("3"),
            scope_predecessor_operation_id=None,
            predecessor_target_generation=7,
            predecessor_target_uuid="00000000-0000-0000-0000-000000000001",
            predecessor_target_operation_id=_digest("e"),
            predecessor_scope_revision=None,
            predecessor_checkpoint_sha256=None,
            predecessor_checkpoint_operation_id=None,
            predecessor_checkpoint_version=None,
            clickhouse_target_uuid="00000000-0000-0000-0000-000000000001",
            model_definition_proof_sha256=_digest("4"),
            effective_key_template_sha256=_digest("3"),
            effective_key_mapping_sha256=_ARTIFACT_PLAN.effective_key_mapping_sha256,
            writable_schema_sha256=_digest("4"),
            writable_columns=(
                MssqlProtectedWritableColumn("event_id", "bigint", "Int64", False, "EFFECTIVE_KEY"),
                MssqlProtectedWritableColumn(
                    "occurred_at",
                    "datetime2(6)",
                    "DateTime64(6, 'UTC')",
                    False,
                    "EFFECTIVE_KEY_EVENT_TIME",
                ),
                MssqlProtectedWritableColumn("amount", "decimal(18,2)", "Decimal(18, 2)", False, "MUTABLE_VALUE"),
            ),
            resource_policy=MssqlProtectedResourcePolicy(
                *(100 for _ in range(18)),
                resource_policy_sha256=_digest("5"),
            ),
            route_certification_receipt_sha256=_ARTIFACT_PLAN.route_certification_receipt_sha256,
            writer_exclusivity_assurance_receipt_sha256=_digest("1"),
            ddl_freeze_assurance_receipt_sha256=_digest("2"),
            utc_semantics_assurance_receipt_sha256=_digest("3"),
            artifact_authority=MssqlProtectedArtifactAuthority(
                provider="s3",
                provider_profile="s3_create_only_versioned_kms_object_lock_v1",
                endpoint_authority_id="https://s3.amazonaws.com",
                bucket_or_container_authority_id="dpone-semantic-refresh",
                kms_key_authority_id=("arn:aws:kms:eu-central-1:123456789012:key/11111111-2222-3333-4444-555555555555"),
                capability_evidence_sha256=_digest("6"),
                writer_scope="semantic-refresh-writer",
                artifact_prefix="operations",
                encryption_policy_sha256=_ARTIFACT_PLAN.encryption_policy_sha256,
                retention_policy_id="semantic-refresh-30d",
                retention_policy_sha256=_ARTIFACT_PLAN.retention_policy_sha256,
                retention_days=30,
                retention_issued_at="2026-08-09T00:00:00Z",
                retention_until="2026-09-08T00:00:00Z",
                max_artifact_bytes=1_000,
            ),
            before_image_relation=None,
            before_image_sha256=None,
            after_image_relation="dpone_control.after_image_orders",
            after_image_sha256=_digest("f"),
            scope_map_authority_receipt_sha256=_digest("1"),
            prerequisite_authority=_prerequisite_claim(),
        )


class _CanonicalAuthorities:
    def load(self, workflow_execution_binding_sha256: str) -> MssqlCanonicalAuthorityRecord:
        return MssqlCanonicalAuthorityRecord(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            workflow_execution_id="scheduled__2026-08-08",
            authority_sha256=_digest("2"),
            authority_json="{}",
            status="ACTIVE",
        )


def _prerequisite_claim() -> MssqlPrerequisiteAuthorityClaim:
    return MssqlPrerequisiteAuthorityClaim(
        release_id=_digest("1"),
        deployment_id=_digest("2"),
        model_unique_id="model.orders",
        route_certification_receipt_sha256=_digest("8"),
        runtime_assurances=(
            MssqlRuntimeAssuranceClaim("ddl_freeze", _digest("2"), "{}"),
            MssqlRuntimeAssuranceClaim("writer_exclusivity", _digest("1"), "{}"),
        ),
    )


class _PrerequisiteAuthorities:
    def __init__(self) -> None:
        self.claims: list[MssqlPrerequisiteAuthorityClaim] = []

    def require_current(self, claim: MssqlPrerequisiteAuthorityClaim) -> None:
        self.claims.append(claim)


def _publisher_authorities() -> tuple[
    MssqlProtectedOperationAuthority,
    SemanticRefreshSealAuthorizationReceipt,
    MssqlCommittedAfterImageSnapshot,
    DponeParquetV1Codec,
]:
    base = _ProtectedOperations().load_operation(
        workflow_execution_binding_sha256=_digest("c"),
        operation_id=_digest("0"),
    )
    operation = replace(
        base,
        workflow_id=_digest("1"),
        baseline_receipt_sha256=_digest("6"),
        resource_policy=replace(
            base.resource_policy,
            max_after_image_bytes=100_000,
        ),
        artifact_authority=replace(
            base.artifact_authority,
            max_artifact_bytes=100_000,
        ),
    )
    fields = tuple(
        SemanticRefreshParquetField(
            column.name,
            column.source_type,
            column.nullable,
        )
        for column in operation.writable_columns
    )
    codec = DponeParquetV1Codec()
    rows = (
        (
            101,
            datetime(2026, 8, 8, 0, 0, 0, 1, tzinfo=UTC),
            Decimal("10.25"),
        ),
        (
            102,
            datetime(2026, 8, 8, 0, 0, 0, 2, tzinfo=UTC),
            Decimal("20.50"),
        ),
    )
    seal = SemanticRefreshSealAuthorizationReceipt.build(
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        model_unique_id=operation.model_unique_id,
        workflow_id=operation.workflow_id or _digest("1"),
        workflow_plan_sha256=operation.workflow_plan_sha256,
        workflow_execution_id=operation.workflow_execution_id,
        workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
        attempt_binding_sha256=operation.attempt_binding_sha256,
        fencing_epoch=operation.fencing_epoch,
        journal_version=operation.journal_version,
        event_time_source_type="datetime2(6)",
        effective_key_template_sha256=operation.effective_key_template_sha256,
        effective_key_mapping_sha256=operation.effective_key_mapping_sha256,
        ordered_writable_schema_sha256=operation.writable_schema_sha256,
        serializer_sha256=codec.serializer_sha256,
        parquet_schema_mapping_sha256=codec.schema_mapping_sha256(fields),
        clickhouse_input_mapping_sha256=_digest("4"),
        codec_mapping_certification_sha256=_digest("3"),
        before_image_relation_id="dpone_control.before_image_orders",
        before_image_sha256=_digest("4"),
        before_image_row_count=1,
        after_image_relation_id=operation.after_image_relation or "missing",
        after_image_sha256=operation.after_image_sha256 or _digest("f"),
        after_image_row_count=len(rows),
        model_build_receipt_sha256=_digest("5"),
        baseline_adoption_receipt_sha256=operation.baseline_receipt_sha256,
        route_certification_receipt_sha256=(operation.route_certification_receipt_sha256),
        writer_exclusivity_assurance_receipt_sha256=(operation.writer_exclusivity_assurance_receipt_sha256),
        utc_semantics_assurance_receipt_sha256=(operation.utc_semantics_assurance_receipt_sha256),
        ddl_freeze_assurance_receipt_sha256=(operation.ddl_freeze_assurance_receipt_sha256),
        artifact_authority_sha256=semantic_refresh_artifact_authority_sha256(operation.artifact_authority),
        seal_policy_sha256=_digest("5"),
        created_at="2026-08-08T12:35:00Z",
        issuer_authority="mssql-protected-control/test",
        issuer_attestation_sha256=_digest("6"),
        issuer_signature_sha256=_digest("7"),
    )
    snapshot = MssqlCommittedAfterImageSnapshot(
        relation_id=operation.after_image_relation or "missing",
        image_sha256=operation.after_image_sha256 or _digest("f"),
        row_count=len(rows),
        columns=operation.writable_columns,
        rows=rows,
    )
    return operation, seal, snapshot, codec


def test_concrete_after_image_publisher_seals_only_verified_protected_rows() -> None:
    operation, seal, snapshot, codec = _publisher_authorities()
    prerequisites = _PrerequisiteAuthorities()
    store = InMemoryCreateOnlyArtifactStore()

    class _OperationAuthority:
        def load_operation(self, **_identity: object) -> MssqlProtectedOperationAuthority:
            return operation

    class _AfterImage:
        calls = 0

        def read(
            self,
            loaded: MssqlProtectedOperationAuthority,
        ) -> MssqlCommittedAfterImageSnapshot:
            assert loaded is operation
            self.calls += 1
            return snapshot

    after_image = _AfterImage()
    receipt = MssqlCommittedAfterImageArtifactPublisher(
        protected_operation=_OperationAuthority(),
        prerequisites=prerequisites,
        seal_authorization=StaticSemanticRefreshSealAuthorization((seal,)),
        after_image=after_image,
        artifact_stores=_publisher_store_resolver(operation, store),
        codec=codec,
    ).seal(
        workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
        operation_id=operation.operation_id,
    )

    assert receipt.status == "SEALED"
    assert receipt.sealed_manifest.matches_seal_authorization(seal)
    assert receipt.sealed_manifest.total_rows == snapshot.row_count
    expected_prefix = operation_artifact_prefix(
        operation.artifact_authority.artifact_prefix,
        operation.operation_id,
    )
    assert store.create_events[-1] == f"{expected_prefix}/manifest.json"
    assert receipt.manifest.encryption_scope == operation.artifact_authority.writer_scope
    assert receipt.manifest.encryption_scope != operation.artifact_authority.kms_key_authority_id
    assert after_image.calls == 1
    assert prerequisites.claims == [
        operation.prerequisite_authority,
        operation.prerequisite_authority,
    ]


def test_concrete_after_image_publisher_rejects_snapshot_swap_before_store_write() -> None:
    operation, seal, snapshot, codec = _publisher_authorities()
    store = InMemoryCreateOnlyArtifactStore()

    class _OperationAuthority:
        def load_operation(self, **_identity: object) -> MssqlProtectedOperationAuthority:
            return operation

    class _AfterImage:
        def read(
            self,
            _loaded: MssqlProtectedOperationAuthority,
        ) -> MssqlCommittedAfterImageSnapshot:
            return replace(snapshot, image_sha256=_digest("e"))

    with pytest.raises(MssqlCommittedAfterImageSealError, match="differs from protected authority"):
        MssqlCommittedAfterImageArtifactPublisher(
            protected_operation=_OperationAuthority(),
            prerequisites=_PrerequisiteAuthorities(),
            seal_authorization=StaticSemanticRefreshSealAuthorization((seal,)),
            after_image=_AfterImage(),
            artifact_stores=_publisher_store_resolver(operation, store),
            codec=codec,
        ).seal(
            workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
            operation_id=operation.operation_id,
        )

    assert store.create_events == ()


def _publisher_store_resolver(
    operation: MssqlProtectedOperationAuthority,
    store: InMemoryCreateOnlyArtifactStore,
) -> StaticCreateOnlyArtifactStoreResolver:
    binding = artifact_store_binding(operation.artifact_authority)
    return StaticCreateOnlyArtifactStoreResolver(
        replace(
            binding,
            artifact_prefix=operation_artifact_prefix(binding.artifact_prefix, operation.operation_id),
        ),
        store,
    )


def test_authority_bound_reader_uses_same_exact_operation_store_binding() -> None:
    operation, seal, snapshot, codec = _publisher_authorities()
    store = InMemoryCreateOnlyArtifactStore()

    class _OperationAuthority:
        def load_operation(self, **_identity: object) -> MssqlProtectedOperationAuthority:
            return operation

    class _AfterImage:
        def read(self, _operation: MssqlProtectedOperationAuthority) -> MssqlCommittedAfterImageSnapshot:
            return snapshot

    resolver = _publisher_store_resolver(operation, store)
    receipt = MssqlCommittedAfterImageArtifactPublisher(
        protected_operation=_OperationAuthority(),
        prerequisites=_PrerequisiteAuthorities(),
        seal_authorization=StaticSemanticRefreshSealAuthorization((seal,)),
        after_image=_AfterImage(),
        artifact_stores=resolver,
        codec=codec,
    ).seal(
        workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
        operation_id=operation.operation_id,
    )

    artifact = AuthorityBoundVersionPinnedSealedArtifactReader(
        protected_operation=_OperationAuthority(),
        artifact_stores=resolver,
    ).read(
        manifest_key=receipt.manifest.key,
        manifest_version=receipt.manifest.version,
        artifact_manifest_sha256=receipt.sealed_manifest.artifact_manifest_sha256,
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
        attempt_binding_sha256=operation.attempt_binding_sha256,
        seal_authorization=seal,
    )

    assert artifact.manifest.artifact_prefix == operation_artifact_prefix(
        operation.artifact_authority.artifact_prefix,
        operation.operation_id,
    )


def test_authority_bound_reader_rejects_kms_authority_swap_before_object_io() -> None:
    operation, seal, _snapshot, _codec = _publisher_authorities()
    swapped = replace(
        operation,
        artifact_authority=replace(
            operation.artifact_authority,
            kms_key_authority_id=("arn:aws:kms:eu-central-1:123456789012:key/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        ),
    )

    class _OperationAuthority:
        def load_operation(self, **_identity: object) -> MssqlProtectedOperationAuthority:
            return swapped

    class _Resolver:
        calls = 0

        def resolve(self, _binding: object) -> InMemoryCreateOnlyArtifactStore:
            self.calls += 1
            return InMemoryCreateOnlyArtifactStore()

    resolver = _Resolver()
    with pytest.raises(SealedArtifactReadError, match="identity differs"):
        AuthorityBoundVersionPinnedSealedArtifactReader(
            protected_operation=_OperationAuthority(),
            artifact_stores=resolver,
        ).read(
            manifest_key=f"{operation_artifact_prefix(operation.artifact_authority.artifact_prefix, operation.operation_id)}/manifest.json",
            manifest_version="v1",
            artifact_manifest_sha256=_digest("9"),
            operation_id=operation.operation_id,
            operation_plan_sha256=operation.operation_plan_sha256,
            workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
            attempt_binding_sha256=operation.attempt_binding_sha256,
            seal_authorization=seal,
        )

    assert resolver.calls == 0


def test_publication_authority_is_derived_from_authenticated_mssql_operation() -> None:
    authority = MssqlProtectedSemanticRefreshClickHouseAuthority(
        protected=_ProtectedOperations(),
        canonical=_CanonicalAuthorities(),
        prerequisites=_PrerequisiteAuthorities(),
    ).load(
        _digest("c"),
        _digest("0"),
    )

    assert authority.target_resource_id == "clickhouse-target://analytics/orders"
    assert authority.target_authority_id == f"clickhouse://{_CLUSTER}/analytics/orders"
    assert authority.clickhouse_cluster_authority_id == _CLUSTER
    assert authority.database == "analytics"
    assert authority.target_table == "orders"
    assert authority.scope_id == "2026-08-08T00:00:00Z/2026-08-09T00:00:00Z"
    assert authority.scope_revision == 1
    assert authority.authority_sha256 == _digest("2")


def test_publication_authority_projects_nullable_type_into_schema_digest() -> None:
    class _NullableProtectedOperations(_ProtectedOperations):
        def load_operation(
            self,
            *,
            workflow_execution_binding_sha256: str,
            operation_id: str,
        ) -> MssqlProtectedOperationAuthority:
            operation = super().load_operation(
                workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                operation_id=operation_id,
            )
            amount = replace(operation.writable_columns[2], nullable=True)
            return replace(operation, writable_columns=(*operation.writable_columns[:2], amount))

    authority = MssqlProtectedSemanticRefreshClickHouseAuthority(
        protected=_NullableProtectedOperations(),
        canonical=_CanonicalAuthorities(),
        prerequisites=_PrerequisiteAuthorities(),
    ).load(_digest("c"), _digest("0"))

    assert authority.expected_schema_sha256 == clickhouse_authority_rows_sha256(
        (
            ("event_id", "Int64", 1, "", "", ""),
            ("occurred_at", "DateTime64(6, 'UTC')", 2, "", "", ""),
            ("amount", "Nullable(Decimal(18, 2))", 3, "", "", ""),
        )
    )


class _CanonicalAuthorityCursor:
    def __init__(self, rows: list[tuple[object, ...] | None]) -> None:
        self.rows = rows
        self.statements: list[str] = []
        self.parameters: list[tuple[object, ...]] = []

    def execute(self, sql: str, *parameters: object) -> _CanonicalAuthorityCursor:
        self.statements.append(sql)
        self.parameters.append(parameters)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.rows.pop(0)

    def close(self) -> None:
        return None


def _protected_state_request() -> dict[str, object]:
    _authority_json, authority_sha256 = _canonical_authority_document()
    return {
        "publication_authority_sha256": authority_sha256,
        "operation_id": _digest("0"),
        "operation_plan_sha256": _digest("a"),
        "workflow_plan_sha256": _digest("b"),
        "workflow_execution_binding_sha256": _digest("c"),
        "workflow_execution_id": "scheduled__2026-08-08",
        "target_resource_id": "clickhouse-target://analytics/orders",
        "target_authority_id": f"clickhouse://{_CLUSTER}/analytics/orders",
        "clickhouse_cluster_authority_id": _CLUSTER,
        "database": "analytics",
        "target_table": "orders",
        "scope_id": "2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
        "scope_revision": 1,
        "target_predecessor_generation_id": _digest("3"),
        "scope_predecessor_operation_id": None,
        "predecessor_target_generation": 7,
        "predecessor_target_uuid": "00000000-0000-0000-0000-000000000001",
        "predecessor_target_operation_id": _digest("e"),
        "predecessor_scope_revision": None,
        "predecessor_checkpoint_sha256": None,
        "predecessor_checkpoint_operation_id": None,
        "predecessor_checkpoint_version": None,
    }


def _canonical_authority_row() -> tuple[object, ...]:
    request = _protected_state_request()
    authority_json, _authority_sha256 = _canonical_authority_document()
    return (
        "scheduled__2026-08-08",
        request["publication_authority_sha256"],
        authority_json,
        *tuple(
            request[field_name]
            for field_name in (
                "operation_plan_sha256",
                "workflow_plan_sha256",
                "target_resource_id",
                "target_authority_id",
                "clickhouse_cluster_authority_id",
                "database",
                "target_table",
                "scope_id",
                "scope_revision",
                "target_predecessor_generation_id",
                "scope_predecessor_operation_id",
            )
        ),
    )


def _canonical_authority_document() -> tuple[str, str]:
    unsigned: dict[str, object] = {
        "attempt_bindings": [],
        "controller_id": "controller-1",
        "execution_binding": {"workflow_execution_binding_sha256": _digest("c")},
        "model_resources": [
            {
                "model_unique_id": "model.orders",
                "publication_database": "analytics",
                "publication_scope_id": "2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
                "publication_target_table": "orders",
                "target_authority_id": f"clickhouse://{_CLUSTER}/analytics/orders",
                "clickhouse_cluster_authority_id": _CLUSTER,
                "target_resource_id": "clickhouse-target://analytics/orders",
            }
        ],
        "operation_plans": [
            {
                "model_unique_id": "model.orders",
                "operation_id": _digest("0"),
                "operation_plan_sha256": _digest("a"),
                "scope_revision": 1,
                "target_predecessor_generation_id": _digest("3"),
                "scope_predecessor_operation_id": None,
            }
        ],
        "owner_id": "owner-1",
        "replacement_plan": None,
        "reservation_id": "reservation-1",
        "resource_budget": {},
        "resource_guards": [],
        "schema": "dpone.semantic-refresh-mssql-canonical-authority.v1",
        "workflow_execution_id": "scheduled__2026-08-08",
        "workflow_guard": {},
        "workflow_plan": {"workflow_plan_sha256": _digest("b")},
    }
    unsigned_json = json.dumps(unsigned, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    authority_sha256 = "sha256:" + hashlib.sha256(unsigned_json.encode()).hexdigest()
    authority_json = json.dumps(
        {**unsigned, "authority_sha256": authority_sha256},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return authority_json, authority_sha256


def test_mssql_publication_authority_locks_and_accepts_exact_target_scope() -> None:
    cursor = _CanonicalAuthorityCursor([_canonical_authority_row(), None])
    journal = (None, None, None, None, None, None, None, "clickhouse-target://analytics/orders")

    MssqlPublicationCanonicalAuthority(
        lambda table: f"[dpone_control].[{table}]",
        required=True,
    ).assert_authorized(cursor, _protected_state_request(), journal)

    assert "UPDLOCK, HOLDLOCK" in cursor.statements[0]


def test_mssql_publication_authority_rejects_unrelated_digest_and_json() -> None:
    row = list(_canonical_authority_row())
    row[1] = _digest("2")
    cursor = _CanonicalAuthorityCursor([tuple(row), None])
    journal = (None, None, None, None, None, None, None, "clickhouse-target://analytics/orders")

    with pytest.raises(ValueError, match="record digest differs"):
        MssqlPublicationCanonicalAuthority(
            lambda table: f"[dpone_control].[{table}]",
            required=True,
        ).assert_authorized(cursor, _protected_state_request(), journal)


@pytest.mark.parametrize(
    ("field_name", "wrong_value"),
    [
        ("database", "other_analytics"),
        ("scope_id", "2026-08-09T00:00:00Z/2026-08-10T00:00:00Z"),
        ("target_resource_id", "clickhouse-target://analytics/other_orders"),
    ],
)
def test_mssql_publication_authority_rejects_wrong_target_or_scope(
    field_name: str,
    wrong_value: str,
) -> None:
    cursor = _CanonicalAuthorityCursor([_canonical_authority_row(), None])
    request = {**_protected_state_request(), field_name: wrong_value}
    journal = (None, None, None, None, None, None, None, "clickhouse-target://analytics/orders")

    with pytest.raises(ValueError, match="target/scope authority differs"):
        MssqlPublicationCanonicalAuthority(
            lambda table: f"[dpone_control].[{table}]",
            required=True,
        ).assert_authorized(cursor, request, journal)


class _PublicationStateCursor(_CanonicalAuthorityCursor):
    rowcount = -1

    def __init__(self, rows: list[tuple[object, ...] | None]) -> None:
        super().__init__(rows)
        self._output_row: tuple[object, ...] | None = None

    def execute(self, sql: str, *parameters: object) -> _PublicationStateCursor:
        super().execute(sql, *parameters)
        if "OUTPUT inserted.operation_id" in sql:
            self._output_row = (_digest("0"),)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        if self._output_row is not None:
            row = self._output_row
            self._output_row = None
            return row
        return super().fetchone()


class _PublicationStateConnection:
    autocommit = True

    def __init__(self, cursor: _PublicationStateCursor) -> None:
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> _PublicationStateCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        return None


def test_mssql_prepared_store_persists_and_loads_full_create_once_documents() -> None:
    state_calls: list[dict[str, object]] = []
    service, plan = _empty_scope_service(_HttpClient(), state_calls)
    prepared = service.prepare(plan)
    request = state_calls[0]
    store = MssqlPreparedPublicationStore(lambda table: f"[dpone_control].[{table}]")
    cursor = _PublicationStateCursor([])

    store.persist_or_reconcile(
        cursor,
        request,
        (None, None, None, "PREPARING", None, None),
    )

    assert "prepare_plan_json" in cursor.statements[-1]
    assert "prepared_receipt_json" in cursor.statements[-1]
    documents = prepared_publication_documents(plan, prepared)
    load_cursor = _PublicationStateCursor(
        [
            (
                documents["prepare_plan_sha256"],
                documents["prepare_plan_json"],
                documents["prepared_receipt_sha256"],
                documents["prepared_receipt_json"],
                documents["artifact_manifest_key"],
                documents["artifact_manifest_version"],
                documents["artifact_manifest_sha256"],
                "PREPARED",
            )
        ]
    )

    loaded = store.load(
        load_cursor,
        plan.workflow_execution_binding_sha256,
        plan.operation_id,
    )

    assert loaded.prepare_plan_sha256 == plan.sha256
    assert loaded.prepared_receipt_sha256 == prepared.receipt_sha256
    assert loaded.artifact_manifest_version == plan.artifact_manifest_version


@pytest.mark.parametrize(
    "durable_state",
    [
        "PREPARED",
        "COMMITTING",
        "TARGET_COMMITTED",
        "COMMIT_UNKNOWN",
        "COMMITTED_INCOMPLETE",
        "COMPLETE",
    ],
)
def test_mssql_prepared_lookup_distinguishes_pristine_preparing_from_durable_retry(
    durable_state: str,
) -> None:
    store = MssqlPreparedPublicationStore(lambda table: f"[dpone_control].[{table}]")
    preparing = _PublicationStateCursor([(None, None, None, None, None, None, None, "PREPARING")])

    assert store.find(preparing, _digest("c"), _digest("0")) is None

    state_calls: list[dict[str, object]] = []
    service, plan = _empty_scope_service(_HttpClient(), state_calls)
    prepared = service.prepare(plan)
    documents = prepared_publication_documents(plan, prepared)
    durable_row = (
        documents["prepare_plan_sha256"],
        documents["prepare_plan_json"],
        documents["prepared_receipt_sha256"],
        documents["prepared_receipt_json"],
        documents["artifact_manifest_key"],
        documents["artifact_manifest_version"],
        documents["artifact_manifest_sha256"],
        durable_state,
    )

    assert (
        store.find(
            _PublicationStateCursor([durable_row]),
            plan.workflow_execution_binding_sha256,
            plan.operation_id,
        )
        is not None
    )


def test_mssql_prepared_lookup_never_treats_partial_or_missing_authority_as_absent() -> None:
    store = MssqlPreparedPublicationStore(lambda table: f"[dpone_control].[{table}]")

    with pytest.raises(MssqlPreparedPublicationConflict, match="state is invalid"):
        store.find(
            _PublicationStateCursor([(_digest("1"), None, None, None, None, None, None, "PREPARING")]),
            _digest("c"),
            _digest("0"),
        )
    with pytest.raises(MssqlPreparedPublicationConflict, match="journal is absent"):
        store.find(_PublicationStateCursor([None]), _digest("c"), _digest("0"))


def test_mssql_prepared_store_rejects_plan_target_swap_under_protected_request() -> None:
    state_calls: list[dict[str, object]] = []
    service, plan = _empty_scope_service(_HttpClient(), state_calls)
    prepared = service.prepare(plan)
    swapped_plan = replace(
        plan,
        target_table="mart.orders",
        target_authority_id=f"clickhouse://{_CLUSTER}/analytics/mart.orders",
        staging_table="mart.orders__dpone_stage__000000000000",
        shadow_table="mart.orders__dpone_shadow__000000000000",
    )
    receipt_values = {
        field_name: getattr(prepared, field_name)
        for field_name in prepared.__dataclass_fields__
        if field_name != "receipt_sha256"
    }
    receipt_values["prepare_plan_sha256"] = swapped_plan.sha256
    swapped_receipt = replace(
        prepared,
        prepare_plan_sha256=swapped_plan.sha256,
        receipt_sha256=semantic_refresh_fingerprint(receipt_values),
    )
    request = {
        **state_calls[0],
        **prepared_publication_documents(swapped_plan, swapped_receipt),
        "prepare_receipt_sha256": swapped_receipt.receipt_sha256,
    }

    delegate_calls: list[dict[str, object]] = []

    def acknowledge(value: Any) -> dict[str, object]:
        delegate_calls.append(dict(value))
        return {**value, "committed": True, "atomic": True, "journal_state": "PREPARED"}

    state = AuthorityBoundSemanticRefreshPublicationState(
        authority=_publication_authority(),
        delegate=CallbackSemanticRefreshPublicationState(
            persist_prepared_callback=acknowledge,
            mark_committing_callback=acknowledge,
            publish_callback=acknowledge,
        ),
    )

    with pytest.raises(ValueError, match="protected publication coordinates"):
        state.persist_prepared(request)

    assert delegate_calls == []


def test_mssql_mark_committing_rejects_head_predecessor_before_exchange() -> None:
    target_uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    request = {
        **_protected_state_request(),
        "attempt_binding_sha256": _digest("d"),
        "fence_epoch": 3,
        "prepare_receipt_sha256": _digest("9"),
        "target_uuid": target_uuid,
        "expected_target_uuid": target_uuid,
        "predecessor_target_uuid": target_uuid,
        "expected_target_generation": 7,
        "expected_scope_revision": 0,
        "expected_checkpoint_sha256": None,
        "expected_journal_state": "PREPARED",
        "next_journal_state": "COMMITTING",
    }
    journal = (
        _digest("a"),
        _digest("d"),
        3,
        "PREPARED",
        _digest("9"),
        target_uuid.upper(),
        "scheduled__2026-08-08",
        "clickhouse-target://analytics/orders",
        None,
        None,
        _digest("3"),
        None,
        7,
        target_uuid,
        _digest("e"),
        None,
        None,
        None,
        None,
    )
    cursor = _PublicationStateCursor(
        [
            (0,),
            journal,
            ("scheduled__2026-08-08", _digest("b"), _digest("c")),
            (_digest("0"), _digest("d"), 3, "HELD"),
            _canonical_authority_row(),
            None,
            (2, _digest("3"), target_uuid, _digest("e")),
            (0, "baseline"),
            (_digest("8"), "baseline"),
        ]
    )
    connection = _PublicationStateConnection(cursor)

    def connection_factory() -> Any:
        return connection

    state = MssqlSemanticRefreshPublicationState(
        connection_factory,
        require_protected_authority=True,
    )

    with pytest.raises(SemanticRefreshMssqlPublicationError, match="target head predecessor differs"):
        state.mark_committing(request)
    assert connection.rolled_back is True
    assert connection.committed is False
    assert not any(statement.startswith("UPDATE") for statement in cursor.statements)


def _post_exchange_request(*, incomplete: bool = False) -> dict[str, object]:
    return {
        **_protected_state_request(),
        "workflow_plan_sha256": _digest("b"),
        "attempt_binding_sha256": _digest("d"),
        "fence_epoch": 3,
        "prepare_receipt_sha256": _digest("9"),
        "clickhouse_commit_receipt_sha256": _digest("a"),
        "expected_target_uuid": "00000000-0000-0000-0000-000000000001",
        "target_uuid": "00000000-0000-0000-0000-000000000003",
        "expected_journal_state": "TARGET_COMMITTED" if incomplete else "COMMITTING",
        "next_journal_state": "COMMITTED_INCOMPLETE" if incomplete else "TARGET_COMMITTED",
    }


def _post_exchange_journal(state: str) -> tuple[object, ...]:
    request = _post_exchange_request()
    predecessor_state = state in {"COMMITTING", "COMMIT_UNKNOWN"}
    return (
        _digest("a"),
        _digest("d"),
        3,
        state,
        _digest("9"),
        request["expected_target_uuid"] if predecessor_state else request["target_uuid"],
        "scheduled__2026-08-08",
        "clickhouse-target://analytics/orders",
        None if predecessor_state else _digest("a"),
        None,
        _digest("3"),
        None,
        7,
        request["expected_target_uuid"],
        _digest("e"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )


def _post_exchange_state(
    journal: tuple[object, ...],
) -> tuple[
    MssqlSemanticRefreshPublicationState,
    _PublicationStateConnection,
    _PublicationStateCursor,
]:
    cursor = _PublicationStateCursor(
        [
            (0,),
            journal,
            ("scheduled__2026-08-08", _digest("b"), _digest("c")),
            (_digest("0"), _digest("d"), 3, "HELD"),
            _canonical_authority_row(),
            None,
        ]
    )
    connection = _PublicationStateConnection(cursor)
    return (
        MssqlSemanticRefreshPublicationState(
            lambda: connection,
            require_protected_authority=True,
        ),
        connection,
        cursor,
    )


def test_mssql_persists_target_committed_exchange_evidence_before_heads() -> None:
    state, connection, cursor = _post_exchange_state(_post_exchange_journal("COMMITTING"))

    acknowledgement = state.record_target_committed(_post_exchange_request())

    assert acknowledgement["journal_state"] == "TARGET_COMMITTED"
    assert connection.committed is True
    update = next(statement for statement in cursor.statements if "SET status = N'TARGET_COMMITTED'" in statement)
    assert "clickhouse_commit_receipt_sha256 = ?" in update
    assert "target_uuid = ?" in update


def test_mssql_reconciles_commit_unknown_to_target_committed() -> None:
    state, connection, cursor = _post_exchange_state(_post_exchange_journal("COMMIT_UNKNOWN"))

    acknowledgement = state.record_target_committed(_post_exchange_request())

    assert acknowledgement["journal_state"] == "TARGET_COMMITTED"
    assert connection.committed is True
    update = next(statement for statement in cursor.statements if "SET status = N'TARGET_COMMITTED'" in statement)
    assert "status = ?" in update


def _commit_reconciliation_request(*, restore: bool = False) -> dict[str, object]:
    return {
        **_protected_state_request(),
        "workflow_plan_sha256": _digest("b"),
        "attempt_binding_sha256": _digest("d"),
        "fence_epoch": 3,
        "prepare_receipt_sha256": _digest("9"),
        "target_uuid": "00000000-0000-0000-0000-000000000001",
        "expected_journal_state": "COMMIT_UNKNOWN" if restore else "COMMITTING",
        "next_journal_state": "PREPARED" if restore else "COMMIT_UNKNOWN",
    }


def test_mssql_persists_and_restores_commit_unknown_under_same_receipt() -> None:
    unknown_state, unknown_connection, unknown_cursor = _post_exchange_state(_post_exchange_journal("COMMITTING"))

    unknown = unknown_state.record_commit_unknown(_commit_reconciliation_request())

    assert unknown["journal_state"] == "COMMIT_UNKNOWN"
    assert unknown_connection.committed is True
    assert any("SET status = ?" in statement for statement in unknown_cursor.statements)

    restore_state, restore_connection, restore_cursor = _post_exchange_state(_post_exchange_journal("COMMIT_UNKNOWN"))
    restored = restore_state.reconcile_prepared(_commit_reconciliation_request(restore=True))

    assert restored["journal_state"] == "PREPARED"
    assert restore_connection.committed is True
    assert any("SET status = ?" in statement for statement in restore_cursor.statements)


def test_mssql_recovers_committing_predecessor_through_commit_unknown_atomically() -> None:
    state, connection, cursor = _post_exchange_state(_post_exchange_journal("COMMITTING"))

    restored = state.reconcile_prepared(_commit_reconciliation_request(restore=True))

    assert restored["journal_state"] == "PREPARED"
    assert connection.committed is True
    recovery_updates = [
        statement
        for statement in cursor.statements
        if "SET status = ?" in statement and "semantic_refresh_journals" in statement
    ]
    assert len(recovery_updates) == 2


def test_mssql_persists_committed_incomplete_without_erasing_exchange_evidence() -> None:
    state, connection, cursor = _post_exchange_state(_post_exchange_journal("TARGET_COMMITTED"))

    acknowledgement = state.record_committed_incomplete(_post_exchange_request(incomplete=True))

    assert acknowledgement["journal_state"] == "COMMITTED_INCOMPLETE"
    assert connection.committed is True
    update = next(statement for statement in cursor.statements if "SET status = N'COMMITTED_INCOMPLETE'" in statement)
    assert "clickhouse_commit_receipt_sha256" not in update
    assert "target_uuid" not in update


@pytest.mark.parametrize(
    ("heads", "message"),
    [
        (
            (
                (3, _digest("6"), "00000000-0000-0000-0000-000000000001", _digest("f")),
                (8, _digest("4")),
                (_digest("8"), 8, _digest("4")),
            ),
            "target head predecessor differs",
        ),
        (
            (
                (3, _digest("6"), "00000000-0000-0000-0000-000000000001", _digest("5")),
                (8, _digest("f")),
                (_digest("8"), 8, _digest("4")),
            ),
            "scope head predecessor differs",
        ),
        (
            (
                (3, _digest("6"), "00000000-0000-0000-0000-000000000001", _digest("5")),
                (8, _digest("4")),
                (_digest("8"), 8, _digest("f")),
            ),
            "checkpoint predecessor differs",
        ),
    ],
)
def test_mssql_predecessor_rejects_same_values_owned_by_another_operation(
    heads: tuple[tuple[object, ...], tuple[object, ...], tuple[object, ...]],
    message: str,
) -> None:
    request = {
        "expected_target_generation": 3,
        "expected_target_uuid": "00000000-0000-0000-0000-000000000001",
        "expected_scope_revision": 8,
        "expected_checkpoint_sha256": _digest("8"),
        "target_predecessor_generation_id": _digest("6"),
        "scope_predecessor_operation_id": _digest("4"),
        "predecessor_target_operation_id": _digest("5"),
        "predecessor_scope_revision": 8,
        "predecessor_checkpoint_sha256": _digest("8"),
        "predecessor_checkpoint_operation_id": _digest("4"),
        "predecessor_checkpoint_version": 8,
    }

    with pytest.raises(MssqlPublicationHeadConflict, match=message):
        MssqlPublicationHeadStore(lambda table: table).assert_head_predecessors(
            request,
            heads,
        )


def test_mssql_first_scope_requires_absent_scope_and_checkpoint_predecessors() -> None:
    request = {
        "expected_target_generation": 0,
        "expected_target_uuid": "00000000-0000-0000-0000-000000000001",
        "target_predecessor_generation_id": _digest("6"),
        "predecessor_target_operation_id": _digest("5"),
        "scope_predecessor_operation_id": None,
        "predecessor_scope_revision": None,
        "predecessor_checkpoint_sha256": None,
        "predecessor_checkpoint_version": None,
        "predecessor_checkpoint_operation_id": None,
    }
    store = MssqlPublicationHeadStore(lambda table: table)
    target = (0, _digest("6"), request["expected_target_uuid"], _digest("5"))

    store.assert_head_predecessors(request, (target, None, None))
    with pytest.raises(MssqlPublicationHeadConflict, match="scope head predecessor differs"):
        store.assert_head_predecessors(request, (target, (0, _digest("f")), None))


def _first_scope_complete_request() -> dict[str, object]:
    return {
        **_protected_state_request(),
        "attempt_binding_sha256": _digest("d"),
        "fence_epoch": 3,
        "prepare_receipt_sha256": _digest("9"),
        "clickhouse_commit_receipt_sha256": _digest("a"),
        "terminal_receipt_sha256": _digest("b"),
        "target_uuid": "00000000-0000-0000-0000-000000000003",
        "expected_target_uuid": "00000000-0000-0000-0000-000000000001",
        "expected_target_generation": 7,
        "target_generation": 8,
        "target_generation_id": _digest("c"),
        "expected_scope_revision": 0,
        "expected_checkpoint_sha256": None,
        "checkpoint_sha256": _digest("d"),
        "target_mutation_outcome": "TARGET_COMMITTED",
        "value_conversion_outcome": "CONFORMANT",
        "expected_journal_state": "TARGET_COMMITTED",
        "terminal_journal_state": "COMPLETE",
    }


def test_mssql_first_scope_complete_allows_absent_checkpoint_predecessor() -> None:
    validate_publication_request(
        _first_scope_complete_request(),
        protected=True,
        authority_fields=PUBLICATION_AUTHORITY_FIELDS,
    )


def test_mssql_first_scope_rejects_caller_checkpoint_predecessor() -> None:
    request = {
        **_first_scope_complete_request(),
        "expected_checkpoint_sha256": _digest("f"),
    }

    with pytest.raises(ValueError, match="expected heads differ from protected predecessors"):
        validate_publication_request(
            request,
            protected=True,
            authority_fields=PUBLICATION_AUTHORITY_FIELDS,
        )


def test_mssql_replay_rejects_old_target_generation_id_after_exchange() -> None:
    request = {
        "target_mutation_outcome": "TARGET_COMMITTED",
        "expected_target_generation": 3,
        "target_generation": 4,
        "target_generation_id": _digest("7"),
        "target_predecessor_generation_id": _digest("6"),
        "expected_target_uuid": "00000000-0000-0000-0000-000000000001",
        "target_uuid": "00000000-0000-0000-0000-000000000003",
        "predecessor_target_operation_id": _digest("5"),
        "operation_id": _digest("0"),
        "scope_revision": 9,
        "checkpoint_sha256": _digest("9"),
        "predecessor_checkpoint_version": 8,
        "clickhouse_commit_receipt_sha256": _digest("a"),
        "terminal_receipt_sha256": _digest("b"),
    }
    journal = (
        None,
        None,
        None,
        "COMPLETE",
        None,
        request["target_uuid"],
        None,
        None,
        _digest("a"),
        _digest("b"),
        *([None] * 9),
        4,
        _digest("7"),
        9,
        _digest("9"),
        9,
        "TARGET_COMMITTED",
        "CONFORMANT",
    )
    old_generation_id_heads = (
        (4, _digest("6"), request["target_uuid"], _digest("0")),
        (9, _digest("0")),
        (_digest("9"), 9, _digest("0")),
    )

    with pytest.raises(MssqlPublicationHeadConflict, match="terminal publication replay differs"):
        MssqlPublicationHeadStore(lambda table: table).assert_successors(
            request,
            journal,
            old_generation_id_heads,
        )


def test_mssql_empty_scope_replay_uses_immutable_terminal_journal_coordinates() -> None:
    target_uuid = "00000000-0000-0000-0000-000000000001"
    request = {
        "target_mutation_outcome": "NOT_REQUIRED_EMPTY_SCOPE",
        "value_conversion_outcome": "NOT_APPLICABLE_NO_DATA",
        "expected_target_generation": 3,
        "target_generation": 3,
        "target_generation_id": _digest("6"),
        "target_predecessor_generation_id": _digest("6"),
        "expected_target_uuid": target_uuid,
        "target_uuid": target_uuid,
        "predecessor_target_operation_id": _digest("5"),
        "operation_id": _digest("0"),
        "scope_revision": 1,
        "checkpoint_sha256": _digest("9"),
        "predecessor_checkpoint_version": None,
        "clickhouse_commit_receipt_sha256": _digest("a"),
        "terminal_receipt_sha256": _digest("b"),
    }
    terminal = (
        3,
        _digest("6"),
        1,
        _digest("9"),
        1,
        "NOT_REQUIRED_EMPTY_SCOPE",
        "NOT_APPLICABLE_NO_DATA",
    )
    journal = (
        None,
        None,
        None,
        "COMPLETE",
        None,
        target_uuid,
        None,
        None,
        _digest("a"),
        _digest("b"),
        *([None] * 9),
        *terminal,
    )
    heads = (
        (3, _digest("6"), target_uuid, _digest("5")),
        (1, _digest("0")),
        (_digest("9"), 1, _digest("0")),
    )

    MssqlPublicationHeadStore(lambda table: table).assert_successors(request, journal, heads)
    with pytest.raises(MssqlPublicationHeadConflict, match="terminal publication replay differs"):
        MssqlPublicationHeadStore(lambda table: table).assert_successors(
            request,
            (*journal[:19], 4, *journal[20:]),
            heads,
        )


def test_mssql_empty_scope_terminal_cas_uses_prepared_predecessor() -> None:
    cursor = _PublicationStateCursor([])
    request = {
        "target_mutation_outcome": "NOT_REQUIRED_EMPTY_SCOPE",
        "value_conversion_outcome": "NOT_APPLICABLE_NO_DATA",
        "database": "analytics",
        "target_table": "orders",
        "scope_id": "2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
        "scope_revision": 1,
        "operation_id": _digest("0"),
        "checkpoint_sha256": _digest("9"),
        "predecessor_checkpoint_sha256": None,
        "predecessor_checkpoint_version": None,
        "predecessor_checkpoint_operation_id": None,
        "clickhouse_commit_receipt_sha256": _digest("a"),
        "terminal_receipt_sha256": _digest("b"),
        "target_uuid": "00000000-0000-0000-0000-000000000001",
        "target_generation": 3,
        "target_generation_id": _digest("6"),
        "expected_journal_state": "PREPARED",
    }

    MssqlPublicationHeadStore(lambda table: table).publish(
        cursor,
        request,
        scope_exists=False,
    )

    assert "status = ?" in cursor.statements[-1]
    assert cursor.parameters[-1][-1] == "PREPARED"
