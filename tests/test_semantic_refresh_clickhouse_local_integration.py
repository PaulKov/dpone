"""Opt-in local Docker proof for the semantic-refresh publication boundary.

Run from the repository root:

    docker compose -f docker/docker-compose.integration.yml up -d clickhouse minio
    DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1 \
      uv run --extra s3 pytest \
      tests/test_semantic_refresh_clickhouse_local_integration.py -q

This is local integration evidence, not production route certification.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from threading import Thread
from urllib.request import Request, urlopen

import pytest

from dpone.adapters.semantic_refresh_artifact_memory import (
    InMemoryCreateOnlyArtifactStore,
    StaticSemanticRefreshArtifactAuthority,
    StaticSemanticRefreshSealAuthorization,
)
from dpone.adapters.semantic_refresh_artifact_reader import VersionPinnedSealedArtifactReader
from dpone.adapters.semantic_refresh_artifact_s3 import S3CreateOnlyArtifactStore
from dpone.adapters.semantic_refresh_artifact_s3_resolver import S3ArtifactStorePolicy
from dpone.adapters.semantic_refresh_clickhouse_authority import (
    StaticSemanticRefreshClickHousePublicationAuthority,
)
from dpone.adapters.semantic_refresh_clickhouse_callbacks import CallbackSemanticRefreshPublicationState
from dpone.adapters.semantic_refresh_clickhouse_connection import (
    ClickHouseConnectionAuthorityVerifier,
)
from dpone.adapters.semantic_refresh_clickhouse_http import (
    ClickHouseHttpSemanticRefreshGateway,
)
from dpone.adapters.semantic_refresh_clickhouse_http_client import (
    ClickHousePublicationHttpClient,
)
from dpone.adapters.semantic_refresh_clickhouse_http_queries import (
    operation_query_id,
    physical_query,
)
from dpone.adapters.semantic_refresh_clickhouse_quiescence import (
    ClickHouseAttemptQuiescenceError,
    ClickHouseHttpAttemptQuiescenceObserver,
)
from dpone.contracts.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationReceipt,
)
from dpone.ports.semantic_refresh_artifact_seal import ArtifactSealPlan
from dpone.ports.semantic_refresh_artifact_store import (
    ArtifactCreateConflict,
    ArtifactStoreUnavailable,
)
from dpone.ports.semantic_refresh_clickhouse_authority import (
    ClickHousePublicationAuthority,
    clickhouse_authority_rows_sha256,
    clickhouse_operation_table_names,
    clickhouse_plain_mergetree_physical_authority_rows,
)
from dpone.ports.semantic_refresh_clickhouse_connection import (
    ClickHouseClusterConnectionAuthority,
    clickhouse_cluster_topology_sha256,
)
from dpone.runtime.semantic_refresh_clickhouse_authority_state import (
    AuthorityBoundSemanticRefreshPublicationState,
)
from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHousePreparePlan,
    GroupedMultisetConformance,
    ShadowEquation,
)
from dpone.runtime.semantic_refresh_clickhouse_plan_factory import (
    ClickHouseHeadPublicationPlanFactory,
)
from dpone.runtime.semantic_refresh_clickhouse_service import ClickHousePublicationService
from dpone.runtime.semantic_refresh_parquet_codec import (
    DponeParquetV1Codec,
    SemanticRefreshParquetField,
)
from dpone.services.semantic_refresh_artifact import SealedArtifactService

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.integration_clickhouse]

if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)
if str(os.getenv("DPONE_RUN_INTEGRATION_LIVE", "0")).lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Live integration tests are disabled", allow_module_level=True)

boto3 = pytest.importorskip("boto3")
_LOCAL_KMS_KEY_ARN = "arn:aws:kms:dpone-semref-v2-key"
_LOCAL_CLUSTER_AUTHORITY_ID = "dpone-semref-local-clickhouse-24-8"


def _digest(char: str) -> str:
    return "sha256:" + char * 64


def _seal_authorization(
    *, operation_id: str, serializer_sha256: str, after_image_row_count: int
) -> SemanticRefreshSealAuthorizationReceipt:
    return SemanticRefreshSealAuthorizationReceipt.build(
        operation_id=operation_id,
        operation_plan_sha256=_digest("a"),
        model_unique_id="model.local.events",
        workflow_id=_digest("d"),
        workflow_plan_sha256=_digest("e"),
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("f"),
        attempt_binding_sha256=_digest("b"),
        fencing_epoch=1,
        journal_version=11,
        event_time_source_type="datetime2(6)",
        effective_key_template_sha256=_digest("0"),
        effective_key_mapping_sha256=_digest("1"),
        ordered_writable_schema_sha256=_digest("6"),
        serializer_sha256=serializer_sha256,
        parquet_schema_mapping_sha256=_digest("7"),
        clickhouse_input_mapping_sha256=_digest("8"),
        codec_mapping_certification_sha256=_digest("9"),
        before_image_relation_id="local.dpone_images.events_before",
        before_image_sha256=_digest("c"),
        before_image_row_count=2,
        after_image_relation_id="local.dpone_images.events_after",
        after_image_sha256=_digest("d"),
        after_image_row_count=after_image_row_count,
        model_build_receipt_sha256=_digest("d"),
        baseline_adoption_receipt_sha256=_digest("1"),
        route_certification_receipt_sha256=_digest("2"),
        writer_exclusivity_assurance_receipt_sha256=_digest("3"),
        utc_semantics_assurance_receipt_sha256=_digest("4"),
        ddl_freeze_assurance_receipt_sha256=_digest("5"),
        artifact_authority_sha256=_digest("6"),
        seal_policy_sha256=_digest("7"),
        created_at="2026-08-08T12:35:00Z",
        issuer_authority="mssql-protected-control/local",
        issuer_attestation_sha256=_digest("8"),
        issuer_signature_sha256=_digest("9"),
    )


def _local_s3_policy(
    *,
    bucket: str,
    artifact_prefix: str,
    writer_scope: str,
    retention_until: str,
    retention_issued_at: str | None = None,
    encryption_policy_sha256: str = _digest("3"),
    retention_policy_sha256: str = _digest("4"),
    conditional_create_authorized: bool = True,
) -> S3ArtifactStorePolicy:
    return S3ArtifactStorePolicy(
        provider_profile="MINIO_LOCAL_UNVERIFIED",
        endpoint_authority_id=os.getenv("DPONE_IT_MINIO_ENDPOINT", "http://127.0.0.1:59290"),
        bucket_or_container_authority_id=bucket,
        kms_key_authority_id=_LOCAL_KMS_KEY_ARN,
        capability_evidence_sha256=_digest("5"),
        writer_scope=writer_scope,
        artifact_prefix=artifact_prefix,
        encryption_policy_sha256=encryption_policy_sha256,
        retention_policy_id="local-compliance-1d",
        retention_policy_sha256=retention_policy_sha256,
        retention_days=1,
        retention_issued_at=(
            retention_issued_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        ),
        retention_until=retention_until,
        max_artifact_bytes=1_000_000,
        conditional_create_authorized=conditional_create_authorized,
        require_object_lock=True,
        object_lock_mode="COMPLIANCE",
    )


def _expected_schema_sha256() -> str:
    return clickhouse_authority_rows_sha256(
        (
            ("event_id", "Int64", 1, "", "", ""),
            ("occurred_at", "DateTime64(6, 'UTC')", 2, "", "", ""),
            ("amount", "Decimal(18, 2)", 3, "", "", ""),
        )
    )


def _expected_physical_sha256() -> str:
    return clickhouse_authority_rows_sha256(clickhouse_plain_mergetree_physical_authority_rows("event_id, occurred_at"))


def _publication_authority(
    plan: ClickHousePreparePlan,
    artifact_plan: ArtifactSealPlan,
) -> StaticSemanticRefreshClickHousePublicationAuthority:
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
                owner_id="local-owner",
                guard_resource_id=plan.target_resource_id,
                guard_status="HELD",
                target_resource_id=plan.target_resource_id,
                target_authority_id=plan.target_authority_id,
                clickhouse_cluster_authority_id=plan.clickhouse_cluster_authority_id,
                database=plan.database,
                target_table=plan.target_table,
                scope_id=plan.scope_id,
                scope_start=plan.scope_start,
                scope_end=plan.scope_end,
                scope_revision=plan.scope_revision,
                target_predecessor_generation_id=_digest("6"),
                scope_predecessor_operation_id=None,
                predecessor_target_generation=0,
                predecessor_target_uuid=plan.expected_target_uuid,
                predecessor_target_operation_id=_digest("5"),
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
                effective_key_mapping_sha256=artifact_plan.effective_key_mapping_sha256,
                route_certification_receipt_sha256=artifact_plan.route_certification_receipt_sha256,
                artifact_prefix=artifact_plan.artifact_prefix,
                artifact_provider=artifact_plan.provider,
                encryption_policy_sha256=artifact_plan.encryption_policy_sha256,
                retention_policy_sha256=artifact_plan.retention_policy_sha256,
                max_artifact_bytes=1_000_000,
                max_staging_rows=plan.max_staging_rows,
                max_target_scope_rows=plan.max_target_scope_rows,
                max_staging_bytes=plan.max_staging_bytes,
                max_shadow_bytes=plan.max_shadow_bytes,
                max_retained_backup_bytes=plan.max_retained_backup_bytes,
                max_total_transient_bytes=plan.max_total_transient_bytes,
            ),
        )
    )


def test_local_minio_seal_clickhouse_prepare_exchange_and_terminal_state() -> None:
    operation_slug = f"semref_{uuid.uuid4().hex[:12]}"
    operation_id = "sha256:" + hashlib.sha256(operation_slug.encode()).hexdigest()
    database = os.getenv("DPONE_IT_CH_DATABASE", "dpone_it")
    target = f"{operation_slug}_target"
    staging, shadow = clickhouse_operation_table_names(target, operation_id)
    bucket = os.getenv("DPONE_IT_MINIO_BUCKET", "dpone-semref-v2-live")
    s3 = _s3_client()
    tables = (target, staging, shadow)
    try:
        for table in tables:
            _ch(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
        _ch(
            f"CREATE TABLE `{database}`.`{target}` ("
            "event_id Int64, occurred_at DateTime64(6, 'UTC'), amount Decimal(18, 2)"
            ") ENGINE = MergeTree ORDER BY (event_id, occurred_at)"
        )
        _ch(
            f"INSERT INTO `{database}`.`{target}` VALUES "
            "(1, '2026-08-08 00:00:01.000000', 10.00),"
            "(2, '2026-08-08 00:00:02.000000', 20.00),"
            "(3, '2026-08-08 00:00:03.000000', 30.00)"
        )
        codec = DponeParquetV1Codec()
        parquet_fields = (
            SemanticRefreshParquetField("event_id", "bigint", False),
            SemanticRefreshParquetField("occurred_at", "datetime2(6)", False),
            SemanticRefreshParquetField("amount", "decimal(18,2)", False),
        )
        chunks = codec.encode(
            fields=parquet_fields,
            rows=(
                (2, datetime(2026, 8, 8, 0, 0, 2, tzinfo=UTC), Decimal("25.00")),
                (4, datetime(2026, 8, 8, 0, 0, 4, tzinfo=UTC), Decimal("40.00")),
            ),
            maximum_rows_per_chunk=10,
        )
        artifact_plan = ArtifactSealPlan(
            seal_authorization=_seal_authorization(
                operation_id=operation_id,
                serializer_sha256=codec.serializer_sha256,
                after_image_row_count=sum(chunk.row_count for chunk in chunks),
            ),
            artifact_prefix=f"semantic-refresh/{operation_id}",
            provider="s3",
            encryption_policy_sha256=_digest("3"),
            retention_policy_sha256=_digest("4"),
            encryption_scope="local-minio-integration",
            retention_until=(datetime.now(UTC) + timedelta(days=2))
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            chunks=chunks,
        )
        artifact_store = S3CreateOnlyArtifactStore(
            client=s3,
            bucket=bucket,
            operation_prefix=artifact_plan.artifact_prefix,
            policy=_local_s3_policy(
                bucket=bucket,
                artifact_prefix=artifact_plan.artifact_prefix,
                writer_scope=artifact_plan.encryption_scope,
                retention_until=artifact_plan.retention_until,
                encryption_policy_sha256=artifact_plan.encryption_policy_sha256,
                retention_policy_sha256=artifact_plan.retention_policy_sha256,
            ),
        )
        seal = SealedArtifactService(
            store=artifact_store,
            authority=StaticSemanticRefreshArtifactAuthority(
                authorized_inventory=artifact_plan.authority_inventory(),
            ),
        ).seal(artifact_plan)
        _assert_s3_version_authority(
            s3,
            bucket=bucket,
            key=seal.manifest.key,
            version=seal.manifest.version,
            sha256=seal.manifest.sha256,
            encryption_scope=artifact_plan.encryption_scope,
            retention_until=artifact_plan.retention_until,
        )
        _assert_manifest_version(
            s3,
            bucket=bucket,
            plan={
                "artifact_manifest_key": seal.manifest.key,
                "artifact_manifest_version": seal.manifest.version,
                "artifact_manifest_sha256": seal.manifest.sha256,
                "operation_id": operation_id,
            },
        )
        with pytest.raises(ArtifactCreateConflict):
            artifact_store.create(
                key=seal.manifest.key,
                content=b"different",
                sha256="sha256:" + hashlib.sha256(b"different").hexdigest(),
                encryption_scope=artifact_plan.encryption_scope,
                retention_until=artifact_plan.retention_until,
            )
        plan = ClickHousePreparePlan(
            operation_id=operation_id,
            operation_plan_sha256=_digest("a"),
            workflow_plan_sha256=_digest("e"),
            workflow_execution_id="scheduled__2026-08-08",
            workflow_execution_binding_sha256=_digest("f"),
            attempt_binding_sha256=_digest("b"),
            fence_epoch=1,
            artifact_manifest_key=seal.manifest.key,
            artifact_manifest_version=seal.manifest.version,
            artifact_manifest_sha256=seal.sealed_manifest.artifact_manifest_sha256,
            target_resource_id=f"clickhouse-target://{database}/{target}",
            target_authority_id=f"clickhouse://{_LOCAL_CLUSTER_AUTHORITY_ID}/{database}/{target}",
            clickhouse_cluster_authority_id=_LOCAL_CLUSTER_AUTHORITY_ID,
            database=database,
            target_table=target,
            scope_id="2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
            scope_start="2026-08-08T00:00:00Z",
            scope_end="2026-08-09T00:00:00Z",
            scope_revision=1,
            event_time_column="occurred_at",
            staging_table=staging,
            shadow_table=shadow,
            expected_target_uuid=_table_uuid(database, target),
            expected_schema_sha256=_expected_schema_sha256(),
            expected_physical_sha256=_expected_physical_sha256(),
            business_columns=("event_id", "occurred_at", "amount"),
            effective_key_columns=("event_id", "occurred_at"),
            max_staging_rows=10,
            max_target_scope_rows=10,
            max_staging_bytes=10_000_000,
            max_shadow_bytes=10_000_000,
            max_retained_backup_bytes=10_000_000,
            max_total_transient_bytes=30_000_000,
            shadow_equation=ShadowEquation(
                retained_target_rule="TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY",
                append_rule="APPEND_ALL_STAGING_ROWS",
            ),
            conformance=GroupedMultisetConformance(mode="BIDIRECTIONAL_GROUPED_MULTISET"),
            artifact_chunk_count=seal.sealed_manifest.chunk_count,
            artifact_total_rows=seal.sealed_manifest.total_rows,
        )
        publication_authority = _publication_authority(plan, artifact_plan)
        clickhouse_client = _clickhouse_client()
        gateway = ClickHouseHttpSemanticRefreshGateway(
            client=clickhouse_client,
            authority=publication_authority,
            artifact_reader=VersionPinnedSealedArtifactReader(artifact_store),
            seal_authorization=StaticSemanticRefreshSealAuthorization((artifact_plan.seal_authorization,)),
            connection_authority=_local_connection_authority(clickhouse_client),
        )
        state_requests: list[Mapping[str, object]] = []

        def publish(request: Mapping[str, object]) -> Mapping[str, object]:
            state_requests.append(request)
            return {**request, "committed": True, "atomic": True, "journal_state": "COMPLETE"}

        def transition(request: Mapping[str, object]) -> Mapping[str, object]:
            return {
                **request,
                "committed": True,
                "atomic": True,
                "journal_state": request["next_journal_state"],
            }

        service = ClickHousePublicationService(
            gateway=gateway,
            state=AuthorityBoundSemanticRefreshPublicationState(
                authority=publication_authority,
                delegate=CallbackSemanticRefreshPublicationState(
                    persist_prepared_callback=transition,
                    mark_committing_callback=transition,
                    publish_callback=publish,
                ),
            ),
            authority=publication_authority,
        )
        prepared = service.prepare(plan)
        receipt = service.commit(
            plan,
            prepared=prepared,
            heads=ClickHouseHeadPublicationPlanFactory.build(
                publication_authority.records[0],
                prepared,
            ),
        )

        assert receipt.status == "COMPLETE"
        assert receipt.cleanup_status == "COMPLETE"
        assert receipt.retained_generation_status == "RETAINED_FOR_POLICY"
        assert state_requests and state_requests[0]["target_uuid"] == prepared.shadow_uuid
        assert _ch(
            f"SELECT event_id, toString(amount) FROM `{database}`.`{target}` ORDER BY event_id",
            read=True,
        ).splitlines() == ["1\t10", "2\t25", "3\t30", "4\t40"]
        assert _operation_relations(database, staging, shadow) == [shadow]
    finally:
        for table in tables:
            _ch(f"DROP TABLE IF EXISTS `{database}`.`{table}`")


def test_local_s3_adapter_fails_closed_on_missing_provider_capabilities() -> None:
    s3 = _s3_client()
    suffix = uuid.uuid4().hex[:16]
    buckets = {
        "unversioned": f"dpone-semref-unversioned-{suffix}",
        "unlocked": f"dpone-semref-unlocked-{suffix}",
        "conditional": f"dpone-semref-conditional-{suffix}",
    }
    s3.create_bucket(Bucket=buckets["unversioned"])
    s3.create_bucket(Bucket=buckets["unlocked"])
    s3.put_bucket_versioning(
        Bucket=buckets["unlocked"],
        VersioningConfiguration={"Status": "Enabled"},
    )
    s3.create_bucket(Bucket=buckets["conditional"], ObjectLockEnabledForBucket=True)
    retention_until = (datetime.now(UTC) + timedelta(days=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    try:
        for label, conditional, message in (
            ("unversioned", True, "versioning"),
            ("unlocked", True, "object lock"),
            ("conditional", False, "conditional create"),
        ):
            store = S3CreateOnlyArtifactStore(
                client=s3,
                bucket=buckets[label],
                operation_prefix="semantic-refresh/probe",
                policy=_local_s3_policy(
                    bucket=buckets[label],
                    artifact_prefix="semantic-refresh/probe",
                    writer_scope="local-negative",
                    retention_until=retention_until,
                    conditional_create_authorized=conditional,
                ),
            )
            with pytest.raises(ArtifactStoreUnavailable, match=message):
                store.create(
                    key="semantic-refresh/probe/manifest.json",
                    content=b"probe",
                    sha256="sha256:" + hashlib.sha256(b"probe").hexdigest(),
                    encryption_scope="local-negative",
                    retention_until=retention_until,
                )
    finally:
        for bucket_name in buckets.values():
            _delete_bucket_versions(s3, bucket_name)


def test_local_physical_authority_observes_materialized_view_writer() -> None:
    suffix = uuid.uuid4().hex[:12]
    database = os.getenv("DPONE_IT_CH_DATABASE", "dpone_it")
    target = f"semref_mv_target_{suffix}"
    source = f"semref_mv_source_{suffix}"
    view = f"semref_mv_{suffix}"
    try:
        _ch(f"CREATE TABLE `{database}`.`{source}` (event_id Int64) ENGINE = MergeTree ORDER BY event_id")
        _ch(f"CREATE TABLE `{database}`.`{target}` (event_id Int64) ENGINE = MergeTree ORDER BY event_id")
        _ch(
            f"CREATE MATERIALIZED VIEW `{database}`.`{view}` TO `{database}`.`{target}` "
            f"AS SELECT event_id FROM `{database}`.`{source}`"
        )

        fields = _ch(physical_query(database, target), read=True).split("\t")

        assert len(fields) == 15
        assert fields[12] == "1"
    finally:
        _ch(f"DROP VIEW IF EXISTS `{database}`.`{view}`")
        _ch(f"DROP TABLE IF EXISTS `{database}`.`{target}`")
        _ch(f"DROP TABLE IF EXISTS `{database}`.`{source}`")


def test_local_continuation_observer_blocks_active_operation_query() -> None:
    binding = "sha256:" + "a" * 64
    operation = "sha256:" + "b" * 64
    attempt = "sha256:" + "c" * 64
    client = _clickhouse_client()
    observer = ClickHouseHttpAttemptQuiescenceObserver(
        client=client,
        connection=ClickHouseConnectionAuthorityVerifier(
            client=client,
            authority=_local_connection_authority(client),
        ),
    )
    failure: list[BaseException] = []

    def run_query() -> None:
        try:
            client.execute_operation(
                "SELECT sleep(3)",
                query_id=operation_query_id(binding, operation, "live_quiescence"),
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            failure.append(exc)

    worker = Thread(target=run_query, daemon=True)
    worker.start()
    blocked = False
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not blocked:
        try:
            observer.prove_quiescent(
                workflow_execution_binding_sha256=binding,
                operation_id=operation,
                original_attempt_binding_sha256=attempt,
                clickhouse_cluster_authority_id=_LOCAL_CLUSTER_AUTHORITY_ID,
                observed_at="2026-08-10T08:00:00.000000Z",
            )
        except ClickHouseAttemptQuiescenceError as exc:
            blocked = "still active" in str(exc)
        if not blocked:
            time.sleep(0.05)

    worker.join(timeout=5)
    assert blocked is True
    assert worker.is_alive() is False
    assert failure == []
    assert (
        observer.prove_quiescent(
            workflow_execution_binding_sha256=binding,
            operation_id=operation,
            original_attempt_binding_sha256=attempt,
            clickhouse_cluster_authority_id=_LOCAL_CLUSTER_AUTHORITY_ID,
            observed_at="2026-08-10T08:00:04.000000Z",
        ).active_query_ids
        == ()
    )


def test_local_physical_authority_observes_every_forbidden_table_feature() -> None:
    suffix = uuid.uuid4().hex[:12]
    database = os.getenv("DPONE_IT_CH_DATABASE", "dpone_it")
    target = f"semref_physical_{suffix}"
    try:
        _ch(
            f"CREATE TABLE `{database}`.`{target}` ("
            "event_id UInt64 CODEC(ZSTD), occurred_at DateTime, "
            "INDEX event_index event_id TYPE minmax GRANULARITY 1, "
            "CONSTRAINT positive_event CHECK event_id >= 0, "
            "PROJECTION event_projection (SELECT event_id ORDER BY event_id)"
            ") ENGINE = MergeTree ORDER BY event_id "
            "TTL occurred_at + INTERVAL 1 DAY"
        )

        fields = _ch(physical_query(database, target), read=True).split("\t")

        assert len(fields) == 15
        assert fields[7:12] == ["1", "1", "1", "1", "1"]
        assert fields[13] == "1"
    finally:
        _ch(f"DROP TABLE IF EXISTS `{database}`.`{target}`")


def test_local_clickhouse_http_gateway_reconciles_lost_exchange_ack() -> None:
    operation_slug = f"semref_http_{uuid.uuid4().hex[:10]}"
    operation_id = "sha256:" + hashlib.sha256(operation_slug.encode()).hexdigest()
    database = os.getenv("DPONE_IT_CH_DATABASE", "dpone_it")
    target = f"{operation_slug}_target"
    staging, shadow = clickhouse_operation_table_names(target, operation_id)
    tables = (target, staging, shadow)
    try:
        for table in tables:
            _ch(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
        _ch(
            f"CREATE TABLE `{database}`.`{target}` ("
            "event_id Int64, occurred_at DateTime64(6, 'UTC'), amount Decimal(18, 2)"
            ") ENGINE = MergeTree ORDER BY (event_id, occurred_at)"
        )
        _ch(
            f"INSERT INTO `{database}`.`{target}` VALUES "
            "(1, '2026-08-08 00:00:01.000000', 10.00),"
            "(2, '2026-08-08 00:00:02.000000', 20.00)"
        )
        codec = DponeParquetV1Codec()
        fields = (
            SemanticRefreshParquetField("event_id", "bigint", False),
            SemanticRefreshParquetField("occurred_at", "datetime2(6)", False),
            SemanticRefreshParquetField("amount", "decimal(18,2)", False),
        )
        chunks = codec.encode(
            fields=fields,
            rows=(
                (2, datetime(2026, 8, 8, 0, 0, 2, tzinfo=UTC), Decimal("25.00")),
                (3, datetime(2026, 8, 8, 0, 0, 3, tzinfo=UTC), Decimal("30.00")),
            ),
            maximum_rows_per_chunk=10,
        )
        artifact_plan = ArtifactSealPlan(
            seal_authorization=_seal_authorization(
                operation_id=operation_id,
                serializer_sha256=codec.serializer_sha256,
                after_image_row_count=sum(chunk.row_count for chunk in chunks),
            ),
            artifact_prefix=f"semantic-refresh/{operation_id}",
            provider="s3",
            encryption_policy_sha256=_digest("3"),
            retention_policy_sha256=_digest("4"),
            encryption_scope="local-clickhouse",
            retention_until="2026-09-08T00:00:00Z",
            chunks=chunks,
        )
        store = InMemoryCreateOnlyArtifactStore()
        seal = SealedArtifactService(
            store=store,
            authority=StaticSemanticRefreshArtifactAuthority(artifact_plan.authority_inventory()),
        ).seal(artifact_plan)
        plan = ClickHousePreparePlan(
            operation_id=operation_id,
            operation_plan_sha256=_digest("a"),
            workflow_plan_sha256=_digest("e"),
            workflow_execution_id="scheduled__2026-08-08",
            workflow_execution_binding_sha256=_digest("f"),
            attempt_binding_sha256=_digest("b"),
            fence_epoch=1,
            artifact_manifest_key=seal.manifest.key,
            artifact_manifest_version=seal.manifest.version,
            artifact_manifest_sha256=seal.sealed_manifest.artifact_manifest_sha256,
            target_resource_id=f"clickhouse-target://{database}/{target}",
            target_authority_id=f"clickhouse://{_LOCAL_CLUSTER_AUTHORITY_ID}/{database}/{target}",
            clickhouse_cluster_authority_id=_LOCAL_CLUSTER_AUTHORITY_ID,
            database=database,
            target_table=target,
            scope_id="2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
            scope_start="2026-08-08T00:00:00Z",
            scope_end="2026-08-09T00:00:00Z",
            scope_revision=1,
            event_time_column="occurred_at",
            staging_table=staging,
            shadow_table=shadow,
            expected_target_uuid=_table_uuid(database, target),
            expected_schema_sha256=_expected_schema_sha256(),
            expected_physical_sha256=_expected_physical_sha256(),
            business_columns=("event_id", "occurred_at", "amount"),
            effective_key_columns=("event_id", "occurred_at"),
            max_staging_rows=10,
            max_target_scope_rows=10,
            max_staging_bytes=10_000_000,
            max_shadow_bytes=10_000_000,
            max_retained_backup_bytes=10_000_000,
            max_total_transient_bytes=30_000_000,
            shadow_equation=ShadowEquation(
                retained_target_rule="TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY",
                append_rule="APPEND_ALL_STAGING_ROWS",
            ),
            conformance=GroupedMultisetConformance(mode="BIDIRECTIONAL_GROUPED_MULTISET"),
            artifact_chunk_count=seal.sealed_manifest.chunk_count,
            artifact_total_rows=seal.sealed_manifest.total_rows,
        )
        publication_authority = _publication_authority(plan, artifact_plan)
        clickhouse_client = _clickhouse_client(ack_loss=True)
        gateway = ClickHouseHttpSemanticRefreshGateway(
            client=clickhouse_client,
            authority=publication_authority,
            artifact_reader=VersionPinnedSealedArtifactReader(store),
            seal_authorization=StaticSemanticRefreshSealAuthorization((artifact_plan.seal_authorization,)),
            connection_authority=_local_connection_authority(clickhouse_client),
        )

        def transition(request: Mapping[str, object]) -> Mapping[str, object]:
            return {
                **request,
                "committed": True,
                "atomic": True,
                "journal_state": request["next_journal_state"],
            }

        def publish(request: Mapping[str, object]) -> Mapping[str, object]:
            return {**request, "committed": True, "atomic": True, "journal_state": "COMPLETE"}

        service = ClickHousePublicationService(
            gateway=gateway,
            state=AuthorityBoundSemanticRefreshPublicationState(
                authority=publication_authority,
                delegate=CallbackSemanticRefreshPublicationState(
                    persist_prepared_callback=transition,
                    mark_committing_callback=transition,
                    publish_callback=publish,
                ),
            ),
            authority=publication_authority,
        )
        prepared = service.prepare(plan)
        receipt = service.commit(
            plan,
            prepared=prepared,
            heads=ClickHouseHeadPublicationPlanFactory.build(
                publication_authority.records[0],
                prepared,
            ),
        )

        assert receipt.status == "COMPLETE"
        assert receipt.cleanup_status == "COMPLETE"
        assert receipt.retained_generation_status == "RETAINED_FOR_POLICY"
        assert _ch(
            f"SELECT event_id, toString(amount) FROM `{database}`.`{target}` ORDER BY event_id",
            read=True,
        ).splitlines() == ["1\t10", "2\t25", "3\t30"]
        assert _operation_relations(database, staging, shadow) == [shadow]
    finally:
        for table in tables:
            _ch(f"DROP TABLE IF EXISTS `{database}`.`{table}`")


def _s3_client():
    access_key, secret_key = _local_minio_credentials()
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("DPONE_IT_MINIO_ENDPOINT", "http://127.0.0.1:59290"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def _operation_relations(database: str, staging: str, shadow: str) -> list[str]:
    rows = _ch(
        "SELECT name FROM system.tables "
        f"WHERE database = '{database}' AND name IN ('{staging}', '{shadow}') ORDER BY name",
        read=True,
    )
    return rows.splitlines() if rows else []


def _local_minio_credentials() -> tuple[str, str]:
    access_key = os.getenv("DPONE_IT_MINIO_ACCESS_KEY")
    secret_key = os.getenv("DPONE_IT_MINIO_SECRET_KEY")
    if access_key and secret_key:
        return access_key, secret_key
    result = subprocess.run(  # noqa: S603 - exact opt-in local Docker container
        ["docker", "inspect", "dpone-semref-v2-minio-kms"],
        check=True,
        capture_output=True,
        text=True,
    )
    document = json.loads(result.stdout)
    environment = document[0]["Config"]["Env"]
    values = dict(item.split("=", maxsplit=1) for item in environment if "=" in item)
    return values["MINIO_ROOT_USER"], values["MINIO_ROOT_PASSWORD"]


def _assert_manifest_version(s3, *, bucket: str, plan: Mapping[str, object]) -> None:
    response = s3.get_object(
        Bucket=bucket,
        Key=str(plan["artifact_manifest_key"]),
        VersionId=str(plan["artifact_manifest_version"]),
    )
    content = bytes(response["Body"].read())
    assert "sha256:" + hashlib.sha256(content).hexdigest() == plan["artifact_manifest_sha256"]
    manifest = json.loads(content)
    assert manifest["operation_id"] == plan["operation_id"]


def _assert_s3_version_authority(
    s3,
    *,
    bucket: str,
    key: str,
    version: str,
    sha256: str,
    encryption_scope: str,
    retention_until: str,
) -> None:
    response = s3.head_object(Bucket=bucket, Key=key, VersionId=version)
    assert response["VersionId"] == version
    assert response["Metadata"]["dpone-sha256"] == sha256
    assert response["Metadata"]["dpone-encryption-scope"] == encryption_scope
    assert response["Metadata"]["dpone-retention-until"] == retention_until
    assert response["ServerSideEncryption"] == "aws:kms"
    assert response["SSEKMSKeyId"] == _LOCAL_KMS_KEY_ARN
    assert response["ObjectLockMode"] == "COMPLIANCE"


class _AckLossClickHouseClient(ClickHousePublicationHttpClient):
    def execute_operation(
        self,
        statement: str,
        *,
        query_id: str,
    ) -> list[tuple[object, ...]]:
        if statement.startswith("EXCHANGE TABLES"):
            super().execute_operation(statement, query_id=query_id)
            raise TimeoutError("simulated lost exchange acknowledgement")
        return super().execute_operation(statement, query_id=query_id)


def _clickhouse_client(*, ack_loss: bool = False) -> ClickHousePublicationHttpClient:
    client_class = _AckLossClickHouseClient if ack_loss else ClickHousePublicationHttpClient
    return client_class(
        endpoint=os.getenv("DPONE_IT_CH_HTTP_URL", "http://127.0.0.1:58123/"),
        username=os.getenv("DPONE_IT_CH_USER", "default"),
        password=os.getenv("DPONE_IT_CH_PASSWORD", "dpone"),
        timeout_seconds=20,
        transport_profile="LOCAL_HTTP_UNVERIFIED",
    )


def _local_connection_authority(
    client: ClickHousePublicationHttpClient,
) -> ClickHouseClusterConnectionAuthority:
    topology = (("localhost", 9000, 1, 1),)
    return ClickHouseClusterConnectionAuthority(
        clickhouse_cluster_authority_id=_LOCAL_CLUSTER_AUTHORITY_ID,
        endpoint_authority_id=client.endpoint_authority_id,
        cluster_name="default",
        host_names=("localhost",),
        topology_sha256=clickhouse_cluster_topology_sha256(topology),
    )


def _ch(sql: str, *, read: bool = False) -> str:
    url = os.getenv("DPONE_IT_CH_HTTP_URL", "http://127.0.0.1:58123/")
    user = os.getenv("DPONE_IT_CH_USER", "default")
    password = os.getenv("DPONE_IT_CH_PASSWORD", "dpone")
    request = Request(url, data=sql.encode("utf-8"), method="POST")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    with urlopen(request, timeout=20) as response:  # noqa: S310 - explicit opt-in local endpoint
        body = response.read().decode("utf-8").rstrip("\r\n")
    return body if read else body


def _scalar(sql: str) -> int:
    return int(_ch(sql, read=True))


def _table_uuid(database: str, table: str) -> str:
    return _ch(
        f"SELECT toString(uuid) FROM system.tables WHERE database = '{database}' AND name = '{table}'",
        read=True,
    )


def _database_engine(database: str) -> str:
    return _ch(
        f"SELECT engine FROM system.databases WHERE name = '{database}'",
        read=True,
    )


def _table_engine(database: str, table: str) -> str:
    return _ch(
        f"SELECT engine FROM system.tables WHERE database = '{database}' AND name = '{table}'",
        read=True,
    )


def _duplicate_groups(database: str, table: str) -> int:
    return _scalar(
        "SELECT count() FROM ("
        f"SELECT event_id, occurred_at FROM `{database}`.`{table}` "
        "GROUP BY event_id, occurred_at HAVING count() > 1)"
    )


def _delete_bucket_versions(s3, bucket: str) -> None:
    try:
        versions = s3.list_object_versions(Bucket=bucket)
        objects = [
            {"Key": item["Key"], "VersionId": item["VersionId"]}
            for field in ("Versions", "DeleteMarkers")
            for item in versions.get(field, [])
        ]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        s3.delete_bucket(Bucket=bucket)
    except Exception:  # noqa: BLE001 - best-effort cleanup of the exact unique test bucket
        pass
