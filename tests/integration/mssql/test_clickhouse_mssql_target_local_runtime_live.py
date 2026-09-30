"""Synthetic ClickHouse rows through the complete target-local MSSQL runtime."""

from __future__ import annotations

import json
import os
import platform
import time
import uuid
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.integration.mssql.mssql_certification_identity import baked_source_identity
from tests.integration.mssql.mssql_live_support import clickhouse_connector, mssql_connector
from tools.mssql_stress_governance import governed_mssql_route

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal_v2 import NativeChunkJournalV2
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.adapters.mssql_native_delivery_evidence import write_mssql_native_delivery_evidence
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_native_chunks import NativeChunkLimits, NativeChunkPlan
from dpone.contracts.mssql_native_delivery_evidence import validate_mssql_native_delivery_evidence
from dpone.contracts.mssql_native_delivery_evidence_builder import (
    DeliveryCorrectness,
    DeliveryRecovery,
    DeliveryTimings,
    build_sqlclient_delivery_evidence,
)
from dpone.contracts.mssql_native_recovery_authority import restore_mssql_native_recovery_admission
from dpone.contracts.mssql_native_verification_identity import build_bcp_target_local_verification_identity
from dpone.contracts.mssql_sqlclient_ipc import MssqlSqlClientCredentials
from dpone.runtime.connector_logging import etl_logger
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.etl.mssql_schema_preplan import MSSQL_SCHEMA_PREPLAN_OPTION
from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION, MssqlTransactionAdmissionService
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService
from dpone.runtime.sinks.mssql_native_target_digest import build_target_digest_sql, decode_target_digest_row
from dpone.runtime.sinks.mssql_sqlclient_composition import compose_sqlclient_stage_context
from dpone.runtime.sources.clickhouse import ClickHouseSource
from dpone.services.mssql_native_evidence_privacy import scan_mssql_native_shareable_artifacts
from dpone.type_system.source_sink.provenance import SourceRelationDialect
from dpone.version import installed_version

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


@pytest.mark.parametrize("import_backend", ["bcp", "mssql_sqlclient"])
def test_clickhouse_mssql_target_local_runtime_orders_publication_and_cleanup(tmp_path, import_backend) -> None:
    if import_backend == "mssql_sqlclient" and os.environ.get("DPONE_RUN_SQLCLIENT_LIVE") != "1":
        pytest.skip("set DPONE_RUN_SQLCLIENT_LIVE=1 inside the certified Linux x86-64 runner")
    layout_version = int(os.environ.get("DPONE_SQLCLIENT_CERT_LAYOUT_VERSION", "1"))
    if layout_version not in {1, 2}:
        pytest.fail("DPONE_SQLCLIENT_CERT_LAYOUT_VERSION must select one or two")
    clickhouse = clickhouse_connector()
    target = mssql_connector()
    suffix = uuid.uuid4().hex[:16]
    source_table = f"dpone_runtime_source_{suffix}"
    target_table = f"dpone_runtime_target_{suffix}"
    target_id = f"synthetic-runtime-{suffix}"
    schema = (
        ("row_key", "bigint"),
        ("ratio", "float(53) nullable"),
        ("text_value", "nvarchar(max) nullable"),
        ("happened_at", "datetime2(6) nullable"),
    )
    wire = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format="mssql_native")
    plan = NativeChunkPlan(
        "synthetic-run", target_id, "synthetic-query", "synthetic-window", "synthetic-schema", wire.type_layout_hash
    )
    bcp_identity = build_bcp_target_local_verification_identity(plan, timeout_seconds=30)
    config = LoadConfig(
        "source",
        "target",
        clickhouse.database,
        source_table,
        "dbo",
        target_table,
        target_database=target.database,
        staging_database=target.database,
        staging_schema="dbo",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "source_type": "clickhouse",
            "sink_type": "mssql",
            "lineage": False,
            "__dpone_load_identity": {"run_id": "synthetic-run", "load_id": "synthetic-load"},
            "native_transfer": {
                "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
                "execution": {
                    "import_backend": import_backend,
                    "verification_backend": "target_local",
                    "chunking": {"mode": "bounded_stream", "checkpointing": "resumable", "parallelism": 1},
                    "native_chunks": {
                        "max_total_encoded_bytes": 8 << 20,
                        "stage_allocated_bytes_stop_threshold": 1 << 30,
                        "max_rows": 1024,
                        "max_bytes": 4 << 20,
                        "max_row_bytes": 1 << 20,
                        "max_pending": 1,
                        "max_staging_tables": 32,
                    },
                },
            },
        },
    )
    store = SQLiteWindowStore(tmp_path / "runtime-state.sqlite", clock=lambda: 1.0)
    observer = BoundedNativeDeliveryObserver()
    rows: list[tuple[object, ...]] = []
    bindings_seen: dict[str, object] = {}
    stages: dict[str, object] = {}
    backends_seen: dict[str, object] = {}
    evidence_artifacts: dict[str, Path] = {}
    order: list[str] = []
    timings: dict[str, float] = {}
    writer_observations = []
    source_entries = 0
    runtime_started = 0.0
    scopes = ExitStack()

    try:
        clickhouse.execute_query(
            f"CREATE TABLE `{source_table}` (row_key Int64, ratio Nullable(Float64), "
            "text_value Nullable(String), happened_at Nullable(DateTime64(6, 'UTC'))) ENGINE=Memory"
        )
        clickhouse.execute_query(
            f"INSERT INTO `{source_table}` VALUES "
            "(1,-1.5,'alpha','2024-02-29 23:59:59.999999'),"
            "(2,0.0,'',NULL),(3,NULL,NULL,'2030-06-01 12:30:45.123456'),"
            "(3,NULL,NULL,'2030-06-01 12:30:45.123456')"
        )
        target.execute_query(
            f"CREATE TABLE [dbo].[{target_table}] ("
            "[row_key] bigint NOT NULL,[ratio] float NULL,[text_value] nvarchar(max) NULL,"
            "[happened_at] datetime2(6) NULL)"
        )
        target.execute_query(f"INSERT INTO [dbo].[{target_table}] VALUES (-1,NULL,N'before-publication',NULL)")
        route = scopes.enter_context(
            governed_mssql_route(
                target,
                target_database=target.database,
                target_schema="dbo",
                target_table=target_table,
            )
        )
        sink = route.sink(logger=etl_logger)
        source_authority = ClickHouseSource(clickhouse, etl_logger, sink_connector=target)
        config = MssqlTransactionAdmissionService().prepare(
            config,
            source=source_authority,
            sink=sink,
            run_context=SimpleNamespace(
                run_id=f"synthetic-runtime-{suffix}",
                config={"pipeline_id": "sqlclient_certification", "task_id": "load"},
            ),
            load_record=SimpleNamespace(load_id=f"synthetic-{suffix}"),
            dag_id="sqlclient_certification",
        )
        admission = config.options[ADMISSION_OPTION]
        mutation = config.options[MSSQL_SCHEMA_PREPLAN_OPTION].target_mutation_plan
        fetched_schema = source_authority.fetch_schema_projection(config)

        @contextmanager
        def importer_connection():
            connector = mssql_connector()
            connector.get_records_iterator = lambda *_args, **_kwargs: pytest.fail(
                "target-local verification must not return business rows"
            )
            try:
                yield connector
            finally:
                connector.close()

        def credentials(_request):
            return MssqlSqlClientCredentials(
                host=target.host,
                port=target.port,
                database=target.database,
                username=target.user or "",
                password=target.password or "",
                encrypt=True,
                trust_server_certificate=target.trust_server_certificate == "yes",
            )

        def bindings(_cfg, _owner, lease, cancelled):
            options = dict(
                store=store,
                lease=lease,
                wire_contract=wire,
                limits=NativeChunkLimits(
                    max_total_encoded_bytes=8 << 20,
                    stage_allocated_bytes_stop_threshold=1 << 30,
                    max_rows=1024,
                    max_bytes=4 << 20,
                    max_row_bytes=1 << 20,
                    max_pending=1,
                    max_staging_tables=32,
                    parallelism=1,
                ),
                work_dir=tmp_path / "native-files",
                target_connector=target,
                importer_connection=importer_connection,
                bcp_options_factory=lambda **values: BcpOptions(bcp_path=target.bcp_path, **values),
                database=target.database,
                schema="dbo",
                row_source=lambda: iter(rows),
                journal_factory=lambda: pytest.fail("target-local route must select journal v2"),
                cancelled=cancelled,
                required_target_headroom_bytes=8 << 20,
                observer=observer,
            )
            if import_backend == "mssql_sqlclient":
                composed = compose_sqlclient_stage_context(
                    plan,
                    timeout_seconds=30,
                    credentials_provider=credentials,
                    write_observer=writer_observations.append,
                    persisted_hash_layout=layout_version == 2,
                    **options,
                )
                context, identity = composed.stage_context, composed.backend.identity
                backends_seen["backend"] = composed.backend
            else:
                identity = bcp_identity
                context = compose_native_stage_context(
                    **options,
                    plan=plan,
                    verification_identity=identity,
                    target_local_timeout_seconds=30,
                )
            preparer = MssqlNativeStagePreparer(sink, lambda *_args: context)
            service = MssqlNativeStagedLoadService(sink, preparer)
            bindings_seen["context"] = context
            return NativeRuntimeBindings(service, context, admission, identity)

        @contextmanager
        def source(_cfg, _binding):
            nonlocal source_entries
            source_entries += 1
            started = time.monotonic()
            fetched = clickhouse.get_records(
                f"SELECT row_key,ratio,text_value,happened_at FROM `{source_table}` ORDER BY row_key,happened_at"
            )
            rows[:] = [
                (key, ratio, text, happened.replace(tzinfo=None) if happened is not None else None)
                for key, ratio, text, happened in fetched
            ]
            timings["source_read_seconds"] = time.monotonic() - started
            timings["source_read_finished"] = time.monotonic()
            now = datetime(2026, 1, 1, tzinfo=UTC)
            yield SimpleNamespace(
                schema=schema,
                relation_schema=fetched_schema.relation_schema,
                relation_metadata=fetched_schema.relation_metadata,
                relation_dialect=SourceRelationDialect.CLICKHOUSE,
                target_projection=None,
                mssql_transaction_admission=admission,
                mssql_target_mutation_plan=mutation,
                require_completed_extraction=lambda: ExtractionLifecycleReceipt(
                    now, "synthetic.clickhouse", extraction_completed_at=now
                ),
            )

        def stage_names(journal):
            completed = journal.completed()
            publication = journal.publication.state()
            assert completed is not None and publication is not None
            raw = tuple(receipt.stage_id for receipt in completed.receipts)
            stage = publication["prepared"]["stage"]
            prepared = target.qualified_name(stage["schema"], stage["table"], database=stage["database"])
            return raw, prepared

        def assert_present(names):
            assert all(target.get_records("SELECT OBJECT_ID(?)", (name,))[0][0] is not None for name in names)

        def quality(_cfg, _handle, _lease):
            timings["preparation_seconds"] = time.monotonic() - timings["source_read_finished"]
            context = bindings_seen["context"]
            journal = context.journal_factory()
            assert journal.publication.state()["phase"] == "prepared"
            assert target.get_records(f"SELECT [row_key] FROM [dbo].[{target_table}]") == [(-1,)]
            raw, prepared = stage_names(journal)
            stages.update(raw=raw, prepared=prepared)
            assert_present((*raw, prepared))
            order.append("quality")

        def evidence(_cfg, _result, context, lease):
            journal = context.journal_factory()
            assert journal.publication.state()["phase"] == "published"
            receipt = journal.completed().receipts[0]
            digest_started = time.monotonic()
            aggregate = decode_target_digest_row(
                target.get_records(
                    build_target_digest_sql(f"[{target.database}].[dbo].[{target_table}]", wire, len(rows))
                )[0],
                expected_rows=len(rows),
            )
            assert aggregate.typed_digest == receipt.typed_digest
            timings["target_digest_seconds"] = time.monotonic() - digest_started
            timings["confirmed_visibility_seconds"] = time.monotonic() - runtime_started
            store.save("cert/evidence", None, json.dumps({"rows": aggregate.rows}), lease)
            if import_backend == "mssql_sqlclient":
                revision, pointer = _write_runtime_evidence(
                    root=os.environ.get("DPONE_SQLCLIENT_EVIDENCE_DIR"),
                    identity=backends_seen["backend"].identity,
                    companion=backends_seen["backend"].companion,
                    source_rows=len(rows),
                    published_rows=aggregate.rows,
                    receipt_count=len(journal.completed().receipts),
                    timings=timings,
                    writer_observations=writer_observations,
                    observations=observer.snapshot()["observations"],
                    privacy_needles=(target.host, target.database, target.user, target.password),
                )
                evidence_artifacts.update(revision=revision, pointer=pointer)
            order.append("evidence")

        def advance_state(_cfg, _result, lease):
            context = bindings_seen["context"]
            assert context.journal_factory().publication.state()["phase"] == "evidence-complete"
            assert store.load("cert/evidence") is not None
            if import_backend == "mssql_sqlclient":
                _assert_durable_evidence(evidence_artifacts)
            assert_present((*stages["raw"], stages["prepared"]))
            store.save("cert/checkpoint", None, json.dumps({"state": "advanced"}), lease)
            order.append("checkpoint")

        class AuditedCustody(NativeTargetCustody):
            def release(self, lease, invocation_key, reason, *, assert_release_authority):
                context = bindings_seen["context"]
                assert context.journal_factory().publication.state()["phase"] == "succeeded"
                assert store.load("cert/evidence") is not None and store.load("cert/checkpoint") is not None
                if import_backend == "mssql_sqlclient":
                    _assert_durable_evidence(evidence_artifacts)
                assert all(
                    target.get_records("SELECT OBJECT_ID(?)", (name,))[0][0] is None
                    for name in (*stages["raw"], stages["prepared"])
                )
                order.append("custody-release")
                return super().release(
                    lease,
                    invocation_key,
                    reason,
                    assert_release_authority=assert_release_authority,
                )

        runtime = NativeMssqlRuntime(
            store=store,
            target_id=target_id,
            bindings=bindings,
            source=source,
            preflight=lambda _cfg: None,
            quality=quality,
            evidence=evidence,
            advance_state=advance_state,
            custody_factory=AuditedCustody,
            v2_journal_admission=lambda journal, current: (
                isinstance(journal, NativeChunkJournalV2) and journal.identity == current
            ),
            lease_ttl=300,
            observer=observer,
        )

        runtime_started = time.monotonic()
        result = runtime.run(config, owner="synthetic-runtime")
        assert result.status == "success" and result.extracted_rows == len(rows) and result.final_rows == len(rows)
        assert source_entries == 1
        assert order == ["quality", "evidence", "checkpoint", "custody-release"]
        context = bindings_seen["context"]
        assert context.journal_factory().publication.state()["phase"] == "succeeded"
        completed_metadata = context.journal_factory().completed_metadata()
        restored_admission = restore_mssql_native_recovery_admission(
            completed_metadata["recovery_authority_v1"],
            expected_bindings=context.recovery_bindings(admission),
        )
        assert restored_admission.operation.operation_key == admission.operation.operation_key
        inspect_lease = store.acquire(target_id, "inspect", 60)
        custody = NativeTargetCustody(store, target_id).inspect(inspect_lease)
        assert custody.state == "clear" and custody.release_reason == "published_cleanup"
        store.release(inspect_lease)
        phases = [item["phase"] for item in observer.snapshot()["observations"]]
        assert {"raw_verify", "prepared_verify", "quality", "publish", "evidence", "checkpoint"} <= set(phases)
    finally:
        scopes.close()
        target.execute_query(f"DROP TABLE IF EXISTS [dbo].[{target_table}]")
        clickhouse.execute_query(f"DROP TABLE IF EXISTS `{source_table}`")
        target.close()
        clickhouse.close()


def _source_identity() -> tuple[str, bool]:
    """Bind certification to source identity baked into the immutable runner."""
    return baked_source_identity()[0], False


def _phase_seconds(observations: list[dict[str, object]], phase: str) -> float:
    selected = [item for item in observations if item["phase"] == phase]
    if not selected:
        raise AssertionError(f"missing measured phase: {phase}")
    return sum((int(item["end_monotonic_ns"]) - int(item["start_monotonic_ns"])) / 1e9 for item in selected)


def _write_runtime_evidence(
    *,
    root: str | None,
    identity: object,
    companion: object,
    source_rows: int,
    published_rows: int,
    receipt_count: int,
    timings: dict[str, float],
    writer_observations: list[object],
    observations: list[dict[str, object]],
    privacy_needles: tuple[object, ...],
) -> tuple[Path, Path]:
    if root is None:
        pytest.fail("DPONE_SQLCLIENT_EVIDENCE_DIR is required in certification mode")
    commit_sha, dirty = _source_identity()
    bulk_write_seconds = sum(float(item.metrics.write_seconds) for item in writer_observations)
    environment = {
        "architecture": platform.machine(),
        "python": platform.python_version(),
        "core_package": installed_version(),
        "companion_package": companion.package_version,
        "companion_artifact_sha256": companion.artifact_sha256,
        "runtime_identity_sha256": companion.runtime_identity_sha256,
        "protocol": companion.protocol,
    }
    environment_bytes = json.dumps(environment, sort_keys=True, separators=(",", ":")).encode()
    environment_receipt_sha256 = sha256(environment_bytes).hexdigest()
    runner_image_sha256 = os.environ.get("DPONE_CERTIFICATION_IMAGE_SHA256", "")
    if len(runner_image_sha256) != 64 or any(character not in "0123456789abcdef" for character in runner_image_sha256):
        pytest.fail("DPONE_CERTIFICATION_IMAGE_SHA256 is required in certification mode")
    payload = build_sqlclient_delivery_evidence(
        identity=identity,
        companion=companion,
        source_commit_sha=commit_sha,
        dirty=dirty,
        runner_image_sha256=runner_image_sha256,
        environment_receipt_sha256=environment_receipt_sha256,
        timings=DeliveryTimings(
            source_read_seconds=timings["source_read_seconds"],
            bulk_write_seconds=bulk_write_seconds,
            target_digest_seconds=timings["target_digest_seconds"],
            preparation_seconds=timings["preparation_seconds"],
            publication_seconds=_phase_seconds(observations, "publish"),
            confirmed_visibility_seconds=timings["confirmed_visibility_seconds"],
        ),
        correctness=DeliveryCorrectness(source_rows, published_rows, receipt_count, True, True),
        recovery=DeliveryRecovery(0, 0, "not_required"),
        synthetic_only=True,
        privacy_scan_status="PASS",
    )
    evidence_root = Path(root) / identity.invocation_key
    environment_path = evidence_root / f"environment.{environment_receipt_sha256}.json"
    evidence_root.mkdir(parents=True, exist_ok=True)
    environment_path.write_bytes(environment_bytes)
    revision = write_mssql_native_delivery_evidence(evidence_root, payload)
    pointer = evidence_root / "current.json"
    scan_mssql_native_shareable_artifacts(
        (revision, pointer, environment_path),
        secret_needles=tuple(str(value) for value in privacy_needles if value),
    )
    return revision, pointer


def _assert_durable_evidence(artifacts: dict[str, Path]) -> None:
    """Prove the immutable revision and atomic pointer before state or custody advances."""

    revision, pointer = artifacts["revision"], artifacts["pointer"]
    payload = json.loads(revision.read_text(encoding="utf-8"))
    validate_mssql_native_delivery_evidence(payload)
    assert sha256(revision.read_bytes()).hexdigest() == revision.stem
    assert json.loads(pointer.read_text(encoding="utf-8")) == {"schema_version": 1, "sha256": revision.stem}
