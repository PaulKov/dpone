"""Real native runtime session, durable completion and source-free recovery."""

from __future__ import annotations

import hashlib
import json
import sys
import time
from contextlib import contextmanager
from uuid import UUID

from tools.native_delivery_live_support.profiles import Dataset

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.manifest.mssql_native_policy import native_limits, native_window, validate_native_config
from dpone.runtime.connector_logging import etl_logger
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.etl.source_extraction_lifecycle import SourceExtractionLifecycleService
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import MssqlGenericTransactionFinalizer
from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource

from .bindings import admission_binding, storage_binding
from .configuration import load_configuration
from .faults import Faults, FaultStore, ReceiptDelegate, TargetDelegate
from .inventory import canonical
from .source_guard import source_guard


class Session:
    """No SQL or source handles survive run/recover/close boundaries."""

    def __init__(self, environment, inventory_store, inventory, clock):
        self.environment, self.inventory_store, self.inventory = environment, inventory_store, inventory
        self.ownership_id, self.clock = inventory.invocation_id, clock
        self.results = inventory_store.provisioned(self.ownership_id)
        self.directory = inventory_store.directory(self.ownership_id)
        self.faults = Faults()
        self.store = FaultStore(SQLiteWindowStore(self.directory / "journal.sqlite3", clock=time.time), self.faults)
        self.config = load_configuration(inventory)
        columns = Dataset(inventory.profile, inventory.rows, inventory.seed).schema()
        schema = [
            (c["name"], c["target"] + (" nullable" if c["source"].startswith("Nullable(") else "")) for c in columns
        ]
        self.query = f"SELECT * FROM `{inventory.source_database}`.`{inventory.schema}`"
        self.wire = build_mssql_bcp_native_contract(schema=schema, query=self.query, target_format="mssql_native")
        window = native_window(self.config)
        self.plan = NativeChunkPlan(
            self.ownership_id,
            self.results["attempt"]["target_identity"],
            self.ownership_id,
            hashlib.sha256(canonical(window.to_dict() if window else {"strategy": "full_refresh"})).hexdigest(),
            self.wire.schema_hash,
            self.wire.type_layout_hash,
        )
        self.journal_key = (
            "mssql-native-chunks-v1/" + hashlib.sha256(canonical([self.plan.target_id, self.plan.run_id])).hexdigest()
        )
        self.expectation_key = "local-expected/" + self.ownership_id
        self._payload = None
        self._source_allowed = True
        telemetry = self.store.load("local-telemetry/" + self.ownership_id)
        if telemetry:
            saved = json.loads(telemetry.payload)
            for key in ("armed", "events", "source_queries", "publications", "probes", "stage_reads", "known"):
                setattr(self.faults, key, saved[key])

    @property
    def invocation_id(self):
        """Canonical public UUID; durable paths and native identity remain hex."""
        return str(UUID(hex=self.ownership_id))

    def journal_data(self):
        record = self.store.load(self.journal_key)
        return json.loads(record.payload) if record else None

    def arm_fault(self, fault):
        self.faults.arm(fault)
        self._save_telemetry()

    def _save_telemetry(self):
        lease = self.store.acquire(self.plan.target_id, "telemetry:" + self.ownership_id, 60)
        try:
            key = "local-telemetry/" + self.ownership_id
            prior = self.store.load(key)
            data = {
                name: getattr(self.faults, name)
                for name in ("armed", "events", "source_queries", "publications", "probes", "stage_reads", "known")
            }
            self.store.save(key, prior.revision if prior else None, canonical(data).decode(), lease)
        finally:
            self.store.release(lease)

    def run(self):
        try:
            self._run()
        finally:
            self._save_telemetry()

    def _run(self):
        data = self.journal_data()
        if data and data.get("rollback_history"):
            raise RuntimeError("local_fixture.rollback_replay_not_supported")
        with self.environment.sql_scope() as raw_target, self.environment.sql_scope() as state_connector:
            target = TargetDelegate(raw_target, self.faults)
            storage = storage_binding(self.environment, self.inventory, self.results, state_connector)
            sink = MSSQLSink(
                target, state_storage=storage, logger=etl_logger, transaction_finalizer_factory=self._finalizer_factory
            )
            self.config, admission, state = admission_binding(
                self.config, self.inventory, self.results, target, storage, faults=self.faults
            )

            def bindings(config, owner, lease, lost):
                self._lease = lease
                self._state = ReceiptDelegate(state, self.faults, self.store, lease, self.expectation_key)
                context = compose_native_stage_context(
                    store=self.store,
                    plan=self.plan,
                    lease=lease,
                    wire_contract=self.wire,
                    limits=native_limits(config),
                    work_dir=self.directory / "spool",
                    target_connector=target,
                    importer_connection=self._importer_scope,
                    bcp_options_factory=BcpOptions,
                    database=self.inventory.target_database,
                    schema=self.inventory.schema,
                    row_source=self._rows,
                    journal_factory=lambda: NativeChunkJournal(self.store, lease, self.plan),
                    cancelled=lost,
                    required_target_headroom_bytes=native_limits(config).stage_allocated_bytes_stop_threshold,
                    interval=native_window(config),
                )
                self._context = context
                preparer = MssqlNativeStagePreparer(sink, lambda *_: context)
                service = MssqlNativeStagedLoadService(sink, preparer)
                return NativeRuntimeBindings(service, context, admission)

            (self.directory / "spool").mkdir(exist_ok=True, mode=0o700)
            runtime = NativeMssqlRuntime(
                store=self.store,
                target_id=self.plan.target_id,
                bindings=bindings,
                source=self._source,
                preflight=validate_native_config,
                quality=self._quality,
                evidence=self._evidence,
                advance_state=self._checkpoint,
            )
            runtime.run(self.config, owner=self.ownership_id)

    @contextmanager
    def _importer_scope(self):
        with self.environment.sql_scope() as connector:
            yield TargetDelegate(connector, self.faults)

    def _finalizer_factory(self, strategy, storage):
        real = MssqlGenericTransactionFinalizer(strategy, storage, transaction_state=self._state)
        session = self

        class Finalizer:
            def __getattr__(self, name):
                return getattr(real, name)

            def finalize(self, *args, **kwargs):
                session.faults.begin_finalizer()
                try:
                    result = real.finalize(*args, **kwargs)
                    session.faults.known = True
                    return result
                except BaseException:
                    if session.faults.rollback_acknowledged and not session.faults.commit_attempted:
                        session._context.journal_factory().publication.publication_rolled_back(
                            {
                                "operation_key": args[1].operation.operation_key.hex(),
                                "owner": session.ownership_id,
                                "fence": session._lease.fence,
                                "proof": "real_connector_rollback_acknowledged_before_commit",
                            }
                        )
                        session.faults.known = True
                    raise
                finally:
                    session.faults.active_finalizer = False

        return Finalizer()

    @contextmanager
    def _source(self, config, binding):
        if not self._source_allowed:
            raise AssertionError("local_fixture.poison_source_opened")
        connector = self.environment.clickhouse(binary=self.inventory.profile == "binary")
        extracted = None
        outcome = ArtifactTerminalOutcome.ABORT
        try:
            source = ClickHouseNativeSource(
                connector,
                schema_guard_factory=lambda _: source_guard(self.directory, connector, self.inventory, self.results),
            )
            extracted = SourceExtractionLifecycleService().capture(
                lambda: source.extract(config, query_id=self.plan.source_query_id)
            )
            from dpone.runtime.etl.mssql_schema_preplan import MSSQL_SCHEMA_PREPLAN_OPTION

            payload = LoadPayload(
                extracted.artifact,
                extracted.schema,
                relation_schema=extracted.relation_schema,
                relation_metadata=extracted.relation_metadata,
                relation_dialect=extracted.relation_dialect,
                mssql_transaction_admission=binding.admission,
                mssql_target_mutation_plan=config.options[MSSQL_SCHEMA_PREPLAN_OPTION].target_mutation_plan,
                extraction_lifecycle=extracted.extraction_lifecycle,
            )
            self._payload = payload
            yield payload
            outcome = ArtifactTerminalOutcome.SUCCESS
        finally:
            self._payload = None
            primary = sys.exc_info()[1]
            try:
                if extracted is not None:
                    terminal = extracted.artifact.terminate(outcome)
                    if not terminal.cleanup_succeeded:
                        if primary is None:
                            raise RuntimeError("local_fixture.source_cleanup_failed")
                        primary.add_note("local_fixture.source_cleanup_failed")
            finally:
                connector.close()

    def _rows(self):
        if not self._source_allowed or self._payload is None:
            raise AssertionError("local_fixture.poison_source_rows")
        self.clock.source_acquired()
        self.faults.source_queries += 1
        yield from self._payload.artifact.iter_native_rows()

    def _quality(self, config, handle, lease):
        self.store.assert_lease(lease)
        if handle.staged_rows != self.inventory.rows:
            raise ValueError("local_fixture.staged_row_count_mismatch")

    def _persist(self, phase, payload, lease):
        key = "local-" + phase + "/" + self.ownership_id
        prior = self.store.load(key)
        serialized = canonical(payload).decode()
        if prior is not None and prior.payload != serialized:
            raise ValueError("local_fixture.completion_changed")
        if prior is None:
            self.store.save(key, None, serialized, lease)

    def _evidence(self, config, result, context, lease):
        snapshot = self.snapshot()
        if snapshot.receipt_expected is None or snapshot.receipt_expected != snapshot.receipt_observed:
            raise ValueError("local_fixture.receipt_mismatch")
        if snapshot.metadata_expected is None or snapshot.metadata_expected != snapshot.metadata_observed:
            raise ValueError("local_fixture.metadata_mismatch")
        if self.clock.start is not None and self.clock.visible is None:
            self.clock.committed_visible()
        self._persist("evidence", {"receipt": snapshot.receipt_observed, "metadata": snapshot.metadata_observed}, lease)

    def _checkpoint(self, config, result, lease):
        evidence = self.store.load("local-evidence/" + self.ownership_id)
        if evidence is None:
            raise ValueError("local_fixture.evidence_required")
        self._persist(
            "checkpoint",
            {"receipt_id": result.commit_receipt_id, "evidence": hashlib.sha256(evidence.payload.encode()).hexdigest()},
            lease,
        )

    def recover(self, *, source_allowed):
        self._source_allowed = source_allowed
        self.run()

    def snapshot(self):
        from .observations import snapshot

        return snapshot(self)

    def cleanup(self):
        from .cleanup import cleanup

        cleanup(self)

    def close(self):
        """Connections are invocation-scoped; close does not mutate outcome or objects."""
