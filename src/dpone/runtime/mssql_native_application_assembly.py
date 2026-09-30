"""Process-local service assembly for the MSSQL native application runtime."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from contextlib import contextmanager
from hashlib import sha256
from typing import Any

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.adapters.mssql_native_recovery_plan import persist_mssql_native_recovery_plan
from dpone.manifest.mssql_native_policy import (
    native_import_backend,
    native_limits,
    native_sqlclient_layout_version,
    native_verification_backend,
    native_window,
)
from dpone.ports.mssql_native import (
    MssqlSqlClientCredentials,
    NativeChunkPlan,
    NativeStageWriteObservation,
    build_bcp_target_local_verification_identity,
)
from dpone.ports.native_delivery_observer import NativeDeliveryObserver
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.etl.mssql_schema_preplan import MSSQL_SCHEMA_PREPLAN_OPTION
from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService
from dpone.runtime.sinks.mssql_sqlclient_composition import compose_sqlclient_stage_context
from dpone.runtime.sources.clickhouse_native_guard import ClickHouseSchemaStabilityGuard
from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource
from dpone.runtime.storage_policy import RuntimeStoragePolicy, StoragePreflightService

_TIMEOUT_SECONDS = 3600


class _NativeRuntimeAssembly:
    """Own one fully admitted runtime and its process-local resources."""

    def __init__(
        self,
        process: Any,
        config: Any,
        *,
        observer: NativeDeliveryObserver | None = None,
        write_observer: Callable[[NativeStageWriteObservation], None] | None = None,
    ) -> None:
        self.process, self.config = process, config
        self.source, self.sink = process.source_obj, process.sink_obj
        self.target = self.sink.connector
        self.admission = config.options[ADMISSION_OPTION]
        self.preplan = config.options[MSSQL_SCHEMA_PREPLAN_OPTION]
        self.source_projection = self.source.fetch_schema_projection(config)
        self.schema = _wire_schema(self.source_projection.relation_schema, self.preplan)
        self.wire = build_mssql_bcp_native_contract(
            schema=self.schema,
            query=_digest((self.preplan.source_relation_identity, self.preplan.source_schema_sha256.hex())),
            target_format="mssql_native",
        )
        self.plan = _chunk_plan(config, self.admission, self.preplan, self.wire.type_layout_hash)
        policy = RuntimeStoragePolicy.from_sources(
            runtime=(process.raw_config or {}).get("runtime"),
            source_options=config.options,
        )
        preflight = StoragePreflightService().check(policy)
        if not preflight.passed:
            raise ValueError("mssql_native.storage_preflight_failed:" + ",".join(preflight.blockers))
        root = policy.work_dir / "mssql-native" / self.plan.target_id
        root.mkdir(parents=True, exist_ok=True)
        state_root = policy.checkpoint_dir / "mssql-native"
        self.store = SQLiteWindowStore(state_root / f"{self.plan.target_id}.sqlite", clock=time.time)
        self.work_dir = root
        self._recovery_only = False
        self._initialize_runtime(observer=observer, write_observer=write_observer)

    @classmethod
    def for_recovery(
        cls,
        process: Any,
        config: Any,
        *,
        admission: Any,
        identity: Any,
        store: SQLiteWindowStore,
        work_dir: Any,
        schema: tuple[tuple[str, str], ...],
    ) -> _NativeRuntimeAssembly:
        """Compose the same target services from sealed EOF authority only."""

        self = cls.__new__(cls)
        self.process, self.config = process, config
        self.source, self.sink = process.source_obj, process.sink_obj
        self.target = self.sink.connector
        self.admission = admission
        self.preplan = None
        self.source_projection = None
        self.schema = schema
        self.wire = build_mssql_bcp_native_contract(
            schema=schema,
            query=identity.plan.source_query_id,
            target_format="mssql_native",
        )
        if self.wire.type_layout_hash != identity.plan.wire_fingerprint:
            raise ValueError("mssql_native.recovery_wire_changed")
        self.plan = identity.plan
        self.store, self.work_dir = store, work_dir
        self._artifact = None
        self._recovery_only = True
        self._initialize_runtime()
        return self

    def _initialize_runtime(
        self,
        *,
        observer: NativeDeliveryObserver | None = None,
        write_observer: Callable[[NativeStageWriteObservation], None] | None = None,
    ) -> None:
        self.observer = observer if observer is not None else BoundedNativeDeliveryObserver()
        self._write_observer = write_observer
        if not hasattr(self, "_artifact"):
            self._artifact: Any = None
        self.runtime = NativeMssqlRuntime(
            store=self.store,
            target_id=self.plan.target_id,
            bindings=self._bindings,
            source=self._source,
            preflight=lambda _config: None,
            quality=lambda _config, _handle, _lease: None,
            evidence=self._evidence,
            advance_state=self._advance_state,
            lease_ttl=60,
            observer=self.observer,
        )

    def _bindings(self, config: Any, _owner: str, lease: Any, cancelled: Any) -> NativeRuntimeBindings:
        limits = native_limits(config)

        @contextmanager
        def importer_connection():
            connector = self.target.open_session(application_name="dpone-native-import")
            try:
                yield connector
            finally:
                connector.close()

        common = dict(
            store=self.store,
            lease=lease,
            wire_contract=self.wire,
            limits=limits,
            work_dir=self.work_dir,
            target_connector=self.target,
            importer_connection=importer_connection,
            bcp_options_factory=lambda **values: BcpOptions(
                bcp_path=self.target.bcp_path,
                timeout_seconds=_TIMEOUT_SECONDS,
                **values,
            ),
            database=str(config.target_database),
            schema=str(config.staging_schema or config.target_schema).split(".")[-1],
            row_source=lambda: self._artifact.iter_native_rows(),
            journal_factory=lambda: NativeChunkJournal(self.store, lease, self.plan),
            cancelled=cancelled,
            required_target_headroom_bytes=limits.max_total_encoded_bytes,
            interval=native_window(config),
            observer=self.observer,
        )
        importer = native_import_backend(config).value
        verifier = native_verification_backend(config).value
        identity: Any
        if importer == "mssql_sqlclient":
            composed = compose_sqlclient_stage_context(
                self.plan,
                timeout_seconds=_TIMEOUT_SECONDS,
                credentials_provider=lambda _request: _sqlclient_credentials(self.target),
                write_observer=self._write_observer,
                persisted_hash_layout=native_sqlclient_layout_version(config) == 2,
                **common,
            )
            context, identity = composed.stage_context, composed.backend.identity
        else:
            identity = (
                build_bcp_target_local_verification_identity(self.plan, timeout_seconds=_TIMEOUT_SECONDS)
                if verifier == "target_local"
                else None
            )
            context = compose_native_stage_context(
                **common,
                plan=self.plan,
                verification_identity=identity,
                target_local_timeout_seconds=_TIMEOUT_SECONDS,
            )
        if identity is not None and not self._recovery_only:
            persist_mssql_native_recovery_plan(
                self.store,
                lease,
                identity=identity,
                schema=self.schema,
                admission=self.admission,
                recovery_bindings=context.recovery_bindings,
                window=native_window(config),
            )
        service = MssqlNativeStagedLoadService(self.sink, MssqlNativeStagePreparer(self.sink, lambda *_: context))
        return NativeRuntimeBindings(service, context, self.admission, identity)

    @contextmanager
    def _source(self, config: Any, _binding: NativeRuntimeBindings):
        if self._recovery_only:
            raise RuntimeError("mssql_native.recovery_source_forbidden")
        native = ClickHouseNativeSource(
            self.source.connector,
            schema_guard_factory=lambda value: ClickHouseSchemaStabilityGuard(
                self.source.connector,
                value.source_schema,
                value.source_table,
            ),
        )
        result = native.extract(config)
        artifact: Any = result.artifact
        if artifact.source_relation_uuid != self.preplan.source_relation_identity:
            raise ValueError("mssql_native.source_identity_changed")
        self._artifact = artifact
        payload = LoadPayload(
            artifact=result.artifact,
            schema=self.schema,
            relation_schema=result.relation_schema,
            relation_metadata=self.source_projection.relation_metadata,
            relation_dialect=result.relation_dialect,
            target_projection=None,
            mssql_transaction_admission=self.admission,
            mssql_target_mutation_plan=self.preplan.target_mutation_plan,
            extraction_lifecycle=result.extraction_lifecycle,
        )
        try:
            yield payload
        except BaseException:
            artifact.terminate(ArtifactTerminalOutcome.ABORT)
            raise
        else:
            receipt = artifact.terminate(ArtifactTerminalOutcome.SUCCESS)
            if not receipt.cleanup_succeeded:
                raise RuntimeError("mssql_native.source_cleanup_failed")

    def _evidence(self, _config: Any, result: Any, context: Any, lease: Any) -> None:
        completed = context.journal_factory().completed()
        if completed is None:
            raise ValueError("mssql_native.completion_evidence_missing")
        payload = {
            "schema_version": 1,
            "kind": "dpone.mssql-native-operational-evidence.v1",
            "invocation_id": context.verification_identity.invocation_key
            if context.verification_identity is not None
            else _digest(tuple(self.plan.__dict__.values())),
            "import_backend": native_import_backend(self.config).value,
            "rows": completed.rows,
            "receipt_digest": completed.receipt_digest,
            "commit_receipt_id": result.commit_receipt_id,
        }
        _save_once(self.store, f"mssql-native/evidence/{self.plan.run_id}", payload, lease)

    def _advance_state(self, _config: Any, result: Any, lease: Any) -> None:
        _save_once(
            self.store,
            f"mssql-native/checkpoint/{self.plan.run_id}",
            {
                "schema_version": 1,
                "kind": "dpone.mssql-native-stateless-checkpoint.v1",
                "commit_receipt_id": result.commit_receipt_id,
                "source_checkpoint": "stateless",
            },
            lease,
        )


def _wire_schema(source_schema: Any, preplan: Any) -> tuple[tuple[str, str], ...]:
    mapping = dict(preplan.column_mapping)
    if any(source != target for source, target in mapping.items()):
        raise ValueError("mssql_native.renamed_wire_columns_not_admitted")
    target_types = {str(name).casefold(): str(dtype) for name, dtype in preplan.target_column_types}
    resolved = []
    for name, source_type in source_schema:
        dtype = target_types.get(str(name).casefold())
        if dtype is None:
            raise ValueError("mssql_native.target_type_missing")
        if str(source_type).startswith("Nullable("):
            dtype += " nullable"
        resolved.append((str(name), dtype))
    return tuple(resolved)


def _chunk_plan(config: Any, admission: Any, preplan: Any, wire_fingerprint: str) -> NativeChunkPlan:
    operation = admission.operation
    if operation is None or not preplan.source_relation_identity:
        raise ValueError("mssql_native.identity_unavailable")
    request = operation.attempt.request
    window = native_window(config)
    window_value = None if window is None else (window.column, window.start.isoformat(), window.end.isoformat())
    target_id = _digest(
        (request.target_identity.hex(), request.target_database, request.target_schema, request.target_table)
    )
    return NativeChunkPlan(
        operation.operation_key.hex(),
        target_id,
        _digest((preplan.source_relation_identity, request.route_fingerprint.hex(), window_value)),
        _digest(window_value),
        preplan.source_schema_sha256.hex(),
        wire_fingerprint,
    )


def _sqlclient_credentials(connector: Any) -> MssqlSqlClientCredentials:
    return MssqlSqlClientCredentials(
        host=connector.host,
        port=connector.port,
        database=connector.database,
        username=connector.user or "",
        password=connector.password or "",
        encrypt=str(connector.encrypt).lower() in {"yes", "true", "1"},
        trust_server_certificate=str(connector.trust_server_certificate).lower() in {"yes", "true", "1"},
    )


def _save_once(store: Any, key: str, value: dict[str, Any], lease: Any) -> None:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    current = store.load(key)
    if current is None:
        store.save(key, None, payload, lease)
    elif current.payload != payload:
        raise ValueError("mssql_native.operational_evidence_changed")


def _digest(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
