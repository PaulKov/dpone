"""Application entry point for the ClickHouse to MSSQL native route."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import dpone.runtime.mssql_native_application_assembly as _assembly
from dpone.ports.mssql_native import NativeStageWriteObservation
from dpone.ports.native_delivery_observer import NativeDeliveryObserver
from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService
from dpone.runtime.governance.quality_execution import QualityExecutionSnapshot

# Recovery composition and established focused tests import these private names.
_digest = _assembly._digest
_NativeRuntimeAssembly = _assembly._NativeRuntimeAssembly
_sqlclient_credentials = _assembly._sqlclient_credentials
_wire_schema = _assembly._wire_schema


class DefaultMssqlNativeRuntimeFactory:
    """Create a per-process native application without importing optional .NET code."""

    def __init__(
        self,
        *,
        observer_factory: Callable[[], NativeDeliveryObserver] | None = None,
        write_observer: Callable[[NativeStageWriteObservation], None] | None = None,
    ) -> None:
        self._observer_factory = observer_factory
        self._write_observer = write_observer

    def __call__(self, process_config: Any) -> MssqlNativeApplicationRuntime:
        return MssqlNativeApplicationRuntime(
            process_config,
            observer_factory=self._observer_factory,
            write_observer=self._write_observer,
        )


class MssqlNativeApplicationRuntime:
    """Hydrate endpoints, freeze admission, and execute the native state machine."""

    def __init__(
        self,
        process_config: Any,
        *,
        observer_factory: Callable[[], NativeDeliveryObserver] | None = None,
        write_observer: Callable[[NativeStageWriteObservation], None] | None = None,
    ) -> None:
        self._process_config = process_config
        self._observer_factory = observer_factory
        self._write_observer = write_observer

    def run(self, load_config: Any, *, owner: str) -> Any:
        process = self._process_config
        process.ensure_runtime_bindings()
        if not QualityExecutionSnapshot.from_load_config(load_config).is_inert():
            raise ValueError("mssql_native.quality_policy_requires_composition")
        identity_service = process.load_identity_service
        record = identity_service.start(load_config, process_name=process.name)
        options = dict(load_config.options or {})
        options["__dpone_load_identity"] = {"run_id": record.run_id, "load_id": record.load_id}
        identified = replace(load_config, options=options)
        prepared = MssqlTransactionAdmissionService().prepare(
            identified,
            source=process.source_obj,
            sink=process.sink_obj,
            run_context=SimpleNamespace(run_id=owner, config={"pipeline_id": process.name, "task_id": process.name}),
            load_record=record,
            dag_id=process.name,
        )
        observer = None if self._observer_factory is None else self._observer_factory()
        assembly = _NativeRuntimeAssembly(
            process,
            prepared,
            observer=observer,
            write_observer=self._write_observer,
        )
        try:
            result = assembly.runtime.run(prepared, owner=owner)
            identity_service.mark_committed(record, _audit_result(result))
            return result
        except Exception as error:
            identity_service.mark_failed(record, error)
            raise


def _audit_result(result: Any) -> Any:
    details = result.details or {}
    return SimpleNamespace(
        inserted_rows=result.inserted_rows,
        updated_rows=result.updated_rows,
        total_rows=result.final_rows,
        staging_rows=result.extracted_rows,
        commit_receipt_id=details.get("commit_receipt_id"),
        commit_outcome="committed",
    )


__all__ = ["DefaultMssqlNativeRuntimeFactory", "MssqlNativeApplicationRuntime"]
