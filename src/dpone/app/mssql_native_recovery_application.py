"""Manifest-bound, source-free application service for MSSQL native recovery."""

from __future__ import annotations

import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_recovery_journal import MssqlNativeRecoveryJournalReader
from dpone.contracts.mssql_native_recovery import (
    MssqlNativeRecoveryBindings,
    MssqlTransactionAdmission,
    canonical_json_bytes,
    restore_mssql_native_recovery_admission,
)
from dpone.manifest.mssql_native_policy import native_window, validate_native_config
from dpone.runtime.mssql_native_recovery_composition import (
    ADMISSION_OPTION,
    LEASE_OPTION,
    MssqlGenericTransactionState,
    MssqlOperationLeaseHeartbeat,
    QualityExecutionSnapshot,
    RuntimeStoragePolicy,
    StoragePreflightService,
    _digest,
    _NativeRuntimeAssembly,
    live_target_coordinates,
    resolve_atomic_mssql_target,
)

_RECOVERY_LEASE_TTL = timedelta(hours=1)
_MUTATING_ACTIONS = frozenset({"reconcile", "resume", "retire"})


class MssqlNativeRecoveryApplication:
    """Execute one journal-authorized target-only recovery action."""

    def execute(
        self,
        process: Any,
        *,
        journal_root: Path,
        invocation_id: str,
        action: str,
        owner: str,
        confirmed: bool,
    ) -> dict[str, Any]:
        if action not in _MUTATING_ACTIONS:
            raise ValueError("mssql_native.recovery_action_invalid")
        if confirmed is not True:
            raise ValueError("mssql_native.recovery_confirmation_required")
        reader = MssqlNativeRecoveryJournalReader(journal_root)
        before = reader.inspect(invocation_id)
        if action not in before["permitted_actions"]:
            raise ValueError("mssql_native.recovery_action_not_permitted")
        snapshot = reader.load(invocation_id)

        config = self._restore_window(
            process.load_config, snapshot.recovery_plan, snapshot.identity.plan.window_fingerprint
        )
        validate_native_config(config)
        if not QualityExecutionSnapshot.from_load_config(config).is_inert():
            raise ValueError("mssql_native.quality_policy_requires_composition")
        identity, plan = snapshot.identity, snapshot.identity.plan
        process.load_config = config
        process.ensure_runtime_bindings()

        policy = RuntimeStoragePolicy.from_sources(
            runtime=(process.raw_config or {}).get("runtime"),
            source_options=config.options,
        )
        work_dir = policy.work_dir / "mssql-native" / plan.target_id
        coordinates = live_target_coordinates(config)
        target_identity = resolve_atomic_mssql_target(
            process.sink_obj.connector,
            process.sink_obj.state_storage,
            database=coordinates[0],
            schema=coordinates[1],
            table=coordinates[2],
        )
        expected_target_id = _digest(
            (
                target_identity.digest.hex(),
                target_identity.database_name,
                target_identity.schema_name,
                target_identity.table_name,
            )
        )
        if expected_target_id != plan.target_id:
            raise ValueError("mssql_native.recovery_target_changed")

        schema = self._schema(snapshot.projection, snapshot.recovery_plan)
        admission = self._restore_admission(
            snapshot.projection,
            recovery_plan=snapshot.recovery_plan,
            identity=identity,
            target_identity=target_identity.digest,
            state_path=reader.path,
            work_dir=work_dir,
            config=config,
            target_coordinates=(
                target_identity.database_name,
                target_identity.schema_name,
                target_identity.table_name,
            ),
        )
        preflight = StoragePreflightService().check(policy)
        if not preflight.passed:
            raise ValueError("mssql_native.storage_preflight_failed:" + ",".join(preflight.blockers))
        store = SQLiteWindowStore(reader.path, clock=time.time)
        admission, heartbeat = self._refresh_operation(process.sink_obj.state_storage, admission)
        options = dict(config.options or {})
        options[ADMISSION_OPTION] = admission
        if heartbeat is not None:
            options[LEASE_OPTION] = heartbeat
        recovered_config = replace(config, options=options)
        assembly = _NativeRuntimeAssembly.for_recovery(
            process,
            recovered_config,
            admission=admission,
            identity=identity,
            store=store,
            work_dir=work_dir,
            schema=schema,
        )
        try:
            if heartbeat is not None:
                heartbeat.start()
            try:
                assembly.runtime.run(recovered_config, owner=owner)
            except Exception as error:
                if not self._settled_nonpublication(action, error):
                    raise
        finally:
            if heartbeat is not None:
                heartbeat.stop()
        after = reader.inspect(invocation_id)
        self._require_action_postcondition(action, after)
        return after

    @staticmethod
    def _restore_admission(
        projection: dict[str, Any],
        *,
        recovery_plan: dict[str, Any] | None,
        identity: Any,
        target_identity: bytes,
        state_path: Path,
        work_dir: Path,
        config: Any,
        target_coordinates: tuple[str, str, str],
    ) -> MssqlTransactionAdmission:
        metadata = projection.get("completion_metadata")
        completed_authority = metadata.get("recovery_authority_v1") if isinstance(metadata, dict) else None
        planned_authority = None if recovery_plan is None else recovery_plan.get("recovery_authority_v1")
        if (
            completed_authority is not None
            and planned_authority is not None
            and completed_authority != planned_authority
        ):
            raise ValueError("mssql_native.recovery_authority_changed")
        authority = completed_authority if completed_authority is not None else planned_authority
        if not isinstance(authority, dict):
            raise ValueError("mssql_native.recovery_authority_required")
        operation = authority.get("operation")
        bindings = authority.get("bindings")
        route = operation.get("route_fingerprint") if isinstance(operation, dict) else None
        if (
            not isinstance(bindings, dict)
            or not isinstance(route, str)
            or route != bindings.get("authored_route_sha256")
        ):
            raise ValueError("mssql_native.recovery_authority_invalid")
        state_identity = {
            "adapter": "dpone.adapters.bounded_window_sqlite.SQLiteWindowStore",
            "location": str(state_path.resolve()),
            "target": identity.plan.target_id,
        }
        expected = MssqlNativeRecoveryBindings(
            authored_route_sha256=route,
            verification_identity_sha256=identity.invocation_key,
            target_connection_sha256=target_identity.hex(),
            state_store_sha256=sha256(canonical_json_bytes(state_identity)).hexdigest(),
            work_root_sha256=sha256(str(work_dir.resolve()).encode()).hexdigest(),
        )
        admission = restore_mssql_native_recovery_admission(authority, expected_bindings=expected)
        request = admission.operation.attempt.request if admission.operation is not None else None
        if (
            request is None
            or (
                request.target_database,
                request.target_schema,
                request.target_table,
            )
            != target_coordinates
        ):
            raise ValueError("mssql_native.recovery_target_changed")
        strategy = getattr(config.load_strategy, "value", config.load_strategy)
        if request.strategy != strategy or request.target_identity != target_identity:
            raise ValueError("mssql_native.recovery_route_changed")
        return admission

    @staticmethod
    def _refresh_operation(state_storage: Any, admission: MssqlTransactionAdmission) -> tuple[Any, Any | None]:
        operation = admission.operation
        if operation is None:
            return admission, None
        state = MssqlGenericTransactionState.from_state_storage(state_storage)
        receipt = state.probe_receipt_fresh(operation)
        if receipt is not None:
            return MssqlTransactionAdmission(replay_receipt=receipt), None
        if operation.lease_expires_at_utc is None:
            return admission, None
        expires = datetime.now(UTC) + _RECOVERY_LEASE_TTL
        if not state.renew_operation_lease(operation, lease_expires_at_utc=expires):
            receipt = state.probe_receipt_fresh(operation)
            if receipt is None:
                raise ValueError("mssql_native.recovery_operation_unavailable")
            return MssqlTransactionAdmission(replay_receipt=receipt), None
        refreshed = replace(operation, lease_expires_at_utc=expires)
        current = MssqlTransactionAdmission(operation=refreshed)
        return current, MssqlOperationLeaseHeartbeat(state, current)

    @staticmethod
    def _schema(projection: dict[str, Any], recovery_plan: dict[str, Any] | None) -> tuple[tuple[str, str], ...]:
        metadata = projection.get("completion_metadata")
        completed_schema = metadata.get("schema") if isinstance(metadata, dict) else None
        planned_schema = None if recovery_plan is None else recovery_plan.get("schema")
        if completed_schema is not None and planned_schema is not None and completed_schema != planned_schema:
            raise ValueError("mssql_native.recovery_schema_changed")
        raw = completed_schema if completed_schema is not None else planned_schema
        if not isinstance(raw, list) or not raw:
            raise ValueError("mssql_native.completed_payload_required")
        schema: list[tuple[str, str]] = []
        for column in raw:
            if (
                not isinstance(column, list)
                or len(column) != 2
                or any(not isinstance(value, str) or not value for value in column)
            ):
                raise ValueError("mssql_native.completed_payload_required")
            schema.append((column[0], column[1]))
        return tuple(schema)

    @staticmethod
    def _require_window(config: Any, expected: str) -> None:
        window = native_window(config)
        value = None if window is None else (window.column, window.start.isoformat(), window.end.isoformat())
        if _digest(value) != expected:
            raise ValueError("mssql_native.recovery_window_changed")

    @classmethod
    def _restore_window(cls, config: Any, recovery_plan: dict[str, Any] | None, expected: str) -> Any:
        if recovery_plan is None or recovery_plan.get("schema_version") == 1:
            cls._require_window(config, expected)
            return config
        sealed = recovery_plan.get("window")
        if sealed is None:
            cls._require_window(config, expected)
            return config
        options = dict(config.options or {})
        options["interval"] = {
            "interval_start": sealed["start"],
            "interval_end": sealed["end"],
        }
        restored = replace(config, options=options)
        cls._require_window(restored, expected)
        window = native_window(restored)
        if (
            window is None
            or {
                "column": window.column,
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
            }
            != sealed
        ):
            raise ValueError("mssql_native.recovery_window_changed")
        return restored

    @staticmethod
    def _settled_nonpublication(action: str, error: Exception) -> bool:
        return action in {"reconcile", "retire"} and str(error) in {
            "mssql_native.reextract_required",
            "mssql_native.pre_eof_reextract_required",
        }

    @staticmethod
    def _require_action_postcondition(action: str, projection: dict[str, Any]) -> None:
        state = projection["state"]
        accepted = {
            "resume": {"CUSTODY_RELEASED", "SUCCEEDED"},
            "reconcile": {"CUSTODY_RELEASED", "SUCCEEDED", "RETIRED", "REEXTRACT_REQUIRED"},
            "retire": {"RETIRED", "EMPTY_STAGING", "CUSTODY_RELEASED"},
        }
        if state not in accepted[action]:
            raise ValueError("mssql_native.recovery_postcondition_unproved")


__all__ = ["MssqlNativeRecoveryApplication"]
